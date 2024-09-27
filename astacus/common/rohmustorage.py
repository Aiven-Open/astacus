"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Rohmu-specific actual object storage implementation

"""

from .storage import Storage, StorageUploadResult
from .utils import AstacusModel, fifo_cache
from astacus.common import exceptions
from collections.abc import Iterator, Mapping
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from enum import Enum
from pydantic.v1 import Field
from rohmu import errors, rohmufile
from rohmu.compressor import CompressionStream
from rohmu.encryptor import EncryptorStream
from rohmu.filewrap import Stream
from rohmu.typing import FileLike, Metadata
from typing import BinaryIO, IO, TypeAlias

import contextlib
import io
import logging
import mmap
import os
import rohmu
import tempfile

logger = logging.getLogger(__name__)


class RohmuModel(AstacusModel):
    class Config:
        # As we're keen to both export and decode json, just using enum
        # values is much saner than the alternatives
        use_enum_values = True


RohmuStorageConfig: TypeAlias = (
    rohmu.LocalObjectStorageConfig
    | rohmu.S3ObjectStorageConfig
    | rohmu.AzureObjectStorageConfig
    | rohmu.GoogleObjectStorageConfig
)


class RohmuEncryptionKey(RohmuModel):
    # public RSA key
    public: str
    # private RSA key
    private: str


class RohmuCompressionType(Enum):
    lzma = "lzma"
    snappy = "snappy"
    zstd = "zstd"


class RohmuCompression(RohmuModel):
    algorithm: RohmuCompressionType | None = None
    level: int = 0
    # threads: int = 0

    @property
    def algorithm_value(self) -> str | None:
        # The parent model is configured with `use_enum_values=True`,
        # therefore the algorithm attribute can be string typed.
        match self.algorithm:
            case RohmuCompressionType():
                return self.algorithm.value
            case str():
                return self.algorithm  # type: ignore[unreachable]
            case None:
                return None
            case _:
                raise TypeError(f"Expected compression algorithm, got {self.algorithm!r}")


class RohmuConfig(RohmuModel):
    temporary_directory: str

    # Targets we support for backing up
    default_storage: str
    storages: Mapping[str, RohmuStorageConfig]

    # Encryption (optional)
    encryption_key_id: str | None = None
    encryption_keys: Mapping[str, RohmuEncryptionKey] = {}

    # Compression (optional)
    compression: RohmuCompression = RohmuCompression()


class RohmuMetadata(RohmuModel):
    encryption_key_id: str | None = Field(None, alias="encryption-key-id")
    compression_algorithm: RohmuCompressionType | None = Field(None, alias="compression-algorithm")


def rohmu_error_wrapper(fun):
    """Wrap rohmu exceptions in astacus ones; to be seen what is complete set"""

    def _f(*a, **kw):
        try:
            return fun(*a, **kw)
        except errors.FileNotFoundFromStorageError as ex:
            raise exceptions.NotFoundException from ex
        except Exception as ex:  # pylint: disable=broad-except
            raise exceptions.RohmuException from ex

    return _f


class RohmuStorage(Storage):
    """Implementation of the storage API, on top of rohmu."""

    def __init__(self, config: RohmuConfig, *, storage: str | None = None) -> None:
        assert config
        self.config = config
        self.hexdigest_key = "data"
        self.json_key = "json"
        self._choose_storage(storage)
        os.makedirs(config.temporary_directory, exist_ok=True)
        if not self.config.compression.algorithm and not self.config.encryption_key_id:
            raise exceptions.CompressionOrEncryptionRequired()

    def close(self) -> None:
        self.storage.close()

    @rohmu_error_wrapper
    def _download_key_to_file(self, key, f: FileLike) -> bool:
        with tempfile.TemporaryFile(dir=self.config.temporary_directory) as temp_file:
            raw_metadata: Metadata = self.storage.get_contents_to_fileobj(key, temp_file)
            temp_file.seek(0)
            rohmufile.read_file(
                input_obj=temp_file,
                output_obj=f,
                metadata=raw_metadata,
                key_lookup=self._loaded_private_key_lookup,
                log_func=logger.debug,
            )
        return True

    def _list_key(self, key: str) -> list[str]:
        return [os.path.basename(o["name"]) for o in self.storage.list_iter(key, with_metadata=False)]

    @fifo_cache(size=1)
    def _loaded_private_key_lookup(self, key_id: str) -> RSAPrivateKey:
        private_key_pem = self.config.encryption_keys[key_id].private.encode("ascii")
        # RSA key validation is compute-intense and require caching
        private_key = serialization.load_pem_private_key(data=private_key_pem, password=None, backend=default_backend())
        if not isinstance(private_key, RSAPrivateKey):
            raise ValueError("Key must be RSA")
        return private_key

    @fifo_cache(size=1)
    def _loaded_public_key_lookup(self, key_id: str) -> RSAPublicKey:
        public_key_pem = self.config.encryption_keys[key_id].public.encode("ascii")
        public_key = serialization.load_pem_public_key(public_key_pem, backend=default_backend())
        if not isinstance(public_key, RSAPublicKey):
            raise ValueError("Key must be RSA")
        return public_key

    @rohmu_error_wrapper
    def _upload_key_from_file(self, key: str, f: IO[bytes], file_size: int) -> StorageUploadResult:
        encryption_key_id = self.config.encryption_key_id
        compression = self.config.compression
        metadata = RohmuMetadata()
        wrapped_file: IO[bytes] | Stream = f
        if compression.algorithm_value:
            metadata.compression_algorithm = RohmuCompressionType(compression.algorithm)
            wrapped_file = CompressionStream(wrapped_file, compression.algorithm_value, compression.level)
        if encryption_key_id:
            metadata.encryption_key_id = encryption_key_id
            rsa_public_key = self._loaded_public_key_lookup(encryption_key_id)
            wrapped_file = EncryptorStream(wrapped_file, rsa_public_key)
        rohmu_metadata = metadata.dict(exclude_defaults=True, by_alias=True)
        # BaseTransfer.store_file_object is compatible with file descriptors of type IO[bytes] and Stream
        # However, the BinaryIO parameter type is too strict for the union type we want to pass here.
        self.storage.store_file_object(key, wrapped_file, metadata=rohmu_metadata)  # type: ignore[arg-type]
        return StorageUploadResult(size=file_size, stored_size=wrapped_file.tell())

    storage_name: str = ""

    def _choose_storage(self, storage: str | None = None) -> None:
        if storage is None or storage == "":
            storage = self.config.default_storage
        self.storage_name = storage
        self.storage_config = self.config.storages[storage]
        self.storage = rohmu.get_transfer_from_model(self.storage_config)

    def copy(self) -> "RohmuStorage":
        return RohmuStorage(config=self.config, storage=self.storage_name)

    # HexDigestStorage implementation

    @rohmu_error_wrapper
    def delete_hexdigest(self, hexdigest: str) -> None:
        key = os.path.join(self.hexdigest_key, hexdigest)
        self.storage.delete_key(key)

    def list_hexdigests(self) -> list[str]:
        return self._list_key(self.hexdigest_key)

    def download_hexdigest_to_file(self, hexdigest: str, f: FileLike) -> bool:
        key = os.path.join(self.hexdigest_key, hexdigest)
        return self._download_key_to_file(key, f)

    def upload_hexdigest_from_file(self, hexdigest, f: BinaryIO, file_size: int) -> StorageUploadResult:
        key = os.path.join(self.hexdigest_key, hexdigest)
        return self._upload_key_from_file(key, f, file_size)

    # JsonStorage implementation
    @rohmu_error_wrapper
    def delete_json(self, name: str) -> None:
        key = os.path.join(self.json_key, name)
        self.storage.delete_key(key)

    @contextlib.contextmanager
    def open_json_bytes(self, name: str) -> Iterator[mmap.mmap]:
        key = os.path.join(self.json_key, name)
        with tempfile.TemporaryFile(dir=self.config.temporary_directory) as temp_file:
            self._download_key_to_file(key, temp_file)
            temp_file.seek(0)
            with mmap.mmap(temp_file.fileno(), 0, access=mmap.ACCESS_READ) as mapped_file:
                yield mapped_file

    def list_jsons(self) -> list[str]:
        return self._list_key(self.json_key)

    def upload_json_bytes(self, name: str, data: bytes | mmap.mmap) -> bool:
        key = os.path.join(self.json_key, name)
        if isinstance(data, bytes):
            f = io.BytesIO(data)
            self._upload_key_from_file(key, f, len(data))
        else:
            data.seek(0)
            self._upload_key_from_file(key, data, len(data))
        return True
