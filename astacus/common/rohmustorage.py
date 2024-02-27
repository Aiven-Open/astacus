"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Rohmu-specific actual object storage implementation

"""
from .storage import MultiStorage, Storage, StorageUploadResult
from .utils import AstacusModel
from astacus.common import exceptions
from collections.abc import Iterator, Mapping
from enum import Enum
from pydantic import Field
from rohmu import errors, rohmufile
from rohmu.compressor import CompressionStream
from rohmu.encryptor import EncryptorStream
from typing import BinaryIO, TypeAlias

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

    @rohmu_error_wrapper
    def _download_key_to_file(self, key, f: BinaryIO) -> bool:
        raw_metadata: dict = self.storage.get_metadata_for_key(key)
        with tempfile.TemporaryFile(dir=self.config.temporary_directory) as temp_file:
            self.storage.get_contents_to_fileobj(key, temp_file)
            temp_file.seek(0)
            rohmufile.read_file(
                input_obj=temp_file,
                output_obj=f,
                metadata=raw_metadata,
                key_lookup=self._private_key_lookup,
                log_func=logger.debug,
            )
        return True

    def _list_key(self, key: str) -> list[str]:
        return [os.path.basename(o["name"]) for o in self.storage.list_iter(key, with_metadata=False)]

    def _private_key_lookup(self, key_id: str) -> str:
        return self.config.encryption_keys[key_id].private

    def _public_key_lookup(self, key_id: str) -> str:
        return self.config.encryption_keys[key_id].public

    @rohmu_error_wrapper
    def _upload_key_from_file(self, key: str, f: BinaryIO, file_size: int) -> StorageUploadResult:
        encryption_key_id = self.config.encryption_key_id
        compression = self.config.compression
        metadata = RohmuMetadata()
        wrapped_file = f
        if compression.algorithm:
            metadata.compression_algorithm = compression.algorithm
            wrapped_file = CompressionStream(wrapped_file, compression.algorithm, compression.level)
        if encryption_key_id:
            metadata.encryption_key_id = encryption_key_id
            rsa_public_key = self._public_key_lookup(encryption_key_id)
            wrapped_file = EncryptorStream(wrapped_file, rsa_public_key)
        rohmu_metadata = metadata.dict(exclude_defaults=True, by_alias=True)
        self.storage.store_file_object(key, wrapped_file, metadata=rohmu_metadata)
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

    def download_hexdigest_to_file(self, hexdigest: str, f: BinaryIO) -> bool:
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


class MultiRohmuStorage(MultiStorage[RohmuStorage]):
    def __init__(self, *, config: RohmuConfig) -> None:
        self.config = config

    def get_storage(self, name: str | None) -> RohmuStorage:
        return RohmuStorage(config=self.config, storage=name)

    def get_default_storage_name(self) -> str:
        return self.config.default_storage

    def list_storages(self) -> list[str]:
        return sorted(self.config.storages.keys())
