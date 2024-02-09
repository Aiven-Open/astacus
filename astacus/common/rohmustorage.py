"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Rohmu-specific actual object storage implementation

"""
from .msgspec_glue import enc_hook
from .storage import MultiStorage, Storage, StorageUploadResult
from astacus.common import exceptions
from collections.abc import Iterator, Mapping
from enum import StrEnum
from pathlib import Path
from rohmu import errors, rohmufile, StorageDriver
from rohmu.common.models import ProxyType
from rohmu.compressor import CompressionStream
from rohmu.encryptor import EncryptorStream
from rohmu.object_storage.config import S3_MULTIPART_CHUNK_SIZE, S3AddressingStyle
from typing import Any, BinaryIO, Optional, Union

import contextlib
import io
import logging
import mmap
import msgspec
import os
import rohmu
import tempfile

logger = logging.getLogger(__name__)


class ProxyInfo(msgspec.Struct, kw_only=True, frozen=True):
    host: str
    port: int
    type: ProxyType
    user: Optional[str] = None
    password: Optional[str] = msgspec.field(default=None, name="pass")  # pydantic.Field(None, alias="pass")


class LocalObjectStorageConfig(
    msgspec.Struct, kw_only=True, frozen=True, tag=StorageDriver.local.value, tag_field="storage_type"
):
    # Don't use pydantic DirectoryPath, that class checks the dir exists at the wrong time
    directory: Path
    prefix: Optional[str] = None


class S3ObjectStorageConfig(msgspec.Struct, kw_only=True, frozen=True, tag=StorageDriver.s3.value, tag_field="storage_type"):
    region: str
    bucket_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None  # repr=False
    prefix: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    addressing_style: S3AddressingStyle = S3AddressingStyle.path
    is_secure: bool = False
    is_verify_tls: bool = False
    cert_path: Optional[Path] = None
    segment_size: int = S3_MULTIPART_CHUNK_SIZE
    encrypted: bool = False
    proxy_info: Optional[ProxyInfo] = None
    connect_timeout: Optional[str] = None
    read_timeout: Optional[str] = None
    aws_session_token: Optional[str] = None  # repr=False

    def __post_init__(self) -> None:
        if not self.is_verify_tls and self.cert_path is not None:
            raise ValueError("cert_path is set but is_verify_tls is False")


class AzureObjectStorageConfig(
    msgspec.Struct, kw_only=True, frozen=True, tag=StorageDriver.azure.value, tag_field="storage_type"
):
    bucket_name: Optional[str] = None
    account_name: str
    account_key: Optional[str] = None  # repr=False
    sas_token: Optional[str] = None  # repr=False
    prefix: Optional[str] = None
    azure_cloud: Optional[str] = None
    proxy_info: Optional[ProxyInfo] = None


class GoogleObjectStorageConfig(
    msgspec.Struct, kw_only=True, frozen=True, tag=StorageDriver.google.value, tag_field="storage_type"
):
    project_id: str
    bucket_name: Optional[str] = None
    # Don't use pydantic FilePath, that class checks the file exists at the wrong time
    credential_file: Optional[Path] = None
    credentials: Optional[Mapping[str, Any]] = None  # repr=False
    proxy_info: Optional[ProxyInfo] = None
    prefix: Optional[str] = None


RohmuStorageConfig = Union[
    LocalObjectStorageConfig,
    S3ObjectStorageConfig,
    AzureObjectStorageConfig,
    GoogleObjectStorageConfig,
]


class RohmuEncryptionKey(msgspec.Struct, kw_only=True, frozen=True):
    # public RSA key
    public: str
    # private RSA key
    private: str


class RohmuCompressionType(StrEnum):
    lzma = "lzma"
    snappy = "snappy"
    zstd = "zstd"


class RohmuCompression(msgspec.Struct, kw_only=True, frozen=True):
    algorithm: Optional[RohmuCompressionType] = None
    level: int = 0
    # threads: int = 0


class RohmuConfig(msgspec.Struct, kw_only=True, frozen=True):
    temporary_directory: str

    # Targets we support for backing up
    default_storage: str
    storages: Mapping[str, RohmuStorageConfig]

    # Encryption (optional)
    encryption_key_id: Optional[str] = None
    encryption_keys: Mapping[str, RohmuEncryptionKey] = msgspec.field(default_factory=dict)

    # Compression (optional)
    compression: RohmuCompression = msgspec.field(default_factory=RohmuCompression)


class RohmuMetadata(msgspec.Struct, kw_only=True):
    encryption_key_id: Optional[str] = msgspec.field(default=None, name="encryption-key-id")
    compression_algorithm: Optional[RohmuCompressionType] = msgspec.field(default=None, name="compression-algorithm")


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
        rohmu_metadata = msgspec.to_builtins(metadata)
        self.storage.store_file_object(key, wrapped_file, metadata=rohmu_metadata)
        return StorageUploadResult(size=file_size, stored_size=wrapped_file.tell())

    storage_name: str = ""

    def _choose_storage(self, storage: str | None = None) -> None:
        if storage is None or storage == "":
            storage = self.config.default_storage
        self.storage_name = storage
        self.storage_config = self.config.storages[storage]
        storage_config = rohmu.get_transfer_model(msgspec.to_builtins(self.storage_config, enc_hook=enc_hook))
        storage_config.storage_type = storage_config.storage_type.value
        self.storage = rohmu.get_transfer_from_model(storage_config)

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
    def open_json_bytes(self, name: str) -> Iterator[bytearray]:
        key = os.path.join(self.json_key, name)
        with tempfile.TemporaryFile(dir=self.config.temporary_directory) as temp_file:
            self._download_key_to_file(key, temp_file)
            temp_file.seek(0)
            with mmap.mmap(temp_file.fileno(), 0, access=mmap.ACCESS_READ) as mapped_file:
                # https://docs.python.org/3/library/mmap.html
                # > Memory-mapped file objects behave like both bytearray and like file objects.
                yield mapped_file  # type: ignore

    def list_jsons(self) -> list[str]:
        return self._list_key(self.json_key)

    def upload_json_bytes(self, name: str, data: bytes | bytearray) -> bool:
        key = os.path.join(self.json_key, name)
        f = io.BytesIO(data)
        self._upload_key_from_file(key, f, len(data))
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
