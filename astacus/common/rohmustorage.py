"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Rohmu-specific actual object storage implementation

"""
from .magic import StrEnum
from .storage import Json, MultiStorage, Storage, StorageUploadResult
from .utils import AstacusModel
from astacus.common import exceptions
from enum import Enum
from pydantic import Field
from rohmu import errors, rohmufile
from rohmu.compressor import CompressionStream
from rohmu.encryptor import EncryptorStream
from typing import BinaryIO, Dict, Optional, Union
from typing_extensions import Literal

import io
import json
import logging
import os
import rohmu
import tempfile

logger = logging.getLogger(__name__)


class RohmuStorageType(StrEnum):
    """Embodies what is detected in rohmu.get_class_for_transfer"""

    azure = "azure"
    google = "google"
    local = "local"
    s3 = "s3"
    # sftp = "sftp"
    # swift = "swift"


class RohmuModel(AstacusModel):
    class Config:
        # As we're keen to both export and decode json, just using enum
        # values is much saner than the alternatives
        use_enum_values = True


class RohmuProxyType(StrEnum):
    socks5 = "socks5"
    http = "http"


class RohmuProxyInfo(RohmuModel):
    type: RohmuProxyType
    host: str
    port: int
    user: Optional[str] = None
    password: Optional[str] = Field(None, alias="pass")


class StatsdInfo(RohmuModel):
    host: str
    port: int
    tags: dict[str, str | int | None]


class ObjectStorageNotifier(RohmuModel):
    notifier_type: str
    url: str


class ObjectStorageConfig(RohmuModel):
    storage_type: RohmuStorageType
    prefix: Optional[str] = None
    notifier: Optional[ObjectStorageNotifier] = None
    statsd_info: Optional[StatsdInfo] = None


class RohmuProxyStorage(ObjectStorageConfig):
    """Storage backend with support for optional socks5 or http proxy connections"""

    proxy_info: Optional[RohmuProxyInfo] = None


class RohmuLocalStorageConfig(ObjectStorageConfig):
    storage_type: Literal[RohmuStorageType.local]
    directory: str
    prefix: Optional[str] = None


class RohmuS3StorageConfig(RohmuProxyStorage):
    storage_type: Literal[RohmuStorageType.s3]
    region: str
    bucket_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    is_secure: Optional[bool] = False
    is_verify_tls: Optional[bool] = False
    prefix: Optional[str] = None
    # Some more obscure options with defaults are omitted


class RohmuAzureStorageConfig(RohmuProxyStorage):
    storage_type: Literal[RohmuStorageType.azure]
    account_name: str
    bucket_name: str
    account_key: Optional[str] = None
    sas_token: Optional[str] = None
    prefix: Optional[str] = None
    azure_cloud: Optional[str] = None


class RohmuGoogleStorageConfig(RohmuProxyStorage):
    storage_type: Literal[RohmuStorageType.google]
    # rohmu.object_storage.google:GoogleTransfer.__init__ arguments
    project_id: str
    bucket_name: str
    credentials: Dict[str, str]
    prefix: Optional[str] = None
    # credential_file # omitted, n/a


RohmuStorageConfig = Union[RohmuLocalStorageConfig, RohmuS3StorageConfig, RohmuAzureStorageConfig, RohmuGoogleStorageConfig]


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
    algorithm: Optional[RohmuCompressionType] = None
    level: int = 0
    # threads: int = 0


class RohmuConfig(RohmuModel):
    temporary_directory: str

    # Targets we support for backing up
    default_storage: str
    storages: Dict[str, RohmuStorageConfig]

    # Encryption (optional)
    encryption_key_id: Optional[str] = None
    encryption_keys: Dict[str, RohmuEncryptionKey] = {}

    # Compression (optional)
    compression: RohmuCompression = RohmuCompression()


class RohmuMetadata(RohmuModel):
    encryption_key_id: Optional[str] = Field(None, alias="encryption-key-id")
    compression_algorithm: Optional[RohmuCompressionType] = Field(None, alias="compression-algorithm")


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
        self.storage = rohmu.get_transfer(self.storage_config.dict(by_alias=True, exclude_unset=True))

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

    def download_json(self, name: str) -> Json:
        key = os.path.join(self.json_key, name)
        f = io.BytesIO()
        self._download_key_to_file(key, f)
        f.seek(0)
        return json.load(f)

    def list_jsons(self) -> list[str]:
        return self._list_key(self.json_key)

    def upload_json_str(self, name: str, data: str) -> bool:
        key = os.path.join(self.json_key, name)
        f = io.BytesIO()
        encoded_data = data.encode()
        f.write(encoded_data)
        f.seek(0)
        self._upload_key_from_file(key, f, len(encoded_data))
        return True


class MultiRohmuStorage(MultiStorage[RohmuStorage]):
    def __init__(self, *, config: RohmuConfig) -> None:
        self.config = config

    def get_storage(self, name: str) -> RohmuStorage:
        return RohmuStorage(config=self.config, storage=name)

    def get_default_storage_name(self) -> str:
        return self.config.default_storage

    def list_storages(self) -> list[str]:
        return sorted(self.config.storages.keys())
