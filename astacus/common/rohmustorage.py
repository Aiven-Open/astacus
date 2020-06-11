"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Rohmu-specific actual object storage implementation

"""

from .exceptions import CompressionOrEncryptionRequired, NotFoundException
from .storage import MultiStorage, Storage, StorageUploadResult
from .utils import AstacusModel
from enum import Enum
from pghoard import rohmu  # type: ignore
from pghoard.rohmu import rohmufile  # type: ignore
from pghoard.rohmu import errors
from pydantic import DirectoryPath, Field
from typing import Dict, Optional, Union
from typing_extensions import Literal

import io
import json
import logging
import os
import tempfile

logger = logging.getLogger(__name__)


class RohmuStorageType(str, Enum):
    """ Embodies what is detected in rohmu.get_class_for_transfer """
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


class RohmuLocalStorageConfig(RohmuModel):
    storage_type: Literal[RohmuStorageType.local]
    directory: DirectoryPath
    prefix: Optional[str] = None


class RohmuS3StorageConfig(RohmuModel):
    storage_type: Literal[RohmuStorageType.s3]
    region: str
    bucket_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    prefix: Optional[str] = None
    # Some more obscure options with defaults are omitted


class RohmuAzureStorageConfig(RohmuModel):
    storage_type: Literal[RohmuStorageType.azure]
    account_name: str
    account_key: str
    bucket_name: str
    prefix: Optional[str] = None
    azure_cloud: Optional[str] = None


class RohmuGoogleStorageConfig(RohmuModel):
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
    """ Wrap rohmu exceptions in astacus ones; to be seen what is complete set """
    def _f(*a, **kw):
        try:
            return fun(*a, **kw)
        except errors.FileNotFoundFromStorageError as ex:
            raise NotFoundException from ex

    return _f


class RohmuStorage(Storage):
    """Implementation of the storage API, on top of pghoard.rohmu."""
    def __init__(self, config: RohmuConfig, *, storage=None):
        assert config
        self.config = config
        self.hexdigest_key = "data"
        self.json_key = "json"
        self._choose_storage(storage)
        os.makedirs(config.temporary_directory, exist_ok=True)
        if not self.config.compression.algorithm and not self.config.encryption_key_id:
            raise CompressionOrEncryptionRequired()

    @rohmu_error_wrapper
    def _download_key_to_file(self, key, f) -> bool:
        raw_metadata: dict = self.storage.get_metadata_for_key(key)
        with tempfile.TemporaryFile(dir=self.config.temporary_directory) as temp_file:
            self.storage.get_contents_to_fileobj(key, temp_file)
            temp_file.seek(0)
            rohmufile.read_file(
                input_obj=temp_file,
                output_obj=f,
                metadata=raw_metadata,
                key_lookup=self._private_key_lookup,
                log_func=logger.debug
            )
        return True

    def _list_key(self, key):
        return [os.path.basename(o["name"]) for o in self.storage.list_iter(key, with_metadata=False)]

    def _private_key_lookup(self, key_id: str) -> str:
        return self.config.encryption_keys[key_id].private

    def _public_key_lookup(self, key_id: str) -> str:
        return self.config.encryption_keys[key_id].public

    @rohmu_error_wrapper
    def _upload_key_from_file(self, key, f) -> StorageUploadResult:
        encryption_key_id = self.config.encryption_key_id
        compression = self.config.compression
        metadata = RohmuMetadata()
        rsa_public_key = None
        if encryption_key_id:
            metadata.encryption_key_id = encryption_key_id
            rsa_public_key = self._public_key_lookup(encryption_key_id)
        if compression.algorithm:
            metadata.compression_algorithm = compression.algorithm
        rohmu_metadata = metadata.dict(exclude_defaults=True, by_alias=True)
        plain_size = f.seek(0, 2)
        f.seek(0)
        with tempfile.TemporaryFile(dir=self.config.temporary_directory) as temp_file:
            rohmufile.write_file(
                input_obj=f,
                output_obj=temp_file,
                compression_algorithm=compression.algorithm,
                compression_level=compression.level,
                rsa_public_key=rsa_public_key,
                log_func=logger.debug
            )
            # compression_threads=compression.threads, # I wish
            # currently not supported by write_file API
            compressed_size = temp_file.tell()
            temp_file.seek(0)
            self.storage.store_file_object(key, temp_file, metadata=rohmu_metadata)
        return StorageUploadResult(size=plain_size, stored_size=compressed_size)

    storage_name: str = ""

    def _choose_storage(self, storage=None):
        if storage is None or storage == "":
            storage = self.config.default_storage
        self.storage_name = storage
        self.storage_config = self.config.storages[storage]
        self.storage = rohmu.get_transfer(self.storage_config.dict())

    def copy(self):
        return RohmuStorage(config=self.config, storage=self.storage_name)

    # HexDigestStorage implementation

    @rohmu_error_wrapper
    def delete_hexdigest(self, hexdigest):
        key = os.path.join(self.hexdigest_key, hexdigest)
        self.storage.delete_key(key)

    def list_hexdigests(self):
        return self._list_key(self.hexdigest_key)

    def download_hexdigest_to_file(self, hexdigest, f) -> bool:
        key = os.path.join(self.hexdigest_key, hexdigest)
        return self._download_key_to_file(key, f)

    def upload_hexdigest_from_file(self, hexdigest, f) -> StorageUploadResult:
        key = os.path.join(self.hexdigest_key, hexdigest)
        return self._upload_key_from_file(key, f)

    # JsonStorage implementation
    @rohmu_error_wrapper
    def delete_json(self, name: str):
        key = os.path.join(self.json_key, name)
        self.storage.delete_key(key)

    def download_json(self, name: str):
        key = os.path.join(self.json_key, name)
        f = io.BytesIO()
        self._download_key_to_file(key, f)
        f.seek(0)
        return json.load(f)

    def list_jsons(self):
        return self._list_key(self.json_key)

    def upload_json_str(self, name: str, data: str):
        key = os.path.join(self.json_key, name)
        f = io.BytesIO()
        f.write(data.encode())
        f.seek(0)
        self._upload_key_from_file(key, f)
        return True


class MultiRohmuStorage(MultiStorage):
    def __init__(self, *, config: RohmuConfig):
        self.config = config

    def get_storage(self, name):
        return RohmuStorage(config=self.config, storage=name)

    def get_default_storage_name(self):
        return self.config.default_storage

    def list_storages(self):
        return sorted(self.config.storages.keys())
