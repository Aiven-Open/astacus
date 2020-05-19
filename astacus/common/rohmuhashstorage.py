"""

Copyright (c) 2020 Aiven Ltd
See LICENSE for details

Rohmu-specific actual object storage implementation

"""

from .hashstorage import HashStorage
from .utils import AstacusModel
from enum import Enum
from pghoard import rohmu  # type: ignore
from pghoard.rohmu import rohmufile  # type: ignore
from pydantic import Field
from typing import Dict, Optional, Union
from typing_extensions import Literal

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
    directory: str
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
    project_id: str
    bucket_name: str
    account_key: str
    prefix: Optional[str] = None
    azure_cloud: Optional[str] = None


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


class RohmuHashStorage(HashStorage):
    """Implementation of the hash storage API, on top of pghoard.rohmu.

Note that this isn't super optimized insofar reading is concerned.  We
could store metadata somewhere, but keeping it in the actual storage
layer makes the design somewhat more clean.
    """
    def __init__(self, config: RohmuConfig, *, storage=None):
        assert config
        self.config = config
        self.data_key = "hashblock"
        self.choose_storage(storage)
        os.makedirs(config.temporary_directory, exist_ok=True)

    def choose_storage(self, storage=None):
        if storage is None:
            storage = self.config.default_storage
        self.storage_config = self.config.storages[storage]
        self.storage = rohmu.get_transfer(self.storage_config.dict())

    def delete_hexdigest(self, hexdigest):
        key = os.path.join(self.data_key, hexdigest)
        self.storage.delete_key(key)

    def list_hexdigests(self):
        return [os.path.basename(o["name"]) for o in self.storage.list_iter(self.data_key, with_metadata=False)]

    def _private_key_lookup(self, key_id: str) -> str:
        return self.config.encryption_keys[key_id].private

    def _public_key_lookup(self, key_id: str) -> str:
        return self.config.encryption_keys[key_id].public

    def download_hexdigest_to_file(self, hexdigest, f) -> bool:
        key = os.path.join(self.data_key, hexdigest)
        raw_metadata: dict = self.storage.get_metadata_for_key(key)
        logger.debug("download_hexdigest_to_file - raw_metadata: %r", raw_metadata)
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

    def upload_hexdigest_from_file(self, hexdigest, f) -> bool:
        key = os.path.join(self.data_key, hexdigest)
        encryption_key_id = self.config.encryption_key_id
        compression = self.config.compression
        metadata = RohmuMetadata()
        rsa_public_key = None
        if encryption_key_id:
            metadata.encryption_key_id = encryption_key_id
            rsa_public_key = self._public_key_lookup(encryption_key_id)
        if compression.algorithm:
            metadata.compression_algorithm = compression.algorithm
        rohmu_metadata = metadata.dict(exclude_unset=True, by_alias=True)
        logger.debug("upload_hexdigest_from_file - %r", rohmu_metadata)
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
            temp_file.seek(0)
            self.storage.store_file_object(key, temp_file, metadata=rohmu_metadata)
        return True
