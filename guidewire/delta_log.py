from abc import ABC, abstractmethod
from time import sleep
from deltalake.transaction import AddAction, create_table_with_add_actions, CommitProperties
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Schema
from deltalake import DeltaTable, PostCommitHookProperties
import pyarrow as pa
from guidewire.logging import logger as L
from guidewire.storage import AzureStorage, AWSStorage
from typing import List, Dict, Optional, Union, Literal
import os


class DeltaError(Exception):
    """Base exception class for Delta-related errors."""
    pass


class DeltaValidationError(DeltaError):
    """Exception raised for validation errors in Delta operations."""
    pass


class BaseDeltaLog(ABC):
    """Abstract base class for Delta Lake transaction logs and checkpoints.
    
    This abstract class provides common Delta Lake operations while requiring
    subclasses to implement cloud-specific URI construction and initialization.
    """
    
    # Class constants
    DEFAULT_MODE = "append"
    CHECKPOINT_DIR = "_checkpoints/log"
    VALID_MODES = ("append", "overwrite")
    
    def __init__(self):
        """Base initialization. Subclasses must set required attributes."""
        self.delta_log: Optional[DeltaTable] = None
        self.log_uri = ""
        self.storage_options = {}
        self.transaction_count = 0
        self.checkpoint_interval = int(os.getenv("DELTA_LOG_CHECKPOINT_INTERVAL", 100))
        self.table_name = ""
        self.fs = None
        
    @abstractmethod
    def construct_log_uri(self) -> str:
        """Construct the Delta log URI for the specific cloud provider.
        
        Returns:
            str: The properly formatted Delta log URI
        """
        pass
    
    def _log_exists(self) -> None:
        """Check if the Delta log exists and initialize it if found."""
        try:
            self.delta_log = DeltaTable(
                table_uri=self.log_uri, storage_options=self.storage_options
            )
        except Exception as e:
            # If it's a file not found error, that is ok
            if isinstance(e, TableNotFoundError):
                L.debug(f"Log does not exist for {self.table_name}: {e}")
            else:
                L.error(f"Error reading log for {self.table_name}: {e}")
                raise DeltaError(f"Error reading log: {e}")

    def table_exists(self) -> bool:
        """Check if the Delta table exists.
        
        Returns:
            bool: True if the table exists, False otherwise
        """
        return self.delta_log is not None

    def get_table_stats(self) -> Dict[str, Union[int, str]]:
        """Get basic statistics about the Delta table.
        
        Returns:
            Dict[str, Union[int, str]]: Dictionary containing table statistics
            
        Raises:
            DeltaError: If the table doesn't exist or stats can't be retrieved
        """
        if not self.table_exists():
            raise DeltaError(f"Table {self.table_name} does not exist")
            
        try:
            return {
                "version": self.delta_log.version(),
                "num_files": len(self.delta_log.files()),
                "table_uri": self.log_uri
            }
        except Exception as e:
            raise DeltaError(f"Failed to get table stats: {e}")

    def remove_log(self) -> bool:
        """Remove the Delta log.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.fs.delete_dir(path=self.log_uri)
            return True
        except Exception as e:
            L.error(f"Failed to remove log for {self.table_name}: {e}")
            return False

    def _validate_parquet_info(self, parquet: Dict[str, Union[str, int]]) -> None:
        """Validate parquet file information.
        
        Args:
            parquet: Dictionary containing parquet file information
            
        Raises:
            DeltaValidationError: If required keys are missing or values are invalid
        """
        required_keys = ["path", "size", "last_modified"]
        for key in required_keys:
            if key not in parquet:
                raise DeltaValidationError(f"Parquet info must contain '{key}'")
        
        if not isinstance(parquet["path"], str) or not parquet["path"]:
            raise DeltaValidationError("Parquet path must be a non-empty string")
            
        if not isinstance(parquet["size"], int) or parquet["size"] < 0:
            raise DeltaValidationError("Parquet size must be a non-negative integer")
            
        if not isinstance(parquet["last_modified"], int) or parquet["last_modified"] < 0:
            raise DeltaValidationError("Parquet last_modified must be a non-negative integer")

    def _get_watermark_from_log(self) -> dict[str, int]:
        """Get the watermark and schema timestamp from the most recent Delta log entry.
        
        Returns:
            dict[str, int]: Dictionary containing watermark and schema_timestamp
        """
        if not self.table_exists():
            return {"watermark": 0, "schema_timestamp": 0}
            
        try:
            history = self.delta_log.history()
            if history:
                latest_entry = history[0]
                operation_parameters = latest_entry.get("operationParameters", {})
                metadata = latest_entry.get("operationMetrics", {})
                
                watermark = int(operation_parameters.get("watermark", 0))
                schema_timestamp = int(operation_parameters.get("schema_timestamp", 0))
                
                return {"watermark": watermark, "schema_timestamp": schema_timestamp}
            return {"watermark": 0, "schema_timestamp": 0}
        except Exception as e:
            L.warning(f"Failed to get watermark from log for {self.table_name}: {e}")
            return {"watermark": 0, "schema_timestamp": 0}

    def _create_checkpoint(self) -> bool:
        """Create a checkpoint for the Delta table.
        
        Returns:
            bool: True if checkpoint creation was successful, False otherwise
        """
        if not self.table_exists():
            L.warning(f"Cannot create checkpoint for non-existent table: {self.table_name}")
            return False
            
        try:
            self.delta_log.create_checkpoint()
            L.debug(f"Successfully created checkpoint for {self.table_name}")
            return True
        except Exception as e:
            L.warning(f"Failed to create checkpoint for {self.table_name}: {e}")
            return False

    def add_transaction(
        self, 
        parquets: List[Dict[str, Union[str, int]]], 
        schema: pa.Schema, 
        watermark: int,
        schema_timestamp: int, 
        mode: Literal["append", "overwrite"] = DEFAULT_MODE,
    ) -> None:
        """Add a transaction to the Delta log.
        
        Args:
            parquets: List of dictionaries containing parquet file information
            schema: The PyArrow schema for the data
            watermark: The watermark value for this transaction
            schema_timestamp: The schema timestamp for this transaction
            mode: The write mode ("append" or "overwrite")
            
        Raises:
            DeltaValidationError: If parquet information is invalid or mode is invalid
            DeltaError: If adding the transaction fails
        """
        if mode not in self.VALID_MODES:
            raise DeltaValidationError(f"Mode must be one of {self.VALID_MODES}")
            
        if not parquets:
            raise DeltaValidationError("At least one parquet file must be provided")

        if not self.table_exists():
            self._log_exists()   
        actions = []
        for file in parquets:
            self._validate_parquet_info(file)
            actions.append(
                AddAction(
                    path=file["path"],
                    size=file["size"],
                    partition_values={},
                    modification_time=file["last_modified"],
                    data_change=False,
                    stats="{}",
                )
            )

        try:
            schema = Schema.from_arrow(schema)
            commit_properties = CommitProperties(custom_metadata={"watermark": str(watermark), "schema_timestamp": str(schema_timestamp)})
            post_commithook_properties = PostCommitHookProperties(create_checkpoint=False, cleanup_expired_logs=False)
            if self.delta_log is None:
                L.debug(f"Creating new table: {self.table_name}")       
                create_table_with_add_actions(
                    table_uri=self.log_uri,
                    schema=schema,
                    add_actions=actions,
                    mode="overwrite",
                    partition_by=[],
                    name=self.table_name,
                    storage_options=self.storage_options,
                    commit_properties=commit_properties,
                    post_commithook_properties=post_commithook_properties
                )
            else:
                L.debug(f"Adding to table: {self.table_name} - watermark: {watermark}")
                
                self.delta_log.create_write_transaction(
                    actions=actions, mode=mode, schema=schema, partition_by=[],
                    commit_properties=commit_properties,
                    post_commithook_properties=post_commithook_properties
                )
                # This update is optional as it only refreshes the delta log reference. 
                # Will cause warning on fail but stops azure failure bringing down the pipeline
                try:
                    self.delta_log.update_incremental()
                except:
                    L.warning(f"Failed to update delta log for {self.table_name} after transaction, sleeping for some time")
                    sleep(10)
                    
            # Increment transaction counter and check for checkpoint
            self.transaction_count += 1
            if self.transaction_count % self.checkpoint_interval == 0:
                L.debug(f"Reached {self.checkpoint_interval} transactions for {self.table_name}, creating checkpoint")
                self._create_checkpoint()
                
        except Exception as e:
            L.error(f"Failed to add transaction for {self.table_name}: {e}")
            raise DeltaError(f"Failed to add transaction: {e}")


class AzureDeltaLog(BaseDeltaLog):
    """Azure Blob Storage implementation of Delta Lake operations.
    
    Azure Environment Variables (inherited from AzureStorage):
        - AZURE_STORAGE_ACCOUNT_NAME (required): Azure storage account name
        - AZURE_STORAGE_ACCOUNT_KEY (optional): Azure storage account key
        - AZURE_TENANT_ID (optional): Azure tenant ID for service principal auth
        - AZURE_CLIENT_ID (optional): Azure client ID for service principal auth
        - AZURE_CLIENT_SECRET (optional): Azure client secret for service principal auth
        - AZURE_BLOB_STORAGE_AUTHORITY (optional): Custom blob storage authority (hostname:port)
        - AZURE_BLOB_STORAGE_SCHEME (optional): http or https, defaults to https
        - AZURE_DFS_STORAGE_AUTHORITY (optional): Custom DFS storage authority (hostname:port)
        - AZURE_DFS_STORAGE_SCHEME (optional): http or https, defaults to https
    """
    
    def __init__(
        self,
        storage_account: str,
        storage_container: str,
        table_name: str,
        subfolder: Optional[str] = None,
    ) -> None:
        """Initialize the Azure Delta log instance.
        
        Args:
            storage_account: The Azure storage account name
            storage_container: The storage container name
            table_name: The name of the Delta table
            subfolder: Optional subfolder path
            
        Raises:
            DeltaValidationError: If any of the required parameters are empty
        """
        super().__init__()
        
        if not all([storage_account, storage_container, table_name]):
            raise DeltaValidationError("storage_account, storage_container, and table_name must be non-empty strings")
        
        self.storage_account = storage_account
        self.storage_container = storage_container
        self.table_name = table_name
        self.subfolder = subfolder
        
        # Initialize Azure storage with no prefix (direct instantiation)
        self.fs = AzureStorage()
        self.storage_options = self.fs.storage_options
        
        # Construct log URI and check if log exists
        self.log_uri = self.construct_log_uri()
        self._log_exists()

    def construct_log_uri(self) -> str:
        """Construct the Azure Delta log URI."""
        if self.subfolder:
            path_part = f"{self.subfolder}/{self.table_name}"
        else:
            path_part = self.table_name
        
        log_uri = f"abfss://{self.storage_container}@{self.storage_account}.dfs.core.windows.net/{path_part}/"
        L.debug(f"Using Azure URI format: {log_uri}")
        
        return log_uri


class AWSDeltaLog(BaseDeltaLog):
    """AWS S3 implementation of Delta Lake operations.
    
    AWS Environment Variables (inherited from AWSStorage):
        - AWS_TARGET_REGION or AWS_REGION (required): AWS region
        - AWS_TARGET_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID (required): AWS access key ID
        - AWS_TARGET_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY (required): AWS secret access key
        - AWS_TARGET_ENDPOINT_URL or AWS_ENDPOINT_URL (optional): Custom S3 endpoint override
    """
    
    def __init__(
        self,
        bucket_name: str,
        table_name: str,
        subfolder: Optional[str] = None,
    ) -> None:
        """Initialize the AWS S3 Delta log instance.
        
        Args:
            bucket_name: The S3 bucket name
            table_name: The name of the Delta table
            subfolder: Optional subfolder path
            
        Raises:
            DeltaValidationError: If any of the required parameters are empty
        """
        super().__init__()
        
        if not all([bucket_name, table_name]):
            raise DeltaValidationError("bucket_name and table_name must be non-empty strings")
        
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.subfolder = subfolder
        
        # Initialize AWS storage with TARGET prefix for delta table writing credentials
        self.fs = AWSStorage(prefix="TARGET")
        self.storage_options = self.fs.storage_options
        
        # Construct log URI and check if log exists
        self.log_uri = self.construct_log_uri()
        self._log_exists()

    def construct_log_uri(self) -> str:
        """Construct the S3 Delta log URI."""
        if self.subfolder:
            path_part = f"{self.subfolder}/{self.table_name}"
        else:
            path_part = self.table_name
        
        log_uri = f"s3://{self.bucket_name}/{path_part}/"
        L.debug(f"Using S3 URI format: {log_uri}")
        
        return log_uri
