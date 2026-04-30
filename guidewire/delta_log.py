from abc import ABC, abstractmethod
from time import sleep
from deltalake.transaction import AddAction, create_table_with_add_actions, CommitProperties
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import Schema as DeltaSchema
from deltalake import DeltaTable, PostCommitHookProperties, write_deltalake
import pyarrow as pa
from guidewire.logging import logger as L
from guidewire.storage import AzureStorage, AWSStorage, BaseStorage
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
                table_uri=self.log_uri, storage_options=self.storage_options,log_buffer_size=1
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

    def _make_schema_nullable(self, schema: pa.Schema) -> pa.Schema:
        """Convert all fields in a schema to nullable=True.
        
        Args:
            schema: PyArrow Schema to convert
            
        Returns:
            pa.Schema: New schema with all fields set to nullable=True
        """

        return pa.schema([
            pa.field(field.name, field.type, nullable=True) 
            for field in schema
        ])

    def _get_watermark_from_log(self) -> dict[str, int]:
        """Get the watermark and schema timestamp from the Delta log entry with fallback strategy.
        
        Fallback strategy:
        1. If table doesn't exist, return zeros
        2. Check first history record for valid watermarks
        3. If first record doesn't have valid watermarks, check second record
        4. If neither has valid watermarks, fail with error (safer than returning zeros)
        
        Returns:
            dict[str, int]: Dictionary containing watermark and schema_timestamp
            
        Raises:
            DeltaError: If no valid watermarks found in first two history records of existing table
        """
        # Return zeros if table doesn't exist
        if not self.table_exists():
            L.debug(f"Table {self.table_name} does not exist, returning zero watermarks")
            return {"watermark": 0, "schema_timestamp": 0}
            
        try:
            history = self.delta_log.history()
            if not history:
                L.debug(f"No history found for {self.table_name}, returning zero watermarks")
                return {"watermark": 0, "schema_timestamp": 0}
            
            # Helper function to safely extract and validate watermarks
            def extract_valid_watermarks(entry):
                try:
                    watermark_val = entry.get("watermark")
                    schema_val = entry.get("schema_timestamp")
                    if watermark_val and schema_val:
                        # Try to convert to int - if it fails, these aren't valid watermarks
                        watermark = int(watermark_val)
                        schema_timestamp = int(schema_val)
                        return watermark, schema_timestamp
                except (ValueError, TypeError):
                    # Invalid values that can't be converted to int
                    pass
                return None, None
            
            # Check first history record
            watermark, schema_timestamp = extract_valid_watermarks(history[0])
            if watermark is not None and schema_timestamp is not None:
                L.debug(f"Found watermarks in first history record for {self.table_name}: watermark={watermark}, schema_timestamp={schema_timestamp}")
                return {"watermark": watermark, "schema_timestamp": schema_timestamp}
            
            # Check second history record if available
            if len(history) > 1:
                watermark, schema_timestamp = extract_valid_watermarks(history[1])
                if watermark is not None and schema_timestamp is not None:
                    L.debug(f"Found watermarks in second history record for {self.table_name}: watermark={watermark}, schema_timestamp={schema_timestamp}")
                    return {"watermark": watermark, "schema_timestamp": schema_timestamp}
            
            # Neither first nor second record has valid watermarks - this is an error for existing tables
            L.error(f"No valid watermarks found in first two history records for existing table {self.table_name}")
            raise DeltaError(f"No valid watermarks found in available history records for table {self.table_name}")
            
        except DeltaError:
            # Re-raise DeltaError as-is
            raise
        except Exception as e:
            L.error(f"Failed to get watermark from log for {self.table_name}: {e}")
            raise DeltaError(f"Failed to get watermark from log: {e}")

    
    def add_schema_metadata_change(self, schema: pa.Schema) -> None:
        """Add an empty metadata schema merge operation for schema changes.
        
        Args:
            schema: PyArrow Schema for the new schema structure
        """
        try:
            #is there a chance that the latest watermark is not there
            # Create an empty table with the new schema
            # Need to provide empty arrays for each field in the schema
            nullable_schema = self._make_schema_nullable(schema)
            empty_arrays = []
            for field in schema:
                empty_array = pa.array([], type=field.type)
                empty_arrays.append(empty_array)
            empty_table = pa.table(empty_arrays, schema=nullable_schema)
            
            write_deltalake(
                table_or_uri=self.log_uri,
                data=empty_table,
                mode="append",
                schema_mode="merge",
                storage_options=self.storage_options
            )
            L.debug(f"Added schema metadata change for {self.table_name}")
            # Update delta_log reference after schema change
            try:
                self._log_exists()
            except Exception as e:
                L.warning(f"Failed to refresh delta log after schema change: {e}")
        except Exception as e:
            L.error(f"Failed to add schema metadata change for {self.table_name}: {e}")
            raise DeltaError(f"Failed to add schema metadata change: {e}")

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
                    data_change=True,
                    stats="{}",
                )
            )

        try:

            # Make schema nullable for both create and append operations
            nullable_pa_schema = self._make_schema_nullable(schema)
            # Convert PyArrow schema to deltalake Schema for delta-rs operations
            nullable_delta_schema = DeltaSchema.from_arrow(nullable_pa_schema)
            commit_properties = CommitProperties(custom_metadata={"watermark": str(watermark), "schema_timestamp": str(schema_timestamp)})
            if self.delta_log is None:
                L.debug(f"Creating new table: {self.table_name}")       
                create_table_with_add_actions(
                    table_uri=self.log_uri,
                    schema=nullable_delta_schema,
                    add_actions=actions,
                    mode="overwrite",
                    partition_by=[],
                    name=self.table_name,
                    storage_options=self.storage_options,
                    commit_properties=commit_properties,
                    configuration={"delta.checkpointPolicy": "v2",
                                   "delta.checkpointInterval": "50",
                                   "delta.isolationLevel": "WriteSerializable"
                                   },
                )
                # Initialize delta_log reference after creating the table
                self._log_exists()
            else:
                L.debug(f"Adding to table: {self.table_name} - watermark: {watermark}")
                
                self.delta_log.create_write_transaction(
                    actions=actions, mode=mode, schema=nullable_delta_schema, partition_by=[],
                    commit_properties=commit_properties,
                )
                # This update is optional as it only refreshes the delta log reference. 
                # Will cause warning on fail but stops azure failure bringing down the pipeline
                try:
                    self.delta_log.update_incremental()
                except:
                    L.warning(f"Failed to update delta log for {self.table_name} after transaction, sleeping for some time")
                    sleep(10)

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
        storage: Optional[BaseStorage] = None,
    ) -> None:
        """Initialize the AWS S3 Delta log instance.

        Args:
            bucket_name: The S3 bucket name
            table_name: The name of the Delta table
            subfolder: Optional subfolder path
            storage: Optional pre-configured storage instance. If omitted,
                an :class:`AWSStorage` is constructed with the ``TARGET``
                env-var prefix (legacy behavior). Pass a :class:`UCStorage`
                instance to write the Delta log under Unity Catalog
                governance with credential vending.

        Raises:
            DeltaValidationError: If any of the required parameters are empty
        """
        super().__init__()

        if not all([bucket_name, table_name]):
            raise DeltaValidationError("bucket_name and table_name must be non-empty strings")

        self.bucket_name = bucket_name
        self.table_name = table_name
        self.subfolder = subfolder

        # Use the caller-supplied storage if present; otherwise preserve the
        # legacy default of AWSStorage(prefix="TARGET").
        self.fs = storage if storage is not None else AWSStorage(prefix="TARGET")
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
