from typing import Tuple, Optional, Dict
import os
import ray
from guidewire.manifest import Manifest
from guidewire.batch import Batch
from guidewire.logging import logger as L
from guidewire.results import Result
class Processor:
    """A class to handle table processing operations."""
    
    def __init__(self, target_cloud: str, table_names: Tuple[str, ...] = None, parallel: bool = True, exceptions: list = None) -> None:
        """Initialize the Processor with table names and parallel processing.
        if table_names is not provided, all tables in the manifest will be processed.
        if parallel is False, the tables will be processed sequentially.
        if parallel is True, the tables will be processed in parallel using Ray.
        
        Args:
            target_cloud: Target cloud provider for delta tables ("azure" or "aws")
            table_names: Tuple of table names to process
            parallel: Whether to process tables in parallel using Ray (default: True)
            exceptions: List of table names to exclude from processing
        """
        self.table_names = table_names
        self.exceptions = exceptions
        self.parallel = parallel
        self.target_cloud = target_cloud
        self._validate_environment()
        
        # Set target cloud storage configuration based on target_cloud parameter
        if target_cloud == "azure":
            self.log_storage_account = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
            self.log_storage_container = os.environ["AZURE_STORAGE_ACCOUNT_CONTAINER"]
            self.subfolder = os.environ.get("AZURE_STORAGE_SUBFOLDER")
        elif target_cloud == "aws":
            # Use target-prefixed S3 bucket with fallback
            self.log_storage_account = (os.environ.get("AWS_TARGET_S3_BUCKET") or 
                                       os.environ.get("AWS_S3_BUCKET"))
            self.log_storage_container = None  # S3 doesn't use containers  
            self.subfolder = (os.environ.get("AWS_TARGET_S3_PREFIX") or 
                             os.environ.get("AWS_S3_PREFIX"))
        else:
            raise ValueError(f"Invalid target_cloud: {target_cloud}. Must be 'azure' or 'aws'")
            
        self.manifest_location = os.environ["AWS_MANIFEST_LOCATION"]
        self.manifest = Manifest(
            location=self.manifest_location,
            table_names=self.table_names
        )
        if self.table_names is None:
            self.table_names = self.manifest.get_table_names()
            if exceptions is not None:
                self.table_names = [name for name in self.table_names if name not in exceptions]
        if self.table_names is None:
            raise ValueError("Table names must be provided")
        self.results = []

    def _validate_environment(self) -> None:
        """Validate that all required environment variables are set for both source and target."""
        
        # Validate common required variables
        common_required_vars = {
            "AWS_MANIFEST_LOCATION": os.environ.get("AWS_MANIFEST_LOCATION")
        }
        
        # Validate source S3 credentials (for manifest reading)
        source_required_vars = self._get_aws_source_vars()
        
        # Validate target cloud specific required variables
        if self.target_cloud == "azure":
            target_required_vars = {
                "AZURE_STORAGE_ACCOUNT_NAME": os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
                "AZURE_STORAGE_ACCOUNT_CONTAINER": os.environ.get("AZURE_STORAGE_ACCOUNT_CONTAINER")
            }
        elif self.target_cloud == "aws":
            target_required_vars = self._get_aws_target_vars()
        else:
            raise ValueError(f"Invalid target_cloud: {self.target_cloud}. Must be 'azure' or 'aws'")
        
        # Check for missing variables
        all_required = {**common_required_vars, **source_required_vars, **target_required_vars}
        missing_vars = [var for var, value in all_required.items() if not value]
        
        if missing_vars:
            # Separate missing vars by category for better error messages
            missing_common = [v for v in missing_vars if v in common_required_vars]
            missing_source = [v for v in missing_vars if v in source_required_vars]  
            missing_target = [v for v in missing_vars if v in target_required_vars]
            
            error_parts = []
            if missing_common:
                error_parts.append(f"Common: {', '.join(missing_common)}")
            if missing_source:
                error_parts.append(f"Source S3: {', '.join(missing_source)}")
            if missing_target:
                target_type = "Azure" if self.target_cloud == "azure" else "Target S3"
                error_parts.append(f"{target_type}: {', '.join(missing_target)}")
                
            raise EnvironmentError(f"Missing required environment variables - {'; '.join(error_parts)}")
    
    def _get_aws_source_vars(self) -> Dict[str, str]:
        """Get AWS source environment variables with fallback logic."""
        return {
            "AWS_SOURCE_REGION (or AWS_REGION)": (
                os.environ.get("AWS_SOURCE_REGION") or 
                os.environ.get("AWS_REGION")
            ),
            "AWS_SOURCE_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID)": (
                os.environ.get("AWS_SOURCE_ACCESS_KEY_ID") or 
                os.environ.get("AWS_ACCESS_KEY_ID")
            ),
            "AWS_SOURCE_SECRET_ACCESS_KEY (or AWS_SECRET_ACCESS_KEY)": (
                os.environ.get("AWS_SOURCE_SECRET_ACCESS_KEY") or 
                os.environ.get("AWS_SECRET_ACCESS_KEY")
            )
        }
    
    def _get_aws_target_vars(self) -> Dict[str, str]:
        """Get AWS target environment variables with fallback logic."""
        return {
            "AWS_TARGET_S3_BUCKET (or AWS_S3_BUCKET)": (
                os.environ.get("AWS_TARGET_S3_BUCKET") or 
                os.environ.get("AWS_S3_BUCKET")
            ),
            "AWS_TARGET_REGION (or AWS_REGION)": (
                os.environ.get("AWS_TARGET_REGION") or 
                os.environ.get("AWS_REGION")
            ),
            "AWS_TARGET_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID)": (
                os.environ.get("AWS_TARGET_ACCESS_KEY_ID") or 
                os.environ.get("AWS_ACCESS_KEY_ID")
            ),
            "AWS_TARGET_SECRET_ACCESS_KEY (or AWS_SECRET_ACCESS_KEY)": (
                os.environ.get("AWS_TARGET_SECRET_ACCESS_KEY") or 
                os.environ.get("AWS_SECRET_ACCESS_KEY")
            )
        }


    @staticmethod
    @ray.remote
    def process_table_async(entry: str, manifest: Manifest, target_cloud: str, log_storage_account: str, log_storage_container: str, subfolder: str = None) -> Optional[Result]:
        """
        Process a single table entry using Ray distributed computing.
        
        Args:
            entry: The table name to process
            manifest: The manifest object containing table information
            target_cloud: Target cloud provider for delta tables ("azure" or "aws")
            log_storage_account: Storage account name (Azure) or S3 bucket name (AWS)
            log_storage_container: Storage container name (Azure only, None for AWS)
            subfolder: Optional subfolder name
        """
        batch_result = None
        try:
            L.info(f"Processing table: {entry}")
            manifest_entry = manifest.read(entry)
            if manifest_entry:
                batch_result = Batch(
                    table_name=entry,
                    manifest=manifest,
                    target_cloud=target_cloud,
                    storage_or_s3_name=log_storage_account,
                    storage_container=log_storage_container,
                    subfolder=subfolder,
                ).process_batch()
                L.info(f"Successfully processed table: {entry}")
                return batch_result
            else:
                L.warning(f"No manifest entry found for table: {entry}")
                return None
        except Exception as e:
            L.error(f"Error processing table {entry}: {str(e)}")
            return batch_result


            
        
    @staticmethod
    def process_table(entry: str, manifest: Manifest, target_cloud: str, log_storage_account: str, log_storage_container: str, subfolder: str = None) -> Optional[Result]:
        """
        Process a single table entry sequentially (non-parallel).
        
        Args:
            entry: The table name to process
            manifest: The manifest object containing table information
            target_cloud: Target cloud provider for delta tables ("azure" or "aws")
            log_storage_account: Storage account name (Azure) or S3 bucket name (AWS)
            log_storage_container: Storage container name (Azure only, None for AWS)
            subfolder: Optional subfolder name
        """
        batch_result = None
        try:
            L.info(f"Processing table: {entry}")
            manifest_entry = manifest.read(entry)
            if manifest_entry:
                batch_result = Batch(
                    table_name=entry,
                    manifest=manifest,
                    target_cloud=target_cloud,
                    storage_or_s3_name=log_storage_account,
                    storage_container=log_storage_container,
                    subfolder=subfolder,
                ).process_batch()
                L.info(f"Successfully processed table: {entry}")
                return batch_result
            else:
                L.warning(f"No manifest entry found for table: {entry}")
                return None
        except Exception as e:
            L.error(f"Error processing table {entry}: {str(e)}")
            return batch_result
            
    

    def run(self) -> None:
        """Execute the table processing workflow."""
        try:
            if self.parallel:
                # Initialize Ray for parallel processing (tqdm_ray handles output properly)
                ray.init(ignore_reinit_error=True, log_to_driver=True)
                
                # Process tables in parallel - each will show its own progress bars
                futures = [
                    self.process_table_async.remote(entry, self.manifest, self.target_cloud, self.log_storage_account, self.log_storage_container, self.subfolder)
                    for entry in self.table_names
                ]
                
                # Wait for all tasks to complete
                self.results = ray.get(futures)
                ray.shutdown()
            else:
                # Process tables sequentially
                for entry in self.table_names:
                    result = self.process_table(entry, self.manifest, self.target_cloud, self.log_storage_account, self.log_storage_container, self.subfolder)
                    self.results.append(result)
        except Exception as e:
            L.error(f"Application error: {str(e)}")
            if hasattr(ray, 'is_initialized') and ray.is_initialized():
                ray.shutdown()
            raise
