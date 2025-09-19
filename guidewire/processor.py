from typing import Tuple, Optional, Dict
import os
import ray
from guidewire.manifest import Manifest
from guidewire.batch import Batch
from guidewire.logging import logger as L
from guidewire.results import Result
from guidewire.progress_managers import SimpleProgressManager, MultiProgressManager
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
        
        # Check if progress should be shown based on environment variable
        self.show_progress = os.environ.get("SHOW_TABLE_PROGRESS", "1") != "0"
        
        
        self.largest_first_flag = os.environ.get("LARGEST_TABLES_FIRST_COUNT")
        if self.largest_first_flag:
            self.table_names = self._order_tables_by_size(self.table_names)
        
        # Initialize progress manager for sequential processing only if progress should be shown
        self.progress_manager = SimpleProgressManager(show_progress=True) if self.show_progress else None


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
        
    def _order_tables_by_size(self, table_names: list, largest_first_count: int) -> list:
        """Order tables by size for optimal parallel processing.
        
        Process the largest tables first to avoid having large tables 
        running alone at the end while other threads are idle.
        
        Args:
            table_names: List of table names to order
            largest_first_count: Number of largest tables to prioritize
        Returns:
            list: Ordered table names with largest tables first
        """
        
        # Get table sizes for ordering
        table_sizes = []
        for table_name in table_names:
            try:
                entry = self.manifest.read(table_name)
                record_count = entry.get("totalProcessedRecordsCount", 0) if entry else 0
                table_sizes.append((table_name, record_count))
            except Exception as e:
                L.warning(f"Could not get record count for table {table_name}: {e}")
                # Default to 0 if we can't read the manifest entry
                table_sizes.append((table_name, 0))
        
        # Sort by record count descending (largest first)
        table_sizes.sort(key=lambda x: x[1], reverse=True)
        
        if largest_first_count <= 0 or len(table_names) <= largest_first_count:
            # If count is 0/negative or we have fewer tables than the threshold,
            # just sort all tables by size descending
            ordered_tables = [table_name for table_name, _ in table_sizes]
            L.debug(f"Ordered all {len(ordered_tables)} tables by size (largest first)")
            return ordered_tables
        
        # Split into largest N tables and remaining tables
        largest_tables = [table_name for table_name, _ in table_sizes[:largest_first_count]]
        remaining_tables = [table_name for table_name, _ in table_sizes[largest_first_count:]]
        
        # Combine: largest tables first, then remaining tables
        ordered_tables = largest_tables + remaining_tables
        L.debug(f"Ordered tables: {largest_first_count} largest tables first, then {len(remaining_tables)} remaining tables")

        return ordered_tables

        
    @staticmethod
    @ray.remote
    def process_table_async(entry: str, manifest: Manifest, target_cloud: str, log_storage_account: str, log_storage_container: str, subfolder: str = None, tracker_actor = None) -> Optional[Result]:
        """
        Process a single table entry asynchronously with Ray.
        
        Args:
            entry: The table name to process
            manifest: The manifest object containing table information
            target_cloud: Target cloud provider for delta tables ("azure" or "aws")
            log_storage_account: Storage account name (Azure) or S3 bucket name (AWS)
            log_storage_container: Storage container name (Azure only, None for AWS)
            subfolder: Optional subfolder name
            tracker_actor: Ray actor for progress tracking
        """
        batch_result = None
        try:
            manifest_entry = manifest.read(entry)
            if manifest_entry:
                batch_result = Batch(
                    table_name=entry,
                    manifest=manifest,
                    target_cloud=target_cloud,
                    storage_or_s3_name=log_storage_account,
                    storage_container=log_storage_container,
                    subfolder=subfolder,
                    progress_manager=tracker_actor,  # Pass Ray actor directly
                    parallel=True,  # This is the async parallel processing path
                ).process_batch()
                return batch_result
            else:
                return None
        except Exception as e:
            return batch_result

    @staticmethod
    def process_table(entry: str, manifest: Manifest, target_cloud: str, log_storage_account: str, log_storage_container: str, subfolder: str = None, progress_manager = None) -> Optional[Result]:
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
            manifest_entry = manifest.read(entry)
            if manifest_entry:
                batch_result = Batch(
                    table_name=entry,
                    manifest=manifest,
                    target_cloud=target_cloud,
                    storage_or_s3_name=log_storage_account,
                    storage_container=log_storage_container,
                    subfolder=subfolder,
                    progress_manager=progress_manager,
                    parallel=False,  # This is the sequential processing path
                ).process_batch()
                return batch_result
            else:
                return None
        except Exception as e:
            return batch_result
            
    

    def run(self) -> None:
        """Execute the table processing workflow."""
        progress_display = None
        multi_progress = None  # Initialize to None to avoid NameError in exception handler
        try:
            if self.parallel:
                # Initialize Ray for parallel processing with minimal logging
                ray.init(
                    ignore_reinit_error=True, 
                    log_to_driver=False,
                    include_dashboard=False,
                    configure_logging=False,  # Disable Ray's logging configuration
                    logging_level="ERROR"     # Only show errors
                )
                if self.show_progress:
                # Start Ray-based progress tracking for parallel processing only if progress should be shown
                    multi_progress = MultiProgressManager(show_progress=self.show_progress) 
                    progress_display = multi_progress.start(list(self.table_names))  # Registers tables internally
                    
                    # Process tables in parallel - pass the tracker actor directly (or None if no progress)
                    tracker_actor = multi_progress.get_tracker_actor()
                    futures = [
                        self.process_table_async.remote(
                            entry, self.manifest, self.target_cloud, 
                            self.log_storage_account, self.log_storage_container, 
                            self.subfolder, tracker_actor
                        )
                        for entry in self.table_names
                    ]      
                    self.results = multi_progress.wait_for_completion(futures)
                    multi_progress.stop()
                else:
                    # No progress manager, just wait for completion normally
                    futures = [
                        self.process_table_async.remote(
                            entry, self.manifest, self.target_cloud, 
                            self.log_storage_account, self.log_storage_container, 
                            self.subfolder
                        )
                        for entry in self.table_names
                    ]
                    self.results = ray.get(futures)
                ray.shutdown()
            else:
                # Process tables sequentially with optional progress tracking
                progress_display = None
                if self.progress_manager:
                    progress_display = self.progress_manager.start(table_names=list(self.table_names))    
                    for table_name in self.table_names:
                        self.progress_manager.register_table(table_name)
                
                for entry in self.table_names:
                    result = self.process_table(entry, self.manifest, self.target_cloud, self.log_storage_account, self.log_storage_container, self.subfolder, self.progress_manager)
                    self.results.append(result)
                    
                if progress_display and self.progress_manager:
                    self.progress_manager.stop()
        except Exception as e:
            L.error(f"Application error: {str(e)}")
            # Clean up progress managers if they exist
            if self.parallel and multi_progress is not None:
                try:
                    multi_progress.stop()
                except Exception as cleanup_error:
                    L.warning(f"Error stopping multi progress manager: {cleanup_error}")
            elif not self.parallel and self.progress_manager is not None:
                try:
                    self.progress_manager.stop()
                except Exception as cleanup_error:
                    L.warning(f"Error stopping progress manager: {cleanup_error}")
            
            # Always ensure Ray is shut down in parallel mode
            if hasattr(ray, 'is_initialized') and ray.is_initialized():
                try:
                    ray.shutdown()
                except Exception as ray_cleanup_error:
                    L.warning(f"Error shutting down Ray: {ray_cleanup_error}")
            raise
            