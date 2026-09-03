import os
import pyarrow as pa
from pyarrow.fs import FileType
from guidewire.logging import logger as L
from guidewire.delta_log import AzureDeltaLog, AWSDeltaLog
from guidewire.manifest import Manifest
from guidewire.storage import BaseStorage, UCStorage
from guidewire.staging import StagingExecutor
from typing import Optional
from guidewire.results import Result
from datetime import datetime
import ray

class Batch:
    def __init__(
        self,
        table_name: str,
        manifest: Manifest,
        target_cloud: str,
        storage_or_s3_name: str,
        storage_container: Optional[str],
        reset: bool = False,
        subfolder: Optional[str] = None,
        progress_manager = None,
        parallel: bool = False,
        maintain_timestamp_transactions: bool = True,
        target_storage: Optional[BaseStorage] = None,
        staging_mode: bool = False,
    ):
        """Initialize a new Batch instance.

        Args:
            table_name: Name of the table to process
            manifest: Manifest object containing file information
            target_cloud: Target cloud provider for delta tables ("azure" or "aws")
            storage_or_s3_name: Storage account name (Azure) or S3 bucket name (AWS)
            storage_container: Storage container name (Azure only, None for AWS)
            reset: Whether to reset the processing state
            subfolder: Optional subfolder to process
            progress_manager: Optional progress manager (Ray actor if parallel=True)
            parallel: Whether this batch is running in parallel mode with Ray
            maintain_timestamp_transactions: Whether to maintain timestamp transactions (default: True)
            target_storage: Optional pre-configured storage instance for the
                target Delta log. AWS-only today. When omitted, the legacy
                ``AWSStorage(prefix="TARGET")`` default is used. Pass a
                :class:`guidewire.storage.UCStorage` instance to write the
                Delta log under Unity Catalog governance.
            staging_mode: When True, copy each source parquet into the
                target table root before recording its AddAction. This is
                required for Guidewire CDA SaaS deployments where the source
                bucket cannot be registered as a UC external location.
                Requires ``target_storage`` to be a :class:`UCStorage`
                instance and ``target_cloud='aws'``.
        Raises:
            ValueError: If required parameters are invalid
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("table_name must be a non-empty string")
        if not target_cloud or not isinstance(target_cloud, str):
            raise ValueError("target_cloud must be a non-empty string")
        if not storage_or_s3_name or not isinstance(storage_or_s3_name, str):
            raise ValueError("storage_or_s3_name must be a non-empty string")
        if target_cloud == "azure" and (not storage_container or not isinstance(storage_container, str)):
            raise ValueError("storage_container must be a non-empty string for Azure target cloud")

        self.table_name = table_name
        self.manifest = manifest
        self.entry = self.manifest.read(entry=self.table_name)
        self.cached_schema = None
        self.progress_manager = progress_manager
        self.parallel = parallel
        if target_cloud == "azure":
            self.log_entry = AzureDeltaLog(
                storage_account=storage_or_s3_name,
                storage_container=storage_container,
                table_name=self.table_name,
                subfolder=subfolder,
            )
        elif target_cloud == "aws":
            self.log_entry = AWSDeltaLog(
                bucket_name=storage_or_s3_name,
                table_name=self.table_name,
                subfolder=subfolder,
                storage=target_storage,
            )
        else:
            raise ValueError(f"Invalid target_cloud: {target_cloud}. Must be 'azure' or 'aws'")
        # Optional staging executor for UC-governed CDA pipelines.
        # Constructed only when staging_mode=True; otherwise None and Batch
        # behaves exactly as before.
        self.staging_executor: Optional[StagingExecutor] = None
        if staging_mode:
            if not isinstance(target_storage, UCStorage):
                raise ValueError(
                    "staging_mode=True requires target_storage to be a UCStorage "
                    "instance (staging only makes sense when the target is UC-governed)."
                )
            if target_cloud != "aws":
                raise NotImplementedError(
                    f"staging_mode=True is supported only for target_cloud='aws' in v1; "
                    f"got target_cloud={target_cloud!r}."
                )
            self.staging_executor = StagingExecutor(
                source_storage=self.manifest.fs,
                target_storage=target_storage,
                target_table_root=self.log_entry.log_uri,
                source_table_root=self.entry["dataFilesPath"],
            )

        self.watermark_info = self.log_entry._get_watermark_from_log()
        self.low_watermark = 0 if reset else self.watermark_info["watermark"]
        self.watermark_schema_timestamp = 0 if reset else self.watermark_info["schema_timestamp"]
        
        # Configuration to control timestamp transaction behavior
        # False = batch all add actions and commit once per schema with latest timestamp as watermark
        # True = maintain existing behavior of one commit per timestamp folder (default)
        self.maintain_timestamp_transactions = maintain_timestamp_transactions
        
        if reset:
            self.log_entry.remove_log()
        self.result = Result(
            table=self.table_name,
            process_start_time=datetime.now(),
            process_start_watermark=self.low_watermark,
            process_start_version=self.log_entry.delta_log.version() if self.log_entry.delta_log else 0,
            manifest_records=self.entry["totalProcessedRecordsCount"],
            manifest_watermark=self.entry["lastSuccessfulWriteTimestamp"],
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=[],
            warnings=[]
        )
    
    def _update_progress_safe(self, folders_processed: int, total_folders: int):
        """Safely update progress using unified interface."""
        try:
            if self.progress_manager:
                if self.parallel:
                    # Ray actor call - use fire-and-forget for better performance
                    self.progress_manager.update_progress.remote(
                        self.table_name, folders_processed, total_folders, ""
                    )
                else:
                    # Direct progress manager call
                    self.progress_manager.update_progress(
                        self.table_name, folders_processed, total_folders, ""
                    )
        except Exception as e:
            # Don't let progress errors break processing
            print(f"Progress update error: {e}")
    
    def _complete_table_safe(self, error_message=None):
        """Safely complete table progress using unified interface."""
        try:
            if self.progress_manager:
                if self.parallel:
                    # Ray actor call - use fire-and-forget for completion too
                    self.progress_manager.complete_table.remote(self.table_name, error_message)
                else:
                    # Direct progress manager call
                    self.progress_manager.complete_table(self.table_name, error_message)
        except Exception as e:
            # Don't let progress errors break processing
            print(f"Progress complete error: {e}")

    def _log_error(self, error_message: str) -> None:
        """Log an error message and add it to the result's errors list."""
        L.error(error_message)
        self.result.add_error(error_message)
    
    def _log_warning(self, warning_message: str) -> None:
        """Log a warning message and add it to the result's warnings list."""
        L.warning(warning_message)
        self.result.add_warning(warning_message)


    def _schema_finder(self, file_list: list[dict[str, str | int]]) -> bool:
        """Attempts to find and cache the schema from a list of files.
        
        Args:
            file_list: List of dictionaries containing file metadata with keys:
                      'relative_path', 'path', 'last_modified', 'size'
        
        Returns:
            bool: True if schema was successfully found and cached, False otherwise
        """
        self.cached_schema = None
        file_list.sort(key=lambda x: x["size"])
        L.debug(f"  Found {len(file_list)} potential schema files.")

        for schema_file_info in file_list:
            file_path_to_try = schema_file_info["relative_path"]
            L.debug(f"Attempting to read schema from: {file_path_to_try}")
            try:
                self.cached_schema = self._get_parquet_schema(file_path_to_try)
                L.debug(
                    f"Successfully determined schema for '{self.table_name}' using file: {file_path_to_try}"
                )
                return True
            except Exception as e:
                L.warning(f"    Failed to read schema from {file_path_to_try}: {e}")
        return False

    def _get_dir_list(self, directory: str) -> tuple[bool, list[str]]:
        """
        Returns (is_part_way, directory_list).
        is_part_way = True if part of the schema has been processed (i.e., only some dirs meet watermark criteria),
        is_part_way = False if all or none meet criteria.
        directory_list is always sorted.
        """
        # Get all directory paths within the given directory, sorted
        
        listed_paths = self.manifest.fs.get_file_info(directory)
        full_list = sorted(
            path.path
            for path in listed_paths
            if path.type == FileType.Directory
        )

        # Filter for directories with base_name greater than the low watermark
        part_list = sorted(
            path.path 
            for path in listed_paths 
            if path.type == FileType.Directory
            and int(path.base_name) > self.low_watermark
        )

        if not part_list:
            L.debug(
                f"No directories found in {directory} greater than low watermark {self.low_watermark}"
            )
            return True, []

        if 0 < len(part_list) < len(full_list):
            L.debug(
                f"Filtered directories in {directory} to those with timestamps greater than low watermark {self.low_watermark}"
            )
            return True, part_list

        # All present (or none filtered out)
        L.debug(
            f"All directories in {directory} are greater than low watermark {self.low_watermark} (or none filtered out)"
        )
        return False, full_list


    def _get_parquet_list(self, directory: str) -> list[dict]:
        """Returns a list of parquet files with metadata from the given directory.

        Three behaviors, mutually exclusive:

        1. **Legacy (no target_storage, no staging)**: ``log_action_path`` on
           the default :class:`AWSStorage` returns absolute ``s3a://...``
           paths -- bit-identical to the upstream behavior.
        2. **UC mode without staging**: ``target_storage`` is a
           :class:`UCStorage`; ``log_action_path`` emits paths relative to the
           target table root. Source files must already be enclosed by the
           target root (i.e. customer pre-staged or self-hosted CDA layout).
        3. **UC mode with staging**: each source parquet is copied into the
           target table root via :class:`StagingExecutor`, and the AddAction
           records a path relative to that staged location.

        The dispatch happens via polymorphism on the target storage; this
        method does not know about specific storage subclasses.
        """
        target_fs = self.log_entry.fs
        table_root = self.log_entry.log_uri
        source_files = [
            file
            for file in self.manifest.fs.get_file_info(directory)
            if file.type == FileType.File and file.path.endswith(".parquet")
        ]

        if self.staging_executor is not None:
            return [
                {
                    "relative_path": file.path,
                    "path": target_fs.log_action_path(
                        self.staging_executor.stage_file(file.path, file.size),
                        table_root,
                    ),
                    "last_modified": file.mtime_ns,
                    "size": file.size,
                }
                for file in source_files
            ]

        return [
            {
                "relative_path": file.path,
                "path": target_fs.log_action_path(file.path, table_root),
                "last_modified": file.mtime_ns,
                "size": file.size,
            }
            for file in source_files
        ]

    def _get_parquet_schema(self, path: str) -> pa.schema:
        """Reads and returns the schema from a parquet file.
        
        Args:
            path: Path to the parquet file
            
        Returns:
            pa.schema: PyArrow schema object
            
        Raises:
            Exception: If the parquet file cannot be read or is invalid
        """
        try:
            table = self.manifest.fs.read_parquet(path)
            if table is None or table.schema is None:
                raise ValueError(f"Invalid parquet file at {path}: no schema found")
            return table.schema
        except Exception as e:
            L.error(f"Failed to read parquet schema from {path}: {str(e)}")
            raise

    def _process_schema_history(self, item: dict) -> None:
        """Processes a single schema history item."""
        folder = item["uri"]
        schema_timestamp = item["schema_timestamp"]
        
        try:
            partial,timestamp_folders = self._get_dir_list(folder)
        except Exception as e:
            L.warning(f"Failed to list contents of {folder}: {e}")
            raise
        if partial:
            L.debug(f"  Found partial schema history in {folder}, processing only new timestamps.")

        # Filter out invalid folders before creating progress bar
        valid_timestamp_folders = []
        for folder in timestamp_folders:
            try:
                int(folder.split("/")[-1])  # Test if numeric
                valid_timestamp_folders.append(folder)
            except ValueError:
                L.warning(f"Skipping non-numeric timestamp folder: {folder}")
        
        if self.progress_manager:
            # Start table processing with actual total (table should already be registered)
            try:
                if self.parallel:
                    # Ray actor call - fire-and-forget for start_table
                    self.progress_manager.start_table.remote(
                        self.table_name, 
                        len(valid_timestamp_folders)
                    )
                else:
                    # Direct progress manager call
                    self.progress_manager.start_table(
                        self.table_name, 
                        len(valid_timestamp_folders)
                    )
            except Exception as e:
                # Don't let progress tracking errors break processing
                print(f"Progress tracking error: {e}")
        
        try:
            if not self.maintain_timestamp_transactions:
                # Batch mode: collect all files and commit once with latest timestamp as watermark
                self._process_schema_history_batched(valid_timestamp_folders, schema_timestamp, partial, folder)
            else:
                # Original mode: one commit per timestamp folder
                self._process_schema_history_individual(valid_timestamp_folders, schema_timestamp, partial, folder)
        finally:
            # Complete progress tracking (Ray-compatible)
            self._complete_table_safe()

    def _process_schema_history_batched(self, valid_timestamp_folders: list, schema_timestamp: int, partial: bool, folder: str) -> None:
        """Process schema history with batched commits - all add actions in one commit per schema."""
        all_parquet_files = []
        latest_timestamp = 0
        schema_found = False
        
        # Collect all files from all timestamp folders
        for i, timestamp_folder in enumerate(valid_timestamp_folders):
            timestamp_value = int(timestamp_folder.split("/")[-1])
            L.debug(f"  Collecting files from timestamp path: {timestamp_folder}")
            
            try:
                files_in_timestamp = self._get_parquet_list(timestamp_folder)
            except Exception as e:
                error_message = f"Failed to list contents of {timestamp_folder}: {e}"
                L.error(f"  {error_message}")
                self._complete_table_safe(error_message)
                raise

            if not schema_found and files_in_timestamp:
                # Find schema from first available files
                if self._schema_finder(files_in_timestamp):
                    schema_found = True
                    self.result.add_schema_timestamp(schema_timestamp)
                    L.debug(f"Schema found for batched processing of '{self.table_name}'")

            # Add files to batch collection
            all_parquet_files.extend(files_in_timestamp)
            
            # Track latest timestamp and watermarks for result tracking
            if timestamp_value > latest_timestamp:
                latest_timestamp = timestamp_value
            self.result.add_watermark(timestamp_value)
            
            # Update progress (Ray-compatible)
            self._update_progress_safe(i + 1, len(valid_timestamp_folders))

        if not schema_found:
            # A drained fingerprint (no timestamp folders remain above the low watermark) is a normal,
            # expected state after a schema change: every folder for this schema-version hash has already
            # been ingested, so there is simply nothing new to read here. In that case we must NOT raise -
            # raising abandons the whole table loop in process_batch() before the NEWER fingerprint(s) in
            # schema_history_list are ever processed, which silently stalls the table while the job stays
            # green. Instead, log a warning and return so process_batch() advances to the next fingerprint.
            # Only raise the genuine failure: folders DID exist for this fingerprint but none yielded a
            # readable parquet schema.
            if not valid_timestamp_folders:
                warning_message = (
                    f"No unprocessed timestamp folders for '{self.table_name} {folder}' "
                    f"(drained fingerprint above low watermark {self.low_watermark}); "
                    f"skipping to next fingerprint."
                )
                self._log_warning(warning_message)
                self._complete_table_safe()
                return
            error_message = f"Schema not found for '{self.table_name} {folder}'"
            self._log_error(error_message)
            self._complete_table_safe(error_message)
            raise ValueError(error_message)

        if all_parquet_files:
            # Check if this is a schema change and not partial
            if not partial and self.log_entry.table_exists():
                # Add schema metadata change operation first
                self.log_entry.add_schema_metadata_change(self.cached_schema)
            
            # Single commit with all files and latest timestamp as watermark
            L.debug(f"Committing {len(all_parquet_files)} files in batch for '{self.table_name}' with watermark {latest_timestamp}")
            self.log_entry.add_transaction(
                parquets=all_parquet_files,
                schema=self.cached_schema,
                watermark=latest_timestamp,  # Use latest timestamp as watermark
                schema_timestamp=schema_timestamp,
                mode="append",
            )
            self.result.update(
                process_finish_watermark=latest_timestamp,
                process_finish_version=self.log_entry.delta_log.version() if self.log_entry.delta_log else 0
            )

    def _process_schema_history_individual(self, valid_timestamp_folders: list, schema_timestamp: int, partial: bool, folder: str) -> None:
        """Process schema history with individual commits - one commit per timestamp folder (original behavior)."""
        first_folder_for_schema = True
        
        for i, timestamp_folder in enumerate(valid_timestamp_folders):
            timestamp_value = int(timestamp_folder.split("/")[-1])
            L.debug(f"  Checking timestamp path: {timestamp_folder}")
            try:
                files_in_timestamp = self._get_parquet_list(timestamp_folder)
            except Exception as e:
                error_message = f"Failed to list contents of {timestamp_folder}: {e}"
                L.error(f"  {error_message}")
                # Complete with error - this is a critical failure
                self._complete_table_safe(error_message)
                raise

            if first_folder_for_schema:
                self.result.add_schema_timestamp(schema_timestamp)
                if self._schema_finder(files_in_timestamp):
                    first_folder_for_schema = False
                    
                    # Check if this is a schema change and not partial
                    if not partial and self.log_entry.table_exists():
                        # Add schema metadata change operation first
                        self.log_entry.add_schema_metadata_change(self.cached_schema)
                    
                    self.log_entry.add_transaction(
                        parquets=files_in_timestamp,
                        schema=self.cached_schema,
                        watermark=timestamp_value,
                        schema_timestamp=schema_timestamp,
                        mode="append",
                    )
                else:
                    error_message = f"Schema not found for '{self.table_name} {folder}'"
                    self._log_error(error_message)
                    # Update progress with error (Ray-compatible)
                    self._complete_table_safe(error_message)
                    # Don't return here - let the caller handle the error
                    raise ValueError(error_message)
            else:
                self.log_entry.add_transaction(
                    parquets=files_in_timestamp,
                    schema=self.cached_schema,
                    watermark=timestamp_value,
                    schema_timestamp=schema_timestamp,
                    mode="append",
                )
            self.result.add_watermark(timestamp_value)
            self.result.update(
                process_finish_watermark=timestamp_value,
                process_finish_version=self.log_entry.delta_log.version() if self.log_entry.delta_log else 0
            )
            # Update progress (Ray-compatible)
            self._update_progress_safe(i + 1, len(valid_timestamp_folders))


    def process_batch(self) -> Result:
        """Processes the batch for the current table."""
        

        if self.low_watermark == -1:
            error_message = f"Skipping batch for {self.table_name} as the low watermark is -1, indicating somethings gone wrong."
            self._log_error(error_message)
            self.result.update(
                process_finish_time=datetime.now(),
                process_finish_watermark=self.low_watermark
            )
            self._complete_table_safe(error_message=error_message)
            return self.result
            
        if int(self.entry["lastSuccessfulWriteTimestamp"]) <= self.low_watermark:
            error_message = f"Skipping batch for {self.table_name} as it matches or is older than the low watermark."
            self._log_warning(error_message)
            self.result.update(
                process_finish_time=datetime.now(),
                process_finish_watermark=self.low_watermark
            )
            self._complete_table_safe()
            return self.result
        
        L.debug(f"Processing batch for {self.table_name}")
        filepath = self.entry["dataFilesPath"].replace('s3://', '', 1)
        schema_history = self.entry["schemaHistory"]
        
        if not filepath or not schema_history:
            error_message = f"Missing 'dataFilesPath' or 'schemaHistory' for entry {self.table_name}"
            self._log_error(error_message)
            self.result.update(
                process_finish_time=datetime.now(),
                process_finish_watermark=self.low_watermark
            )
            self._complete_table_safe(error_message=error_message)
            return self.result

        # Uses sorted to ensure the schema history is processed in order
        # makes sure to only process schema history entries that are greater than the low watermark
        # schema timestamp cannot be used here as its lower that the timestamp folders inside, its the orginal schema change time
        # need to sort the schemas by the value not the key and take higher or equal than the self.watermark_schema_timestamp
        sorted_schema_history = sorted(
            (item for item in schema_history.items() if int(item[1]) >= self.watermark_schema_timestamp),
            key=lambda kv: int(kv[1])
        )

        schema_history_list= [
            {
                "key": key,
                "uri": f"{filepath.rstrip('/')}/{key}/",
                "schema_timestamp": int(value)
            }
            for key, value in sorted_schema_history
        ]
        
        try:
            for item in schema_history_list:
                L.debug(f"Processing URI: {item['uri']} for entry {self.table_name}")
                self._process_schema_history(item)
            self.result.update(process_finish_time=datetime.now())
            return self.result
        except Exception as e:
            error_message = f"Error processing schema history for {self.table_name}: {e} processing abandoned"
            self._log_error(error_message)
            self.result.update(process_finish_time=datetime.now())
            return self.result

