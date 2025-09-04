import os
import pyarrow as pa
from pyarrow.fs import FileType
from guidewire.logging import logger as L
from guidewire.delta_log import AzureDeltaLog, AWSDeltaLog
from guidewire.manifest import Manifest
from typing import Optional
from guidewire.results import Result
from datetime import datetime

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
            )
        else:
            raise ValueError(f"Invalid target_cloud: {target_cloud}. Must be 'azure' or 'aws'")
        self.watermark_info = self.log_entry._get_watermark_from_log()
        self.low_watermark = 0 if reset else self.watermark_info["watermark"]
        self.watermark_schema_timestamp = 0 if reset else self.watermark_info["schema_timestamp"]
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
        if os.environ.get("SHOW_TABLE_PROGRESS") == "0":
            self.show_progress = False
            self._progress_bar = None
        else:
            self.show_progress = True
            self._progress_bar = self._get_progress_bar_class()

    def _log_error(self, error_message: str) -> None:
        """Log an error message and add it to the result's errors list."""
        L.error(error_message)
        self.result.add_error(error_message)
    
    def _log_warning(self, warning_message: str) -> None:
        """Log a warning message and add it to the result's warnings list."""
        L.warning(warning_message)
        self.result.add_warning(warning_message)

    def _get_progress_bar_class(self):
        """Determine which tqdm class to use based on Ray availability and initialization."""
        try:
            import ray
            if ray.is_initialized():
                from ray.experimental.tqdm_ray import tqdm
                return tqdm
            else:
                from tqdm import tqdm
                return tqdm
        except (ImportError, AttributeError):
            from tqdm import tqdm
            return tqdm

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
        """Returns a list of parquet files with metadata from the given directory."""
        return [
            {
                "relative_path": file.path,
                "path": f"s3://{file.path}",
                "last_modified": file.mtime_ns,
                "size": file.size,
            }
            for file in self.manifest.fs.get_file_info(directory)
            if file.type == FileType.File and file.path.endswith(".parquet")
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



        first_folder_for_schema = True
        
# Filter out invalid folders before creating progress bar
        valid_timestamp_folders = []
        for folder in timestamp_folders:
            try:
                int(folder.split("/")[-1])  # Test if numeric
                valid_timestamp_folders.append(folder)
            except ValueError:
                L.warning(f"Skipping non-numeric timestamp folder: {folder}")

        # Initialize progress bar variable if there are more than 50 folders. Lower than this can kill the UI
        pbar = None
        if len(valid_timestamp_folders) > 50 and self.show_progress and self._progress_bar:
            # Create progress bar outside the loop
            pbar = self._progress_bar(total=len(valid_timestamp_folders),
                                    desc=f"Table: {self.table_name} Schema: {schema_timestamp}",
                                    unit="folder")
        
        try:
            for timestamp_folder in valid_timestamp_folders:
                timestamp_value = int(timestamp_folder.split("/")[-1])
                L.debug(f"  Checking timestamp path: {timestamp_folder}")
                try:
                    files_in_timestamp = self._get_parquet_list(timestamp_folder)
                except Exception as e:
                    L.error(f"  Failed to list contents of {timestamp_folder}: {e}")
                    if pbar:
                        pbar.update(1)
                    continue

                if first_folder_for_schema:
                    self.result.add_schema_timestamp(schema_timestamp)
                    if self._schema_finder(files_in_timestamp):
                        first_folder_for_schema = False
                        self.log_entry.add_transaction(
                            parquets=files_in_timestamp,
                            schema=self.cached_schema,
                            watermark=timestamp_value,
                            schema_timestamp=schema_timestamp,
                            mode="overwrite" if not partial else "append",
                        )
                    else:
                        error_message = f"Schema not found for '{self.table_name} {folder}'"
                        self._log_error(error_message)
                        if pbar:
                            pbar.close()
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
                # Increment the progress bar
                if pbar:
                    pbar.update(1)
        finally:
            # Close the progress bar at the end
            if pbar:
                pbar.close()


    def process_batch(self) -> Result:
        """Processes the batch for the current table."""
        

        if self.low_watermark == -1:
            error_message = f"Skipping batch for {self.table_name} as the low watermark is -1, indicating somethings gone wrong."
            self._log_error(error_message)
            self.result.update(
                process_finish_time=datetime.now(),
                process_finish_watermark=self.low_watermark
            )
            return self.result
            
        if int(self.entry["lastSuccessfulWriteTimestamp"]) <= self.low_watermark:
            error_message = f"Skipping batch for {self.table_name} as it matches or is older than the low watermark."
            self._log_warning(error_message)
            self.result.update(
                process_finish_time=datetime.now(),
                process_finish_watermark=self.low_watermark
            )
            return self.result
        
        L.debug(f"Processing batch for {self.table_name}")
        filepath = self.entry["dataFilesPath"].lstrip("s3://")
        schema_history = self.entry["schemaHistory"]
        
        if not filepath or not schema_history:
            error_message = f"Missing 'dataFilesPath' or 'schemaHistory' for entry {self.table_name}"
            self._log_error(error_message)
            self.result.update(
                process_finish_time=datetime.now(),
                process_finish_watermark=self.low_watermark
            )
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
            #self.log_entry.write_checkpoint(int(self.entry["lastSuccessfulWriteTimestamp"]))
        except Exception as e:
            error_message = f"Error processing schema history for {self.table_name}: {e} processing abandoned"
            self._log_error(error_message)
            self.result.update(process_finish_time=datetime.now())
            return self.result

