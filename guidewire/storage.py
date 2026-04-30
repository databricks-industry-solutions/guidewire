from abc import ABC, abstractmethod
from datetime import datetime, timezone
from threading import Lock
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pyarrow.json as pj
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableOperation
from guidewire.logging import logger as L
import os
from typing import Literal, List, Dict, Any, Optional


class BaseStorage(ABC):
    """Abstract base class for cloud storage operations using PyArrow filesystem interface.
    
    This abstract class provides common storage operations while requiring
    subclasses to implement cloud-specific initialization and configuration.
    """
    
    def __init__(self):
        """Base initialization. Subclasses must set self.filesystem and self._storage_options."""
        self.filesystem = None
        self._storage_options = None
    
    @property
    @abstractmethod
    def storage_options(self) -> Dict[str, str]:
        """Get storage options dictionary for delta-rs integration.
        
        Returns:
            Dictionary of storage options specific to the cloud provider
        """
        pass
    
    def read_parquet(self, path: str) -> pa.Table:
        """Read a Parquet file from the storage.
        
        Args:
            path: Path to the Parquet file
            
        Returns:
            PyArrow Table containing the data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        try:
            return pq.read_table(source=path, filesystem=self.filesystem)
        except Exception as e:
            L.warning(f"Failed to read parquet file {path}: {str(e)}")
            raise
    
    def write_parquet(self, path: str, table: pa.Table) -> None:
        """Write a PyArrow Table to Parquet format in storage.
        
        Args:
            path: Destination path for the Parquet file
            table: PyArrow Table to write
            
        Raises:
            IOError: If writing fails
        """
        try:
            pq.write_table(filesystem=self.filesystem, table=table, where=path)
        except Exception as e:
            L.error(f"Failed to write parquet file {path}: {str(e)}")
            raise
    
    def read_json(self, path: str) -> Dict[str, Any]:
        """Read a JSON file from storage.
        
        Args:
            path: Path to the JSON file
            
        Returns:
            Dictionary containing the JSON data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the JSON is invalid
        """
        try:
            return pj.read_json(self.filesystem.open_input_stream(path)).to_pydict()
        except Exception as e:
            L.error(f"Failed to read JSON file {path}: {str(e)}")
            raise

    def list_files(self, path: str) -> List[str]:
        """List files in a directory.
        
        Args:
            path: Directory path to list
            
        Returns:
            List of file paths
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        try:
            file_selector = pa_fs.FileSelector(path, recursive=False)
            file_info_list = self.filesystem.get_file_info(file_selector)
            return [info.path for info in file_info_list]
        except Exception as e:
            L.error(f"Failed to list files in {path}: {str(e)}")
            raise
    
    def delete_file(self, path: str) -> bool:
        """Delete a file from storage.
        
        Args:
            path: Path to the file to delete
            
        Returns:
            True if deletion was successful
            
        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        try:
            return self.filesystem.delete_file(path)
        except Exception as e:
            L.error(f"Failed to delete file {path}: {str(e)}")
            raise
    
    def delete_dir(self, path: str) -> bool:
        """Delete a directory from storage.
        
        Args:
            path: Path to the directory to delete
            
        Returns:
            True if deletion was successful
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        try:
            return self.filesystem.delete_dir(path)
        except Exception as e:
            L.error(f"Failed to delete directory {path}: {str(e)}")
            raise
    
    def get_file_info(self, path: str) -> List[pa_fs.FileInfo]:
        """Get information about files in a directory.

        Args:
            path: Directory path to get info for

        Returns:
            List of FileInfo objects

        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        try:
            selector = pa.fs.FileSelector(path)
            return self.filesystem.get_file_info(selector)
        except Exception as e:
            L.error(f"Failed to get file info for {path}: {str(e)}")
            raise

    def log_action_path(self, file_path: str, table_root: str) -> str:
        """Format a file path for inclusion in a Delta AddAction.

        Default behavior preserves the legacy upstream output: an absolute
        ``s3a://`` path. Subclasses (notably ``UCStorage``) may override this
        to emit paths relative to the table root, which is required for the
        AddAction to stay within a Unity Catalog external location.

        Args:
            file_path: Source parquet path as returned by the manifest filesystem
                (e.g. ``"bucket/schema_hash/timestamp/file.parquet"``).
            table_root: Target Delta table URI (e.g. ``"s3://bucket/table/"``).

        Returns:
            Path string to record in the AddAction.
        """
        return f"s3a://{file_path}"


class AzureStorage(BaseStorage):
    """Azure Blob Storage implementation using PyArrow filesystem interface.
    
    Azure Environment Variables:
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
    
    def __init__(self):
        """Initialize Azure storage client.
        """
        super().__init__()
        
        account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        tenant_id = os.environ.get("AZURE_TENANT_ID")
        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        blob_storage_authority = os.environ.get("AZURE_BLOB_STORAGE_AUTHORITY")
        blob_storage_scheme = os.environ.get("AZURE_BLOB_STORAGE_SCHEME", "https")
        dfs_storage_authority = os.environ.get("AZURE_DFS_STORAGE_AUTHORITY")
        dfs_storage_scheme = os.environ.get("AZURE_DFS_STORAGE_SCHEME", "https")
        
        if not account_name:
            raise KeyError("AZURE_STORAGE_ACCOUNT_NAME must be set")
        
        # Build filesystem arguments
        fs_args = {"account_name": account_name}
        if blob_storage_authority:
            L.debug(f"Using custom blob storage authority: {blob_storage_authority}")
            fs_args["blob_storage_authority"] = blob_storage_authority
            fs_args["blob_storage_scheme"] = blob_storage_scheme
            if not dfs_storage_authority:
                dfs_storage_authority = blob_storage_authority
                dfs_storage_scheme = blob_storage_scheme
                
        if dfs_storage_authority:
            L.debug(f"Using custom DFS storage authority: {dfs_storage_authority}")
            fs_args["dfs_storage_authority"] = dfs_storage_authority
            fs_args["dfs_storage_scheme"] = dfs_storage_scheme
        
        if client_id and client_secret and tenant_id:
            L.debug("Using Client ID and Client Secret for Azure storage")
            self._storage_options = {
                "account_name": account_name,
                "tenant_id": tenant_id,
                "client_id": client_id,
                "client_secret": client_secret,
            }
            # Add endpoint override info for delta-rs
            if blob_storage_authority:
                endpoint_url = f"{blob_storage_scheme}://{blob_storage_authority}"
                self._storage_options["blob_endpoint"] = endpoint_url
                self._storage_options["azure_storage_endpoint"] = endpoint_url
                self._storage_options["AZURE_STORAGE_ENDPOINT"] = endpoint_url
                if blob_storage_scheme == "http":
                    self._storage_options["azure_storage_use_emulator"] = "true"
                    self._storage_options["AZURE_STORAGE_USE_EMULATOR"] = "true"
            if dfs_storage_authority:
                dfs_endpoint_url = f"{dfs_storage_scheme}://{dfs_storage_authority}"
                self._storage_options["dfs_endpoint"] = dfs_endpoint_url
            self.filesystem = pa_fs.AzureFileSystem(**fs_args)
        elif account_key:
            L.debug("Using Account Key for Azure storage")
            self._storage_options = {
                "account_name": account_name,
                "account_key": account_key,
            }
            # Add endpoint override info for delta-rs
            if blob_storage_authority:
                endpoint_url = f"{blob_storage_scheme}://{blob_storage_authority}"
                self._storage_options["blob_endpoint"] = endpoint_url
                self._storage_options["azure_storage_endpoint"] = endpoint_url
                self._storage_options["AZURE_STORAGE_ENDPOINT"] = endpoint_url
                if blob_storage_scheme == "http":
                    self._storage_options["azure_storage_use_emulator"] = "true"
                    self._storage_options["AZURE_STORAGE_USE_EMULATOR"] = "true"
            if dfs_storage_authority:
                dfs_endpoint_url = f"{dfs_storage_scheme}://{dfs_storage_authority}"
                self._storage_options["dfs_endpoint"] = dfs_endpoint_url
            fs_args["account_key"] = account_key
            self.filesystem = pa_fs.AzureFileSystem(**fs_args)
        else:
            L.error("Azure storage credentials must be set")
            raise KeyError("Azure storage credentials must be set")
    
    @property
    def storage_options(self) -> Dict[str, str]:
        """Get storage options dictionary for delta-rs integration."""
        return self._storage_options


class AWSStorage(BaseStorage):
    """AWS S3 storage implementation using PyArrow filesystem interface.
    
    AWS Environment Variables:
        - AWS_REGION (required): AWS region
        - AWS_ACCESS_KEY_ID (required): AWS access key ID
        - AWS_SECRET_ACCESS_KEY (required): AWS secret access key
        - AWS_ENDPOINT_URL (optional): Custom S3 endpoint override
        
        With prefix support (e.g., prefix="SOURCE"):
        - AWS_SOURCE_REGION (required): AWS region
        - AWS_SOURCE_ACCESS_KEY_ID (required): AWS access key ID
        - AWS_SOURCE_SECRET_ACCESS_KEY (required): AWS secret access key
        - AWS_SOURCE_ENDPOINT_URL (optional): Custom S3 endpoint override
    """
    
    def __init__(self, prefix: Literal["SOURCE", "TARGET"] = None):
        """Initialize AWS S3 storage client.
        
        Args:
            prefix: Optional prefix for environment variables (e.g., "SOURCE" or "TARGET")
            
        Raises:
            KeyError: If required environment variables are missing
        """
        super().__init__()
        
        # Build prefixed environment variable names with fallbacks
        prefix_upper = prefix.upper() + "_" if prefix else ""
        
        # Get AWS credentials with prefix support and fallback
        region = (os.environ.get(f"AWS_{prefix_upper}REGION") or 
                 os.environ.get("AWS_REGION"))
        access_key = (os.environ.get(f"AWS_{prefix_upper}ACCESS_KEY_ID") or 
                     os.environ.get("AWS_ACCESS_KEY_ID"))
        secret_key = (os.environ.get(f"AWS_{prefix_upper}SECRET_ACCESS_KEY") or 
                     os.environ.get("AWS_SECRET_ACCESS_KEY"))
        endpoint = (os.environ.get(f"AWS_{prefix_upper}ENDPOINT_URL") or 
                   os.environ.get("AWS_ENDPOINT_URL"))
        
        # Build error message with prefix awareness
        required_vars = []
        if not region:
            required_vars.append(f"AWS_{prefix_upper}REGION" if prefix else "AWS_REGION")
        if not access_key:
            required_vars.append(f"AWS_{prefix_upper}ACCESS_KEY_ID" if prefix else "AWS_ACCESS_KEY_ID")
        if not secret_key:
            required_vars.append(f"AWS_{prefix_upper}SECRET_ACCESS_KEY" if prefix else "AWS_SECRET_ACCESS_KEY")
            
        if required_vars:
            fallback_msg = "" if not prefix else f" (or fallback variables without {prefix_upper} prefix)"
            raise KeyError(f"Required AWS environment variables must be set: {', '.join(required_vars)}{fallback_msg}")
        
        # Set storage options for delta-rs
        self._storage_options = {
            "region": region,
            "access_key_id": access_key,
            "secret_access_key": secret_key,
        }
        
        if endpoint:
            prefix_desc = f" ({prefix} S3)" if prefix else ""
            L.debug(f"Using custom endpoint {endpoint} for AWS S3{prefix_desc}")
            self._storage_options["endpoint"] = endpoint
            
            # Add LocalStack/S3-compatible service specific options for delta-rs
            if "localhost" in endpoint or "127.0.0.1" in endpoint:
                L.debug("Adding LocalStack compatibility options for delta-rs")
                self._storage_options["allow_http"] = "true"
                self._storage_options["force_path_style"] = "true" 
                self._storage_options["allow_invalid_certificates"] = "true"
            
            self.filesystem = pa_fs.S3FileSystem(
                region=region,
                access_key=access_key,
                secret_key=secret_key,
                endpoint_override=endpoint,
            )  
        else:
            prefix_desc = f" ({prefix} S3)" if prefix else ""
            L.debug(f"Using default AWS S3 endpoint{prefix_desc}")
            self.filesystem = pa_fs.S3FileSystem(
                region=region,
                access_key=access_key,
                secret_key=secret_key,
            )

    @property
    def storage_options(self) -> Dict[str, str]:
        """Get storage options dictionary for delta-rs integration."""
        return self._storage_options


class UCStorage(BaseStorage):
    """Unity Catalog credential-vending storage for AWS S3.

    Vends short-lived AWS credentials via the Databricks SDK's
    ``temporary_table_credentials`` API and refreshes them proactively before
    expiry. Unlike :class:`AWSStorage` (which reads static keys from the
    environment), this class produces vended credentials whose use is recorded
    in ``system.access.audit`` and attributed to the UC principal.

    Two behaviors distinguish this class:

    1. ``storage_options`` includes ``session_token`` -- required for delta-rs
       to authenticate temporary credentials against S3.
    2. ``log_action_path`` returns a path *relative to the table root*. UC
       governance can only enforce policy on AddActions whose paths resolve
       inside a UC external location; an absolute ``s3a://other-bucket/...``
       AddAction would be resolved by the engine using whatever IAM identity
       is present, bypassing UC entirely.

    Args:
        uc_table_id: Unity Catalog table ID for which to vend credentials.
            Use the table's UC metastore-scoped UUID.
        operation: ``"READ"`` or ``"READ_WRITE"``. Use ``"READ_WRITE"`` for
            the target Delta log writer.
        region: AWS region to bind the PyArrow ``S3FileSystem`` to. Falls back
            to the ``AWS_REGION`` environment variable if not provided.
    """

    REFRESH_BUFFER_SECONDS = 300  # refresh 5 min before expiry

    def __init__(
        self,
        uc_table_id: str,
        operation: Literal["READ", "READ_WRITE"] = "READ_WRITE",
        region: Optional[str] = None,
    ):
        super().__init__()
        if not uc_table_id:
            raise KeyError("uc_table_id must be a non-empty string")

        self._wc = WorkspaceClient()
        self._uc_table_id = uc_table_id
        self._operation = TableOperation[operation]
        self._region = region or os.environ.get("AWS_REGION", "")
        self._lock = Lock()
        self._expires_at = None
        self._refresh_credentials()

    def _refresh_credentials(self) -> None:
        with self._lock:
            resp = self._wc.temporary_table_credentials.generate_temporary_table_credentials(
                table_id=self._uc_table_id,
                operation=self._operation,
            )
            aws = resp.aws_temp_credentials
            if aws is None:
                raise RuntimeError(
                    f"UC did not return AWS credentials for table_id={self._uc_table_id}; "
                    f"the credential may be Azure or GCP-backed."
                )
            self._storage_options = {
                "region": self._region,
                "access_key_id": aws.access_key_id,
                "secret_access_key": aws.secret_access_key,
                "session_token": aws.session_token,
            }
            self.filesystem = pa_fs.S3FileSystem(
                region=self._region or None,
                access_key=aws.access_key_id,
                secret_key=aws.secret_access_key,
                session_token=aws.session_token,
            )
            # expiration_time is epoch milliseconds per the SDK contract.
            self._expires_at = resp.expiration_time

    def _ensure_fresh(self) -> None:
        if self._expires_at is None:
            return
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if (self._expires_at - now_ms) / 1000 < self.REFRESH_BUFFER_SECONDS:
            self._refresh_credentials()

    @property
    def storage_options(self) -> Dict[str, str]:
        """Get storage options dictionary for delta-rs integration.

        Refreshes credentials proactively if they're within
        :attr:`REFRESH_BUFFER_SECONDS` of expiry.
        """
        self._ensure_fresh()
        return self._storage_options

    def log_action_path(self, file_path: str, table_root: str) -> str:
        """Emit a path relative to the table root, or raise.

        UC governance requires AddAction paths to resolve inside a UC external
        location. The simplest and most robust enforcement is to require the
        source files to be enclosed by the target table root.

        Raises:
            ValueError: If ``file_path`` is not enclosed by ``table_root``
                (cross-bucket layout). Configure source and target prefixes
                under the same UC external location to fix.
        """
        prefix = self._normalize_table_root(table_root)
        if not file_path.startswith(prefix):
            raise ValueError(
                "UC governance requires source files to be enclosed by the target table root. "
                f"file={file_path!r} table_root={table_root!r}. "
                "Configure source and target prefixes under the same UC external location."
            )
        return file_path[len(prefix):].lstrip("/")

    @staticmethod
    def _normalize_table_root(table_root: str) -> str:
        """Strip the URI scheme from ``table_root`` so it can be compared against
        the bucket-relative paths emitted by PyArrow's S3 filesystem."""
        for scheme in ("s3://", "s3a://", "abfss://"):
            if table_root.startswith(scheme):
                return table_root[len(scheme):]
        return table_root

