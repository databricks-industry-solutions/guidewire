from abc import ABC, abstractmethod
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pyarrow.json as pj
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


