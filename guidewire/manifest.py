from guidewire.logging import logger as L
from guidewire.storage import AWSStorage
import os
from typing import List, Optional, Dict, Any

class Manifest:
    """A class to manage and read manifest files for database tables.
    
    The manifest file is expected to be a JSON file containing table metadata.
    Each table entry should be a list of dictionaries containing the table's metadata.
    """
    
    def __init__(self, location: str, table_names: Optional[List[str]] = None):
        """Initialize the Manifest class.
        
        Args:
            location: The path to the manifest file directory
            table_names: List of table names to load from the manifest
            
        Raises:
            ValueError: If location or table_names is empty
        """
        if not location:
            raise ValueError("Location cannot be empty.")
        
        self.location = location
        self.table_names = table_names
        # Use SOURCE prefix for manifest/data reading S3 credentials
        self.fs = AWSStorage(prefix="SOURCE")
        self.manifest: Optional[Dict[str, List[Dict[str, Any]]]] = None
        self._initialize()

    def _initialize(self) -> None:
        """Initialize the manifest by reading and validating the manifest file.
        
        Raises:
            FileNotFoundError: If the manifest file doesn't exist
            ValueError: If the manifest file is invalid or missing required tables
            Exception: For other unexpected errors
        """
        manifest_path = f"{self.location}/manifest.json"
        try:
            L.info(f"Attempting to read manifest file from {manifest_path}")
            table = self.fs.read_json(manifest_path)
            
            if not isinstance(table, dict):
                raise ValueError("Manifest file must contain a dictionary")
                
            # Filter table dictionary to keys in table_names
            if self.table_names:
                self.manifest = {k: v for k, v in table.items() if k in self.table_names}
            else:
                self.manifest = table
            L.info(
                f"Successfully loaded manifest for tables: {self.table_names} from {self.location}"
            )
        except FileNotFoundError:
            L.error(f"Manifest file not found at {manifest_path}")
            raise
        except ValueError as e:
            L.error(f"Invalid manifest file: {e}")
            raise
        except Exception as e:
            L.error(f"Failed to initialize manifest: {e}")
            raise
        
    def get_table_names(self) -> Optional[List[str]]:
        """Get the table names from the manifest.
        
        Returns:
            List[str]: The table names from the manifest
        """
        if not self.is_initialized():
            L.error("Manifest is not initialized.")
            raise ValueError("Manifest is not initialized.")
        return list(self.manifest.keys())

    def is_initialized(self) -> bool:
        """Check if the manifest is properly initialized.
        
        Returns:
            bool: True if manifest is initialized, False otherwise
        """
        return self.manifest is not None

    def read(self, entry: str) -> Optional[Dict[str, Any]]:
        """Read a specific entry from the manifest.
        
        Args:
            entry: The table name to read from the manifest
            
        Returns:
            Optional[Dict[str, Any]]: The manifest entry if found, None otherwise
        """
        if not self.is_initialized():
            L.error("Manifest is not initialized.")
            return None

        if entry not in self.manifest:
            L.error(f"'{entry}' does not exist in the manifest.")
            return None

        try:
            entry_data = self.manifest[entry]
            # Handle PyArrow JSON conversion which may add extra nesting
            if isinstance(entry_data[0], list):
                # PyArrow converted structure: [[{...}]] -> get the dict
                json_object = entry_data[0][0].copy()
            else:
                # Normal structure: [{...}] -> get the dict
                json_object = entry_data[0].copy()
            
            json_object["entry"] = entry
            return json_object
        except (IndexError, KeyError, TypeError) as e:
            L.error(f"Error reading entry '{entry}' from manifest: {e}")
            return None
