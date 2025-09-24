from guidewire.processor import Processor
from typing import Tuple

# Define table names
TABLE_NAMES: Tuple[str, ...] = (
    "policy_holders",
    "policy_holders2",
    "policy_holders3",
    "policy_holders4",
)

def main() -> None:
    """Main entry point for the application."""
    import os
    
    # Get target cloud from environment variable, default to "azure" for backward compatibility
    target_cloud = os.environ.get("DELTA_TARGET_CLOUD", "azure")
    
    processor = Processor(target_cloud=target_cloud, table_names=TABLE_NAMES, parallel=False)
    processor.run()

if __name__ == "__main__":
    main()


## Read all tables from manfiest, accept the table names as an arguement 
