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

    # Optional staging mode for Unity Catalog governance. When enabled, source
    # parquets are copied into the UC-governed target before AddActions are
    # written, so all reads stay inside a UC external location the customer
    # controls. Required for Guidewire CDA SaaS deployments.
    staging_mode = os.environ.get("STAGING_MODE", "false").lower() == "true"
    uc_catalog = os.environ.get("UC_CATALOG_NAME")
    uc_schema = os.environ.get("UC_SCHEMA_NAME")
    uc_region = os.environ.get("UC_REGION") or os.environ.get("AWS_REGION")

    processor = Processor(
        target_cloud=target_cloud,
        table_names=TABLE_NAMES,
        parallel=False,
        staging_mode=staging_mode,
        uc_catalog=uc_catalog,
        uc_schema=uc_schema,
        uc_region=uc_region,
    )
    processor.run()

if __name__ == "__main__":
    main()


## Read all tables from manfiest, accept the table names as an arguement 
