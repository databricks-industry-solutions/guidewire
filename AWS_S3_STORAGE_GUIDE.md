# AWS S3 and Multi-Cloud Storage Configuration Guide

This guide covers the comprehensive storage configuration options for the Guidewire CDA Delta Clone project, including support for separate source and target credentials across AWS S3 and Azure Blob Storage.

## Overview

The system supports flexible storage configurations with:

- **Multi-cloud targets**: Write delta tables to either AWS S3 or Azure Blob Storage
- **Separate credentials**: Different AWS accounts, regions, and credentials for source vs target
- **Backward compatibility**: Existing configurations continue to work unchanged
- **Local development**: Support for LocalStack (S3) and Azurite (Azure) emulators

### Key Capabilities

- **Cross-account processing**: Source data in one AWS account, delta tables in another
- **Multi-region support**: Different regions for source and target storage
- **Mixed cloud environments**: AWS source with Azure target (or vice versa)
- **Security isolation**: Separate read/write credentials
- **Development flexibility**: LocalStack to AWS, or any combination

## Architecture

The system separates **source** and **target** storage operations:

```
Source Storage (AWS_SOURCE_*) → Processing → Target Storage (AWS_TARGET_* or AZURE_*)
          ↑                                        ↑
  AWS_MANIFEST_LOCATION                   DELTA_TARGET_CLOUD
```

### Data Flow
1. **Source**: Reads manifest files and parquet data from S3
2. **Processing**: Converts data to Delta Lake format
3. **Target**: Writes delta tables to configured cloud storage (S3 or Azure)

## Environment Variable Configuration

### Required for All Scenarios
```bash
# Manifest location (always required)
export AWS_MANIFEST_LOCATION="s3://your-source-bucket/manifest.json"

# Target cloud selection (optional, defaults to 'azure')
export DELTA_TARGET_CLOUD="aws"  # or "azure"
```

### Azure Target Configuration (Default)
```bash
# Azure-specific (for delta table target)
export AZURE_STORAGE_ACCOUNT_NAME="yourstorageaccount"
export AZURE_STORAGE_ACCOUNT_CONTAINER="yourcontainer"
export AZURE_STORAGE_SUBFOLDER="optional/subfolder"  # Optional

# Azure authentication (choose one method):
# Method 1: Account Key
export AZURE_STORAGE_ACCOUNT_KEY="youraccountkey"

# Method 2: Service Principal
export AZURE_TENANT_ID="yourtenant"
export AZURE_CLIENT_ID="yourclientid"
export AZURE_CLIENT_SECRET="yourclientsecret"

# Optional: Custom endpoints (for Azurite/emulator)
export AZURE_BLOB_STORAGE_AUTHORITY="127.0.0.1:10000"
export AZURE_BLOB_STORAGE_SCHEME="http"
export AZURE_DFS_STORAGE_AUTHORITY="127.0.0.1:10001" 
export AZURE_DFS_STORAGE_SCHEME="http"
```

### AWS S3 Target Configuration

#### Option 1: Separate Source and Target Credentials (Recommended)
```bash
# Source S3 (for reading manifest and parquet files)
export AWS_SOURCE_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="source_access_key"
export AWS_SOURCE_SECRET_ACCESS_KEY="source_secret_key"
export AWS_SOURCE_ENDPOINT_URL="http://localhost:4566"  # Optional

# Target S3 (for writing delta tables - can be different account/region)
export AWS_TARGET_S3_BUCKET="your-delta-bucket"
export AWS_TARGET_REGION="us-west-2"
export AWS_TARGET_ACCESS_KEY_ID="target_access_key"
export AWS_TARGET_SECRET_ACCESS_KEY="target_secret_key"
export AWS_TARGET_S3_PREFIX="delta_tables"  # Optional
export AWS_TARGET_ENDPOINT_URL="https://s3.us-west-2.amazonaws.com"  # Optional
```

#### Option 2: Shared Credentials (Backward Compatible)
```bash
# Shared AWS credentials (used for both source and target)
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="shared_access_key"
export AWS_SECRET_ACCESS_KEY="shared_secret_key"
export AWS_S3_BUCKET="your-bucket"
export AWS_S3_PREFIX="delta_tables"  # Optional
export AWS_ENDPOINT_URL="http://localhost:4566"  # Optional
```

### Credential Resolution Logic

The system uses a **fallback hierarchy**:

1. **First Priority**: Prefixed environment variables
   - `AWS_SOURCE_*` for source operations
   - `AWS_TARGET_*` for target operations

2. **Fallback**: Standard environment variables
   - `AWS_REGION`, `AWS_ACCESS_KEY_ID`, etc.

**Example Resolution:**
```python
# For source region:
region = (os.environ.get("AWS_SOURCE_REGION") or 
          os.environ.get("AWS_REGION"))

# For target bucket:
bucket = (os.environ.get("AWS_TARGET_S3_BUCKET") or 
          os.environ.get("AWS_S3_BUCKET"))
```

This maintains backward compatibility while enabling separation when needed.

## Use Cases and Examples

### 1. Cross-Account Processing
```bash
# Source data in Account A (us-east-1)
export AWS_SOURCE_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="AKIA_ACCOUNT_A_KEY"
export AWS_SOURCE_SECRET_ACCESS_KEY="account_a_secret"

# Delta tables in Account B (us-west-2) 
export AWS_TARGET_S3_BUCKET="account-b-delta-bucket"
export AWS_TARGET_REGION="us-west-2"
export AWS_TARGET_ACCESS_KEY_ID="AKIA_ACCOUNT_B_KEY"
export AWS_TARGET_SECRET_ACCESS_KEY="account_b_secret"
export DELTA_TARGET_CLOUD="aws"
```

### 2. Different Regions (Same Account)
```bash
# Source in us-east-1 (cheaper storage)
export AWS_SOURCE_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="shared_key"
export AWS_SOURCE_SECRET_ACCESS_KEY="shared_secret"

# Target in us-west-2 (closer to analytics workloads)
export AWS_TARGET_S3_BUCKET="analytics-delta-tables"
export AWS_TARGET_REGION="us-west-2"
export AWS_TARGET_ACCESS_KEY_ID="shared_key"
export AWS_TARGET_SECRET_ACCESS_KEY="shared_secret"
export DELTA_TARGET_CLOUD="aws"
```

### 3. LocalStack Development to AWS Production
```bash
# Source from LocalStack (development)
export AWS_SOURCE_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="test"
export AWS_SOURCE_SECRET_ACCESS_KEY="test"
export AWS_SOURCE_ENDPOINT_URL="http://localhost:4566"

# Target to real AWS (production)
export AWS_TARGET_S3_BUCKET="prod-delta-tables"
export AWS_TARGET_REGION="us-east-1"
export AWS_TARGET_ACCESS_KEY_ID="AKIA_PROD_KEY"
export AWS_TARGET_SECRET_ACCESS_KEY="prod_secret"
export DELTA_TARGET_CLOUD="aws"
```

### 4. Mixed Cloud Environment
```bash
# Source from AWS S3
export AWS_SOURCE_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="aws_key"
export AWS_SOURCE_SECRET_ACCESS_KEY="aws_secret"

# Target to Azure Blob Storage
export DELTA_TARGET_CLOUD="azure"
export AZURE_STORAGE_ACCOUNT_NAME="companydelta"
export AZURE_STORAGE_ACCOUNT_CONTAINER="deltatables"
export AZURE_STORAGE_ACCOUNT_KEY="azure_key"
```

## Usage Examples

### Python API

#### Using Azure as Target (Default)
```python
from guidewire.processor import Processor

# Uses Azure by default (backward compatible)
processor = Processor(
    target_cloud="azure",  # Can be omitted for default
    table_names=("policy_holders",),
    parallel=True
)
processor.run()
```

#### Using S3 as Target
```python
from guidewire.processor import Processor

# Configure to use S3 as target
processor = Processor(
    target_cloud="aws",
    table_names=("policy_holders",),
    parallel=True
)
processor.run()
```

### Environment Variable Configuration
```bash
# Set target cloud via environment
export DELTA_TARGET_CLOUD="aws"

# Run the application (main.py will use the environment variable)
python main.py
```

### Command Line Usage
```bash
# With environment variables configured
python main.py

# Or with explicit target cloud override
DELTA_TARGET_CLOUD="aws" python main.py
```

## Delta Table URI Formats

### Azure Target
```
abfss://container@account.dfs.core.windows.net/[subfolder/]table_name/
```

### S3 Target
```
s3://bucket-name/[prefix/]table_name/
```

## Storage Options for Delta-RS

The system automatically configures storage options for the delta-rs library:

### Azure
- `account_name`, `account_key` or service principal credentials
- Custom endpoints for Azurite emulator support
- ABFS (Azure Data Lake Storage Gen2) protocol

### S3
- `region`, `access_key_id`, `secret_access_key`
- Custom endpoints for LocalStack/MinIO support
- S3 protocol with virtual-hosted-style addressing

## Testing and Development

### Docker Compose Setup
```bash
# Start local services (LocalStack + Azurite)
make docker-up

# Configure for local testing
export AWS_SOURCE_ENDPOINT_URL="http://localhost:4566"
export AWS_TARGET_ENDPOINT_URL="http://localhost:4566"
export AZURE_BLOB_STORAGE_AUTHORITY="127.0.0.1:10000"
export AZURE_BLOB_STORAGE_SCHEME="http"
```

### Running Tests
```bash
# Run all tests
make test

# Test specific scenarios
make test-s3       # S3 credential scenarios
make test-azure    # Azure credential scenarios
make test-e2e      # End-to-end mixed scenarios

# Stop services
make docker-down
```

The integration tests validate:
- Separate credential resolution logic
- Fallback mechanisms
- Cross-account scenarios
- Mixed cloud configurations

### Development Configuration Example
```bash
# Complete development setup with LocalStack and Azurite
export AWS_MANIFEST_LOCATION="s3://test-bucket/manifest.json"

# LocalStack for both source and target
export AWS_SOURCE_ENDPOINT_URL="http://localhost:4566"
export AWS_TARGET_ENDPOINT_URL="http://localhost:4566"
export AWS_SOURCE_REGION="us-east-1"
export AWS_TARGET_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="test"
export AWS_SOURCE_SECRET_ACCESS_KEY="test"
export AWS_TARGET_ACCESS_KEY_ID="test"
export AWS_TARGET_SECRET_ACCESS_KEY="test"
export AWS_TARGET_S3_BUCKET="delta-tables"

# Set target to S3
export DELTA_TARGET_CLOUD="aws"
```

## Migration Guide

### From Single Credential Set to Separate Credentials

#### Backward Compatibility
Existing configurations continue to work unchanged. The system automatically uses fallback variables when prefixed ones aren't provided.

#### Gradual Migration Steps
1. **Start**: Use existing `AWS_*` variables (works as before)
2. **Phase 1**: Add source-specific variables while keeping fallbacks
3. **Phase 2**: Add target-specific variables while keeping fallbacks  
4. **Final**: Remove fallback variables if desired (optional)

#### Migration Example
```bash
# Before (single credential set)
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="shared_key"
export AWS_SECRET_ACCESS_KEY="shared_secret"
export AWS_S3_BUCKET="shared-bucket"

# After (separate credentials)
export AWS_SOURCE_REGION="us-east-1"
export AWS_SOURCE_ACCESS_KEY_ID="source_key"
export AWS_SOURCE_SECRET_ACCESS_KEY="source_secret"

export AWS_TARGET_S3_BUCKET="target-bucket"
export AWS_TARGET_REGION="us-west-2"
export AWS_TARGET_ACCESS_KEY_ID="target_key"
export AWS_TARGET_SECRET_ACCESS_KEY="target_secret"
export DELTA_TARGET_CLOUD="aws"
```

### From Azure-only to S3 Target
1. Set `DELTA_TARGET_CLOUD="aws"`
2. Configure AWS environment variables
3. Existing code will continue to work unchanged

The system maintains backward compatibility - if `DELTA_TARGET_CLOUD` is not set, it defaults to Azure.

## Error Handling

The system validates required environment variables on startup and provides clear error messages:

### Missing AWS Target Configuration
```
Missing required environment variables for target cloud 'aws': 
AWS_TARGET_S3_BUCKET (or AWS_S3_BUCKET), AWS_TARGET_REGION (or AWS_REGION)
```

### Missing Azure Target Configuration
```
Missing required environment variables for target cloud 'azure': 
AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_CONTAINER
```

### Missing Source Configuration
```
Missing required environment variables - 
Source S3: AWS_SOURCE_REGION (or AWS_REGION), AWS_SOURCE_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID)
```

### Invalid Target Cloud
```
Invalid target_cloud: invalid_cloud. Must be 'azure' or 'aws'
```

## Best Practices

### Configuration
1. **Use prefixed variables** (`AWS_SOURCE_*`, `AWS_TARGET_*`) for clarity and separation
2. **Keep fallback variables** during migration for safety
3. **Document your credential strategy** for team members
4. **Test credential separation** in development before production

### Security
1. **Use least-privilege principles** - separate read/write access as needed
2. **Source credentials can be read-only** for better security
3. **Target credentials need write access** to delta table locations
4. **Consider using IAM roles** instead of access keys where possible
5. **Rotate credentials regularly**, especially if using access keys
6. **Use different AWS accounts** for additional isolation if needed

### Operations
1. **Monitor credential usage** across different AWS accounts/regions
2. **Validate configurations** in development environments first
3. **Use meaningful bucket and prefix names** for organization
4. **Plan for cross-region data transfer costs** when using different regions

## Implementation Details

### Storage Class Changes
- Added `prefix` parameter to `Storage.__init__()`
- Supports `"SOURCE"` and `"TARGET"` prefixes for AWS
- Maintains fallback logic for backward compatibility

### Component Updates
- **Manifest**: Uses `Storage(cloud="aws", prefix="SOURCE")`
- **DeltaLog**: Uses `Storage(cloud="aws", prefix="TARGET")` for AWS targets
- **Processor**: Validates both source and target credentials separately

### Credential Validation
The system performs comprehensive validation:
- Checks for required variables based on target cloud
- Validates credential combinations
- Provides helpful error messages with fallback suggestions
- Fails fast on startup rather than during processing

## Related Resources

- **[STORAGE_SETUP.md](STORAGE_SETUP.md)**: Local development setup with Docker Compose
- **[TESTING.md](testing/TESTING.md)**: Comprehensive testing guide
- **[README.md](README.md)**: Main project documentation
- **[examples/](examples/)**: Configuration templates and examples

## Troubleshooting

### Common Issues

1. **Credential not found errors**: Check environment variable names and fallback logic
2. **Cross-account access denied**: Verify IAM policies allow cross-account access
3. **Region mismatch**: Ensure source and target regions are correctly configured
4. **LocalStack connectivity**: Verify Docker containers are running and endpoints are correct
5. **Azure authentication failures**: Check account key vs service principal configuration

### Debug Mode
Enable detailed logging:
```bash
export LOG_LEVEL="DEBUG"
python main.py
```

This will show credential resolution, storage configuration, and detailed error information.
