# Guidewire Arrow

This is a fork of:
https://github.com/databricks-industry-solutions/guidewire

I have rewritten this to work using pyarrow, ray and delta-rs so it can run on any python envioronment including Databricks Unity Catalog runtimes

<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-16.4_LTS_ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/12.2.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-AWS-orange?style=for-the-badge)](https://databricks.com/try-databricks)
[![CLOUD](https://img.shields.io/badge/CLOUD-Azure-blue?style=for-the-badge)](https://databricks.com/try-databricks)



**Interpreting guidewire CDA as delta table:** 
*As a technology company, [Guidewire](https://www.guidewire.com/) offers an industry platform for property and casualty 
insurance carriers worldwide. Through different products and services under their insurance suite, they provide users
with the operation capabilities required to acquire, process and settle claims, maintains policies, support underwriting
and adjustment processes. Databricks on the other hand provides users with analytical capabilities (from basic 
reporting to complex ML solutions) through their
[lakehouse for Insurance](https://www.databricks.com/solutions/industries/financial-services). By combining both
platforms together, P&C insurance companies have now the ability to start integrating advanced analytics capabilities
(AI/ML) into their core business processes, enriching customer information with alternative data (e.g. weather data) 
but equally reconciling and reporting critical information at enterprise scale.*

___


## Approach

Guidewire supports data access to analytical environment via their Cloud Data Access offering 
([CDA](https://developer.guidewire.com/introducing-guidewire-data-platform/)). Storing files as individual parquet files
under different timestamps and schema evolution is unfortunately making processing difficult for end users. Instead
of processing files individually, why wouldn't we generate the `delta log` manifest files to only read information
we need, when we need it, without having to download, process and reconcile complex information? This is the principle
behind this initiative. The generated delta table will not be materialized (data will not be physically moving) but act
as a [shallow clone](https://docs.databricks.com/sql/language-manual/delta-clone.html) to guidewire data. 

<img src="images/approach.png" width=1000>

More specifically, we will process all Guidewire tables independently, in parallel (i.e. as a spark job), where each
task will consist in only listing parquet files and folders and generating delta log accordingly. From an end user 
standpoint, guidewire will look as a Delta table and be processed as such, reducing processing time from days to seconds 
(since we do not have to download and process each file through many spark jobs).

<img src="images/reconcile.png" width=1000>

As the data is now on Delta lake (physically materialized or not), one can benefit from all the downstream 
capabilities of delta lake, "subscribing" to changes via 
[auto-loader](https://docs.databricks.com/ingestion/auto-loader/index.html) capabilities, delta live table
([DLT](https://www.databricks.com/product/delta-live-tables)) or even 
[delta sharing](https://www.databricks.com/product/delta-sharing), accelerating time to insights from days to minutes.

## A word of caution

As this model follows a shallow clone approach, it is recommended to only grant read permission to end user since a 
`VACCUM` operation on the generated delta would possibly result in data loss on the guidewire S3 bucket. We highly
recommend organization not exposing this raw dataset to end users but rather create a SILVER version with materialized
data for consumption. Note that an `OPTIMIZE` command will result in materialization of latest delta snapshot with optimized
parquet files. Only the relevant files will be physically downloaded from original S3 to destination table.


## Features

- **Manifest Management**: Reads and filters manifest files stored in S3.
- **Batch Processing**: Processes data batches using PyArrow and Delta Lake.
- **Delta Lake Integration**: Maintains Delta Lake logs and checkpoints for data consistency.
- **Parallel Processing**: Utilizes Ray for distributed and parallelized data processing.

## Project Structure

```
guidewire-arrow/
├── guidewire/
│   ├── __init__.py
│   ├── manifest.py       # Handles manifest file operations
│   ├── batch.py          # Processes data batches
│   ├── delta_log.py      # Manages Delta Lake logs and checkpoints
│   ├── processor.py      # Orchestrates the data processing workflow
│   ├── storage.py        # Manages cloud storage operations (S3/Azure)
│   ├── results.py        # Tracks processing results and metrics
│   └── logging.py        # Logging configuration and utilities
├── main.py               # Entry point for the pipeline
├── requirements.txt      # Python dependencies
├── README.md             # Project documentation
```

## Installation

### Prerequisites

- Python 3.8 or higher
- AWS and Azure credentials with appropriate permissions  
- Docker and Docker Compose (for testing)
- `pip` for dependency management


1. Clone the repository:
```bash
git clone <repository-url>
cd guidwire-arrow
```
2. Create a virtual environment and activate it:
```bash
python3 -m venv venv
source venv/bin/activate
```
3. Install Dependencies

```bash
pip install -e .
```

4. Set up environment variables in a .env file:

### Core Configuration
```bash
# Manifest location (required)
AWS_MANIFEST_LOCATION=s3://your-bucket/manifest.json

# Target cloud for delta tables (default: "azure") 
DELTA_TARGET_CLOUD=azure  # or "aws"
```

### Azure Target (Default)
```bash
AZURE_STORAGE_ACCOUNT_NAME=yourstorageaccount
AZURE_STORAGE_ACCOUNT_CONTAINER=yourcontainer
AZURE_STORAGE_SUBFOLDER=optional/subfolder  # Optional

# Azure authentication (choose one method):
# Method 1: Account Key
AZURE_STORAGE_ACCOUNT_KEY=youraccountkey

# Method 2: Service Principal  
AZURE_TENANT_ID=yourtenant
AZURE_CLIENT_ID=yourclientid
AZURE_CLIENT_SECRET=yourclientsecret
```

### AWS S3 Target
```bash
# For cross-account or separate source/target configurations:
# Source S3 (manifest/data reading)
AWS_SOURCE_REGION=us-east-1
AWS_SOURCE_ACCESS_KEY_ID=sourcekeyid
AWS_SOURCE_SECRET_ACCESS_KEY=sourcesecretkey

# Target S3 (delta table writing)
AWS_TARGET_S3_BUCKET=your-delta-bucket
AWS_TARGET_REGION=us-west-2  
AWS_TARGET_ACCESS_KEY_ID=targetkeyid
AWS_TARGET_SECRET_ACCESS_KEY=targetsecretkey

# Or use shared credentials (backward compatible):
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=youraccesskey
AWS_SECRET_ACCESS_KEY=yoursecretkey
AWS_S3_BUCKET=yourbucket
```

### Optional Configuration
```bash
RAY_DEDUP_LOGS=0                        # Disable duplicate ray logs
DELTA_LOG_CHECKPOINT_INTERVAL=100       # Checkpoint interval
SHOW_TABLE_PROGRESS=1                   # Enable progress bars
AWS_ENDPOINT_URL=http://localhost:4566  # Custom S3 endpoint (LocalStack)
```

For detailed configuration examples, see:
- [AWS S3 and Storage Configuration Guide](AWS_S3_STORAGE_GUIDE.md)
- [Storage Setup Guide](STORAGE_SETUP.md)

## Key Components
### Manifest
The Manifest class in manifest.py reads and filters manifest files stored in S3. It initializes the manifest schema and provides methods to access specific entries.

### Batch
The Batch class in batch.py processes data batches. It handles schema discovery, processes data files, and interacts with Delta Lake logs.

### DeltaLog
The DeltaLog class in delta_log.py manages Delta Lake logs and checkpoints. It supports creating new Delta tables, adding transactions, and maintaining checkpoints.

### Processor
The Processor class in processor.py orchestrates the overall data processing workflow. It manages the parallel processing of data using Ray, coordinates between manifest reading, batch processing, and delta log management. The processor handles the end-to-end pipeline execution, ensuring efficient and reliable data processing across distributed systems.

### Storage
The Storage class in storage.py provides a unified interface for cloud storage operations across different providers (AWS S3 and Azure Blob Storage). It handles authentication, file operations, and storage-specific configurations, abstracting away the complexities of interacting with different cloud storage services. This component ensures consistent data access patterns regardless of the underlying storage platform.

### Results
The Results module in results.py provides comprehensive tracking and monitoring of processing operations. The `Result` dataclass captures detailed metrics including:

- **Processing Timing**: Start and finish timestamps for each table processing operation
- **Watermark Management**: Tracks data watermarks and schema timestamps throughout processing
- **Version Control**: Records Delta Lake versions before and after processing
- **Manifest Metrics**: Captures record counts and manifest watermarks
- **Error Handling**: Collects and stores errors and warnings that occur during processing
- **Progress Tracking**: Maintains lists of processed watermarks and schema changes

This module enables comprehensive monitoring, debugging, and audit trails for data processing workflows, ensuring transparency and reliability in large-scale data operations.

### Progress UI
The system features an intelligent progress tracking interface that adapts to the execution environment:

- **Adaptive Progress Bars**: Automatically detects whether Ray is initialized for distributed processing
- **Ray Integration**: Uses `ray.experimental.tqdm_ray.tqdm` for proper progress display in distributed environments
- **Fallback Support**: Falls back to standard `tqdm` for single-threaded or non-Ray environments
- **Real-time Updates**: Provides live progress feedback during batch processing operations
- **Error Integration**: Progress bars are integrated with the results tracking system for comprehensive monitoring

The progress UI ensures users have clear visibility into processing status regardless of whether they're running single-threaded or distributed workloads.

## Dependencies

The project requires the following core dependencies:

- **Ray (v2.45.0)**: For distributed and parallel processing
- **DeltaLake (v1.1.4)**: For managing Delta Lake logs and operations
- **PyArrow (v20.0.0)**: For efficient data processing and columnar operations
- **tqdm (v4.67.0)**: For progress bars and monitoring

### Development Dependencies
- **pytest (v8.3.5)**: For testing framework
- **boto3 (v1.35.58)**: For S3 operations and testing
- **requests (v2.32.3)**: For HTTP operations and testing

### Version Information
- **Package Version**: 0.0.2

## Examples

### Azure Target (Default)
```python
from guidewire.processor import Processor

# Using Azure as target (default behavior)
processor = Processor(
    target_cloud="azure",  # Default, can be omitted
    table_names=["policy_holders", "claims"],
    parallel=True
)
processor.run()
```

### AWS S3 Target  
```python
from guidewire.processor import Processor

# Using S3 as target cloud
processor = Processor(
    target_cloud="aws",
    table_names=["policy_holders", "claims"],
    parallel=True
)
processor.run()
```

### Environment Variable Configuration
```python
import os
from guidewire.processor import Processor

# Set target via environment variable
os.environ["DELTA_TARGET_CLOUD"] = "aws"

# Processor will use environment variable
processor = Processor(
    table_names=["policy_holders"],
    parallel=False
)
processor.run()
```

### Processing All Tables
```python
from guidewire.processor import Processor

# Process all tables from manifest (table_names=None)
processor = Processor(
    target_cloud="azure",
    parallel=True  # Process all tables in parallel
)
processor.run()

# Access detailed results
for result in processor.results:
    print(f"Table: {result.table}")
    print(f"Processing time: {result.process_finish_time - result.process_start_time}")
    print(f"Records from manifest: {result.manifest_records}")
    print(f"Watermarks: {result.watermarks}")
    if result.errors:
        print(f"Errors encountered: {result.errors}")
    if result.warnings:
        print(f"Warnings: {result.warnings}")
```

### Working with Results
```python
from guidewire.processor import Processor
from guidewire.results import Result

# Process specific tables and capture detailed results
processor = Processor(
    target_cloud="aws",
    table_names=("policy_holders", "claims"),
    parallel=False
)
processor.run()

# Analyze results for monitoring and debugging
for result in processor.results:
    if result.errors:
        print(f"❌ {result.table}: Failed with errors - {result.errors}")
    elif result.warnings:
        print(f"⚠️  {result.table}: Completed with warnings - {result.warnings}")
    else:
        print(f"✅ {result.table}: Successfully processed {result.manifest_records} records")
        print(f"   - Version: {result.process_start_version} → {result.process_finish_version}")
        print(f"   - Watermark: {result.process_start_watermark} → {result.process_finish_watermark}")
```

## Testing

The project includes comprehensive integration testing with Docker Compose support:

```bash
# Install test dependencies
make install-deps

# Run all tests
make test

# Run specific test suites
make test-s3        # S3/LocalStack integration tests
make test-azure     # Azure/Azurite integration tests  
make test-e2e       # End-to-end workflow tests

# Start Docker services for manual testing
make docker-up

# Stop Docker services
make docker-down
```

For detailed testing information, see [TESTING.md](TESTING.md).

## Documentation

- **[README.md](README.md)**: Main project documentation
- **[AWS_S3_STORAGE_GUIDE.md](AWS_S3_STORAGE_GUIDE.md)**: AWS S3 and multi-cloud storage configuration guide
- **[STORAGE_SETUP.md](STORAGE_SETUP.md)**: Local storage services with Docker Compose
- **[TESTING.md](TESTING.md)**: Comprehensive testing guide

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.