# Local Storage Services Setup

This Docker Compose setup provides local emulation of cloud storage services for development and testing of the Guidewire CDA Delta Clone project.

## Quick Start

```bash
# Start all services
make docker-up

# Run tests
make test

# Stop all services  
make docker-down
```

## Services

### LocalStack (S3 Emulation)
- **Port**: 4566
- **Web UI**: http://localhost:4566
- **Health Check**: http://localhost:4566/health
- **AWS Region**: us-east-1
- **Access Key**: test
- **Secret Key**: test
- **Container Name**: localstack-s3

### Azurite (Azure Blob Storage Emulation)  
- **Blob Service**: http://localhost:10000
- **Account Name**: testingstorage
- **Account Key**: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
- **Container Name**: azurite-storage

## Usage

### Using Makefile (Recommended)
```bash
# Start services and wait for readiness
make docker-up

# Stop all services
make docker-down

# Restart services
make docker-restart

# Clean up all data and volumes
make clean
```

### Using Docker Compose Directly
```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

### View Logs
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f localstack
docker-compose logs -f azurite
```

## Example Usage

### S3 (LocalStack)
```python
import boto3

# Configure boto3 for LocalStack
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

# Create a bucket
bucket_name = 'my-test-bucket'
s3_client.create_bucket(Bucket=bucket_name)

# Upload a file
s3_client.put_object(
    Bucket=bucket_name,
    Key='test-file.txt',
    Body=b'Hello, LocalStack!'
)
```

### Azure Storage (Azurite)
```python
from azure.storage.blob import BlobServiceClient

# Configure Azure Storage client for Azurite
connection_string = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=testingstorage;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://localhost:10000/testingstorage;"
)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Create a container
container_name = 'my-test-container'
blob_service_client.create_container(container_name)

# Upload a blob
blob_client = blob_service_client.get_blob_client(
    container=container_name,
    blob='test-file.txt'
)
blob_client.upload_blob(b'Hello, Azurite!')
```

### AWS CLI with LocalStack
```bash
# Configure AWS CLI for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Use AWS CLI with LocalStack endpoint
aws --endpoint-url=http://localhost:4566 s3 mb s3://my-test-bucket
aws --endpoint-url=http://localhost:4566 s3 ls
```

### Azure CLI with Azurite
```bash
# Configure Azure Storage connection string
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=testingstorage;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/testingstorage;"

# Use Azure CLI
az storage container create --name my-test-container
az storage blob list --container-name my-test-container
```

## Data Persistence

Both services persist data in the `./testing/tmp/` directory:
- LocalStack data: `./testing/tmp/localstack/`
- Azurite data: `./testing/tmp/azurite/`

To reset all data:
```bash
# Stop services and clean data
make clean

# Or manually
docker-compose down -v
rm -rf ./testing/tmp/
```

## Health Checks

Both services include health checks. You can verify they're running:

```bash
# Check LocalStack
curl http://localhost:4566/health

# Check Azurite Blob service
curl http://localhost:10000/testingstorage?comp=properties&restype=service
```

## Integration with Guidewire CDA Delta Clone

The local services are automatically configured for the project's integration tests:

### Environment Variables
The services use these environment variables for testing:
```bash
# AWS/LocalStack
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://localhost:4566

# Azure/Azurite  
AZURE_STORAGE_ACCOUNT_NAME=testingstorage
AZURE_STORAGE_ACCOUNT_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
AZURE_BLOB_STORAGE_AUTHORITY=127.0.0.1:10000
AZURE_BLOB_STORAGE_SCHEME=http
```

### Running Integration Tests
```bash
# Start services and run all tests
make test

# Run specific test suites
make test-s3        # S3/LocalStack tests
make test-azure     # Azure/Azurite tests
make test-e2e       # End-to-end workflow tests

# Manual testing with services running
make docker-up
python main.py
```

## Troubleshooting

### Common Issues
1. **Port conflicts**: If ports 4566 or 10000 are in use, modify the port mappings in docker-compose.yml
2. **Permission issues**: Ensure Docker has access to the project directory for volume mounts
3. **Service not starting**: Check logs with `docker-compose logs [service-name]`
4. **Makefile commands not working**: Ensure you have `make` installed

### Debugging Commands
```bash
# Check service status
make docker-up  # Will show readiness check progress

# View service logs
docker-compose logs -f localstack
docker-compose logs -f azurite

# Test connectivity manually
curl http://localhost:4566/health
curl "http://localhost:10000/testingstorage?comp=properties&restype=service"

# Reset everything
make clean
make docker-up
```

### Docker Issues
If Docker commands fail:
```bash
# Ensure Docker is running
docker --version
docker-compose --version

# Check for existing containers
docker ps -a

# Force remove conflicting containers
docker container prune
docker volume prune
```

## Related Documentation

- **[TESTING.md](TESTING.md)**: Comprehensive testing guide
- **[AWS_S3_STORAGE_GUIDE.md](AWS_S3_STORAGE_GUIDE.md)**: AWS S3 and multi-cloud storage configuration
