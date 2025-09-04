# Integration Testing Guide

This guide covers the comprehensive integration testing setup for the Guidewire CDA Delta Clone project. The tests use Docker Compose to create real S3 (LocalStack) and Azure Blob Storage (Azurite) instances for testing with your example data.

## Overview

The integration tests validate:
- ✅ S3 operations using LocalStack
- ✅ Azure Blob Storage operations using Azurite  
- ✅ Storage class functionality with both backends
- ✅ Manifest file reading and processing
- ✅ End-to-end data processing workflows
- ✅ Error handling and recovery scenarios

## Quick Start

```bash
# Install dependencies
make install-deps

# Start Docker services and run all tests (unit + integration)
make test

# Or run specific test suites
make test-unit      # Unit tests only (fast, no services required)
make test-s3        # S3/LocalStack integration tests
make test-azure     # Azure/Azurite integration tests
make test-e2e       # End-to-end workflow tests
make test-quick     # Quick integration tests (exclude slow tests)
```

## Prerequisites

### Required Software
- Docker and Docker Compose
- Python 3.8+
- Make

### Dependencies
The integration tests require additional Python packages:
```bash
pip install pytest boto3 requests pyarrow azure-storage-blob
```

Or use the Makefile:
```bash
make install-deps
```

## Docker Services

The tests use Docker Compose to run:

### LocalStack (S3 Emulation)
- **Port**: 4566
- **Endpoint**: http://localhost:4566
- **Credentials**: test/test
- **Services**: S3

### Azurite (Azure Blob Storage Emulation)  
- **Port**: 10000
- **Endpoint**: http://localhost:10000
- **Account Name**: testingstorage
- **Account Key**: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
- **Container Name**: azurite-storage

## Test Structure

```
testing/tests/
├── test_s3_integration.py          # Direct S3 API tests
├── test_azure_integration.py       # Direct Azure Blob API tests
├── test_storage_integration.py     # Storage class tests
├── test_manifest_integration.py    # Manifest class tests
└── test_end_to_end_integration.py  # Complete workflow tests
```

## Test Commands

### Basic Commands
```bash
# Start Docker services
make docker-up

# Run all tests (unit + integration)
make test

# Run only unit tests (fast, no services required)  
make test-unit

# Run all integration tests
make test-integration

# Run quick tests (exclude slow ones)
make test-quick

# Stop Docker services
make docker-down

# Clean up all data and containers
make clean
```

### Specific Test Suites
```bash
# S3/LocalStack tests
make test-s3

# Azure/Azurite tests  
make test-azure

# Storage class tests
make test-storage

# Manifest class tests
make test-manifest

# End-to-end workflow tests
make test-e2e

# Slow/comprehensive tests
make test-slow
```

### Direct pytest Commands
```bash
# Run specific test file
pytest testing/tests/test_s3_integration.py -v

# Run tests with specific markers
pytest testing/tests/ -v -m unit         # Unit tests only
pytest testing/tests/ -v -m integration  # Integration tests only
pytest testing/tests/ -v -m slow         # Slow/comprehensive tests

# Exclude specific markers
pytest testing/tests/ -v -m integration -m "not slow"

# Run specific test method
pytest testing/tests/test_s3_integration.py::TestS3Integration::test_upload_example_data -v
```

## Test Categories

### Unit Tests (`-m unit`)  
Fast tests that run without external dependencies:
- Component unit tests
- Utility function tests
- Configuration validation tests
- Mock-based tests

### Integration Tests (`-m integration`)
Tests that require Docker services to be running:
- Real S3 operations via LocalStack
- Real Azure Blob operations via Azurite
- Storage class with real backends
- Manifest reading from real storage
- File upload/download operations
- Cross-cloud scenarios

### Slow Tests (`-m slow`)
Comprehensive tests that process larger datasets:
- End-to-end workflows with all example data
- Large file processing simulations
- Complete data validation workflows
- Performance-focused scenarios

## Example Data Usage

The tests automatically use the example CDA data in the `examples/` directory:

```
examples/cda/
├── manifest.json                    # Manifest metadata
└── policy_holders/                  # Sample parquet data
    ├── 301248659/
    │   ├── 1680350543000/
    │   └── 1680535502000/
    └── 301248660/
        ├── 1680757005000/
        └── 1680945093000/
```

Tests will:
1. Upload this data to test storage buckets/containers
2. Verify file structure preservation
3. Test reading parquet files and manifest
4. Validate data consistency
5. Clean up test resources

## Environment Variables

The tests automatically set required environment variables:

### AWS/LocalStack
```bash
# Source S3 (for manifest reading)
AWS_SOURCE_REGION=us-east-1
AWS_SOURCE_ACCESS_KEY_ID=test
AWS_SOURCE_SECRET_ACCESS_KEY=test
AWS_SOURCE_ENDPOINT_URL=http://localhost:4566

# Target S3 (for delta tables)
AWS_TARGET_S3_BUCKET=test-delta-bucket
AWS_TARGET_REGION=us-east-1
AWS_TARGET_ACCESS_KEY_ID=test
AWS_TARGET_SECRET_ACCESS_KEY=test
AWS_TARGET_ENDPOINT_URL=http://localhost:4566

# Fallback (backward compatible)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_S3_BUCKET=test-bucket
AWS_ENDPOINT_URL=http://localhost:4566
```

### Azure/Azurite
```bash
AZURE_STORAGE_ACCOUNT_NAME=testingstorage
AZURE_STORAGE_ACCOUNT_CONTAINER=deltacontainer
AZURE_STORAGE_ACCOUNT_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
AZURE_BLOB_STORAGE_AUTHORITY=127.0.0.1:10000
AZURE_BLOB_STORAGE_SCHEME=http
```

## Test Output Examples

### Successful Test Run
```
$ make test-s3
docker-compose up -d
✓ Services are ready!
pytest testing/tests/test_s3_integration.py -v -m integration

testing/tests/test_s3_integration.py::TestS3Integration::test_create_and_list_bucket PASSED
testing/tests/test_s3_integration.py::TestS3Integration::test_upload_example_data PASSED
✅ Successfully uploaded 8 example files to S3

testing/tests/test_s3_integration.py::TestS3Integration::test_manifest_upload_and_download PASSED
✅ Manifest contains 3 entities

================= 6 passed in 12.34s =================
```

### End-to-End Workflow Output
```
$ make test-e2e
pytest testing/tests/test_end_to_end_integration.py -v -m integration

📊 Data Validation Results:
--------------------------------------------------
policy_holders:
  Expected records: 71027
  Actual records: 71027  
  Files found: 5

📈 Schema Evolution Analysis:
----------------------------------------
policy_holders:
  Schema versions: 2
  Schema IDs: ['301248659', '301248660']
  Latest timestamp: 1680945093000

✅ Complete workflow test passed - processed 1 tables
================= 5 passed in 45.67s =================
```

## Troubleshooting

### Docker Services Not Starting
```bash
# Check if ports are available
netstat -an | grep -E '(4566|10000)'

# Restart services
make docker-restart

# Check service logs
docker-compose logs localstack
docker-compose logs azurite
```

### Test Failures
```bash
# Clean up Docker volumes
make clean

# Check service health
curl http://localhost:4566/health
curl "http://localhost:10000/testingstorage?comp=properties&restype=service"

# Run tests in debug mode
pytest testing/tests/test_s3_integration.py -v -s
```

### Missing Example Data
The tests require the `examples/` directory with CDA data:
```bash
ls -la examples/cda/
# Should show manifest.json and policy_holders/ directory
```

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.8'
      - name: Install dependencies
        run: make install-deps
      - name: Run integration tests
        run: make ci-integration
```

### Available CI Commands
```bash
make ci-test        # Quick tests suitable for CI (unit + quick integration)
make ci-integration # Full integration test suite  
make ci-e2e         # End-to-end tests only
```

### Full CI Workflow Example
```bash
#!/bin/bash
set -e

echo "Installing dependencies..."
make install-deps

echo "Running unit tests..."
make test-unit

echo "Starting Docker services..." 
make docker-up

echo "Running integration tests..."
make test-integration

echo "Cleaning up..."
make docker-down

echo "All tests passed!"
```

## Performance Notes

- **Unit tests**: ~5-10 seconds (no external dependencies)
- **Integration tests**: ~2-5 minutes total
- **End-to-end tests**: ~5-10 minutes (marked as slow)
- **Individual test suites**: ~30-60 seconds each

The tests automatically handle:
- ✅ Service health checks and waiting for readiness
- ✅ Test isolation (unique bucket/container names)
- ✅ Resource cleanup after each test
- ✅ Error recovery and cleanup on failures
- ✅ Automatic service startup/shutdown with Makefile

## Development Workflow

### Day-to-day Development
```bash
# Quick feedback loop during development
make test-unit                    # Fast unit tests

# Test specific components
make test-storage                 # Storage class tests
make test-manifest                # Manifest functionality 

# Full validation before commit
make test                        # All tests
```

### Before Deployment
```bash
# Comprehensive validation
make test-slow                   # Full end-to-end tests
make test-e2e                    # Complete workflow validation
```

### CI/CD Integration
```bash
make ci-test                     # Fast CI-friendly tests
make ci-integration              # Full integration suite
```

## Next Steps

After running tests successfully:
1. Use `make test-e2e` to validate complete workflows
2. Check `make test-slow` for comprehensive validation  
3. Review test output for performance insights
4. Integrate `make ci-integration` into your CI/CD pipeline
5. Use `make test-unit` during development for fast feedback

The integration tests provide confidence that your tool works correctly with both AWS S3 and Azure Blob Storage using real storage operations and your actual CDA data.

## Related Documentation

- **[README.md](README.md)**: Main project documentation
- **[STORAGE_SETUP.md](STORAGE_SETUP.md)**: Docker Compose setup details
- **[AWS_S3_STORAGE_GUIDE.md](AWS_S3_STORAGE_GUIDE.md)**: AWS S3 and multi-cloud storage configuration
