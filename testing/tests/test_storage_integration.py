"""
Integration tests for the Storage class using real storage backends.

These tests require Docker Compose services to be running:
    docker-compose up -d

Run with: pytest testing/tests/test_storage_integration.py -v -m integration
"""

import pytest
import os
import tempfile
import time
import requests
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from unittest.mock import patch

# Import the Storage class
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from guidewire.storage import AzureStorage, AWSStorage

# Test configuration
LOCALSTACK_ENDPOINT = "http://localhost:4566"
AZURITE_ENDPOINT = "http://localhost:10000"


class TestStorageIntegration:
    """Integration tests for Storage class with real storage backends."""
    
    @classmethod
    def setup_class(cls):
        """Set up class-level resources."""
        cls._check_services_running()
        
    @classmethod
    def _check_services_running(cls):
        """Check if required services are running."""
        # Check LocalStack
        try:
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.list_buckets()  # This will fail if LocalStack isn't working
            cls.localstack_available = True
        except Exception:
            cls.localstack_available = False
            
        # Check Azurite
        try:
            health_url = f"{AZURITE_ENDPOINT}/"
            response = requests.get(health_url, timeout=5)
            cls.azurite_available = response.status_code in [200, 400]
        except requests.RequestException:
            cls.azurite_available = False
            
        if not cls.localstack_available and not cls.azurite_available:
            pytest.skip("Neither LocalStack nor Azurite are running. Start with: docker-compose up -d")

    @pytest.mark.integration
    def test_aws_storage_initialization(self):
        """Test Storage initialization with AWS backend."""
        if not self.localstack_available:
            pytest.skip("LocalStack not available")
            
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            storage = AWSStorage()
            assert storage.filesystem is not None
            print("✅ AWS Storage initialized successfully")

    @pytest.mark.integration
    def test_azure_storage_initialization(self):
        """Test Storage initialization with Azure backend."""
        if not self.azurite_available:
            pytest.skip("Azurite not available")
            
        with patch.dict(os.environ, {
            'AZURE_STORAGE_ACCOUNT_NAME': 'testingstorage',
            'AZURE_STORAGE_ACCOUNT_KEY': 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
        }):
            storage = AzureStorage()
            assert storage.filesystem is not None
            print("✅ Azure Storage initialized successfully")

    @pytest.mark.integration
    def test_aws_parquet_operations(self):
        """Test parquet file operations with AWS S3."""
        if not self.localstack_available:
            pytest.skip("LocalStack not available")
            
        # Set up test environment
        test_bucket = f"test-parquet-{int(time.time())}"
        test_path = f"{test_bucket}/test-data.parquet"
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Create bucket first
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.create_bucket(Bucket=test_bucket)
            
            try:
                storage = AWSStorage()
                
                # Create test data
                test_data = {
                    'policy_id': ['POL-001', 'POL-002', 'POL-003'],
                    'premium': [1200.50, 850.25, 2100.00],
                    'holder_id': ['H001', 'H002', 'H003']
                }
                test_table = pa.table(test_data)
                
                # Write parquet file
                storage.write_parquet(test_path, test_table)
                
                # Read parquet file back
                read_table = storage.read_parquet(test_path)
                
                # Verify data
                assert read_table.num_rows == test_table.num_rows
                assert read_table.column_names == test_table.column_names
                
                # Verify specific data
                policy_ids = read_table.column('policy_id').to_pylist()
                assert policy_ids == ['POL-001', 'POL-002', 'POL-003']
                
                print("✅ AWS parquet operations completed successfully")
                
            finally:
                # Cleanup
                try:
                    s3_client.delete_object(Bucket=test_bucket, Key="test-data.parquet")
                    s3_client.delete_bucket(Bucket=test_bucket)
                except:
                    pass

    @pytest.mark.integration
    def test_aws_json_operations(self):
        """Test JSON file operations with AWS S3."""
        if not self.localstack_available:
            pytest.skip("LocalStack not available")
            
        # Set up test environment
        test_bucket = f"test-json-{int(time.time())}"
        test_path = f"{test_bucket}/test-manifest.json"
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Create bucket first
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.create_bucket(Bucket=test_bucket)
            
            try:
                storage = AWSStorage()
                
                # Create test JSON data
                test_json = {
                    'version': '1.0',
                    'tables': ['policy_holders', 'claims'],
                    'timestamp': '2024-01-01T00:00:00Z'
                }
                
                # Upload JSON using S3 client (since Storage doesn't have write_json)
                s3_client.put_object(
                    Bucket=test_bucket,
                    Key="test-manifest.json",
                    Body=json.dumps(test_json),
                    ContentType='application/json'
                )
                
                # Read JSON file using Storage
                read_json = storage.read_json(test_path)
                
                # Verify data (PyArrow converts JSON to columnar format)
                assert isinstance(read_json, dict)
                assert 'version' in read_json
                assert 'tables' in read_json
                
                # PyArrow stores scalar values as single-item lists
                assert read_json['version'] == ['1.0']
                assert read_json['tables'] == [['policy_holders', 'claims']]
                # Timestamp gets converted to datetime object
                assert len(read_json['timestamp']) == 1
                
                print("✅ AWS JSON operations completed successfully")
                
            finally:
                # Cleanup
                try:
                    s3_client.delete_object(Bucket=test_bucket, Key="test-manifest.json")
                    s3_client.delete_bucket(Bucket=test_bucket)
                except:
                    pass

    @pytest.mark.integration
    def test_aws_file_listing(self):
        """Test file listing operations with AWS S3."""
        if not self.localstack_available:
            pytest.skip("LocalStack not available")
            
        test_bucket = f"test-listing-{int(time.time())}"
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Create bucket and test files
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.create_bucket(Bucket=test_bucket)
            
            # Upload test files
            test_files = [
                "data/file1.parquet",
                "data/file2.parquet", 
                "manifest.json"
            ]
            
            for file_path in test_files:
                s3_client.put_object(
                    Bucket=test_bucket,
                    Key=file_path,
                    Body=f"Test content for {file_path}"
                )
            
            try:
                storage = AWSStorage()
                
                # List all files in bucket
                all_files = storage.list_files(test_bucket)
                
                # Verify we get some files/directories (list_files returns dirs and files at root level)
                assert len(all_files) > 0
                
                # Check that we can find the manifest file and data directory
                file_names = [path.split('/')[-1] for path in all_files]
                has_manifest = any('manifest.json' in path for path in all_files)
                has_data_dir = any('data' in path for path in all_files)
                
                assert has_manifest or has_data_dir, f"Expected to find manifest or data directory in {all_files}"
                
                # List files in data subdirectory
                data_files = storage.list_files(f"{test_bucket}/data")
                assert len(data_files) >= 2  # Should find the parquet files
                
                print(f"✅ AWS file listing completed successfully - found {len(all_files)} files")
                
            finally:
                # Cleanup
                try:
                    for file_path in test_files:
                        s3_client.delete_object(Bucket=test_bucket, Key=file_path)
                    s3_client.delete_bucket(Bucket=test_bucket)
                except:
                    pass

    @pytest.mark.integration
    def test_real_example_data_with_aws(self):
        """Test Storage operations with real example data on AWS."""
        if not self.localstack_available:
            pytest.skip("LocalStack not available")
            
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        if not examples_dir.exists():
            pytest.skip("Examples directory not found")
            
        test_bucket = f"test-real-data-{int(time.time())}"
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Create bucket and upload example data
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.create_bucket(Bucket=test_bucket)
            
            # Upload example files
            uploaded_files = []
            for file_path in examples_dir.rglob('*'):
                if file_path.is_file():
                    relative_path = file_path.relative_to(examples_dir)
                    s3_key = str(relative_path).replace('\\', '/')
                    
                    s3_client.upload_file(str(file_path), test_bucket, s3_key)
                    uploaded_files.append(s3_key)
            
            try:
                storage = AWSStorage()
                
                # Test reading manifest.json
                manifest_path = f"{test_bucket}/cda/manifest.json"
                manifest_data = storage.read_json(manifest_path)
                
                assert isinstance(manifest_data, dict)
                assert 'policy_holders' in manifest_data
                
                # Test reading a parquet file
                parquet_files = [f for f in uploaded_files if f.endswith('.parquet')]
                if parquet_files:
                    parquet_path = f"{test_bucket}/{parquet_files[0]}"
                    parquet_table = storage.read_parquet(parquet_path)
                    
                    assert parquet_table.num_rows > 0
                    assert len(parquet_table.column_names) > 0
                    
                    print(f"✅ Read parquet file with {parquet_table.num_rows} rows and {len(parquet_table.column_names)} columns")
                
                # Test file listing
                cda_files = storage.list_files(f"{test_bucket}/cda")
                assert len(cda_files) > 0
                
                print(f"✅ Real example data operations completed - processed {len(uploaded_files)} files")
                
            finally:
                # Cleanup
                try:
                    for s3_key in uploaded_files:
                        s3_client.delete_object(Bucket=test_bucket, Key=s3_key)
                    s3_client.delete_bucket(Bucket=test_bucket)
                except:
                    pass

    @pytest.mark.integration
    def test_storage_error_handling(self):
        """Test Storage class error handling."""
        if not self.localstack_available:
            pytest.skip("LocalStack not available")
            
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            storage = AWSStorage()
            
            # Test reading non-existent file
            with pytest.raises(Exception):  # Should raise some kind of exception
                storage.read_json("non-existent-bucket/non-existent-file.json")
            
            # Test reading non-existent parquet file
            with pytest.raises(Exception):
                storage.read_parquet("non-existent-bucket/non-existent-file.parquet")
            
            # Test listing non-existent directory
            with pytest.raises(Exception):
                storage.list_files("non-existent-bucket")
                
            print("✅ Error handling tests completed successfully")

    @pytest.mark.integration
    def test_storage_initialization_errors(self):
        """Test Storage initialization error handling."""
        # Test missing AWS credentials
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                AWSStorage()
        
        # Test missing Azure credentials  
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                AzureStorage()
        
        # Test invalid cloud provider - no longer applicable with direct class instantiation
        # Each storage class is now instantiated directly (AzureStorage(), AWSStorage())
        
        print("✅ Initialization error handling tests completed successfully")
