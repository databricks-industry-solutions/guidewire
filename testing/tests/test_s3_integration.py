"""
Integration tests for S3 functionality using LocalStack.

These tests require Docker Compose services to be running:
    docker-compose up -d

Run with: pytest testing/tests/test_s3_integration.py -v -m integration
"""

import pytest
import boto3
import os
import time
import requests
import json
from pathlib import Path
from botocore.exceptions import ClientError

# Test configuration
LOCALSTACK_ENDPOINT = "http://localhost:4566"
TEST_BUCKET_PREFIX = "test-guidewire-cda"


class TestS3Integration:
    """Integration tests for S3 operations using LocalStack."""
    
    @classmethod
    def setup_class(cls):
        """Set up class-level resources."""
        cls._check_localstack_running()
        cls.s3_client = boto3.client(
            's3',
            endpoint_url=LOCALSTACK_ENDPOINT,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        
    @classmethod
    def _check_localstack_running(cls):
        """Check if LocalStack is running and accessible."""
        try:
            # Test S3 functionality directly instead of health endpoint
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.list_buckets()  # This will fail if LocalStack isn't working
        except Exception:
            pytest.skip("LocalStack not running. Start with: docker-compose up -d")
    
    def setup_method(self):
        """Set up method-level resources."""
        self.test_bucket = f"{TEST_BUCKET_PREFIX}-{os.getpid()}-{int(time.time())}"
        self.s3_client.create_bucket(Bucket=self.test_bucket)
        
    def teardown_method(self):
        """Clean up method-level resources."""
        try:
            # Delete all objects in bucket
            response = self.s3_client.list_objects_v2(Bucket=self.test_bucket)
            if 'Contents' in response:
                objects = [{'Key': obj['Key']} for obj in response['Contents']]
                self.s3_client.delete_objects(
                    Bucket=self.test_bucket,
                    Delete={'Objects': objects}
                )
            
            # Delete bucket
            self.s3_client.delete_bucket(Bucket=self.test_bucket)
        except ClientError:
            pass  # Ignore cleanup errors
    
    @pytest.mark.integration
    def test_create_and_list_bucket(self):
        """Test basic S3 bucket operations."""
        # List buckets - our test bucket should exist
        buckets = self.s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in buckets['Buckets']]
        
        assert self.test_bucket in bucket_names
        
    @pytest.mark.integration
    def test_upload_and_download_file(self):
        """Test file upload and download operations."""
        test_content = b"Hello from integration test!"
        test_key = "test-file.txt"
        
        # Upload file
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key=test_key,
            Body=test_content
        )
        
        # Download and verify
        response = self.s3_client.get_object(Bucket=self.test_bucket, Key=test_key)
        downloaded_content = response['Body'].read()
        
        assert downloaded_content == test_content
        
    @pytest.mark.integration
    def test_upload_example_data(self):
        """Test uploading the example CDA data to S3."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        
        if not examples_dir.exists():
            pytest.skip("Examples directory not found")
        
        uploaded_files = []
        
        # Upload all example files
        for file_path in examples_dir.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(examples_dir)
                s3_key = str(relative_path).replace('\\', '/')
                
                self.s3_client.upload_file(
                    str(file_path),
                    self.test_bucket,
                    s3_key
                )
                uploaded_files.append(s3_key)
        
        # Verify files were uploaded
        response = self.s3_client.list_objects_v2(Bucket=self.test_bucket)
        s3_files = [obj['Key'] for obj in response.get('Contents', [])]
        
        assert len(uploaded_files) > 0, "No example files were uploaded"
        
        for uploaded_file in uploaded_files:
            assert uploaded_file in s3_files, f"File {uploaded_file} not found in S3"
            
        print(f"Successfully uploaded {len(uploaded_files)} example files to S3")
        
    @pytest.mark.integration  
    def test_manifest_upload_and_download(self):
        """Test uploading and downloading the manifest.json file."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        manifest_path = examples_dir / "cda" / "manifest.json"
        
        if not manifest_path.exists():
            pytest.skip("Manifest file not found")
            
        # Upload manifest
        s3_key = "cda/manifest.json"
        self.s3_client.upload_file(str(manifest_path), self.test_bucket, s3_key)
        
        # Download and verify content
        response = self.s3_client.get_object(Bucket=self.test_bucket, Key=s3_key)
        downloaded_content = response['Body'].read().decode('utf-8')
        manifest_data = json.loads(downloaded_content)
        
        # Verify manifest structure
        assert isinstance(manifest_data, dict)
        assert 'policy_holders' in manifest_data
        assert 'lastSuccessfulWriteTimestamp' in manifest_data['policy_holders']
        assert 'dataFilesPath' in manifest_data['policy_holders']
        
        print(f"Manifest contains {len(manifest_data)} entities")
        
    @pytest.mark.integration
    def test_parquet_file_handling(self):
        """Test handling of parquet files from examples."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        
        # Find a parquet file
        parquet_files = list(examples_dir.rglob('*.parquet'))
        
        if not parquet_files:
            pytest.skip("No parquet files found in examples")
            
        test_parquet = parquet_files[0]
        relative_path = test_parquet.relative_to(examples_dir)
        s3_key = str(relative_path).replace('\\', '/')
        
        # Upload parquet file
        self.s3_client.upload_file(str(test_parquet), self.test_bucket, s3_key)
        
        # Verify upload
        response = self.s3_client.head_object(Bucket=self.test_bucket, Key=s3_key)
        assert response['ContentLength'] > 0
        
        # Test file properties
        file_info = self.s3_client.list_objects_v2(
            Bucket=self.test_bucket, 
            Prefix=s3_key
        )
        
        found_file = None
        for obj in file_info.get('Contents', []):
            if obj['Key'] == s3_key:
                found_file = obj
                break
                
        assert found_file is not None
        assert found_file['Size'] > 0
        assert s3_key.endswith('.parquet')
        
        print(f"Successfully handled parquet file: {s3_key} ({found_file['Size']} bytes)")

    @pytest.mark.integration
    def test_directory_structure_preservation(self):
        """Test that directory structure is preserved in S3."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        
        if not examples_dir.exists():
            pytest.skip("Examples directory not found")
        
        expected_structure = {}
        
        # Upload files and track structure
        for file_path in examples_dir.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(examples_dir)
                s3_key = str(relative_path).replace('\\', '/')
                
                # Track directory levels
                parts = s3_key.split('/')
                if len(parts) > 1:
                    directory = '/'.join(parts[:-1])
                    if directory not in expected_structure:
                        expected_structure[directory] = []
                    expected_structure[directory].append(parts[-1])
                
                self.s3_client.upload_file(str(file_path), self.test_bucket, s3_key)
        
        # Verify structure in S3
        all_objects = self.s3_client.list_objects_v2(Bucket=self.test_bucket)
        s3_objects = [obj['Key'] for obj in all_objects.get('Contents', [])]
        
        # Check each directory has expected files
        for directory, files in expected_structure.items():
            directory_files = [
                obj.split('/')[-1] for obj in s3_objects 
                if obj.startswith(directory + '/') and obj.count('/') == directory.count('/') + 1
            ]
            
            for expected_file in files:
                assert expected_file in directory_files, f"File {expected_file} not found in directory {directory}"
        
        print(f"Directory structure preserved with {len(expected_structure)} directories")
