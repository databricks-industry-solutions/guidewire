"""
Integration tests for the Manifest class using real storage backends.

These tests require Docker Compose services to be running:
    docker-compose up -d

Run with: pytest testing/tests/test_manifest_integration.py -v -m integration
"""

import pytest
import os
import time
import requests
import json
from pathlib import Path
from unittest.mock import patch

# Import the classes
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from guidewire.manifest import Manifest

# Test configuration
LOCALSTACK_ENDPOINT = "http://localhost:4566"


class TestManifestIntegration:
    """Integration tests for Manifest class with real storage backends."""
    
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
            
        if not cls.localstack_available:
            pytest.skip("LocalStack not running. Start with: docker-compose up -d")

    def setup_method(self):
        """Set up method-level resources."""
        self.test_bucket = f"test-manifest-{int(time.time())}"
        
        # Create bucket
        import boto3
        self.s3_client = boto3.client(
            's3',
            endpoint_url=LOCALSTACK_ENDPOINT,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        self.s3_client.create_bucket(Bucket=self.test_bucket)
        
    def teardown_method(self):
        """Clean up method-level resources."""
        try:
            # Delete all objects and bucket
            response = self.s3_client.list_objects_v2(Bucket=self.test_bucket)
            if 'Contents' in response:
                objects = [{'Key': obj['Key']} for obj in response['Contents']]
                self.s3_client.delete_objects(
                    Bucket=self.test_bucket,
                    Delete={'Objects': objects}
                )
            self.s3_client.delete_bucket(Bucket=self.test_bucket)
        except:
            pass

    @pytest.mark.integration
    def test_manifest_initialization_with_real_data(self):
        """Test Manifest initialization with real example manifest data."""
        # Upload real manifest file
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        manifest_path = examples_dir / "cda" / "manifest.json"
        
        if not manifest_path.exists():
            pytest.skip("Example manifest file not found")
        
        # Upload manifest to S3
        self.s3_client.upload_file(
            str(manifest_path), 
            self.test_bucket, 
            "cda/manifest.json"
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Test initialization with specific table names
            table_names = ['policy_holders']
            manifest = Manifest(
                location=f"{self.test_bucket}/cda",
                table_names=table_names
            )
            
            assert manifest.is_initialized()
            assert manifest.get_table_names() == table_names
            
            print("✅ Manifest initialized successfully with real data")

    @pytest.mark.integration
    def test_manifest_read_specific_entry(self):
        """Test reading specific entries from manifest."""
        # Upload real manifest file
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        manifest_path = examples_dir / "cda" / "manifest.json"
        
        if not manifest_path.exists():
            pytest.skip("Example manifest file not found")
        
        self.s3_client.upload_file(
            str(manifest_path), 
            self.test_bucket, 
            "cda/manifest.json"
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            manifest = Manifest(location=f"{self.test_bucket}/cda")
            
            # Read policy_holders entry
            policy_holders_entry = manifest.read('policy_holders')
            
            assert policy_holders_entry is not None
            assert 'entry' in policy_holders_entry
            assert policy_holders_entry['entry'] == 'policy_holders'
            assert 'lastSuccessfulWriteTimestamp' in policy_holders_entry
            assert 'dataFilesPath' in policy_holders_entry
            assert 'totalProcessedRecordsCount' in policy_holders_entry
            
            print(f"✅ Successfully read manifest entry: {policy_holders_entry['entry']}")
            print(f"   Last timestamp: {policy_holders_entry['lastSuccessfulWriteTimestamp']}")
            print(f"   Records: {policy_holders_entry['totalProcessedRecordsCount']}")

    @pytest.mark.integration
    def test_manifest_get_all_table_names(self):
        """Test getting all table names from manifest."""
        # Upload real manifest file
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        manifest_path = examples_dir / "cda" / "manifest.json"
        
        if not manifest_path.exists():
            pytest.skip("Example manifest file not found")
        
        self.s3_client.upload_file(
            str(manifest_path), 
            self.test_bucket, 
            "cda/manifest.json"
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Initialize without specific table names
            manifest = Manifest(location=f"{self.test_bucket}/cda")
            
            table_names = manifest.get_table_names()
            
            assert isinstance(table_names, list)
            assert len(table_names) > 0
            assert 'policy_holders' in table_names
            
            print(f"✅ Found {len(table_names)} tables: {table_names}")

    @pytest.mark.integration
    def test_manifest_with_custom_data(self):
        """Test Manifest with custom test data."""
        # Create custom test manifest (each table entry is a list with one dict)
        test_manifest = {
            "test_table1": [{
                "lastSuccessfulWriteTimestamp": "1680945093000",
                "totalProcessedRecordsCount": 100,
                "dataFilesPath": "s3://test-bucket/data/test_table1/",
                "schemaHistory": {
                    "schema1": "1680945093000"
                }
            }],
            "test_table2": [{
                "lastSuccessfulWriteTimestamp": "1680945094000", 
                "totalProcessedRecordsCount": 200,
                "dataFilesPath": "s3://test-bucket/data/test_table2/",
                "schemaHistory": {
                    "schema1": "1680945094000"
                }
            }]
        }
        
        # Upload custom manifest
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key="test/manifest.json",
            Body=json.dumps(test_manifest),
            ContentType='application/json'
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Test with specific table filter
            manifest = Manifest(
                location=f"{self.test_bucket}/test",
                table_names=['test_table1']
            )
            
            assert manifest.is_initialized()
            assert manifest.get_table_names() == ['test_table1']
            
            # Read the specific entry
            entry = manifest.read('test_table1')
            assert entry is not None
            assert entry['entry'] == 'test_table1'
            assert entry['totalProcessedRecordsCount'] == 100
            
            # Test reading non-existent entry
            non_existent = manifest.read('test_table2')  # Not in filtered list
            assert non_existent is None
            
            print("✅ Custom manifest data test completed successfully")

    @pytest.mark.integration
    def test_manifest_error_handling(self):
        """Test Manifest error handling scenarios."""
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Test with non-existent manifest location
            with pytest.raises(Exception):
                Manifest(location=f"{self.test_bucket}/non-existent")
            
            # Test with empty location
            with pytest.raises(ValueError):
                Manifest(location="")
            
            print("✅ Error handling tests completed successfully")

    @pytest.mark.integration
    def test_manifest_with_invalid_json(self):
        """Test Manifest behavior with invalid JSON data."""
        # Upload invalid JSON
        invalid_json = "{ invalid json content"
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key="invalid/manifest.json",
            Body=invalid_json,
            ContentType='application/json'
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            # Should raise an exception for invalid JSON
            with pytest.raises(Exception):
                Manifest(location=f"{self.test_bucket}/invalid")
            
            print("✅ Invalid JSON handling test completed successfully")

    @pytest.mark.integration
    def test_manifest_with_empty_json(self):
        """Test Manifest behavior with empty JSON object."""
        # Upload empty JSON object
        empty_json = "{}"
        self.s3_client.put_object(
            Bucket=self.test_bucket,
            Key="empty/manifest.json",
            Body=empty_json,
            ContentType='application/json'
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            manifest = Manifest(location=f"{self.test_bucket}/empty")
            
            assert manifest.is_initialized()
            assert manifest.get_table_names() == []
            
            # Reading any entry should return None
            entry = manifest.read('any_table')
            assert entry is None
            
            print("✅ Empty JSON handling test completed successfully")

    @pytest.mark.integration
    def test_manifest_schema_history_access(self):
        """Test accessing schema history from manifest entries."""
        # Upload real manifest file
        examples_dir = Path(__file__).parent.parent.parent / "examples"
        manifest_path = examples_dir / "cda" / "manifest.json"
        
        if not manifest_path.exists():
            pytest.skip("Example manifest file not found")
        
        self.s3_client.upload_file(
            str(manifest_path), 
            self.test_bucket, 
            "cda/manifest.json"
        )
        
        with patch.dict(os.environ, {
            'AWS_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': 'test',
            'AWS_SECRET_ACCESS_KEY': 'test',
            'AWS_ENDPOINT_URL': LOCALSTACK_ENDPOINT
        }):
            manifest = Manifest(location=f"{self.test_bucket}/cda")
            
            entry = manifest.read('policy_holders')
            assert entry is not None
            
            # Check schema history structure
            if 'schemaHistory' in entry:
                schema_history = entry['schemaHistory']
                assert isinstance(schema_history, dict)
                
                # Should have schema entries with timestamps
                for schema_id, timestamp in schema_history.items():
                    assert isinstance(schema_id, str)
                    assert isinstance(timestamp, str)
                    assert timestamp.isdigit()  # Should be numeric timestamp
                    
                print(f"✅ Schema history contains {len(schema_history)} entries")
            else:
                print("ℹ️  No schema history found in manifest entry")
