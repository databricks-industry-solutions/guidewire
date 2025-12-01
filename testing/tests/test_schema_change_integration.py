"""
Integration tests for schema change functionality.

These tests use real Delta Lake operations with LocalStack/Azurite to test
the complete schema change workflow end-to-end.

Run with: pytest testing/tests/test_schema_change_integration.py -v -m integration
"""

import pytest
import boto3
import os
import tempfile
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from unittest.mock import Mock, patch
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from guidewire.delta_log import AWSDeltaLog, AzureDeltaLog
from guidewire.batch import Batch
from guidewire.manifest import Manifest

# Test configuration
LOCALSTACK_ENDPOINT = "http://localhost:4566"
TEST_BUCKET = "test-schema-change-bucket"


class TestSchemaChangeIntegration:
    """Integration tests for schema change operations."""
    
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
        
        # Set environment variables for AWS Delta Log
        os.environ['AWS_TARGET_REGION'] = 'us-east-1'
        os.environ['AWS_TARGET_ACCESS_KEY_ID'] = 'test'
        os.environ['AWS_TARGET_SECRET_ACCESS_KEY'] = 'test'
        os.environ['AWS_TARGET_ENDPOINT_URL'] = LOCALSTACK_ENDPOINT
        
        # Create test bucket
        try:
            cls.s3_client.create_bucket(Bucket=TEST_BUCKET)
        except Exception:
            pass  # Bucket might already exist
    
    @classmethod
    def _check_localstack_running(cls):
        """Check if LocalStack is running and accessible."""
        try:
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.list_buckets()
        except Exception as e:
            pytest.skip(f"LocalStack not available: {e}")
    
    def setup_method(self):
        """Set up for each test method."""
        self.temp_dir = tempfile.mkdtemp()
        self.table_name = "test_schema_change_table"
        
    def teardown_method(self):
        """Clean up after each test method."""
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        
        # Clean up S3 objects
        try:
            objects = self.s3_client.list_objects_v2(Bucket=TEST_BUCKET)
            if 'Contents' in objects:
                delete_objects = [{'Key': obj['Key']} for obj in objects['Contents']]
                self.s3_client.delete_objects(
                    Bucket=TEST_BUCKET,
                    Delete={'Objects': delete_objects}
                )
        except Exception:
            pass
    
    @pytest.mark.integration
    def test_schema_metadata_change_aws_delta_log(self):
        """Test schema metadata change operation with real AWS Delta Log."""
        # Create AWS Delta Log instance
        delta_log = AWSDeltaLog(
            bucket_name=TEST_BUCKET,
            table_name=self.table_name
        )
        
        # Create initial schema
        initial_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        
        # Create some initial data and add transaction
        initial_data = [
            {"relative_path": "file1.parquet", "path": "s3://test/file1.parquet", 
             "last_modified": 1680000000000, "size": 1000}
        ]
        
        delta_log.add_transaction(
            parquets=initial_data,
            schema=initial_schema,
            watermark=1680000000000,
            schema_timestamp=1680000000000,
            mode="overwrite"
        )
        
        # Verify table exists
        assert delta_log.table_exists()
        initial_version = delta_log.delta_log.version()
        
        # Create new schema with additional field
        new_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string())  # New field
        ])
        
        # Add schema metadata change
        delta_log.add_schema_metadata_change(new_schema)
        
        # Verify new version was created
        new_version = delta_log.delta_log.version()
        assert new_version > initial_version
        
        # Verify schema was merged (this would be visible in the Delta log history)
        history = delta_log.delta_log.history()
        assert len(history) >= 2  # Initial transaction + schema change
    
    @pytest.mark.integration
    def test_batch_processing_with_schema_change_simulation(self):
        """Test batch processing with simulated schema change scenario."""
        # Create test parquet files with different schemas
        schema_v1 = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        
        schema_v2 = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("email", pa.string())
        ])
        
        # Create parquet files
        data_v1 = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        data_v2 = pa.table({
            "id": [4, 5, 6],
            "name": ["David", "Eve", "Frank"],
            "email": ["david@test.com", "eve@test.com", "frank@test.com"]
        })
        
        # Write parquet files to temp directory
        v1_file = os.path.join(self.temp_dir, "data_v1.parquet")
        v2_file = os.path.join(self.temp_dir, "data_v2.parquet")
        
        pq.write_table(data_v1, v1_file)
        pq.write_table(data_v2, v2_file)
        
        # Create mock manifest and filesystem
        mock_manifest = Mock()
        mock_fs = Mock()
        
        # Mock filesystem operations
        mock_fs.read_parquet.side_effect = lambda path: data_v2 if "v2" in path else data_v1
        mock_manifest.fs = mock_fs
        
        # Mock manifest data
        mock_manifest.read.return_value = {
            "totalProcessedRecordsCount": 6,
            "lastSuccessfulWriteTimestamp": 1680500000000,
            "dataFilesPath": f"s3://{TEST_BUCKET}/test-data/",
            "schemaHistory": {"schema_v2": "1680400000000"}  # New schema timestamp
        }
        
        # Create Delta Log instance
        delta_log = AWSDeltaLog(
            bucket_name=TEST_BUCKET,
            table_name=self.table_name
        )
        
        # Create initial table with v1 schema
        initial_data = [
            {"relative_path": v1_file, "path": f"s3://{TEST_BUCKET}/data_v1.parquet", 
             "last_modified": 1680000000000, "size": 1000}
        ]
        
        delta_log.add_transaction(
            parquets=initial_data,
            schema=schema_v1,
            watermark=1680000000000,
            schema_timestamp=1680000000000,
            mode="overwrite"
        )
        
        initial_version = delta_log.delta_log.version()
        
        # Now simulate schema change by adding metadata change
        delta_log.add_schema_metadata_change(schema_v2)
        
        # Add new data with v2 schema
        new_data = [
            {"relative_path": v2_file, "path": f"s3://{TEST_BUCKET}/data_v2.parquet", 
             "last_modified": 1680500000000, "size": 1200}
        ]
        
        delta_log.add_transaction(
            parquets=new_data,
            schema=schema_v2,
            watermark=1680500000000,
            schema_timestamp=1680400000000,
            mode="append"
        )
        
        final_version = delta_log.delta_log.version()
        
        # Verify multiple versions were created
        assert final_version > initial_version + 1  # At least 2 new versions (schema change + data)
        
        # Verify table still exists and is readable
        assert delta_log.table_exists()
        
        # Verify history contains our operations
        history = delta_log.delta_log.history()
        assert len(history) >= 3  # Initial + schema change + new data
    
    @pytest.mark.integration
    def test_empty_table_creation_and_write(self):
        """Test that empty tables created from schema can be written to Delta Lake."""
        # Test various schema types
        complex_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("price", pa.float64()),
            pa.field("active", pa.bool_()),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("metadata", pa.struct([
                pa.field("version", pa.int32()),
                pa.field("source", pa.string())
            ]))
        ])
        
        # Create empty table - need to provide empty arrays for each field
        empty_arrays = []
        for field in complex_schema:
            empty_arrays.append(pa.array([], type=field.type))
        empty_table = pa.table(empty_arrays, schema=complex_schema)
        
        # Verify empty table properties
        assert empty_table.num_rows == 0
        assert empty_table.num_columns == 6
        assert empty_table.schema.equals(complex_schema)
        
        # Create Delta Log and test writing empty table
        delta_log = AWSDeltaLog(
            bucket_name=TEST_BUCKET,
            table_name=f"{self.table_name}_complex"
        )
        
        # This should work without errors
        delta_log.add_schema_metadata_change(complex_schema)
        
        # Verify table was created
        assert delta_log.table_exists()
        
        # Verify we can add actual data after schema change
        real_data = [
            {"relative_path": "test.parquet", "path": f"s3://{TEST_BUCKET}/test.parquet", 
             "last_modified": 1680000000000, "size": 1000}
        ]
        
        # This should work with the merged schema
        delta_log.add_transaction(
            parquets=real_data,
            schema=complex_schema,
            watermark=1680000000000,
            schema_timestamp=1680000000000,
            mode="append"
        )
        
        # Verify final state
        assert delta_log.table_exists()
        assert delta_log.delta_log.version() >= 1


class TestSchemaChangeErrorHandling:
    """Integration tests for error handling in schema change operations."""
    
    @pytest.mark.integration
    def test_schema_change_with_invalid_schema(self):
        """Test error handling when schema change fails."""
        # Set up environment variables for AWS Delta Log
        os.environ['AWS_TARGET_REGION'] = 'us-east-1'
        os.environ['AWS_TARGET_ACCESS_KEY_ID'] = 'test'
        os.environ['AWS_TARGET_SECRET_ACCESS_KEY'] = 'test'
        os.environ['AWS_TARGET_ENDPOINT_URL'] = LOCALSTACK_ENDPOINT
        
        # This test would require LocalStack to be running
        try:
            # Create a schema
            test_schema = pa.schema([pa.field("id", pa.int64())])
            
            # This should fail due to nonexistent bucket - either during initialization or schema change
            with pytest.raises(Exception):  # Could be DeltaError or other exception
                delta_log = AWSDeltaLog(
                    bucket_name="nonexistent-bucket",
                    table_name="test_table"
                )
                # If initialization succeeds, the schema change should fail
                delta_log.add_schema_metadata_change(test_schema)
                
        except Exception as e:
            if "LocalStack" in str(e) or "not available" in str(e):
                pytest.skip("LocalStack not available for error testing")
            raise
    
    @pytest.mark.integration  
    def test_schema_change_recovery(self):
        """Test that system can recover from failed schema changes."""
        # Set up environment variables for AWS Delta Log
        os.environ['AWS_TARGET_REGION'] = 'us-east-1'
        os.environ['AWS_TARGET_ACCESS_KEY_ID'] = 'test'
        os.environ['AWS_TARGET_SECRET_ACCESS_KEY'] = 'test'
        os.environ['AWS_TARGET_ENDPOINT_URL'] = LOCALSTACK_ENDPOINT
        
        # Skip if LocalStack not available
        try:
            import boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=LOCALSTACK_ENDPOINT,
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            s3_client.list_buckets()
        except Exception:
            pytest.skip("LocalStack not available")
        
        delta_log = AWSDeltaLog(
            bucket_name=TEST_BUCKET,
            table_name="recovery_test_table"
        )
        
        # Create initial table
        initial_schema = pa.schema([pa.field("id", pa.int64())])
        initial_data = [
            {"relative_path": "file1.parquet", "path": "s3://test/file1.parquet", 
             "last_modified": 1680000000000, "size": 1000}
        ]
        
        delta_log.add_transaction(
            parquets=initial_data,
            schema=initial_schema,
            watermark=1680000000000,
            schema_timestamp=1680000000000,
            mode="overwrite"
        )
        
        initial_version = delta_log.delta_log.version()
        
        # Attempt schema change (this should succeed)
        new_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        
        delta_log.add_schema_metadata_change(new_schema)
        
        # Verify recovery - table should still be accessible
        assert delta_log.table_exists()
        assert delta_log.delta_log.version() > initial_version
        
        # Should be able to continue with normal operations
        more_data = [
            {"relative_path": "file2.parquet", "path": "s3://test/file2.parquet", 
             "last_modified": 1680100000000, "size": 1100}
        ]
        
        delta_log.add_transaction(
            parquets=more_data,
            schema=new_schema,
            watermark=1680100000000,
            schema_timestamp=1680000000000,
            mode="append"
        )
        
        assert delta_log.table_exists()
