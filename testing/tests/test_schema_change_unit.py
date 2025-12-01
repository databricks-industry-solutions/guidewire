"""
Unit tests for schema change functionality.

These tests focus on the new schema metadata change operations,
using mocks to isolate functionality and run quickly without external dependencies.

Run with: pytest testing/tests/test_schema_change_unit.py -v -m unit
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
import pyarrow as pa
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from guidewire.delta_log import BaseDeltaLog, AWSDeltaLog, AzureDeltaLog, DeltaError
from guidewire.batch import Batch
from guidewire.manifest import Manifest
from guidewire.results import Result


class TestSchemaMetadataChange:
    """Unit tests for schema metadata change functionality."""
    
    @pytest.mark.unit
    def test_add_schema_metadata_change_success(self):
        """Test successful schema metadata change operation."""
        # Create a mock delta log instance
        delta_log = AWSDeltaLog.__new__(AWSDeltaLog)
        delta_log.log_uri = "s3://test-bucket/test-table/"
        delta_log.storage_options = {"aws_access_key_id": "test"}
        delta_log.table_name = "test_table"
        
        # Create a test schema
        test_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("new_field", pa.float64())  # New field in schema
        ])
        
        # Mock the write_deltalake function
        with patch('guidewire.delta_log.write_deltalake') as mock_write, \
             patch.object(delta_log, '_log_exists') as mock_log_exists:
            
            # Call the method
            delta_log.add_schema_metadata_change(test_schema)
            
            # Verify write_deltalake was called correctly
            mock_write.assert_called_once()
            call_args = mock_write.call_args
            
            assert call_args[1]['table_or_uri'] == "s3://test-bucket/test-table/"
            assert call_args[1]['mode'] == "append"
            assert call_args[1]['schema_mode'] == "merge"
            assert call_args[1]['storage_options'] == {"aws_access_key_id": "test"}
            
            # Verify the data is an empty table with correct schema
            empty_table = call_args[1]['data']
            assert isinstance(empty_table, pa.Table)
            assert empty_table.num_rows == 0
            assert empty_table.schema.equals(test_schema)
            
            # Verify _log_exists was called to refresh
            mock_log_exists.assert_called_once()
    
    @pytest.mark.unit
    def test_add_schema_metadata_change_write_failure(self):
        """Test schema metadata change operation when write_deltalake fails."""
        delta_log = AzureDeltaLog.__new__(AzureDeltaLog)
        delta_log.log_uri = "abfss://container@account.dfs.core.windows.net/test-table/"
        delta_log.storage_options = {}
        delta_log.table_name = "test_table"
        
        test_schema = pa.schema([pa.field("id", pa.int64())])
        
        with patch('guidewire.delta_log.write_deltalake') as mock_write:
            mock_write.side_effect = Exception("Write failed")
            
            with pytest.raises(DeltaError, match="Failed to add schema metadata change: Write failed"):
                delta_log.add_schema_metadata_change(test_schema)
    
    @pytest.mark.unit
    def test_add_schema_metadata_change_log_refresh_failure(self):
        """Test schema metadata change when log refresh fails (should not raise error)."""
        delta_log = AWSDeltaLog.__new__(AWSDeltaLog)
        delta_log.log_uri = "s3://test-bucket/test-table/"
        delta_log.storage_options = {}
        delta_log.table_name = "test_table"
        
        test_schema = pa.schema([pa.field("id", pa.int64())])
        
        with patch('guidewire.delta_log.write_deltalake') as mock_write, \
             patch.object(delta_log, '_log_exists') as mock_log_exists, \
             patch('guidewire.delta_log.L') as mock_logger:
            
            mock_log_exists.side_effect = Exception("Refresh failed")
            
            # Should not raise error, just log warning
            delta_log.add_schema_metadata_change(test_schema)
            
            mock_write.assert_called_once()
            mock_logger.warning.assert_called_once_with(
                "Failed to refresh delta log after schema change: Refresh failed"
            )


class TestBatchSchemaChangeIntegration:
    """Unit tests for batch processing with schema changes."""
    
    @pytest.fixture
    def mock_batch(self):
        """Create a mock batch instance for testing."""
        with patch('guidewire.batch.AzureDeltaLog') as mock_delta_log_class, \
             patch('guidewire.batch.Manifest') as mock_manifest_class:
            
            # Setup mock manifest
            mock_manifest = Mock()
            mock_manifest.read.return_value = {
                "totalProcessedRecordsCount": 1000,
                "lastSuccessfulWriteTimestamp": 1680500000000,
                "dataFilesPath": "s3://test-bucket/test-table/",
                "schemaHistory": {"schema1": "1680000000000"}
            }
            
            # Setup mock filesystem
            mock_fs = Mock()
            mock_file_info = Mock()
            mock_file_info.path = "test.parquet"
            mock_file_info.mtime_ns = 1680000000000
            mock_file_info.size = 1000
            
            # Import FileType to use the actual enum
            from pyarrow.fs import FileType
            mock_file_info.type = FileType.File
            
            # Mock get_file_info to return list of file info objects
            mock_fs.get_file_info.return_value = [mock_file_info]
            mock_manifest.fs = mock_fs
            
            # Setup mock delta log
            mock_delta_log = Mock()
            mock_delta_log.table_exists.return_value = True
            mock_delta_log._get_watermark_from_log.return_value = {
                "watermark": 1680000000000,
                "schema_timestamp": 1680000000000
            }
            mock_delta_log.delta_log.version.return_value = 5
            mock_delta_log_class.return_value = mock_delta_log
            
            batch = Batch(
                table_name="test_table",
                manifest=mock_manifest,
                target_cloud="azure",
                storage_or_s3_name="testaccount",
                storage_container="testcontainer",
                reset=False
            )
            
            return batch, mock_delta_log
    
    @pytest.mark.unit
    def test_schema_change_individual_processing_not_partial(self, mock_batch):
        """Test schema change detection in individual processing mode when not partial."""
        batch, mock_delta_log = mock_batch
        
        # Mock schema finding
        test_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        batch.cached_schema = test_schema
        
        with patch.object(batch, '_schema_finder', return_value=True):
            # Test non-partial case (should trigger schema metadata change)
            batch._process_schema_history_individual(
                valid_timestamp_folders=["s3://test-bucket/test-table/1680500000000"],
                schema_timestamp=1680000000000,
                partial=False,  # Not partial - should trigger schema change
                folder="s3://test-bucket/test-table/"
            )
            
            # Verify schema metadata change was called
            mock_delta_log.add_schema_metadata_change.assert_called_once_with(test_schema)
            
            # Verify regular transaction was also called
            mock_delta_log.add_transaction.assert_called()
    
    @pytest.mark.unit
    def test_schema_change_individual_processing_partial(self, mock_batch):
        """Test schema change detection in individual processing mode when partial."""
        batch, mock_delta_log = mock_batch
        
        test_schema = pa.schema([pa.field("id", pa.int64())])
        batch.cached_schema = test_schema
        
        with patch.object(batch, '_schema_finder', return_value=True):
            # Test partial case (should NOT trigger schema metadata change)
            batch._process_schema_history_individual(
                valid_timestamp_folders=["s3://test-bucket/test-table/1680500000000"],
                schema_timestamp=1680000000000,
                partial=True,  # Partial - should NOT trigger schema change
                folder="s3://test-bucket/test-table/"
            )
            
            # Verify schema metadata change was NOT called
            mock_delta_log.add_schema_metadata_change.assert_not_called()
            
            # Verify regular transaction was still called
            mock_delta_log.add_transaction.assert_called()
    
    @pytest.mark.unit
    def test_schema_change_batched_processing_not_partial(self, mock_batch):
        """Test schema change detection in batched processing mode when not partial."""
        batch, mock_delta_log = mock_batch
        
        test_schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.float64())])
        batch.cached_schema = test_schema
        
        with patch.object(batch, '_schema_finder', return_value=True), \
             patch.object(batch, '_get_parquet_list') as mock_get_files:
            
            # Mock file list
            mock_get_files.return_value = [
                {"relative_path": "file1.parquet", "path": "s3://bucket/file1.parquet", 
                 "last_modified": 123456, "size": 1000}
            ]
            
            # Test non-partial case
            batch._process_schema_history_batched(
                valid_timestamp_folders=["s3://test-bucket/test-table/1680500000000"],
                schema_timestamp=1680000000000,
                partial=False,  # Not partial - should trigger schema change
                folder="s3://test-bucket/test-table/"
            )
            
            # Verify schema metadata change was called
            mock_delta_log.add_schema_metadata_change.assert_called_once_with(test_schema)
            
            # Verify regular transaction was also called
            mock_delta_log.add_transaction.assert_called()
    
    @pytest.mark.unit
    def test_schema_change_batched_processing_partial(self, mock_batch):
        """Test schema change detection in batched processing mode when partial."""
        batch, mock_delta_log = mock_batch
        
        test_schema = pa.schema([pa.field("id", pa.int64())])
        batch.cached_schema = test_schema
        
        with patch.object(batch, '_schema_finder', return_value=True), \
             patch.object(batch, '_get_parquet_list') as mock_get_files:
            
            mock_get_files.return_value = [
                {"relative_path": "file1.parquet", "path": "s3://bucket/file1.parquet", 
                 "last_modified": 123456, "size": 1000}
            ]
            
            # Test partial case
            batch._process_schema_history_batched(
                valid_timestamp_folders=["s3://test-bucket/test-table/1680500000000"],
                schema_timestamp=1680000000000,
                partial=True,  # Partial - should NOT trigger schema change
                folder="s3://test-bucket/test-table/"
            )
            
            # Verify schema metadata change was NOT called
            mock_delta_log.add_schema_metadata_change.assert_not_called()
            
            # Verify regular transaction was still called
            mock_delta_log.add_transaction.assert_called()
    
    @pytest.mark.unit
    def test_schema_change_table_not_exists(self, mock_batch):
        """Test that schema change is not triggered when table doesn't exist."""
        batch, mock_delta_log = mock_batch
        
        # Mock table as not existing
        mock_delta_log.table_exists.return_value = False
        
        test_schema = pa.schema([pa.field("id", pa.int64())])
        batch.cached_schema = test_schema
        
        with patch.object(batch, '_schema_finder', return_value=True):
            # Test non-partial case with non-existing table
            batch._process_schema_history_individual(
                valid_timestamp_folders=["s3://test-bucket/test-table/1680500000000"],
                schema_timestamp=1680000000000,
                partial=False,  # Not partial, but table doesn't exist
                folder="s3://test-bucket/test-table/"
            )
            
            # Verify schema metadata change was NOT called (table doesn't exist)
            mock_delta_log.add_schema_metadata_change.assert_not_called()
            
            # Verify regular transaction was still called
            mock_delta_log.add_transaction.assert_called()


class TestEmptyTableCreation:
    """Unit tests for empty table creation from schema."""
    
    @pytest.mark.unit
    def test_empty_table_creation_simple_schema(self):
        """Test creating empty table from simple schema."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string())
        ])
        
        # Create empty arrays for each field
        empty_arrays = []
        for field in schema:
            empty_array = pa.array([], type=field.type)
            empty_arrays.append(empty_array)
        empty_table = pa.table(empty_arrays, schema=schema)
        
        assert empty_table.num_rows == 0
        assert empty_table.num_columns == 2
        assert empty_table.schema.equals(schema)
        assert empty_table.column_names == ["id", "name"]
    
    @pytest.mark.unit
    def test_empty_table_creation_complex_schema(self):
        """Test creating empty table from complex schema with various data types."""
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("price", pa.float64()),
            pa.field("active", pa.bool_()),
            pa.field("created_at", pa.timestamp('us')),
            pa.field("tags", pa.list_(pa.string())),
            pa.field("metadata", pa.struct([
                pa.field("version", pa.int32()),
                pa.field("source", pa.string())
            ]))
        ])
        
        # Create empty arrays for each field
        empty_arrays = []
        for field in schema:
            empty_array = pa.array([], type=field.type)
            empty_arrays.append(empty_array)
        empty_table = pa.table(empty_arrays, schema=schema)
        
        assert empty_table.num_rows == 0
        assert empty_table.num_columns == 7
        assert empty_table.schema.equals(schema)
        assert "metadata" in empty_table.column_names
        assert "tags" in empty_table.column_names


class TestSchemaNullabilityEnforcement:
    """Unit tests for schema nullable enforcement."""
    
    @pytest.mark.unit
    def test_make_schema_nullable_basic(self):
        """Test that _make_schema_nullable converts all fields to nullable=True."""
        # Create a mock delta log instance
        delta_log = AWSDeltaLog.__new__(AWSDeltaLog)
        
        # Create schema with non-nullable fields
        non_nullable_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("price", pa.float64(), nullable=False)
        ])
        
        # Apply nullable transformation
        nullable_schema = delta_log._make_schema_nullable(non_nullable_schema)
        
        # Verify all fields are nullable
        for field in nullable_schema:
            assert field.nullable is True, f"Field {field.name} should be nullable"
        
        # Verify field names and types are preserved
        assert len(nullable_schema) == 3
        assert nullable_schema.field("id").type == pa.int64()
        assert nullable_schema.field("name").type == pa.string()
        assert nullable_schema.field("price").type == pa.float64()
    
    @pytest.mark.unit
    def test_make_schema_nullable_mixed(self):
        """Test _make_schema_nullable with mixed nullable and non-nullable fields."""
        delta_log = AzureDeltaLog.__new__(AzureDeltaLog)
        
        # Create schema with mixed nullable fields
        mixed_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("optional_field", pa.float64(), nullable=True),
            pa.field("required_field", pa.bool_(), nullable=False)
        ])
        
        # Apply nullable transformation
        nullable_schema = delta_log._make_schema_nullable(mixed_schema)
        
        # Verify all fields are now nullable (even those that were already nullable)
        for field in nullable_schema:
            assert field.nullable is True, f"Field {field.name} should be nullable"
    
    @pytest.mark.unit
    def test_make_schema_nullable_complex_types(self):
        """Test _make_schema_nullable with complex data types."""
        delta_log = AWSDeltaLog.__new__(AWSDeltaLog)
        
        # Create schema with complex types
        complex_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("tags", pa.list_(pa.string()), nullable=False),
            pa.field("metadata", pa.struct([
                pa.field("version", pa.int32()),
                pa.field("source", pa.string())
            ]), nullable=False),
            pa.field("timestamp", pa.timestamp('us'), nullable=False)
        ])
        
        # Apply nullable transformation
        nullable_schema = delta_log._make_schema_nullable(complex_schema)
        
        # Verify all top-level fields are nullable
        for field in nullable_schema:
            assert field.nullable is True, f"Field {field.name} should be nullable"
        
        # Verify types are preserved
        assert nullable_schema.field("tags").type == pa.list_(pa.string())
        assert nullable_schema.field("timestamp").type == pa.timestamp('us')
    
    @pytest.mark.unit
    def test_add_transaction_applies_nullable_to_schema(self):
        """Test that add_transaction properly converts schema to nullable before writing."""
        from deltalake.schema import Schema as DeltaSchema
        
        # Create a mock delta log instance
        delta_log = AWSDeltaLog.__new__(AWSDeltaLog)
        delta_log.log_uri = "s3://test-bucket/test-table/"
        delta_log.storage_options = {"aws_access_key_id": "test"}
        delta_log.table_name = "test_table"
        delta_log.delta_log = None  # Simulate new table
        
        # Create schema with non-nullable fields
        non_nullable_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False)
        ])
        
        parquet_info = [{
            "path": "s3a://test-bucket/test-table/part-00000.parquet",
            "size": 1024,
            "last_modified": 1234567890
        }]
        
        # Mock the create_table_with_add_actions function
        with patch('guidewire.delta_log.create_table_with_add_actions') as mock_create, \
             patch.object(delta_log, '_log_exists'):
            
            # Call add_transaction with non-nullable schema
            delta_log.add_transaction(
                parquets=parquet_info,
                schema=non_nullable_schema,
                watermark=1234567890,
                schema_timestamp=1234567890,
                mode="overwrite"
            )
            
            # Verify create_table_with_add_actions was called
            mock_create.assert_called_once()
            call_args = mock_create.call_args
            
            # Get the schema that was passed
            passed_schema = call_args[1]['schema']
            
            # Verify it's a DeltaSchema
            assert isinstance(passed_schema, DeltaSchema)
            
            # Convert back to PyArrow to check nullability
            passed_pa_schema = passed_schema.to_arrow()
            
            # Verify all fields are nullable
            for field in passed_pa_schema:
                assert field.nullable is True, f"Field {field.name} should be nullable in delta table"
    
    @pytest.mark.unit
    def test_add_schema_metadata_change_applies_nullable(self):
        """Test that add_schema_metadata_change properly converts schema to nullable."""
        delta_log = AzureDeltaLog.__new__(AzureDeltaLog)
        delta_log.log_uri = "abfss://container@account.dfs.core.windows.net/test-table/"
        delta_log.storage_options = {}
        delta_log.table_name = "test_table"
        
        # Create schema with non-nullable fields
        non_nullable_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("new_field", pa.string(), nullable=False)
        ])
        
        # Mock the write_deltalake function
        with patch('guidewire.delta_log.write_deltalake') as mock_write, \
             patch.object(delta_log, '_log_exists'):
            
            # Call add_schema_metadata_change with non-nullable schema
            delta_log.add_schema_metadata_change(non_nullable_schema)
            
            # Verify write_deltalake was called
            mock_write.assert_called_once()
            call_args = mock_write.call_args
            
            # Get the empty table that was passed
            empty_table = call_args[1]['data']
            
            # Verify all fields in the table schema are nullable
            for field in empty_table.schema:
                assert field.nullable is True, f"Field {field.name} should be nullable in schema metadata change"
