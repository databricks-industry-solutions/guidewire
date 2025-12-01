"""
Unit tests for watermarking functionality.

These tests focus on individual components and methods related to watermarking,
using mocks to isolate functionality and run quickly without external dependencies.

Run with: pytest testing/tests/test_watermarking_unit.py -v -m unit
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from guidewire.results import Result
from guidewire.delta_log import BaseDeltaLog, AWSDeltaLog, AzureDeltaLog


class TestResultWatermarkTracking:
    """Unit tests for Result class watermark tracking functionality."""
    
    @pytest.mark.unit
    def test_result_initialization(self):
        """Test that Result objects initialize watermark fields correctly."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=1680000000000,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680500000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        assert result.process_start_watermark == 1680000000000
        assert result.manifest_watermark == 1680500000000
        assert result.process_finish_watermark is None
        assert result.watermarks == []
        assert result.schema_timestamps == []

    @pytest.mark.unit
    def test_add_watermark(self):
        """Test adding watermarks to Result object."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        # Test adding single watermark
        result.add_watermark(1680000000000)
        assert len(result.watermarks) == 1
        assert result.watermarks[0] == 1680000000000
        
        # Test adding multiple watermarks
        result.add_watermark(1680100000000)
        result.add_watermark(1680200000000)
        assert len(result.watermarks) == 3
        assert result.watermarks == [1680000000000, 1680100000000, 1680200000000]

    @pytest.mark.unit
    def test_add_schema_timestamp(self):
        """Test adding schema timestamps to Result object."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        # Test adding schema timestamps
        result.add_schema_timestamp(1680000000000)
        result.add_schema_timestamp(1680500000000)
        
        assert len(result.schema_timestamps) == 2
        assert result.schema_timestamps == [1680000000000, 1680500000000]

    @pytest.mark.unit
    def test_result_update_watermark_fields(self):
        """Test updating watermark-related fields using the update method."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        # Test updating finish watermark
        finish_time = datetime.now()
        result.update(
            process_finish_time=finish_time,
            process_finish_watermark=1680300000000,
            process_finish_version=5
        )
        
        assert result.process_finish_time == finish_time
        assert result.process_finish_watermark == 1680300000000
        assert result.process_finish_version == 5

    @pytest.mark.unit
    def test_watermark_tracking_with_errors(self):
        """Test that watermark tracking works correctly even when errors are present."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        # Add watermarks and errors
        result.add_watermark(1680000000000)
        result.add_error("Test error message")
        result.add_watermark(1680100000000)
        result.add_warning("Test warning")
        result.add_watermark(1680200000000)
        
        # Verify watermarks are tracked correctly despite errors
        assert len(result.watermarks) == 3
        assert len(result.errors) == 1
        assert len(result.warnings) == 1
        assert result.watermarks == [1680000000000, 1680100000000, 1680200000000]


class TestBaseDeltaLogWatermarkMethods:
    """Unit tests for BaseDeltaLog watermark-related methods."""
    
    @pytest.mark.unit
    def test_get_watermark_from_log_new_table(self):
        """Test watermark retrieval for a new table (no existing log)."""
        # Create a mock delta log
        mock_delta_log = Mock(spec=AWSDeltaLog)
        mock_delta_log.table_name = "test_table"
        mock_delta_log.table_exists.return_value = False
        
        # Call the method
        result = AWSDeltaLog._get_watermark_from_log(mock_delta_log)
        
        # Should return defaults for new table
        assert result == {"watermark": 0, "schema_timestamp": 0}

    @pytest.mark.unit
    def test_get_watermark_from_log_existing_table(self):
        """Test watermark retrieval for existing table with history."""
        # Create mock delta log with history
        mock_delta_log = Mock(spec=AWSDeltaLog)
        mock_delta_log.table_name = "test_table"
        mock_delta_log.table_exists.return_value = True
        
        # Mock the delta_log attribute and its history method
        mock_delta_log.delta_log = Mock()
        mock_history = [
            {
                "watermark": "1680300000000",
                "schema_timestamp": "1680000000000",
                "version": 5
            },
            {
                "watermark": "1680200000000", 
                "schema_timestamp": "1680000000000",
                "version": 4
            }
        ]
        mock_delta_log.delta_log.history.return_value = mock_history
        
        # Call the method
        result = AWSDeltaLog._get_watermark_from_log(mock_delta_log)
        
        # Should return latest watermark
        assert result == {"watermark": 1680300000000, "schema_timestamp": 1680000000000}

    @pytest.mark.unit
    def test_get_watermark_from_log_missing_metadata(self):
        """Test watermark retrieval when metadata is missing."""
        mock_delta_log = Mock(spec=AWSDeltaLog)
        mock_delta_log.table_name = "test_table"
        mock_delta_log.table_exists.return_value = True
        
        # Mock the delta_log attribute
        mock_delta_log.delta_log = Mock()
        # Mock history without watermark metadata
        mock_history = [
            {"version": 1, "timestamp": "2023-04-01T10:00:00Z"}
        ]
        mock_delta_log.delta_log.history.return_value = mock_history
        
        # Should raise DeltaError when metadata is missing from existing table
        with pytest.raises(Exception) as exc_info:
            AWSDeltaLog._get_watermark_from_log(mock_delta_log)
        
        # Verify it's a DeltaError with appropriate message
        assert "No valid watermarks found" in str(exc_info.value)

    @pytest.mark.unit
    def test_get_watermark_from_log_exception_handling(self):
        """Test watermark retrieval exception handling."""
        mock_delta_log = Mock(spec=AWSDeltaLog)
        mock_delta_log.table_name = "test_table"
        mock_delta_log.table_exists.return_value = True
        # Mock the delta_log attribute
        mock_delta_log.delta_log = Mock()
        mock_delta_log.delta_log.history.side_effect = Exception("Test exception")
        
        with patch('guidewire.delta_log.L') as mock_logger:
            # Should raise DeltaError when exception occurs
            with pytest.raises(Exception) as exc_info:
                AWSDeltaLog._get_watermark_from_log(mock_delta_log)
            
            # Verify it's a DeltaError and logger was called
            assert "Failed to get watermark from log" in str(exc_info.value)
            mock_logger.error.assert_called_once()

    @pytest.mark.unit
    def test_get_watermark_from_log_invalid_values(self):
        """Test watermark retrieval with invalid watermark values."""
        mock_delta_log = Mock(spec=AWSDeltaLog)
        mock_delta_log.table_name = "test_table"
        mock_delta_log.table_exists.return_value = True
        
        # Mock the delta_log attribute
        mock_delta_log.delta_log = Mock()
        # Mock history with invalid watermark values
        mock_history = [
            {
                "watermark": "invalid_value",
                "schema_timestamp": "also_invalid",
                "version": 1
            }
        ]
        mock_delta_log.delta_log.history.return_value = mock_history
        
        with patch('guidewire.delta_log.L') as mock_logger:
            # Should raise DeltaError when invalid values are found
            with pytest.raises(Exception) as exc_info:
                AWSDeltaLog._get_watermark_from_log(mock_delta_log)
            
            # Verify it's a DeltaError with appropriate message
            assert "No valid watermarks found" in str(exc_info.value)
            mock_logger.error.assert_called_once()

    @pytest.mark.unit
    def test_get_watermark_from_log_empty_history(self):
        """Test watermark retrieval with empty history."""
        mock_delta_log = Mock(spec=AWSDeltaLog)
        mock_delta_log.table_name = "test_table"
        mock_delta_log.table_exists.return_value = True
        # Mock the delta_log attribute
        mock_delta_log.delta_log = Mock()
        mock_delta_log.delta_log.history.return_value = []
        
        result = AWSDeltaLog._get_watermark_from_log(mock_delta_log)
        
        # Should return defaults for empty history
        assert result == {"watermark": 0, "schema_timestamp": 0}


class TestWatermarkFiltering:
    """Unit tests for watermark-based directory filtering logic."""
    
    @pytest.mark.unit
    def test_directory_filtering_logic(self):
        """Test the logic for filtering directories based on watermarks."""
        # Mock file info objects
        def create_mock_file_info(path, base_name, is_directory=True):
            mock_info = Mock()
            mock_info.path = path
            mock_info.base_name = base_name
            mock_info.type = Mock()
            mock_info.type.__eq__ = lambda self, other: is_directory
            return mock_info
        
        # Create mock directory structure
        mock_directories = [
            create_mock_file_info("/data/table/1680000000000", "1680000000000"),
            create_mock_file_info("/data/table/1680100000000", "1680100000000"), 
            create_mock_file_info("/data/table/1680200000000", "1680200000000"),
            create_mock_file_info("/data/table/1680300000000", "1680300000000"),
        ]
        
        # Test filtering with different watermarks
        test_cases = [
            (0, 4, "All directories should be included when watermark is 0"),
            (1680050000000, 3, "Three directories should be included"),
            (1680150000000, 2, "Two directories should be included"),
            (1680250000000, 1, "One directory should be included"),
            (1680350000000, 0, "No directories should be included"),
        ]
        
        for watermark, expected_count, description in test_cases:
            # Apply filtering logic (mimicking Batch._get_dir_list logic)
            filtered = [
                d for d in mock_directories 
                if int(d.base_name) > watermark
            ]
            
            assert len(filtered) == expected_count, f"{description}. Expected {expected_count}, got {len(filtered)}"
            
            # Verify all filtered directories have timestamps > watermark
            for directory in filtered:
                assert int(directory.base_name) > watermark

    @pytest.mark.unit
    def test_partial_processing_detection(self):
        """Test detection of partial vs full processing scenarios."""
        # Mock directory structure
        all_directories = [f"168000000000{i}" for i in range(5)]  # 5 directories
        
        test_cases = [
            (0, 5, False, "Full processing when watermark filters nothing"),
            (1680000000001, 3, True, "Partial processing when some directories filtered"),  # Fixed: 1680000000002, 3, 4 are > 1680000000001
            (1680000000003, 1, True, "Partial processing when most directories filtered"),  # Fixed: only 1680000000004 is > 1680000000003
            (1680000000004, 0, True, "Partial processing when all directories filtered"),   # Fixed: no directories > 1680000000004
            (1680000000005, 0, True, "Partial processing when all directories filtered"),   # Same as above
        ]
        
        for watermark, expected_filtered_count, expected_partial, description in test_cases:
            # Apply filtering
            filtered_directories = [d for d in all_directories if int(d) > watermark]
            
            # Determine if partial (matches the logic in Batch._get_dir_list)
            is_partial = 0 < len(filtered_directories) < len(all_directories) or len(filtered_directories) == 0
            
            print(f"Testing: watermark={watermark}, filtered={len(filtered_directories)}, total={len(all_directories)}, partial={is_partial}")
            assert len(filtered_directories) == expected_filtered_count, f"{description}. Expected {expected_filtered_count}, got {len(filtered_directories)}"
            assert is_partial == expected_partial, f"{description}. Expected partial={expected_partial}, got {is_partial}"


class TestWatermarkEnvironmentConfiguration:
    """Unit tests for watermark-related environment configuration."""
    
    @pytest.mark.unit
    def test_maintain_timestamp_transactions_default(self):
        """Test default value for MAINTAIN_TIMESTAMP_TRANSACTIONS."""
        with patch.dict('os.environ', {}, clear=True):
            import os
            # Simulate the logic from Batch.__init__
            maintain_timestamp_transactions = int(os.environ.get("MAINTAIN_TIMESTAMP_TRANSACTIONS", "1"))
            assert maintain_timestamp_transactions == 1

    @pytest.mark.unit
    def test_maintain_timestamp_transactions_override(self):
        """Test overriding MAINTAIN_TIMESTAMP_TRANSACTIONS."""
        with patch.dict('os.environ', {'MAINTAIN_TIMESTAMP_TRANSACTIONS': '0'}):
            import os
            maintain_timestamp_transactions = int(os.environ.get("MAINTAIN_TIMESTAMP_TRANSACTIONS", "1"))
            assert maintain_timestamp_transactions == 0

    @pytest.mark.unit
    def test_maintain_timestamp_transactions_invalid_value(self):
        """Test handling of invalid MAINTAIN_TIMESTAMP_TRANSACTIONS values."""
        with patch.dict('os.environ', {'MAINTAIN_TIMESTAMP_TRANSACTIONS': 'invalid'}):
            import os
            # Should raise ValueError when trying to convert to int
            with pytest.raises(ValueError):
                int(os.environ.get("MAINTAIN_TIMESTAMP_TRANSACTIONS", "1"))


class TestWatermarkDataTypes:
    """Unit tests for watermark data type handling."""
    
    @pytest.mark.unit
    def test_watermark_integer_conversion(self):
        """Test conversion of watermark values to integers."""
        # Test valid string to int conversion
        assert int("1680000000000") == 1680000000000
        
        # Test that floats get truncated
        assert int(1680000000000.5) == 1680000000000
        
        # Test edge cases
        assert int("0") == 0
        assert int(0) == 0

    @pytest.mark.unit
    def test_watermark_comparison_logic(self):
        """Test watermark comparison operations."""
        watermark = 1680100000000
        
        # Test greater than comparisons (used in filtering)
        assert 1680200000000 > watermark
        assert not (1680000000000 > watermark)
        assert not (1680100000000 > watermark)  # Equal should not pass
        
        # Test that string timestamps can be compared after conversion
        assert int("1680200000000") > watermark
        assert not (int("1680050000000") > watermark)

    @pytest.mark.unit
    def test_timestamp_sorting(self):
        """Test that timestamps sort correctly for watermark operations."""
        timestamps = [1680300000000, 1680100000000, 1680200000000, 1680000000000]
        sorted_timestamps = sorted(timestamps)
        
        assert sorted_timestamps == [1680000000000, 1680100000000, 1680200000000, 1680300000000]
        
        # Test that max/min work correctly
        assert max(timestamps) == 1680300000000
        assert min(timestamps) == 1680000000000


class TestWatermarkErrorScenarios:
    """Unit tests for watermark error handling scenarios."""
    
    @pytest.mark.unit
    def test_watermark_none_handling(self):
        """Test handling of None watermark values."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,  # None is valid for unfinished processing
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        assert result.process_finish_watermark is None
        assert result.errors is None
        assert result.warnings is None

    @pytest.mark.unit
    def test_watermark_list_initialization(self):
        """Test that watermark lists initialize correctly."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,
            warnings=None
        )
        
        # Test that lists are properly initialized and can be appended to
        assert isinstance(result.watermarks, list)
        assert isinstance(result.schema_timestamps, list)
        assert len(result.watermarks) == 0
        assert len(result.schema_timestamps) == 0
        
        # Test adding items works
        result.watermarks.append(1680000000000)
        result.schema_timestamps.append(1680000000000)
        assert len(result.watermarks) == 1
        assert len(result.schema_timestamps) == 1

    @pytest.mark.unit
    def test_error_and_warning_list_lazy_initialization(self):
        """Test that error and warning lists are lazily initialized."""
        result = Result(
            table="test_table",
            process_start_time=datetime.now(),
            process_start_watermark=0,
            process_start_version=0,
            manifest_records=1000,
            manifest_watermark=1680000000000,
            process_finish_time=None,
            process_finish_watermark=None,
            process_finish_version=None,
            watermarks=[],
            schema_timestamps=[],
            errors=None,  # Start as None
            warnings=None  # Start as None
        )
        
        # Initially None
        assert result.errors is None
        assert result.warnings is None
        
        # Adding first error should initialize list
        result.add_error("First error")
        assert isinstance(result.errors, list)
        assert len(result.errors) == 1
        assert result.errors[0] == "First error"
        
        # Adding first warning should initialize list
        result.add_warning("First warning")
        assert isinstance(result.warnings, list)
        assert len(result.warnings) == 1
        assert result.warnings[0] == "First warning"
