"""Unit tests for utility modules"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from stream_data_producer.utils.error_logger import ErrorLogger, ErrorTracker


class TestErrorLogger:
    """Test ErrorLogger class"""
    
    @pytest.fixture
    def temp_log_dir(self):
        """Create temporary log directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    def test_error_logger_initialization(self, temp_log_dir):
        """Test error logger initialization"""
        logger = ErrorLogger(
            log_directory=temp_log_dir,
            rolling="daily",
            max_age_days=7
        )
        
        assert logger.log_directory == temp_log_dir
        assert logger.rolling == "daily"
        assert logger.max_age_days == 7
    
    def test_error_logger_initialization_defaults(self, temp_log_dir):
        """Test error logger initialization with default values"""
        logger = ErrorLogger(log_directory=temp_log_dir)
        
        assert logger.log_directory == temp_log_dir
        assert logger.rolling == "daily"
        assert logger.max_age_days == 7
    
    def test_error_logger_with_hourly_rolling(self, temp_log_dir):
        """Test error logger with hourly rolling"""
        logger = ErrorLogger(
            log_directory=temp_log_dir,
            rolling="hourly",
            max_age_days=3
        )
        
        assert logger.rolling == "hourly"
        assert logger.max_age_days == 3
    
    def test_get_log_filename_daily(self, temp_log_dir):
        """Test getting log filename for daily rolling"""
        logger = ErrorLogger(log_directory=temp_log_dir, rolling="daily")
        
        # Mock current time to get predictable filename
        with patch('stream_data_producer.utils.error_logger.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "20240224"
            mock_datetime.now.return_value = mock_now
            
            filename = logger._get_log_filename()
            expected_path = os.path.join(temp_log_dir, "errors_20240224.json")
            assert filename == expected_path
    
    def test_get_log_filename_hourly(self, temp_log_dir):
        """Test getting log filename for hourly rolling"""
        logger = ErrorLogger(log_directory=temp_log_dir, rolling="hourly")
        
        with patch('stream_data_producer.utils.error_logger.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.strftime.return_value = "20240224_15"
            mock_datetime.now.return_value = mock_now
            
            filename = logger._get_log_filename()
            expected_path = os.path.join(temp_log_dir, "errors_20240224_15.json")
            assert filename == expected_path
    
    def test_ensure_log_directory_exists(self, temp_log_dir):
        """Test that log directory is created if it doesn't exist"""
        nested_dir = os.path.join(temp_log_dir, "nested", "deep")
        logger = ErrorLogger(log_directory=nested_dir)
        
        # Directory should be created automatically
        assert os.path.exists(nested_dir)
    
    def test_log_dropped_data(self, temp_log_dir):
        """Test logging dropped data"""
        logger = ErrorLogger(log_directory=temp_log_dir, rolling="daily")
        
        test_data = {
            "id": 123,
            "name": "test_user",
            "timestamp": 1708765432123
        }
        
        test_reason = "Connection timeout"
        
        with patch('stream_data_producer.utils.error_logger.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.isoformat.return_value = "2024-02-24T15:30:45.123456"
            mock_datetime.now.return_value = mock_now
            
            success = logger.log_dropped_data("test-producer", test_data, test_reason)
            assert success is True
        
        # Check that log file was created and contains expected content
        log_files = os.listdir(temp_log_dir)
        assert len(log_files) == 1
        assert log_files[0].startswith("errors_")
        assert log_files[0].endswith(".json")
        
        log_file_path = os.path.join(temp_log_dir, log_files[0])
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            log_entry = json.loads(lines[0].strip())
            assert log_entry["producer"] == "test-producer"
            assert log_entry["reason"] == test_reason
            assert log_entry["data"] == test_data
            assert "timestamp" in log_entry
    
    def test_log_error(self, temp_log_dir):
        """Test logging general errors"""
        logger = ErrorLogger(log_directory=temp_log_dir, rolling="daily")
        
        test_error = "Database connection failed"
        
        with patch('stream_data_producer.utils.error_logger.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.isoformat.return_value = "2024-02-24T15:30:45.123456"
            mock_datetime.now.return_value = mock_now
            
            success = logger.log_error("database-producer", test_error)
            assert success is True
        
        # Check log file content
        log_files = os.listdir(temp_log_dir)
        assert len(log_files) == 1
        
        log_file_path = os.path.join(temp_log_dir, log_files[0])
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            log_entry = json.loads(lines[0].strip())
            assert log_entry["producer"] == "database-producer"
            assert log_entry["error"] == test_error
            assert "timestamp" in log_entry
            assert log_entry["details"] == {}
    
    def test_log_error_with_details(self, temp_log_dir):
        """Test logging error with additional details"""
        logger = ErrorLogger(log_directory=temp_log_dir, rolling="daily")
        
        error_details = {
            "error_code": 500,
            "endpoint": "/api/data",
            "attempts": 3
        }
        
        success = logger.log_error("api-producer", "Service unavailable", error_details)
        assert success is True
        
        log_files = os.listdir(temp_log_dir)
        log_file_path = os.path.join(temp_log_dir, log_files[0])
        
        with open(log_file_path, 'r') as f:
            log_entry = json.loads(f.readline().strip())
            assert log_entry["details"] == error_details
    
    def test_multiple_log_entries(self, temp_log_dir):
        """Test multiple log entries in the same file"""
        logger = ErrorLogger(log_directory=temp_log_dir, rolling="daily")
        
        with patch('stream_data_producer.utils.error_logger.datetime') as mock_datetime:
            # Log first entry
            mock_now = Mock()
            mock_now.isoformat.return_value = "2024-02-24T15:30:45.123456"
            mock_datetime.now.return_value = mock_now
            
            logger.log_dropped_data("producer1", {"id": 1}, "reason1")
            
            # Log second entry
            mock_now.isoformat.return_value = "2024-02-24T15:31:22.654321"
            logger.log_error("producer2", "general error")
        
        # Check that both entries are in the log file
        log_files = os.listdir(temp_log_dir)
        log_file_path = os.path.join(temp_log_dir, log_files[0])
        
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
            
            # First line should be dropped data
            entry1 = json.loads(lines[0].strip())
            assert entry1["producer"] == "producer1"
            assert entry1["reason"] == "reason1"
            
            # Second line should be error
            entry2 = json.loads(lines[1].strip())
            assert entry2["producer"] == "producer2"
            assert entry2["error"] == "general error"
    
    @pytest.mark.skip(reason="Skipping due to system permission issues")
    def test_log_directory_creation_failure(self):
        """Test handling of log directory creation failure"""
        # Try to create logger with unwritable directory
        logger = ErrorLogger(log_directory="/root/protected/logs")
        
        # Should handle gracefully and return False for logging operations
        success = logger.log_error("test-producer", "test error")
        assert success is False
    
    def test_cleanup_old_logs_no_files(self, temp_log_dir):
        """Test cleanup when no log files exist"""
        logger = ErrorLogger(log_directory=temp_log_dir)
        
        # Should not raise any exceptions
        logger.cleanup_old_logs()
        assert len(os.listdir(temp_log_dir)) == 0
    
    def test_cleanup_old_logs_with_recent_files(self, temp_log_dir):
        """Test cleanup with recent log files (should not delete)"""
        logger = ErrorLogger(log_directory=temp_log_dir)
        
        # Create a recent log file
        recent_file = os.path.join(temp_log_dir, "errors_20240224.json")
        with open(recent_file, 'w') as f:
            f.write("recent log content")
        
        # Mock current time to be recent
        with patch('stream_data_producer.utils.error_logger.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.timestamp.return_value = datetime(2024, 2, 24).timestamp()
            mock_datetime.now.return_value = mock_now
            
            logger.cleanup_old_logs()
        
        # File should still exist
        assert os.path.exists(recent_file)
        assert len(os.listdir(temp_log_dir)) == 1
    
    def test_cleanup_old_logs_with_old_files(self, temp_log_dir):
        """Test cleanup with old log files (should delete)"""
        logger = ErrorLogger(log_directory=temp_log_dir, max_age_days=7)
        
        # Create an old log file with old modification time
        old_file = os.path.join(temp_log_dir, "errors_20240210.json")
        with open(old_file, 'w') as f:
            f.write("old log content")
        
        # Set file modification time to be old (14 days ago)
        old_timestamp = (datetime.now() - timedelta(days=14)).timestamp()
        os.utime(old_file, (old_timestamp, old_timestamp))
        
        # Perform cleanup
        logger.cleanup_old_logs()
        
        # Old file should be deleted
        assert not os.path.exists(old_file)
        assert len(os.listdir(temp_log_dir)) == 0
    
    def test_cleanup_old_files_mixed_ages(self, temp_log_dir):
        """Test cleanup with mix of old and recent files"""
        logger = ErrorLogger(log_directory=temp_log_dir, max_age_days=7)
        
        # Create old file (14 days old) with old modification time
        old_file = os.path.join(temp_log_dir, "errors_20240210.json")
        with open(old_file, 'w') as f:
            f.write("old log content")
        old_timestamp = (datetime.now() - timedelta(days=14)).timestamp()
        os.utime(old_file, (old_timestamp, old_timestamp))
        
        # Create recent file (3 days old) with recent modification time
        recent_file = os.path.join(temp_log_dir, "errors_20240221.json")
        with open(recent_file, 'w') as f:
            f.write("recent log content")
        recent_timestamp = (datetime.now() - timedelta(days=3)).timestamp()
        os.utime(recent_file, (recent_timestamp, recent_timestamp))
        
        # Perform cleanup
        logger.cleanup_old_logs()
        
        # Old file should be deleted, recent file should remain
        assert not os.path.exists(old_file)
        assert os.path.exists(recent_file)
        assert len(os.listdir(temp_log_dir)) == 1
        assert "errors_20240221.json" in os.listdir(temp_log_dir)[0]
    
    def test_error_logging_with_special_characters(self, temp_log_dir):
        """Test error logging with special characters"""
        logger = ErrorLogger(log_directory=temp_log_dir)
        
        special_data = {
            "unicode": "测试中文",
            "special_chars": "!@#$%^&*()",
            "quotes": '"quoted" text',
            "newlines": "line1\nline2"
        }
        
        special_error = "Error with unicode: 测试错误 & special chars: !@#$%"
        
        success1 = logger.log_dropped_data("special-producer", special_data, "special reason")
        success2 = logger.log_error("special-producer", special_error)
        
        assert success1 is True
        assert success2 is True
        
        # Check that log file was created and is readable
        log_files = os.listdir(temp_log_dir)
        assert len(log_files) == 1
        
        log_file_path = os.path.join(temp_log_dir, log_files[0])
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) == 2
            
            # Both entries should be valid JSON despite special characters
            entry1 = json.loads(lines[0].strip())
            entry2 = json.loads(lines[1].strip())
            
            assert entry1["producer"] == "special-producer"
            assert entry2["producer"] == "special-producer"


class TestErrorTracker:
    """Test ErrorTracker class"""
    
    def test_error_tracker_initialization(self):
        """Test error tracker initialization"""
        tracker = ErrorTracker()
        assert tracker._error_counts == {}
        assert tracker._last_errors == {}
    
    def test_increment_error_count(self):
        """Test incrementing error count"""
        tracker = ErrorTracker()
        
        # First increment
        tracker.increment_error_count("test-producer")
        stats = tracker.get_error_stats("test-producer")
        assert stats["error_count"] == 1
        assert stats["last_error"] is None
        
        # Second increment
        tracker.increment_error_count("test-producer")
        stats = tracker.get_error_stats("test-producer")
        assert stats["error_count"] == 2
    
    def test_set_last_error(self):
        """Test setting last error message"""
        tracker = ErrorTracker()
        
        tracker.set_last_error("test-producer", "Connection failed")
        stats = tracker.get_error_stats("test-producer")
        assert stats["last_error"] == "Connection failed"
        
        # Update with new error
        tracker.set_last_error("test-producer", "Timeout occurred")
        stats = tracker.get_error_stats("test-producer")
        assert stats["last_error"] == "Timeout occurred"
    
    def test_get_error_stats_default(self):
        """Test getting error stats for unknown producer"""
        tracker = ErrorTracker()
        
        stats = tracker.get_error_stats("unknown-producer")
        assert stats["error_count"] == 0
        assert stats["last_error"] is None
    
    def test_reset_error_count(self):
        """Test resetting error count"""
        tracker = ErrorTracker()
        
        # Increment some errors
        tracker.increment_error_count("test-producer")
        tracker.increment_error_count("test-producer")
        tracker.set_last_error("test-producer", "Some error")
        
        # Reset count
        tracker.reset_error_count("test-producer")
        
        stats = tracker.get_error_stats("test-producer")
        assert stats["error_count"] == 0
        assert stats["last_error"] == "Some error"  # Last error should persist
    
    def test_multiple_producers_tracking(self):
        """Test tracking errors for multiple producers"""
        tracker = ErrorTracker()
        
        # Track errors for different producers
        tracker.increment_error_count("producer-a")
        tracker.increment_error_count("producer-a")
        tracker.increment_error_count("producer-b")
        tracker.set_last_error("producer-a", "Error A")
        tracker.set_last_error("producer-b", "Error B")
        
        # Check stats for each producer
        stats_a = tracker.get_error_stats("producer-a")
        stats_b = tracker.get_error_stats("producer-b")
        
        assert stats_a["error_count"] == 2
        assert stats_a["last_error"] == "Error A"
        assert stats_b["error_count"] == 1
        assert stats_b["last_error"] == "Error B"


# Integration tests for utilities
class TestUtilsIntegration:
    """Integration tests for utility modules"""
    
    @pytest.fixture
    def temp_log_dir(self):
        """Create temporary log directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    def test_error_logger_end_to_end(self, temp_log_dir):
        """End-to-end test of error logger functionality"""
        logger = ErrorLogger(
            log_directory=temp_log_dir,
            rolling="daily",
            max_age_days=7
        )
        
        # Log various types of errors
        test_data = {"id": 123, "value": 45.6}
        success1 = logger.log_dropped_data("test-producer", test_data, "Network timeout")
        assert success1 is True
        
        success2 = logger.log_error("test-producer", "Configuration error")
        assert success2 is True
        
        # Check log files exist and have content
        log_files = os.listdir(temp_log_dir)
        assert len(log_files) == 1
        assert log_files[0].startswith("errors_")
        assert log_files[0].endswith(".json")
        
        # Read and verify content
        log_file_path = os.path.join(temp_log_dir, log_files[0])
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
            
            # Verify JSON structure
            entry1 = json.loads(lines[0].strip())
            entry2 = json.loads(lines[1].strip())
            
            assert entry1["producer"] == "test-producer"
            assert entry1["reason"] == "Network timeout"
            assert entry1["data"] == test_data
            
            assert entry2["producer"] == "test-producer"
            assert entry2["error"] == "Configuration error"
        
        # Test cleanup doesn't remove recent files
        logger.cleanup_old_logs()
        assert os.path.exists(log_file_path)
        
        # Test that logger can be reused
        success3 = logger.log_dropped_data("another-producer", {"test": "data"}, "Another reason")
        assert success3 is True
    
    def test_error_tracker_integration(self):
        """Test error tracker integration with realistic usage"""
        tracker = ErrorTracker()
        
        # Simulate error scenario
        producer_name = "data-producer"
        
        # Multiple errors occur
        for i in range(5):
            tracker.increment_error_count(producer_name)
            tracker.set_last_error(producer_name, f"Error #{i+1}")
        
        # Check final stats
        stats = tracker.get_error_stats(producer_name)
        assert stats["error_count"] == 5
        assert stats["last_error"] == "Error #5"
        
        # Reset and verify
        tracker.reset_error_count(producer_name)
        stats = tracker.get_error_stats(producer_name)
        assert stats["error_count"] == 0
        assert stats["last_error"] == "Error #5"  # Should persist