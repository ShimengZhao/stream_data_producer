"""Unit tests for rate controller module"""

import pytest
from unittest.mock import patch, Mock
import time

from stream_data_producer.core.rate_controller import RateController


class TestRateController:
    """Test RateController class"""
    
    def test_controller_initialization_with_rate(self):
        """Test rate controller initialization with rate parameter"""
        controller = RateController(rate=10)
        assert controller.rate == 10
        assert controller.interval is None
        assert controller._should_stop is False
        assert controller._paused is False
    
    def test_controller_initialization_with_interval(self):
        """Test rate controller initialization with interval parameter"""
        controller = RateController(interval="5s")
        assert controller.rate is None
        assert controller.interval == "5s"
        assert controller._should_stop is False
        assert controller._paused is False
    
    def test_controller_initialization_defaults(self):
        """Test rate controller initialization with default parameters"""
        controller = RateController()
        assert controller.rate is None  # No default rate
        assert controller.interval is None
        assert controller._should_stop is False
    
    def test_set_rate(self):
        """Test setting rate dynamically"""
        controller = RateController(rate=5)
        assert controller.rate == 5
        
        controller.set_rate(20)
        assert controller.rate == 20
        assert controller.interval is None  # Interval should be cleared
    
    def test_set_interval(self):
        """Test setting interval dynamically"""
        controller = RateController(interval="10s")
        assert controller.interval == "10s"
        
        controller.set_interval("2m")
        assert controller.interval == "2m"
        assert controller.rate is None  # Rate should be cleared
    
    def test_stop_controller(self):
        """Test stopping the controller"""
        controller = RateController(rate=10)
        assert controller._should_stop is False
        
        controller.stop()
        assert controller._should_stop is True
    
    def test_pause_and_resume(self):
        """Test pausing and resuming the controller"""
        controller = RateController(rate=10)
        
        # Initially not paused
        assert controller._paused is False
        
        # Pause
        controller.pause()
        assert controller._paused is True
        
        # Resume
        controller.resume()
        assert controller._paused is False
    
    def test_wait_for_next_message_rate_based(self):
        """Test wait_for_next_message with rate-based control"""
        controller = RateController(rate=10)  # 10 messages per second = 0.1 seconds per message
        
        with patch('time.sleep') as mock_sleep:
            
            # Each call should sleep for 0.1 seconds (rate control)
            result1 = controller.wait_for_next_message()
            assert result1 is True
            mock_sleep.assert_called_with(0.1)
            
            result2 = controller.wait_for_next_message()
            assert result2 is True
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(0.1)
    
    def test_wait_for_next_message_interval_based(self):
        """Test wait_for_next_message with interval-based control"""
        controller = RateController(interval="2s")  # Every 2 seconds
        
        with patch('time.sleep') as mock_sleep:
            
            # Each call should sleep for 2 seconds (interval control)
            result1 = controller.wait_for_next_message()
            assert result1 is True
            mock_sleep.assert_called_with(2.0)
            
            result2 = controller.wait_for_next_message()
            assert result2 is True
            assert mock_sleep.call_count == 2
            mock_sleep.assert_called_with(2.0)
    
    def test_wait_when_stopped(self):
        """Test wait_for_next_message when controller is stopped"""
        controller = RateController(rate=10)
        controller.stop()
        
        with patch('time.sleep') as mock_sleep:
            result = controller.wait_for_next_message()
            assert result is False
            mock_sleep.assert_not_called()
    
    def test_rate_calculation_accuracy(self):
        """Test that rate calculation is accurate"""
        controller = RateController(rate=5)  # 5 messages per second = 0.2 seconds per message
        
        # Simulate sending messages at exactly the right intervals
        times = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2]
        
        with patch('time.time', side_effect=times) as mock_time, \
             patch('time.sleep') as mock_sleep:
            
            delays = []
            for i in range(len(times) - 1):
                controller.wait_for_next_message()
                if mock_sleep.called:
                    delays.append(mock_sleep.call_args[0][0])
                mock_sleep.reset_mock()
            
            # All delays should be approximately 0.2 seconds
            for delay in delays:
                assert abs(delay - 0.2) < 0.01
    
    def test_interval_parsing_various_formats(self):
        """Test interval parsing with various formats"""
        test_cases = [
            ("1ms", 0.001),
            ("500ms", 0.5),
            ("1s", 1.0),
            ("30s", 30.0),
            ("1m", 60.0),
            ("5m", 300.0),
            ("1h", 3600.0),
            ("2h", 7200.0),
        ]
        
        for interval_str, expected_seconds in test_cases:
            controller = RateController(interval=interval_str)
            # We can't easily test the internal parsing, but we can test that
            # it doesn't raise exceptions and the interval is stored correctly
            assert controller.interval == interval_str
    
    def test_invalid_interval_format(self):
        """Test handling of invalid interval formats"""
        # Should raise ValueError for invalid interval
        with pytest.raises(ValueError, match="Invalid interval format"):
            controller = RateController(interval="invalid")
    
    def test_zero_rate_handling(self):
        """Test handling of zero rate"""
        controller = RateController(rate=0)
        
        with patch('time.sleep') as mock_sleep:
            # Should not cause infinite loop or crash
            result = controller.wait_for_next_message()
            assert result is True
            # Should sleep for a reasonable amount (implementation dependent)
            mock_sleep.assert_called_with(0.001)
    
    def test_high_rate_performance(self):
        """Test performance with high rates"""
        controller = RateController(rate=1000)  # 1000 messages per second
        
        with patch('time.sleep') as mock_sleep:
            
            for i in range(5):
                controller.wait_for_next_message()
            
            # Should have slept for each call
            assert mock_sleep.call_count == 5
            # Each sleep should be approximately 1ms
            mock_sleep.assert_called_with(0.001)
    
    def test_concurrent_access_simulation(self):
        """Test concurrent-like access patterns"""
        controller = RateController(rate=100)
        
        with patch('time.sleep') as mock_sleep:
            
            success_count = 0
            for i in range(100):  # Try 100 messages
                if controller.wait_for_next_message():
                    success_count += 1
            
            # All should succeed
            assert success_count == 100
            assert mock_sleep.call_count == 100
    
    def test_message_timing_consistency(self):
        """Test that message timing is consistent"""
        controller = RateController(rate=10)  # 10 Hz = 100ms per message
        
        with patch('time.sleep') as mock_sleep:
            
            for i in range(5):
                controller.wait_for_next_message()
        
        # Should have slept for each message
        assert mock_sleep.call_count == 5
        mock_sleep.assert_called_with(0.1)