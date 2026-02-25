"""Tests for Kafka message key generation functionality"""

import pytest
from unittest.mock import Mock, patch
import json
from stream_data_producer.output.kafka import KafkaOutput


class TestKafkaKeyGeneration:
    """Test Kafka key generation strategies"""
    
    def test_kafka_output_with_key_field(self):
        """Test Kafka output with specific key field"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_field="ship_id",
                key_strategy="field"
            )
            
            test_data = {
                "ship_id": "SHIP001",
                "timestamp": 1708765432123,
                "speed": 15.5
            }
            
            success = output.send(test_data)
            assert success is True
            
            # Check that produce was called with key
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]  # keyword arguments
            
            assert kwargs['topic'] == "test-topic"
            assert json.loads(kwargs['value'].decode('utf-8')) == test_data
            assert kwargs['key'].decode('utf-8') == "SHIP001"
            assert kwargs['callback'] is not None
    
    def test_kafka_output_with_missing_key_field(self):
        """Test Kafka output when key field is missing from data"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_field="nonexistent_field",
                key_strategy="field"
            )
            
            test_data = {
                "ship_id": "SHIP001",
                "timestamp": 1708765432123
            }
            
            success = output.send(test_data)
            assert success is True
            
            # Should be called without key since field is missing
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            
            assert 'key' not in kwargs or kwargs['key'] is None
    
    def test_kafka_output_with_random_key(self):
        """Test Kafka output with random key generation"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_strategy="random"
            )
            
            test_data = {"id": 123, "message": "test"}
            
            success = output.send(test_data)
            assert success is True
            
            # Check that produce was called with some key
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            
            assert 'key' in kwargs
            assert kwargs['key'] is not None
            # Key should be a UUID string
            key_str = kwargs['key'].decode('utf-8')
            assert len(key_str) > 0
    
    def test_kafka_output_with_timestamp_key(self):
        """Test Kafka output with timestamp key generation"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_strategy="timestamp"
            )
            
            test_data = {"id": 123, "message": "test"}
            
            success = output.send(test_data)
            assert success is True
            
            # Check that produce was called with timestamp key
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            
            assert 'key' in kwargs
            key_str = kwargs['key'].decode('utf-8')
            # Should be numeric timestamp
            assert key_str.isdigit()
    
    def test_kafka_output_with_composite_key(self):
        """Test Kafka output with composite key from multiple fields"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_field="ship_id,timestamp",
                key_strategy="composite"
            )
            
            test_data = {
                "ship_id": "SHIP001",
                "timestamp": 1708765432123,
                "speed": 15.5
            }
            
            success = output.send(test_data)
            assert success is True
            
            # Check that produce was called with composite key
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            
            expected_key = "SHIP001_1708765432123"
            assert kwargs['key'].decode('utf-8') == expected_key
    
    def test_kafka_output_with_none_key_strategy(self):
        """Test Kafka output with none key strategy (no key)"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_strategy="none"
            )
            
            test_data = {
                "ship_id": "SHIP001",
                "timestamp": 1708765432123
            }
            
            success = output.send(test_data)
            assert success is True
            
            # Should be called without key
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            
            assert 'key' not in kwargs or kwargs['key'] is None
    
    def test_kafka_output_default_key_strategy(self):
        """Test Kafka output with default key strategy (field)"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            # Default is field strategy with no key_field specified
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic"
            )
            
            test_data = {"id": 123, "message": "test"}
            
            success = output.send(test_data)
            assert success is True
            
            # Should be called without key since no key_field specified
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            
            assert 'key' not in kwargs or kwargs['key'] is None
    
    def test_kafka_output_case_insensitive_strategy(self):
        """Test that key strategy is case insensitive"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            # Test with uppercase strategy
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_strategy="RANDOM"  # uppercase
            )
            
            test_data = {"id": 123}
            success = output.send(test_data)
            assert success is True
            
            # Should still generate random key
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            kwargs = call_args[1]
            assert 'key' in kwargs


class TestKafkaKeyIntegration:
    """Integration tests for Kafka key functionality"""
    
    def test_key_generation_consistency(self):
        """Test that key generation is consistent for same data"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_field="ship_id",
                key_strategy="field"
            )
            
            test_data = {"ship_id": "CONSISTENT_KEY", "other": "data"}
            
            # Send same data multiple times
            for i in range(3):
                success = output.send(test_data)
                assert success is True
            
            # All calls should have the same key
            assert mock_producer_instance.produce.call_count == 3
            for call_args in mock_producer_instance.produce.call_args_list:
                kwargs = call_args[1]
                assert kwargs['key'].decode('utf-8') == "CONSISTENT_KEY"
    
    def test_different_data_different_keys(self):
        """Test that different data produces different keys"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                key_strategy="random"
            )
            
            data1 = {"id": 1}
            data2 = {"id": 2}
            
            output.send(data1)
            output.send(data2)
            
            # Should have different keys
            assert mock_producer_instance.produce.call_count == 2
            call1_args = mock_producer_instance.produce.call_args_list[0][1]
            call2_args = mock_producer_instance.produce.call_args_list[1][1]
            
            key1 = call1_args['key'].decode('utf-8')
            key2 = call2_args['key'].decode('utf-8')
            assert key1 != key2