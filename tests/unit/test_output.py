"""Unit tests for output handlers"""

import pytest
import tempfile
import os
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from stream_data_producer.output.console import ConsoleOutput
from stream_data_producer.output.file import FileOutput
from stream_data_producer.output.kafka import KafkaOutput


class TestConsoleOutput:
    """Test ConsoleOutput class"""
    
    def test_console_output_initialization(self):
        """Test console output initialization"""
        output = ConsoleOutput()
        assert output is not None
    
    def test_send_to_console(self, capsys):
        """Test sending data to console"""
        output = ConsoleOutput()
        
        test_data = {
            "id": 123,
            "name": "test_user",
            "score": 87.5,
            "active": True
        }
        
        success = output.send(test_data)
        assert success is True
        
        # Capture stdout and check content
        captured = capsys.readouterr()
        output_line = captured.out.strip()
        
        # Should be valid JSON
        parsed_data = json.loads(output_line)
        assert parsed_data == test_data
    
    def test_send_complex_data_to_console(self, capsys):
        """Test sending complex nested data to console"""
        output = ConsoleOutput()
        
        complex_data = {
            "user": {
                "id": 123,
                "profile": {
                    "name": "test_user",
                    "preferences": ["pref1", "pref2"]
                }
            },
            "metrics": [1.5, 2.7, 3.9],
            "timestamp": 1708765432123
        }
        
        success = output.send(complex_data)
        assert success is True
        
        captured = capsys.readouterr()
        output_line = captured.out.strip()
        
        parsed_data = json.loads(output_line)
        assert parsed_data == complex_data
    
    def test_console_output_close(self):
        """Test closing console output"""
        output = ConsoleOutput()
        # Should not raise any exceptions
        output.close()


class TestFileOutput:
    """Test FileOutput class"""
    
    @pytest.fixture
    def temp_file_path(self):
        """Create temporary file path for testing"""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            file_path = f.name
        yield file_path
        if os.path.exists(file_path):
            os.unlink(file_path)
    
    def test_file_output_initialization(self, temp_file_path):
        """Test file output initialization"""
        output = FileOutput(file_path=temp_file_path, rolling="hourly")
        assert output.file_path == temp_file_path
        assert output.rolling == "hourly"
        assert output._current_file_handle is None
        assert output._current_period is None
    
    def test_send_to_file(self, temp_file_path):
        """Test sending data to file"""
        output = FileOutput(file_path=temp_file_path, rolling="daily")
        
        test_data = {
            "id": 456,
            "message": "test message",
            "value": 99.9
        }
        
        success = output.send(test_data)
        assert success is True
        
        # Close the file to flush content
        output.close()
        
        # Check file content
        with open(temp_file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            parsed_data = json.loads(lines[0].strip())
            assert parsed_data == test_data
    
    def test_send_multiple_records_to_file(self, temp_file_path):
        """Test sending multiple records to file"""
        output = FileOutput(file_path=temp_file_path, rolling="daily")
        
        records = [
            {"id": 1, "value": 10.5},
            {"id": 2, "value": 20.3},
            {"id": 3, "value": 30.1}
        ]
        
        for record in records:
            success = output.send(record)
            assert success is True
        
        # Close to flush content
        output.close()
        
        # Check all records are in file
        with open(temp_file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 3
            
            for i, line in enumerate(lines):
                parsed_data = json.loads(line.strip())
                assert parsed_data == records[i]
    
    def test_file_output_with_different_rolling_options(self, temp_file_path):
        """Test file output with different rolling options"""
        # Test hourly rolling
        output1 = FileOutput(file_path=temp_file_path, rolling="hourly")
        assert output1.rolling == "hourly"
        
        # Test daily rolling
        output2 = FileOutput(file_path=temp_file_path, rolling="daily")
        assert output2.rolling == "daily"
        
        # Test case insensitive rolling
        output3 = FileOutput(file_path=temp_file_path, rolling="HOURLY")
        assert output3.rolling == "hourly"
    
    def test_file_output_close(self, temp_file_path):
        """Test closing file output"""
        output = FileOutput(file_path=temp_file_path, rolling="daily")
        
        # Send some data first
        output.send({"test": "data"})
        output.close()
        
        # File should exist and have content
        assert os.path.exists(temp_file_path)
        with open(temp_file_path, 'r') as f:
            content = f.read()
            assert len(content) > 0
    
    def test_file_output_with_special_characters(self, temp_file_path):
        """Test file output with special characters in data"""
        output = FileOutput(file_path=temp_file_path, rolling="daily")
        
        special_data = {
            "unicode": "测试中文",
            "special_chars": "!@#$%^&*()",
            "quotes": '"quoted" text',
            "newlines": "line1\nline2"
        }
        
        success = output.send(special_data)
        assert success is True
        output.close()
        
        # Check file content
        with open(temp_file_path, 'r') as f:
            content = f.read()
            # Should be valid JSON despite special characters
            parsed_data = json.loads(content.strip())
            assert parsed_data == special_data
    
    def test_file_output_directory_creation(self):
        """Test automatic directory creation"""
        with tempfile.TemporaryDirectory() as temp_dir:
            nested_path = os.path.join(temp_dir, "nested", "deep", "test.json")
            
            # Directory should not exist initially
            assert not os.path.exists(os.path.dirname(nested_path))
            
            output = FileOutput(file_path=nested_path, rolling="daily")
            success = output.send({"test": "data"})
            output.close()
            
            assert success is True
            assert os.path.exists(nested_path)


class TestKafkaOutput:
    """Test KafkaOutput class"""
    
    def test_kafka_output_initialization_plain(self):
        """Test Kafka output initialization with plain configuration"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic",
                security_protocol="PLAINTEXT"
            )
            
            assert output.bootstrap_servers == "localhost:9092"
            assert output.topic == "test-topic"
            assert output.producer == mock_producer_instance
            
            # Check producer config
            mock_producer.assert_called_once()
            config = mock_producer.call_args[0][0]
            assert config['bootstrap.servers'] == "localhost:9092"
            # For PLAINTEXT, security.protocol should not be set
            assert 'security.protocol' not in config
    
    def test_kafka_output_initialization_sasl(self):
        """Test Kafka output initialization with SASL configuration"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            output = KafkaOutput(
                bootstrap_servers="kafka-server:9093",
                topic="secure-topic",
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_username="testuser",
                sasl_password="testpass"
            )
            
            assert output.bootstrap_servers == "kafka-server:9093"
            assert output.topic == "secure-topic"
            assert output.producer == mock_producer_instance
            
            # Check producer config includes SASL settings
            config = mock_producer.call_args[0][0]
            assert config['security.protocol'] == "SASL_PLAINTEXT"
            assert config['sasl.mechanism'] == "PLAIN"
            assert config['sasl.username'] == "testuser"
            assert config['sasl.password'] == "testpass"
    
    def test_send_to_kafka_success(self):
        """Test successful sending to Kafka"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            mock_producer_instance.produce.return_value = None
            mock_producer_instance.poll.return_value = None
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic"
            )
            
            test_data = {
                "id": 789,
                "event": "user_login",
                "timestamp": 1708765432123
            }
            
            success = output.send(test_data)
            assert success is True
            
            # Check that produce was called correctly
            mock_producer_instance.produce.assert_called_once()
            call_args = mock_producer_instance.produce.call_args
            # call_args[0] contains positional args: (topic, value=value, callback=callback)
            assert call_args[0][0] == "test-topic"  # topic is first positional arg
            # value and callback are keyword arguments
            assert 'value' in call_args[1]
            assert json.loads(call_args[1]['value'].decode('utf-8')) == test_data
            assert 'callback' in call_args[1]
            assert call_args[1]['callback'] is not None
    
    def test_send_to_kafka_failure(self):
        """Test Kafka send failure handling"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            mock_producer_instance.produce.side_effect = Exception("Kafka connection failed")
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic"
            )
            
            test_data = {"id": 123, "message": "test"}
            
            success = output.send(test_data)
            assert success is False
            
            # Producer should have been called
            mock_producer_instance.produce.assert_called_once()
    
    def test_kafka_output_close(self):
        """Test closing Kafka output"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            mock_producer_instance.flush.return_value = 0
            
            output = KafkaOutput(
                bootstrap_servers="localhost:9092",
                topic="test-topic"
            )
            
            # Close should flush the producer
            output.close()
            
            # Check that flush was called
            mock_producer_instance.flush.assert_called_once_with(10.0)
            assert output.producer is None
    
    def test_kafka_output_without_producer_initialization(self):
        """Test Kafka output close without producer initialization"""
        with patch('stream_data_producer.output.kafka.Producer') as mock_producer:
            # Test close when producer fails to initialize
            mock_producer.side_effect = Exception("Init failed")
            
            # Should raise RuntimeError during initialization
            with pytest.raises(RuntimeError):
                output = KafkaOutput(
                    bootstrap_servers="localhost:9092",
                    topic="test-topic"
                )


# Integration-style tests for output handlers
class TestOutputHandlerIntegration:
    """Integration tests for output handlers"""
    
    def test_all_outputs_handle_same_data(self, capsys):
        """Test that all output handlers can handle the same data structure"""
        test_data = {
            "event_id": "evt_12345",
            "timestamp": 1708765432123,
            "user": {
                "id": 42,
                "name": "John Doe",
                "email": "john@example.com"
            },
            "payload": {
                "action": "login",
                "ip_address": "192.168.1.100",
                "user_agent": "Mozilla/5.0..."
            },
            "metadata": {
                "version": "1.0",
                "source": "web_app"
            }
        }
        
        # Test console output
        console_output = ConsoleOutput()
        console_success = console_output.send(test_data)
        assert console_success is True
        
        # Test file output with temporary file
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            temp_file_path = f.name
        
        try:
            file_output = FileOutput(file_path=temp_file_path, rolling="daily")
            file_success = file_output.send(test_data)
            file_output.close()
            assert file_success is True
            
            # Test that data was written correctly to file
            with open(temp_file_path, 'r') as f:
                file_content = f.read().strip()
                parsed_file_data = json.loads(file_content)
                assert parsed_file_data == test_data
        finally:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
    
    def test_output_handlers_error_recovery(self):
        """Test that output handlers recover from errors"""
        # Test console output error recovery
        console_output = ConsoleOutput()
        
        # Send normal data
        success1 = console_output.send({"normal": "data"})
        assert success1 is True
        
        # Send problematic data that might cause encoding issues
        problematic_data = {"problematic": "\udcff"}  # Invalid unicode surrogate
        success2 = console_output.send(problematic_data)
        # Depending on system, this might succeed or fail, but shouldn't crash
        assert isinstance(success2, bool)
        
        console_output.close()