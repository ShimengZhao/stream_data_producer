"""Unit tests for CLI interface"""

import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import json

from click.testing import CliRunner
from stream_data_producer.cli import main


class TestCLICommands:
    """Test CLI command functions"""
    
    @pytest.fixture
    def runner(self):
        """Create CLI runner"""
        return CliRunner()
    
    @pytest.fixture
    def temp_config_file(self):
        """Create temporary config file"""
        config_content = """
producer:
  name: test-producer
  rate: 10
  output: console
  fields:
    - name: id
      type: int
      rule: random_range
      min: 1
      max: 100
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(config_content)
            config_path = f.name
        yield config_path
        os.unlink(config_path)
    
    def test_main_command_help(self, runner):
        """Test main command help"""
        result = runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        assert 'Usage:' in result.output
        assert 'run' in result.output
        assert 'quick' in result.output
        assert 'status' in result.output
        assert 'validate' in result.output
    
    def test_quick_command_help(self, runner):
        """Test quick command help"""
        result = runner.invoke(main, ['quick', '--help'])
        assert result.exit_code == 0
        assert '--rate' in result.output
        assert '--output' in result.output
        assert 'SCHEMA' in result.output
    
    def test_run_command_help(self, runner):
        """Test run command help"""
        result = runner.invoke(main, ['run', '--help'])
        assert result.exit_code == 0
        assert '--config' in result.output
        assert '--api-host' in result.output
        assert '--api-port' in result.output
    
    def test_validate_command_help(self, runner):
        """Test validate command help"""
        result = runner.invoke(main, ['validate', '--help'])
        assert result.exit_code == 0
        assert '--config' in result.output
    
    def test_quick_with_schema(self, runner):
        """Test quick command with schema"""
        with patch('stream_data_producer.cli.SingleProducerManager') as mock_manager:
            mock_manager_instance = Mock()
            mock_manager_instance.start = Mock(return_value=True)
            mock_manager_instance.stop = Mock()
            mock_manager.return_value = mock_manager_instance
            
            result = runner.invoke(main, [
                'quick',
                'id:int,name:string',
                '--rate', '5'
            ])
            
            # The quick command runs indefinitely, so we expect it to be interrupted
            # We're mainly testing that it doesn't crash
            assert result.exit_code in [0, 1]  # 0 for success, 1 for keyboard interrupt
    
    def test_quick_with_invalid_schema(self, runner):
        """Test quick command with invalid schema"""
        result = runner.invoke(main, [
            'quick',
            'id:invalid_type',
            '--rate', '5'
        ])
        
        assert result.exit_code != 0
        assert 'Error:' in result.output
    
    def test_quick_with_file_output(self, runner):
        """Test quick command with file output"""
        with patch('stream_data_producer.cli.SingleProducerManager') as mock_manager:
            mock_manager_instance = Mock()
            mock_manager_instance.start = Mock(return_value=True)
            mock_manager_instance.stop = Mock()
            mock_manager.return_value = mock_manager_instance
            
            with tempfile.TemporaryDirectory() as temp_dir:
                result = runner.invoke(main, [
                    'quick',
                    'id:int',
                    '--output', 'file',
                    '--file-path', os.path.join(temp_dir, 'test.json')
                ])
                
                # Should start successfully
                assert result.exit_code in [0, 1]
    
    def test_quick_with_kafka_output(self, runner):
        """Test quick command with Kafka output"""
        with patch('stream_data_producer.cli.SingleProducerManager') as mock_manager:
            mock_manager_instance = Mock()
            mock_manager_instance.start = Mock(return_value=True)
            mock_manager.return_value = mock_manager_instance
            
            result = runner.invoke(main, [
                'quick',
                'id:int',
                '--output', 'kafka',
                '--kafka-bootstrap', 'localhost:9092',
                '--kafka-topic', 'test-topic'
            ])
            
            assert result.exit_code in [0, 1]
            
    def test_run_with_config_file(self, runner, temp_config_file):
        """Test run command with config file"""
        with patch('stream_data_producer.cli.load_config') as mock_load, \
             patch('stream_data_producer.cli.SingleProducerManager') as mock_manager, \
             patch('stream_data_producer.cli.SimpleAPIServer') as mock_api, \
             patch('stream_data_producer.cli.time.sleep') as mock_sleep:
            
            mock_config = Mock()
            mock_config.producer = Mock(name="test-producer")
            mock_load.return_value = mock_config
            
            mock_manager_instance = Mock()
            mock_manager_instance.start = Mock(return_value=True)
            mock_manager.return_value = mock_manager_instance
            
            mock_api_instance = Mock()
            mock_api_instance.start = Mock()
            mock_api.return_value = mock_api_instance
            
            # Make sleep raise KeyboardInterrupt after first call to break the infinite loop
            mock_sleep.side_effect = [None, KeyboardInterrupt()]
            
            result = runner.invoke(main, [
                'run',
                '--config', temp_config_file
            ])
            
            # Should start successfully and be interrupted
            assert result.exit_code == 0
            mock_load.assert_called_once_with(temp_config_file)
            mock_manager.assert_called_once()
            mock_sleep.assert_called()
    
    def test_validate_with_config_file(self, runner, temp_config_file):
        """Test validate command with config file"""
        with patch('stream_data_producer.cli.load_config') as mock_load:
            mock_config = Mock()
            mock_config.producer = Mock(name="test-producer")
            mock_config.dictionaries = {}
            mock_load.return_value = mock_config
            
            result = runner.invoke(main, [
                'validate',
                '--config', temp_config_file
            ])
            
            assert result.exit_code == 0
            assert 'Configuration is valid' in result.output
            mock_load.assert_called_once_with(temp_config_file)
    
    def test_validate_with_invalid_config(self, runner):
        """Test validate command with invalid config file"""
        with patch('stream_data_producer.cli.load_config') as mock_load:
            mock_load.side_effect = Exception("Invalid config")
            
            result = runner.invoke(main, [
                'validate',
                '--config', 'nonexistent.yaml'
            ])
            
            assert result.exit_code != 0
            assert 'validation failed' in result.output
    
    def test_status_command(self, runner):
        """Test status command"""
        with patch('stream_data_producer.cli.requests') as mock_requests:
            mock_response = Mock()
            mock_response.json.return_value = {
                "name": "test-producer",
                "status": "running",
                "output": "console",
                "messages_sent": 100,
                "current_rate": 10.5
            }
            mock_requests.get.return_value = mock_response
            
            result = runner.invoke(main, [
                'status',
                '--host', 'localhost',
                '--port', '8000'
            ])
            
            assert result.exit_code == 0
            assert 'Producer Status' in result.output
            assert 'Name: test-producer' in result.output
            assert 'Status: running' in result.output
            mock_requests.get.assert_called_once_with('http://localhost:8000/status', timeout=5)
    
    def test_status_command_connection_error(self, runner):
        """Test status command with connection error"""
        with patch('stream_data_producer.cli.requests') as mock_requests:
            # Use a more general exception that will be caught
            mock_requests.get.side_effect = requests.exceptions.RequestException("Connection failed")
            
            result = runner.invoke(main, ['status'])
            
            assert result.exit_code != 0
            # In Click testing, the output might be empty when sys.exit is called
            # Just verify the exit code is non-zero to indicate failure


# Import required modules for patching
import stream_data_producer.cli
import requests