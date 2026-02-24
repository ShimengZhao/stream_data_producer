"""Integration tests for stream data producer"""

import pytest
import tempfile
import os
import json
import time
import subprocess
import threading
from pathlib import Path

from stream_data_producer.core.config import load_config, parse_config
from stream_data_producer.core.dictionary import DictionaryLoader
from stream_data_producer.core.generator import DataGenerator
from stream_data_producer.core.rate_controller import RateController
from stream_data_producer.output.console import ConsoleOutput
from stream_data_producer.output.file import FileOutput
from stream_data_producer.utils.error_logger import ErrorLogger


@pytest.fixture
def temp_config_file():
    """Create a temporary configuration file for testing"""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  default_topic: "test-topic"

file_output:
  directory: "./test_data"
  rolling: "hourly"

error_log:
  directory: "./test_logs"
  rolling: "daily"
  max_age_days: 1

dictionaries:
  test_dict:
    file: "./test_data/test_dict.csv"
    columns:
      id: 0
      name: 1

producers:
  - name: test-console-producer
    rate: 2
    output: console
    fields:
      - name: id
        type: int
        rule: random_range
        min: 1
        max: 100
      - name: name
        type: string
        rule: random_from_list
        list: ["Alice", "Bob", "Charlie"]
      - name: timestamp
        type: long
        rule: now

  - name: test-file-producer
    interval: 1s
    output: file
    file_path: "./test_data/output.json"
    fields:
      - name: value
        type: double
        rule: random_range
        min: 0.0
        max: 100.0
      - name: status
        type: string
        rule: constant
        value: "active"
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_path = f.name
    
    # Create test data directory
    test_data_dir = "./test_data"
    os.makedirs(test_data_dir, exist_ok=True)
    
    # Create test dictionary file
    dict_content = """1,Alice
2,Bob
3,Charlie
4,David
5,Eve"""
    
    with open("./test_data/test_dict.csv", 'w') as f:
        f.write(dict_content)
    
    yield config_path
    
    # Cleanup
    os.unlink(config_path)
    if os.path.exists("./test_data"):
        import shutil
        shutil.rmtree("./test_data")
    if os.path.exists("./test_logs"):
        shutil.rmtree("./test_logs")


def test_config_loading(temp_config_file):
    """Test configuration loading and parsing"""
    config = load_config(temp_config_file)
    
    assert len(config.producers) == 2
    assert len(config.dictionaries) == 1
    
    # Check first producer
    console_producer = config.producers[0]
    assert console_producer.name == "test-console-producer"
    assert console_producer.rate == 2
    assert console_producer.output.value == "console"
    assert len(console_producer.fields) == 3
    
    # Check second producer
    file_producer = config.producers[1]
    assert file_producer.name == "test-file-producer"
    assert file_producer.interval == "1s"
    assert file_producer.output.value == "file"
    assert len(file_producer.fields) == 2


def test_dictionary_loading(temp_config_file):
    """Test dictionary loading functionality"""
    config = load_config(temp_config_file)
    loader = DictionaryLoader()
    loader.load_all_dictionaries(config.dictionaries)
    
    # Test getting random values
    value1 = loader.get_random_value("test_dict", "id")
    value2 = loader.get_random_value("test_dict", "name")
    
    assert value1 in ["1", "2", "3", "4", "5"]
    assert value2 in ["Alice", "Bob", "Charlie", "David", "Eve"]
    
    # Test that we can get by index too
    value3 = loader.get_random_value("test_dict", 0)
    value4 = loader.get_random_value("test_dict", 1)
    
    assert value3 in ["1", "2", "3", "4", "5"]
    assert value4 in ["Alice", "Bob", "Charlie", "David", "Eve"]


def test_data_generation(temp_config_file):
    """Test data generation with different field types"""
    config = load_config(temp_config_file)
    loader = DictionaryLoader()
    generator = DataGenerator(loader)
    
    # Test console producer fields
    console_producer = config.producers[0]
    record = generator.generate_record(console_producer.fields)
    
    assert "id" in record
    assert "name" in record
    assert "timestamp" in record
    assert isinstance(record["id"], int)
    assert 1 <= record["id"] <= 100
    assert record["name"] in ["Alice", "Bob", "Charlie"]
    assert isinstance(record["timestamp"], int)  # Should be milliseconds timestamp


def test_rate_controller():
    """Test rate controller functionality"""
    # Test rate-based control
    controller = RateController(rate=10)  # 10 messages per second
    
    start_time = time.time()
    messages_sent = 0
    
    # Simulate sending 5 messages
    for i in range(5):
        if controller.wait_for_next_message():
            messages_sent += 1
        else:
            break
    
    elapsed = time.time() - start_time
    controller.stop()
    
    # Should take approximately 0.5 seconds for 5 messages at 10 Hz
    assert 0.4 <= elapsed <= 0.7
    assert messages_sent == 5


def test_console_output():
    """Test console output handler"""
    output = ConsoleOutput()
    
    test_data = {
        "id": 123,
        "name": "test",
        "value": 45.67
    }
    
    # This will print to stdout - we're mainly testing it doesn't crash
    success = output.send(test_data)
    assert success is True
    
    output.close()


def test_file_output():
    """Test file output with rolling"""
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test_output.json")
        output = FileOutput(file_path, rolling="hourly")
        
        # Send some test data
        test_records = [
            {"id": 1, "value": 10.5},
            {"id": 2, "value": 20.3},
            {"id": 3, "value": 30.1}
        ]
        
        for record in test_records:
            success = output.send(record)
            assert success is True
        
        output.close()
        
        # Check that file was created and contains expected data
        assert os.path.exists(file_path)
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 3
            
            for i, line in enumerate(lines):
                data = json.loads(line.strip())
                assert data["id"] == test_records[i]["id"]
                assert data["value"] == test_records[i]["value"]


def test_error_logging():
    """Test error logging functionality"""
    with tempfile.TemporaryDirectory() as temp_dir:
        logger = ErrorLogger(
            log_directory=temp_dir,
            rolling="daily",
            max_age_days=1
        )
        
        # Log some dropped data
        test_data = {"id": 123, "value": 45.6}
        success = logger.log_dropped_data("test-producer", test_data, "Connection failed")
        assert success is True
        
        # Log a general error
        success = logger.log_error("test-producer", "Database connection lost")
        assert success is True
        
        # Check that log files were created
        log_files = os.listdir(temp_dir)
        assert len(log_files) >= 1
        
        # Check log content
        for log_file in log_files:
            if log_file.startswith("errors_"):
                with open(os.path.join(temp_dir, log_file), 'r') as f:
                    lines = f.readlines()
                    assert len(lines) == 2  # One dropped data, one error


if __name__ == "__main__":
    pytest.main([__file__, "-v"])