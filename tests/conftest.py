"""Pytest configuration and shared fixtures"""

import pytest
import tempfile
import os
import shutil
from pathlib import Path


@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data"""
    temp_dir = tempfile.mkdtemp(prefix="stream_data_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_config_file():
    """Create a temporary configuration file for testing"""
    config_content = """
kafka:
  bootstrap_servers: "localhost:9092"
  default_topic: "test-topic"
  security_protocol: "PLAINTEXT"

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
      type: 2

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
      - name: score
        type: double
        rule: random_range
        min: 0.0
        max: 100.0

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
      - name: flag
        type: boolean
        rule: random_from_list
        list: [true, false]

  - name: test-dict-producer
    rate: 5
    output: console
    fields:
      - name: user_id
        type: string
        rule: random_from_dictionary
        dictionary: test_dict
        dictionary_column: id
      - name: user_name
        type: string
        rule: random_from_dictionary
        dictionary: test_dict
        dictionary_column: name
"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        config_path = f.name
    
    # Create test data directory
    test_data_dir = "./test_data"
    os.makedirs(test_data_dir, exist_ok=True)
    
    # Create test dictionary file
    dict_content = """1,Alice,admin
2,Bob,user
3,Charlie,moderator
4,David,guest
5,Eve,admin"""
    
    with open("./test_data/test_dict.csv", 'w') as f:
        f.write(dict_content)
    
    yield config_path
    
    # Cleanup
    os.unlink(config_path)
    if os.path.exists("./test_data"):
        shutil.rmtree("./test_data")
    if os.path.exists("./test_logs"):
        shutil.rmtree("./test_logs")


@pytest.fixture
def sample_data_record():
    """Sample data record for testing"""
    return {
        "id": 42,
        "name": "test_user",
        "timestamp": 1708765432123,
        "score": 87.5,
        "active": True
    }


@pytest.fixture
def sample_field_configs():
    """Sample field configurations for testing"""
    from stream_data_producer.core.config import FieldConfig, FieldType, RuleType
    
    return [
        FieldConfig(name="id", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=1, max=100),
        FieldConfig(name="name", type=FieldType.STRING, rule=RuleType.RANDOM_FROM_LIST, list=["Alice", "Bob", "Charlie"]),
        FieldConfig(name="timestamp", type=FieldType.LONG, rule=RuleType.NOW),
        FieldConfig(name="score", type=FieldType.DOUBLE, rule=RuleType.RANDOM_RANGE, min=0.0, max=100.0),
        FieldConfig(name="status", type=FieldType.STRING, rule=RuleType.CONSTANT, value="active")
    ]