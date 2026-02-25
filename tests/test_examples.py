"""Test that example configuration files are valid and properly organized"""

import pytest
import os
from stream_data_producer.core.config import load_config


def test_examples_directory_exists():
    """Test that examples directory exists"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    assert os.path.exists(examples_dir), "examples directory should exist"


def test_example_files_exist():
    """Test that example configuration files exist"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    
    expected_files = [
        "config.example.yaml",
        "config_with_kafka_key.example.yaml",
        "README.md"
    ]
    
    for filename in expected_files:
        file_path = os.path.join(examples_dir, filename)
        assert os.path.exists(file_path), f"Example file {filename} should exist"


def test_basic_config_example_valid():
    """Test that basic config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "config.example.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Basic validation
    assert config.producer is not None
    assert config.producer.name == "telemetry-producer"
    assert config.producer.output.value == "kafka"


def test_kafka_key_config_example_valid():
    """Test that Kafka key config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "config_with_kafka_key.example.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Validate Kafka key configuration
    assert config.kafka is not None
    assert config.kafka.key_field == "ship_id"
    assert config.kafka.key_strategy == "field"
    
    # Validate producer
    assert config.producer is not None
    assert config.producer.name == "ship-telemetry-producer"


def test_examples_readme_exists():
    """Test that examples README exists and has content"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    readme_path = os.path.join(examples_dir, "README.md")
    
    assert os.path.exists(readme_path), "examples/README.md should exist"
    
    with open(readme_path, 'r') as f:
        content = f.read()
        assert len(content) > 0, "README should have content"
        assert "# Configuration Examples" in content, "README should have proper title"