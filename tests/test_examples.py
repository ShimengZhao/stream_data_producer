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
        "financial_trading_monitor.yaml",
        "ecommerce_order_processing.yaml",
        "logistics_transport_tracking.yaml",
        "social_media_analytics.yaml",
        "healthcare_monitoring.yaml",
        "ship_telemetry_basic.yaml",
        "ship_telemetry_with_keys.yaml",
        "README.md"
    ]
    
    for filename in expected_files:
        file_path = os.path.join(examples_dir, filename)
        assert os.path.exists(file_path), f"Example file {filename} should exist"


def test_examples_readme_exists():
    """Test that examples README exists and has content"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    readme_path = os.path.join(examples_dir, "README.md")
    
    assert os.path.exists(readme_path), "examples/README.md should exist"
    
    with open(readme_path, 'r') as f:
        content = f.read()
        assert len(content) > 0, "README should have content"
        assert "# Configuration Examples" in content, "README should have proper title"


def test_financial_trading_config_valid():
    """Test that financial trading config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "financial_trading_monitor.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Validate configuration
    assert config.producer is not None
    assert config.producer.name == "financial-trading-monitor"
    assert config.producer.output.value == "kafka"
    assert config.producer.rate == 5
    
    # Validate Kafka key configuration
    assert config.kafka is not None
    assert config.kafka.key_field == "stock_symbol"
    assert config.kafka.key_strategy == "field"
    
    # Validate dictionaries
    assert "stocks" in config.dictionaries
    assert "traders" in config.dictionaries


def test_ecommerce_order_config_valid():
    """Test that ecommerce order config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "ecommerce_order_processing.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Validate configuration
    assert config.producer is not None
    assert config.producer.name == "ecommerce-order-processor"
    assert config.producer.output.value == "kafka"
    assert config.producer.rate == 3
    
    # Validate Kafka key configuration
    assert config.kafka is not None
    assert config.kafka.key_field == "user_id"
    assert config.kafka.key_strategy == "field"
    
    # Validate dictionaries
    assert "products" in config.dictionaries
    assert "users" in config.dictionaries
    assert "couriers" in config.dictionaries


def test_logistics_tracking_config_valid():
    """Test that logistics tracking config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "logistics_transport_tracking.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Validate configuration
    assert config.producer is not None
    assert config.producer.name == "logistics-tracking-system"
    assert config.producer.output.value == "kafka"
    assert config.producer.rate == 2
    
    # Validate Kafka key configuration
    assert config.kafka is not None
    assert config.kafka.key_field == "tracking_number"
    assert config.kafka.key_strategy == "field"
    
    # Validate dictionaries
    assert "shipments" in config.dictionaries
    assert "drivers" in config.dictionaries
    assert "vehicles" in config.dictionaries


def test_social_media_config_valid():
    """Test that social media config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "social_media_analytics.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Validate configuration
    assert config.producer is not None
    assert config.producer.name == "social-media-analyzer"
    assert config.producer.output.value == "kafka"
    assert config.producer.rate == 10
    
    # Validate Kafka key configuration
    assert config.kafka is not None
    assert config.kafka.key_field == "user_id"
    assert config.kafka.key_strategy == "field"
    
    # Validate dictionaries
    assert "users" in config.dictionaries
    assert "posts" in config.dictionaries
    assert "hashtags" in config.dictionaries


def test_healthcare_monitoring_config_valid():
    """Test that healthcare monitoring config example is valid"""
    examples_dir = os.path.join(os.path.dirname(__file__), "..", "examples")
    config_path = os.path.join(examples_dir, "healthcare_monitoring.yaml")
    
    # This should not raise an exception
    config = load_config(config_path)
    
    # Validate configuration
    assert config.producer is not None
    assert config.producer.name == "health-monitoring-system"
    assert config.producer.output.value == "kafka"
    assert config.producer.rate == 4
    
    # Validate Kafka key configuration
    assert config.kafka is not None
    assert config.kafka.key_field == "patient_id"
    assert config.kafka.key_strategy == "field"
    
    # Validate dictionaries
    assert "patients" in config.dictionaries
    assert "doctors" in config.dictionaries
    assert "medical_devices" in config.dictionaries