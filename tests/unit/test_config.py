"""Unit tests for configuration module"""

import pytest
import tempfile
import os
from pathlib import Path

from stream_data_producer.core.config import (
    FieldType, RuleType, OutputType, 
    FieldConfig, ProducerConfig, DictionaryConfig, AppConfig,
    load_config, parse_config
)


class TestFieldTypes:
    """Test field type enums"""
    
    def test_field_types_values(self):
        """Test that field type enum values are correct"""
        assert FieldType.INT.value == "int"
        assert FieldType.LONG.value == "long"
        assert FieldType.DOUBLE.value == "double"
        assert FieldType.STRING.value == "string"
        assert FieldType.BOOLEAN.value == "boolean"
    
    def test_field_types_creation(self):
        """Test creating field types from string values"""
        assert FieldType("int") == FieldType.INT
        assert FieldType("long") == FieldType.LONG
        assert FieldType("double") == FieldType.DOUBLE
        assert FieldType("string") == FieldType.STRING
        assert FieldType("boolean") == FieldType.BOOLEAN
    
    def test_invalid_field_type(self):
        """Test that invalid field types raise ValueError"""
        with pytest.raises(ValueError):
            FieldType("invalid")


class TestRuleTypes:
    """Test rule type enums"""
    
    def test_rule_types_values(self):
        """Test that rule type enum values are correct"""
        assert RuleType.RANDOM_RANGE.value == "random_range"
        assert RuleType.RANDOM_FROM_LIST.value == "random_from_list"
        assert RuleType.RANDOM_FROM_DICTIONARY.value == "random_from_dictionary"
        assert RuleType.NOW.value == "now"
        assert RuleType.CONSTANT.value == "constant"
    
    def test_rule_types_creation(self):
        """Test creating rule types from string values"""
        assert RuleType("random_range") == RuleType.RANDOM_RANGE
        assert RuleType("random_from_list") == RuleType.RANDOM_FROM_LIST
        assert RuleType("random_from_dictionary") == RuleType.RANDOM_FROM_DICTIONARY
        assert RuleType("now") == RuleType.NOW
        assert RuleType("constant") == RuleType.CONSTANT


class TestOutputTypes:
    """Test output type enums"""
    
    def test_output_types_values(self):
        """Test that output type enum values are correct"""
        assert OutputType.CONSOLE.value == "console"
        assert OutputType.FILE.value == "file"
        assert OutputType.KAFKA.value == "kafka"
    
    def test_output_types_creation(self):
        """Test creating output types from string values"""
        assert OutputType("console") == OutputType.CONSOLE
        assert OutputType("file") == OutputType.FILE
        assert OutputType("kafka") == OutputType.KAFKA


class TestFieldConfig:
    """Test FieldConfig class"""
    
    def test_field_config_creation(self):
        """Test creating field configuration"""
        field = FieldConfig(
            name="test_field",
            type=FieldType.STRING,
            rule=RuleType.RANDOM_FROM_LIST,
            list=["value1", "value2"]
        )
        
        assert field.name == "test_field"
        assert field.type == FieldType.STRING
        assert field.rule == RuleType.RANDOM_FROM_LIST
        assert field.list == ["value1", "value2"]
    
    def test_field_config_with_range(self):
        """Test field config with range parameters"""
        field = FieldConfig(
            name="number_field",
            type=FieldType.INT,
            rule=RuleType.RANDOM_RANGE,
            min=1,
            max=100
        )
        
        assert field.min == 1
        assert field.max == 100
    
    def test_field_config_with_constant(self):
        """Test field config with constant value"""
        field = FieldConfig(
            name="const_field",
            type=FieldType.STRING,
            rule=RuleType.CONSTANT,
            value="fixed_value"
        )
        
        assert field.value == "fixed_value"
    
    def test_field_config_with_dictionary(self):
        """Test field config with dictionary parameters"""
        field = FieldConfig(
            name="dict_field",
            type=FieldType.STRING,
            rule=RuleType.RANDOM_FROM_DICTIONARY,
            dictionary="test_dict",
            dictionary_column="name"
        )
        
        assert field.dictionary == "test_dict"
        assert field.dictionary_column == "name"


class TestProducerConfig:
    """Test ProducerConfig class"""
    
    @pytest.fixture
    def sample_fields(self):
        """Sample field configurations"""
        return [
            FieldConfig(name="id", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=1, max=100),
            FieldConfig(name="name", type=FieldType.STRING, rule=RuleType.RANDOM_FROM_LIST, list=["Alice", "Bob"])
        ]
    
    def test_producer_config_creation(self, sample_fields):
        """Test creating producer configuration"""
        producer = ProducerConfig(
            name="test_producer",
            output=OutputType.CONSOLE,
            fields=sample_fields,
            rate=10
        )
        
        assert producer.name == "test_producer"
        assert producer.output == OutputType.CONSOLE
        assert producer.fields == sample_fields
        assert producer.rate == 10
        assert producer.status == "stopped"
        assert producer.total_messages == 0
        assert producer.current_rate == 0.0
    
    def test_producer_config_with_interval(self, sample_fields):
        """Test producer config with interval instead of rate"""
        producer = ProducerConfig(
            name="interval_producer",
            output=OutputType.FILE,
            fields=sample_fields,
            interval="5s"
        )
        
        assert producer.interval == "5s"
        assert producer.rate is None
    
    def test_producer_config_with_kafka(self, sample_fields):
        """Test producer config with Kafka settings"""
        producer = ProducerConfig(
            name="kafka_producer",
            output=OutputType.KAFKA,
            fields=sample_fields,
            rate=5,
            kafka_topic="test-topic"
        )
        
        assert producer.kafka_topic == "test-topic"
    
    def test_producer_config_with_file(self, sample_fields):
        """Test producer config with file settings"""
        producer = ProducerConfig(
            name="file_producer",
            output=OutputType.FILE,
            fields=sample_fields,
            interval="1m",
            file_path="./output.json"
        )
        
        assert producer.file_path == "./output.json"


class TestDictionaryConfig:
    """Test DictionaryConfig class"""
    
    def test_dictionary_config_creation(self):
        """Test creating dictionary configuration"""
        dict_config = DictionaryConfig(
            file="./data/dictionary.csv",
            columns={"id": 0, "name": 1, "type": 2}
        )
        
        assert dict_config.file == "./data/dictionary.csv"
        assert dict_config.columns == {"id": 0, "name": 1, "type": 2}


class TestAppConfig:
    """Test AppConfig class"""
    
    @pytest.fixture
    def sample_producer(self):
        """Sample producer configuration"""
        fields = [FieldConfig(name="id", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=1, max=100)]
        return ProducerConfig(name="producer1", output=OutputType.CONSOLE, fields=fields, rate=10)
    
    @pytest.fixture
    def sample_dictionaries(self):
        """Sample dictionary configurations"""
        return {
            "test_dict": DictionaryConfig(file="./data/test.csv", columns={"id": 0, "name": 1})
        }
    
    def test_app_config_creation(self, sample_producer, sample_dictionaries):
        """Test creating application configuration"""
        app_config = AppConfig(
            producer=sample_producer,
            dictionaries=sample_dictionaries
        )
        
        assert app_config.producer == sample_producer
        assert app_config.dictionaries == sample_dictionaries
    
    def test_app_config_defaults(self):
        """Test application configuration defaults"""
        app_config = AppConfig(producer=None, dictionaries={})
        
        # Check that config objects exist
        assert hasattr(app_config, 'kafka')
        assert hasattr(app_config, 'file_output')
        assert hasattr(app_config, 'error_log')
        assert app_config.file_output.directory == "./data"
        assert app_config.file_output.rolling.value == "hourly"
        assert app_config.error_log.directory == "./logs"
        assert app_config.error_log.rolling.value == "daily"
        assert app_config.error_log.max_age_days == 7


class TestConfigLoading:
    """Test configuration loading functions"""
    
    @pytest.fixture
    def minimal_config_content(self):
        """Minimal configuration content for testing"""
        return """
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

dictionaries: {}
"""
    
    def test_parse_minimal_config(self, minimal_config_content):
        """Test parsing minimal configuration"""
        config_dict = {"producer": {
            "name": "test-producer",
            "rate": 10,
            "output": "console",
            "fields": [{
                "name": "id",
                "type": "int",
                "rule": "random_range",
                "min": 1,
                "max": 100
            }]
        }}
        
        app_config = parse_config(config_dict)
        
        assert app_config.producer is not None
        producer = app_config.producer
        assert producer.name == "test-producer"
        assert producer.rate == 10
        assert producer.output == OutputType.CONSOLE
        assert len(producer.fields) == 1
        
        field = producer.fields[0]
        assert field.name == "id"
        assert field.type == FieldType.INT
        assert field.rule == RuleType.RANDOM_RANGE
        assert field.min == 1
        assert field.max == 100
    
    def test_load_config_from_file(self, minimal_config_content):
        """Test loading configuration from file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(minimal_config_content)
            config_path = f.name
        
        try:
            app_config = load_config(config_path)
            assert app_config.producer is not None
            assert app_config.producer.name == "test-producer"
        finally:
            os.unlink(config_path)
    
    def test_invalid_config_raises_error(self):
        """Test that invalid configuration raises appropriate errors"""
        # Invalid field type
        invalid_config = {"producer": {
            "name": "test",
            "rate": 10,
            "output": "console",
            "fields": [{
                "name": "id",
                "type": "invalid_type",  # This should cause an error
                "rule": "random_range",
                "min": 1,
                "max": 100
            }]
        }}
        
        # Should raise ValueError for invalid field type
        with pytest.raises(ValueError):
            parse_config(invalid_config)
    
    def test_config_with_all_sections(self):
        """Test configuration with all sections present"""
        full_config = {
            "kafka": {
                "bootstrap_servers": "kafka-server:9092",
                "default_topic": "my-topic",
                "security_protocol": "SASL_PLAINTEXT"
            },
            "file_output": {
                "directory": "./custom_data",
                "rolling": "daily"
            },
            "error_log": {
                "directory": "./custom_logs",
                "rolling": "hourly",
                "max_age_days": 3
            },
            "dictionaries": {
                "users": {
                    "file": "./data/users.csv",
                    "columns": {"id": 0, "name": 1}
                }
            },
            "producer": {
                "name": "full-producer",
                "rate": 5,
                "output": "kafka",
                "kafka_topic": "custom-topic",
                "fields": [{
                    "name": "user_id",
                    "type": "string",
                    "rule": "random_from_dictionary",
                    "dictionary": "users",
                    "dictionary_column": "id"
                }]
            }
        }
        
        app_config = parse_config(full_config)
        
        # Check Kafka config
        assert app_config.kafka.bootstrap_servers == "kafka-server:9092"
        assert app_config.kafka.default_topic == "my-topic"
        assert app_config.kafka.security_protocol == "SASL_PLAINTEXT"
        
        # Check file output config
        assert app_config.file_output.directory == "./custom_data"
        assert app_config.file_output.rolling.value == "daily"
        
        # Check error log config
        assert app_config.error_log.directory == "./custom_logs"
        assert app_config.error_log.rolling.value == "hourly"
        assert app_config.error_log.max_age_days == 3
        
        # Check dictionaries
        assert "users" in app_config.dictionaries
        assert app_config.dictionaries["users"].file == "./data/users.csv"
        
        # Check producer
        assert app_config.producer is not None
        producer = app_config.producer
        assert producer.name == "full-producer"
        assert producer.kafka_topic == "custom-topic"