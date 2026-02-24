"""Configuration models and parser for stream data producer"""

import os
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import yaml


class OutputType(Enum):
    CONSOLE = "console"
    FILE = "file"
    KAFKA = "kafka"


class FieldType(Enum):
    INT = "int"
    LONG = "long"
    DOUBLE = "double"
    STRING = "string"
    BOOLEAN = "boolean"


class RuleType(Enum):
    RANDOM_RANGE = "random_range"
    RANDOM_FROM_LIST = "random_from_list"
    RANDOM_FROM_DICTIONARY = "random_from_dictionary"
    NOW = "now"
    CONSTANT = "constant"


class RollingType(Enum):
    HOURLY = "hourly"
    DAILY = "daily"


@dataclass
class FieldConfig:
    """Configuration for a single data field"""
    name: str
    type: FieldType
    rule: RuleType
    # For random_range
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    # For random_from_list
    list: Optional[List] = None
    # For random_from_dictionary
    dictionary: Optional[str] = None
    dictionary_column: Optional[Union[str, int]] = None
    # For constant
    value: Optional[Union[str, int, float, bool]] = None


@dataclass
class DictionaryConfig:
    """Configuration for data dictionary"""
    file: str
    columns: Dict[str, Union[str, int]]


@dataclass
class KafkaConfig:
    """Global Kafka configuration"""
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    default_topic: str = "telemetry"


@dataclass
class FileOutputConfig:
    """Global file output configuration"""
    directory: str = "./data"
    rolling: RollingType = RollingType.HOURLY
    filename_pattern: Optional[str] = None


@dataclass
class ErrorLogConfig:
    """Error log configuration"""
    directory: str = "./logs"
    rolling: RollingType = RollingType.DAILY
    max_age_days: int = 7


@dataclass
class ProducerConfig:
    """Configuration for a single producer"""
    name: str
    output: OutputType
    fields: List[FieldConfig]
    # Rate control - either rate or interval should be specified
    rate: Optional[int] = None  # messages per second
    interval: Optional[str] = None  # e.g., "5s", "1m"
    # Output specific configurations
    kafka_topic: Optional[str] = None
    file_path: Optional[str] = None
    # Status tracking
    status: str = "stopped"
    uptime_seconds: int = 0
    total_messages: int = 0
    current_rate: float = 0.0
    error_count: int = 0
    last_error: Optional[str] = None


@dataclass
class AppConfig:
    """Main application configuration - Single producer mode"""
    kafka: Optional[KafkaConfig] = None
    file_output: FileOutputConfig = field(default_factory=FileOutputConfig)
    error_log: ErrorLogConfig = field(default_factory=ErrorLogConfig)
    dictionaries: Dict[str, DictionaryConfig] = field(default_factory=dict)
    producer: Optional[ProducerConfig] = None  # Single producer instead of list


def load_config(config_path: str) -> AppConfig:
    """Load configuration from YAML file"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config_dict = yaml.safe_load(f)
    
    return parse_config(config_dict)


def parse_config(config_dict: dict) -> AppConfig:
    """Parse configuration dictionary into AppConfig object"""
    config = AppConfig()
    
    # Parse global configurations
    if 'kafka' in config_dict:
        kafka_config = config_dict['kafka']
        config.kafka = KafkaConfig(
            bootstrap_servers=kafka_config.get('bootstrap_servers'),
            security_protocol=kafka_config.get('security_protocol', 'PLAINTEXT'),
            sasl_mechanism=kafka_config.get('sasl_mechanism'),
            sasl_username=kafka_config.get('sasl_username'),
            sasl_password=kafka_config.get('sasl_password'),
            default_topic=kafka_config.get('default_topic', 'telemetry')
        )
    
    if 'file_output' in config_dict:
        file_config = config_dict['file_output']
        config.file_output = FileOutputConfig(
            directory=file_config.get('directory', './data'),
            rolling=RollingType(file_config.get('rolling', 'hourly')),
            filename_pattern=file_config.get('filename_pattern')
        )
    
    if 'error_log' in config_dict:
        error_config = config_dict['error_log']
        config.error_log = ErrorLogConfig(
            directory=error_config.get('directory', './logs'),
            rolling=RollingType(error_config.get('rolling', 'daily')),
            max_age_days=error_config.get('max_age_days', 7)
        )
    
    # Parse dictionaries
    if 'dictionaries' in config_dict:
        for name, dict_config in config_dict['dictionaries'].items():
            config.dictionaries[name] = DictionaryConfig(
                file=dict_config['file'],
                columns=dict_config['columns']
            )
    
    # Parse single producer
    if 'producer' in config_dict:
        prod_config = config_dict['producer']
        fields = []
        for field_dict in prod_config.get('fields', []):
            field = FieldConfig(
                name=field_dict['name'],
                type=FieldType(field_dict['type']),
                rule=RuleType(field_dict['rule'])
            )
            
            # Set rule-specific parameters
            if field.rule == RuleType.RANDOM_RANGE:
                field.min = field_dict.get('min')
                field.max = field_dict.get('max')
            elif field.rule == RuleType.RANDOM_FROM_LIST:
                field.list = field_dict.get('list')
            elif field.rule == RuleType.RANDOM_FROM_DICTIONARY:
                field.dictionary = field_dict.get('dictionary')
                field.dictionary_column = field_dict.get('dictionary_column')
            elif field.rule == RuleType.CONSTANT:
                field.value = field_dict.get('value')
            
            fields.append(field)
        
        config.producer = ProducerConfig(
            name=prod_config['name'],
            output=OutputType(prod_config['output']),
            fields=fields,
            rate=prod_config.get('rate'),
            interval=prod_config.get('interval'),
            kafka_topic=prod_config.get('kafka_topic'),
            file_path=prod_config.get('file_path')
        )
    elif 'producers' in config_dict:
        # Backward compatibility: use first producer from list
        prod_config = config_dict['producers'][0]
        fields = []
        for field_dict in prod_config.get('fields', []):
            field = FieldConfig(
                name=field_dict['name'],
                type=FieldType(field_dict['type']),
                rule=RuleType(field_dict['rule'])
            )
            
            # Set rule-specific parameters
            if field.rule == RuleType.RANDOM_RANGE:
                field.min = field_dict.get('min')
                field.max = field_dict.get('max')
            elif field.rule == RuleType.RANDOM_FROM_LIST:
                field.list = field_dict.get('list')
            elif field.rule == RuleType.RANDOM_FROM_DICTIONARY:
                field.dictionary = field_dict.get('dictionary')
                field.dictionary_column = field_dict.get('dictionary_column')
            elif field.rule == RuleType.CONSTANT:
                field.value = field_dict.get('value')
            
            fields.append(field)
        
        config.producer = ProducerConfig(
            name=prod_config['name'],
            output=OutputType(prod_config['output']),
            fields=fields,
            rate=prod_config.get('rate'),
            interval=prod_config.get('interval'),
            kafka_topic=prod_config.get('kafka_topic'),
            file_path=prod_config.get('file_path')
        )
    
    return config