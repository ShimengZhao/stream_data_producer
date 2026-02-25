# Configuration Examples

This directory contains example configuration files for the Stream Data Producer.

## Available Examples

### 1. Basic Configuration
**File**: `config.example.yaml`
- Basic configuration template
- Includes all required sections with comments
- Good starting point for new users

### 2. Kafka Key Configuration
**File**: `config_with_kafka_key.example.yaml`
- Advanced configuration with Kafka message key enabled
- Demonstrates different key strategies
- Shows how to route messages by ship ID for better partitioning

## Usage

Copy any example file to create your own configuration:

```bash
cp examples/config.example.yaml my_config.yaml
# Then edit my_config.yaml according to your needs
```

## Configuration Sections

All examples include these main sections:

- **kafka**: Kafka connection and key configuration
- **file_output**: File output settings
- **error_log**: Error logging configuration
- **dictionaries**: Data dictionary definitions
- **producer**: Main producer configuration

## Key Features Demonstrated

- Different output types (console, file, Kafka)
- Various field generation rules
- Rate control options
- Kafka authentication
- **NEW**: Kafka message key strategies