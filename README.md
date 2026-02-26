# Stream Data Producer

A configurable, multi-stream data generator for testing and simulation purposes. This tool allows you to generate realistic test data streams and send them to various outputs including console, files, and Kafka.

## Features

- **Single Producer Architecture**: Simplified data generation with one producer at a time
- **Flexible Data Generation**: Support for various field types and generation rules
- **Multiple Output Targets**: Console, file (JSON lines), and Kafka with authentication
- **Kafka Message Keys**: Configurable message keys for partition routing and ordering
- **Rate Control**: Configurable message rates or intervals
- **Monitoring API**: RESTful API for status monitoring
- **Error Handling**: Comprehensive error logging with data retention
- **Dictionary Support**: Random data generation from CSV dictionaries
- **Configuration Examples**: Ready-to-use templates in `examples/` directory

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd stream_data_producer

# Install dependencies
pip install -e .

# Or install directly from PyPI (when available)
pip install stream-data-producer
```

## Quick Start

### Using Command Line Interface

```bash
# Quick start with inline schema
stream-data-producer quick "id:int,name:string,score:double" --rate 10 --output console

# Run with ship telemetry configuration
stream-data-producer run --config examples/ship_telemetry_basic.yaml

# Validate configuration
stream-data-producer validate --config examples/ship_telemetry_basic.yaml

# Check status of running producers
stream-data-producer status

# Control running producers
stream-data-producer update-rate my-producer --rate 50
stream-data-producer stop my-producer
stream-data-producer start my-producer
```

### Using Configuration Examples

The `examples/` directory contains ready-to-use configuration templates for various domains:

#### Domain-Specific Examples

```bash
# Financial Trading Monitoring (5 msg/sec)
cp examples/financial_trading_monitor.yaml trading_config.yaml

# E-commerce Order Processing (3 msg/sec)
cp examples/ecommerce_order_processing.yaml ecommerce_config.yaml

# Logistics Transport Tracking (2 msg/sec)
cp examples/logistics_transport_tracking.yaml logistics_config.yaml

# Social Media Analytics (10 msg/sec)
cp examples/social_media_analytics.yaml social_config.yaml

# Healthcare Monitoring (4 msg/sec)
cp examples/healthcare_monitoring.yaml health_config.yaml
```

#### Template Examples

```bash
# List all available examples
ls examples/

# Copy basic ship telemetry template
cp examples/ship_telemetry_basic.yaml ship_config.yaml

# Copy ship telemetry with Kafka keys
cp examples/ship_telemetry_with_keys.yaml ship_key_config.yaml

# Copy advanced Kafka key configuration
cp examples/config_with_kafka_key.example.yaml kafka_config.yaml

# View detailed examples documentation
cat examples/README.md
```

### Using as a Library

```python
from stream_data_producer.core.config import load_config
from stream_data_producer.core.single_producer import SingleProducerManager

# Load configuration
config = load_config("config.yaml")

# Create and start manager
manager = SingleProducerManager(config)
manager.start()

# Monitor status
status = manager.get_status()
print(status)

# Stop producer
manager.stop()
```

## Configuration

### Basic Configuration Structure

```yaml
# Global Kafka configuration
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: "SASL_PLAINTEXT"
  sasl_mechanism: "PLAIN"
  sasl_username: "user"
  sasl_password: "pass"
  default_topic: "telemetry"
  # Kafka message key configuration
  key_field: "ship_id"           # Field to use as message key
  key_strategy: "field"          # field, random, timestamp, composite, none

# Global file output configuration
file_output:
  directory: "./data"
  rolling: "hourly"          # or daily

# Error log configuration
error_log:
  directory: "./logs"
  rolling: "daily"
  max_age_days: 7

# Data dictionaries
dictionaries:
  ships:
    file: "./data/ships.csv"
    columns:
      id: 0          # Column index
      type: 1        # Column index

# Single Producer definition
producer:
  name: telemetry-producer
  rate: 1          # messages per second
  output: kafka
  kafka_topic: ship_telemetry
  fields:
    - name: ship_id
      type: string
      rule: random_from_dictionary
      dictionary: ships
      dictionary_column: id
    - name: timestamp
      type: long
      rule: now
    - name: speed
      type: double
      rule: random_range
      min: 10.0
      max: 25.0
    - name: wave_height
      type: double
      rule: random_range
      min: 0.5
      max: 6.0
```

### Field Types and Rules

#### Supported Field Types
- `int`: Integer values
- `long`: Long integer values
- `double`: Floating-point values
- `string`: String values
- `boolean`: Boolean values

#### Supported Generation Rules

1. **Random Range** (`random_range`)
   ```yaml
   - name: temperature
     type: double
     rule: random_range
     min: -10.0
     max: 40.0
   ```

2. **Random from List** (`random_from_list`)
   ```yaml
   - name: status
     type: string
     rule: random_from_list
     list: ["active", "inactive", "maintenance"]
   ```

3. **Random from Dictionary** (`random_from_dictionary`)
   ```yaml
   - name: ship_id
     type: string
     rule: random_from_dictionary
     dictionary: ships
     dictionary_column: id
   ```

4. **Current Timestamp** (`now`)
   ```yaml
   - name: timestamp
     type: long  # or string
     rule: now
   ```

5. **Constant Value** (`constant`)
   ```yaml
   - name: version
     type: string
     rule: constant
     value: "1.0.0"
   ```

### Kafka Message Keys

Kafka messages can include keys for partition routing and message ordering:

1. **Field-based Key** (`field` strategy):
   ```yaml
   kafka:
     key_field: "ship_id"
     key_strategy: "field"
   ```
   Uses the specified field value as the message key.

2. **Random Key** (`random` strategy):
   ```yaml
   kafka:
     key_strategy: "random"
   ```
   Generates a random UUID for each message.

3. **Timestamp Key** (`timestamp` strategy):
   ```yaml
   kafka:
     key_strategy: "timestamp"
   ```
   Uses current timestamp as the message key.

4. **Composite Key** (`composite` strategy):
   ```yaml
   kafka:
     key_field: "ship_id,timestamp"
     key_strategy: "composite"
   ```
   Combines multiple fields with underscore separator.

5. **No Key** (`none` strategy):
   ```yaml
   kafka:
     key_strategy: "none"
   ```
   Sends messages without keys.

### Rate Control Options

You can control the data generation rate in two ways:

1. **Rate-based** (messages per second):
   ```yaml
   rate: 10  # 10 messages per second
   ```

2. **Interval-based** (fixed time intervals):
   ```yaml
   interval: 5s  # Every 5 seconds
   ```

Supported interval units: `ms`, `s`, `m`, `h`

## API Endpoints

When running with API enabled, the following endpoints are available:

### GET Endpoints
- `GET /status` - Get status of the single producer
- `GET /health` - Health check endpoint

### Note
In single producer mode, the API provides simplified endpoints focusing on the single active producer rather than managing multiple producers.

### Example API Usage

```bash
# Get producer status
curl http://localhost:8000/status

# Health check
curl http://localhost:8000/health
```

## Output Formats

### Console Output
Data is printed to stdout in JSON lines format:
```json
{"ship_id": "SHIP001", "timestamp": 1708765432123, "speed": 15.7}
{"ship_id": "SHIP002", "timestamp": 1708765433456, "speed": 18.2}
```

### File Output
Files are written in JSON lines format with automatic time-based rolling:
```json
{"ship_id": "SHIP001", "wave_height": 2.3, "wind_speed": 15.0}
{"ship_id": "SHIP002", "wave_height": 1.8, "wind_speed": 12.5}
```

### Kafka Output
Messages are sent as JSON to specified Kafka topics with full SASL/PLAIN authentication support.

## Error Handling

The system provides comprehensive error handling:

- **Dropped Data Logging**: Failed messages are logged with full context
- **Error Statistics**: Track error counts and last error messages
- **Log Rotation**: Error logs are automatically rotated and cleaned up

## Advanced Features

### Simplified Architecture
- Single producer model eliminates process management complexity
- Reduced resource overhead compared to multi-producer approach
- Simpler error handling and debugging

### Real-time Monitoring
- Continuous status reporting via API
- Detailed metrics and statistics
- Health check endpoints for monitoring systems

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest

# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_integration.py

# Run examples validation tests
pytest tests/test_examples.py

# Run with verbose output
pytest tests/ -v
```

### Project Structure

```
stream_data_producer/
├── stream_data_producer/
│   ├── core/           # Core components (config, generator, single_producer)
│   ├── output/         # Output handlers (console, file, kafka)
│   ├── utils/          # Utility functions
│   ├── api/           # REST API server (simplified)
│   └── cli.py         # Command-line interface
├── examples/          # Configuration examples
│   ├── README.md      # Examples documentation
│   ├── financial_trading_monitor.yaml      # Financial trading monitoring
│   ├── ecommerce_order_processing.yaml     # E-commerce order processing
│   ├── logistics_transport_tracking.yaml   # Logistics transport tracking
│   ├── social_media_analytics.yaml         # Social media analytics
│   ├── healthcare_monitoring.yaml          # Healthcare monitoring
│   ├── ship_telemetry_basic.yaml           # Ship telemetry (basic)
│   ├── ship_telemetry_with_keys.yaml       # Ship telemetry with Kafka keys
│   └── config_with_kafka_key.example.yaml  # Advanced Kafka key configuration
├── tests/             # Test suite
├── data/              # Sample data files
└── logs/              # Log directory
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues, questions, or feature requests, please open an issue on the GitHub repository.