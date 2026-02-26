# Configuration Examples

This directory contains example configuration files for the Stream Data Producer.

## Available Examples

### 1. Ship Telemetry Basic
**File**: `ship_telemetry_basic.yaml`
- Basic ship telemetry monitoring configuration
- 1 message per second
- Simple field structure with ship data
- Good starting point for maritime applications

### 2. Ship Telemetry with Keys
**File**: `ship_telemetry_with_keys.yaml`
- Ship telemetry with Kafka message keys enabled
- Uses ship_id as partition key
- Demonstrates key-based message routing

### 3. Kafka Key Configuration
**File**: `config_with_kafka_key.example.yaml`
- Advanced configuration with Kafka message key enabled
- Demonstrates different key strategies
- Shows how to route messages by ship ID for better partitioning

### 3. Financial Trading Monitor
**File**: `financial_trading_monitor.yaml`
- Real-time stock trading data monitoring
- 5 transactions per second
- Uses stock symbol as Kafka key for partition routing
- Fields: trade_id, stock_symbol, company_name, trader_id, price, volume, etc.

### 4. E-commerce Order Processing
**File**: `ecommerce_order_processing.yaml`
- Real-time e-commerce order processing
- 3 orders per second
- Uses user_id as Kafka key for customer-based routing
- Fields: order_id, user_id, product_id, quantity, payment_method, order_status, etc.

### 5. Logistics Transport Tracking
**File**: `logistics_transport_tracking.yaml`
- Real-time cargo transportation tracking
- 2 tracking records per second
- Uses tracking_number as Kafka key for shipment-based routing
- Fields: tracking_number, vehicle_plate, driver_id, latitude, longitude, temperature, etc.

### 6. Social Media Analytics
**File**: `social_media_analytics.yaml`
- Real-time social media user behavior analysis
- 10 interactions per second
- Uses user_id as Kafka key for user-based routing
- Fields: interaction_id, user_id, post_id, interaction_type, engagement_score, etc.

### 7. Healthcare Monitoring
**File**: `healthcare_monitoring.yaml`
- Real-time patient health monitoring
- 4 health records per second
- Uses patient_id as Kafka key for patient-based routing
- Fields: record_id, patient_id, doctor_id, heart_rate, blood_pressure, temperature, etc.

## Usage

Copy any example file to create your own configuration:

```bash
# Copy ship telemetry basic configuration
cp examples/ship_telemetry_basic.yaml my_config.yaml

# Or copy any other example
cp examples/financial_trading_monitor.yaml trading_config.yaml

# Then edit the configuration file according to your needs
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