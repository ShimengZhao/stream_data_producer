"""Command-line interface for stream data producer"""

import click
import sys
import os
import time
import json
from typing import Optional, Dict, Any
import requests

from .core.config import load_config, parse_config, ProducerConfig, FieldConfig, FieldType, RuleType, OutputType
from .core.single_producer import SingleProducerManager
from .api.simple_server import SimpleAPIServer


@click.group()
@click.version_option()
def main():
    """Stream Data Producer - A configurable multi-stream data generator"""
    pass


@main.command()
@click.option('--config', '-c', required=True, help='Path to configuration file')
@click.option('--api-host', default='127.0.0.1', help='API server host')
@click.option('--api-port', default=8000, type=int, help='API server port')
@click.option('--no-api', is_flag=True, help='Run without API server')
def run(config: str, api_host: str, api_port: int, no_api: bool):
    """Run the stream data producer with specified configuration"""
    try:
        # Load configuration
        print(f"Loading configuration from {config}")
        app_config = load_config(config)
        
        if not app_config.producer:
            print("❌ No producer configuration found in config file")
            sys.exit(1)
        
        # Create single producer manager
        manager = SingleProducerManager(app_config)
        
        # Start API server if requested
        api_server = None
        if not no_api:
            api_server = SimpleAPIServer(manager, api_host, api_port)
            api_server.start(background=True)
            time.sleep(1)  # Give API server time to start
        
        # Start the producer
        if not manager.start():
            print("❌ Failed to start producer")
            sys.exit(1)
        
        print(f"\nStream Data Producer is running!")
        print(f"Producer: {app_config.producer.name} ({app_config.producer.output.value})")
        if not no_api:
            print(f"API available at http://{api_host}:{api_port}")
        print("Press Ctrl+C to stop producer")
        
        # Set up signal handlers
        import signal
        def shutdown_handler(signum, frame):
            print(f"\nReceived signal {signum}, shutting down...")
            manager.stop()
            print("Shutdown complete")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        # Keep running until interrupted
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            shutdown_handler(signal.SIGINT, None)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


@main.command()
@click.option('--config', '-c', required=True, help='Path to configuration file')
def validate(config: str):
    """Validate configuration file"""
    try:
        app_config = load_config(config)
        print(f"✅ Configuration is valid")
        
        if app_config.producer:
            print(f"📊 Found 1 producer:")
            print(f"  - {app_config.producer.name} ({app_config.producer.output.value})")
        else:
            print(f"⚠️  No producer configuration found")
            
        print(f"📚 Found {len(app_config.dictionaries)} dictionaries")
        
        # Validate dictionaries exist
        for name, dict_config in app_config.dictionaries.items():
            if os.path.exists(dict_config.file):
                print(f"  ✅ {name}: {dict_config.file}")
            else:
                print(f"  ❌ {name}: {dict_config.file} (file not found)")
                
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        sys.exit(1)


@main.command()
@click.argument('schema')
@click.option('--rate', '-r', default=1, help='Messages per second')
@click.option('--output', '-o', default='console', type=click.Choice(['console', 'file', 'kafka']), help='Output type')
@click.option('--kafka-bootstrap', help='Kafka bootstrap servers')
@click.option('--kafka-topic', help='Kafka topic')
@click.option('--file-path', help='Output file path')
def quick(schema: str, rate: int, output: str, kafka_bootstrap: str, kafka_topic: str, file_path: str):
    """Quick start with inline schema definition"""
    try:
        # Parse schema string like "id:int,name:string,score:double"
        fields = []
        for field_def in schema.split(','):
            if ':' not in field_def:
                raise ValueError(f"Invalid field definition: {field_def}")
            
            name, type_str = field_def.split(':', 1)
            field_type = FieldType(type_str.strip().lower())
            
            # Create field with appropriate rule
            if field_type in [FieldType.INT, FieldType.LONG, FieldType.DOUBLE]:
                field = FieldConfig(
                    name=name.strip(),
                    type=field_type,
                    rule=RuleType.RANDOM_RANGE,
                    min=1,
                    max=100 if field_type != FieldType.DOUBLE else 100.0
                )
            elif field_type == FieldType.STRING:
                field = FieldConfig(
                    name=name.strip(),
                    type=field_type,
                    rule=RuleType.RANDOM_FROM_LIST,
                    list=["value1", "value2", "value3"]
                )
            elif field_type == FieldType.BOOLEAN:
                field = FieldConfig(
                    name=name.strip(),
                    type=field_type,
                    rule=RuleType.RANDOM_FROM_LIST,
                    list=[True, False]
                )
            else:
                raise ValueError(f"Unsupported field type: {field_type}")
            
            fields.append(field)
        
        # Create producer config
        producer = ProducerConfig(
            name="quick-producer",
            output=OutputType(output),
            fields=fields,
            rate=rate,
            kafka_topic=kafka_topic,
            file_path=file_path
        )
        
        # Create minimal global config
        global_config = {
            "producers": [producer],
            "kafka": {
                "bootstrap_servers": kafka_bootstrap or "localhost:9092"
            } if kafka_bootstrap or output == "kafka" else {},
            "file_output": {
                "directory": "./data"
            } if output == "file" else {}
        }
        
        # Convert to AppConfig and run
        # Convert ProducerConfig to dict manually
        producer_dict = {
            "name": producer.name,
            "output": producer.output.value,
            "fields": [{
                "name": f.name,
                "type": f.type.value,
                "rule": f.rule.value,
                "min": f.min,
                "max": f.max,
                "list": f.list,
                "dictionary": f.dictionary,
                "dictionary_column": f.dictionary_column,
                "value": f.value
            } for f in producer.fields],
            "rate": producer.rate,
            "interval": producer.interval,
            "kafka_topic": producer.kafka_topic,
            "file_path": producer.file_path
        }
        app_config = parse_config({"producer": producer_dict})
        if kafka_bootstrap:
            app_config.kafka.bootstrap_servers = kafka_bootstrap
        if kafka_topic:
            app_config.kafka.default_topic = kafka_topic
        
        # For console output, run directly in main process for immediate output
        if output == "console":
            # Create and run producer directly
            from stream_data_producer.core.single_producer import SingleProducerManager
            manager = SingleProducerManager(app_config)
            
            # Setup signal handler
            import signal
            def signal_handler(signum, frame):
                print("\nShutting down...")
                manager.stop()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Run the producer directly
            manager.start()
        else:
            # For file/kafka output, use subprocess approach
            # Create temporary config file
            import tempfile
            import json
            
            # Convert to serializable format
            config_dict = {
                "producer": producer_dict,
                "kafka": {
                    "bootstrap_servers": kafka_bootstrap or "localhost:9092"
                } if kafka_bootstrap or output == "kafka" else {},
                "file_output": {
                    "directory": "./data"
                } if output == "file" else {}
            }
            
            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(config_dict, f, indent=2)
                temp_config_path = f.name
            
            try:
                manager = SingleProducerManager(app_config)
                manager.start()
                
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    manager.stop()
            finally:
                # Clean up temporary file
                import os
                if os.path.exists(temp_config_path):
                    os.unlink(temp_config_path)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


@main.command()
@click.option('--host', default='127.0.0.1', help='API server host')
@click.option('--port', default=8000, type=int, help='API server port')
def status(host: str, port: int):
    """Get status of running producers"""
    try:
        response = requests.get(f"http://{host}:{port}/status", timeout=5)
        response.raise_for_status()
        status_data = response.json()
        
        print(f"📊 Producer Status")
        print(f"📝 Name: {status_data.get('name', 'Unknown')}")
        print(f"🔄 Status: {status_data.get('status', 'unknown')}")
        print(f"📤 Output: {status_data.get('output', 'unknown')}")
        print(f"📊 Rate: {status_data.get('rate') or status_data.get('interval', 'N/A')}")
        print(f"📨 Messages Sent: {status_data.get('messages_sent', 0)}")
        print(f"📈 Current Rate: {status_data.get('current_rate', 0):.2f} msg/sec")
        if status_data.get('error_count', 0) > 0:
            print(f"❌ Errors: {status_data.get('error_count', 0)}")
            print(f"   Last Error: {status_data.get('last_error', 'N/A')}")
        print()
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to API server: {e}")
        print("Make sure the producer is running with API enabled")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error getting status: {e}")
        sys.exit(1)


@main.command()
@click.argument('producer_name')
@click.option('--rate', type=int, help='New rate (messages per second)')
@click.option('--interval', help='New interval (e.g., 5s, 1m)')
@click.option('--host', default='127.0.0.1', help='API server host')
@click.option('--port', default=8000, type=int, help='API server port')
def update_rate(producer_name: str, rate: int, interval: str, host: str, port: int):
    """Update the rate of a running producer"""
    try:
        data = {}
        if rate is not None:
            data['rate'] = rate
        if interval is not None:
            data['interval'] = interval
            
        response = requests.post(
            f"http://{host}:{port}/rate",
            json=data,
            timeout=5
        )
        response.raise_for_status()
        result = response.json()
        
        if result.get('success'):
            print(f"✅ {result.get('message')}")
        else:
            print(f"❌ {result.get('message')}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to API server: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error updating rate: {e}")
        sys.exit(1)


@main.command()
@click.argument('producer_name')
@click.option('--host', default='127.0.0.1', help='API server host')
@click.option('--port', default=8000, type=int, help='API server port')
def start(producer_name: str, host: str, port: int):
    """Start a stopped producer"""
    try:
        response = requests.post(
            f"http://{host}:{port}/start",
            timeout=5
        )
        response.raise_for_status()
        result = response.json()
        
        if result.get('success'):
            print(f"✅ {result.get('message')}")
        else:
            print(f"❌ {result.get('message')}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to API server: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error starting producer: {e}")
        sys.exit(1)


@main.command()
@click.argument('producer_name')
@click.option('--host', default='127.0.0.1', help='API server host')
@click.option('--port', default=8000, type=int, help='API server port')
def stop(producer_name: str, host: str, port: int):
    """Stop a running producer"""
    try:
        response = requests.post(
            f"http://{host}:{port}/stop",
            timeout=5
        )
        response.raise_for_status()
        result = response.json()
        
        if result.get('success'):
            print(f"✅ {result.get('message')}")
        else:
            print(f"❌ {result.get('message')}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to API server: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error stopping producer: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()