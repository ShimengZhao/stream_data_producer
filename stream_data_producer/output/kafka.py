"""Kafka output handler with SASL/PLAIN authentication"""

import json
from typing import Dict, Any, Optional
from confluent_kafka import Producer, KafkaException
import socket


class KafkaOutput:
    """Output handler for Kafka with authentication support"""
    
    def __init__(self, bootstrap_servers: str, topic: str,
                 security_protocol: str = "PLAINTEXT",
                 sasl_mechanism: Optional[str] = None,
                 sasl_username: Optional[str] = None,
                 sasl_password: Optional[str] = None,
                 key_field: Optional[str] = None,
                 key_strategy: str = "field",  # field, random, timestamp, none
                 **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        self.key_field = key_field
        self.key_strategy = key_strategy.lower()
        self._producer_config = self._build_producer_config(
            bootstrap_servers, security_protocol, sasl_mechanism,
            sasl_username, sasl_password, **kwargs
        )
        self._initialize_producer()
    
    def _build_producer_config(self, bootstrap_servers: str, 
                              security_protocol: str,
                              sasl_mechanism: Optional[str],
                              sasl_username: Optional[str],
                              sasl_password: Optional[str],
                              **kwargs) -> Dict[str, Any]:
        """Build Kafka producer configuration"""
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'stream-data-producer-{socket.gethostname()}',
            'message.timeout.ms': 30000,  # 30 seconds
            'request.timeout.ms': 30000,
            'socket.connection.setup.timeout.ms': 30000,
        }
        
        # Add security configuration
        if security_protocol != "PLAINTEXT":
            config['security.protocol'] = security_protocol
            
            if sasl_mechanism:
                config['sasl.mechanism'] = sasl_mechanism
                
            if sasl_username and sasl_password:
                config['sasl.username'] = sasl_username
                config['sasl.password'] = sasl_password
        
        # Add any additional configuration
        config.update(kwargs)
        
        return config
    
    def _initialize_producer(self) -> None:
        """Initialize the Kafka producer"""
        try:
            self.producer = Producer(self._producer_config)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kafka producer: {e}")
    
    def _generate_key(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Generate message key based on configured strategy.
        Returns key string or None if no key should be used.
        """
        if self.key_strategy == "none":
            return None
        
        if self.key_strategy == "field" and self.key_field:
            # Use specified field as key
            if self.key_field in data:
                return str(data[self.key_field])
            else:
                print(f"Warning: Key field '{self.key_field}' not found in data")
                return None
        
        elif self.key_strategy == "random":
            # Generate random key
            import uuid
            return str(uuid.uuid4())
        
        elif self.key_strategy == "timestamp":
            # Use timestamp as key
            import time
            return str(int(time.time() * 1000))
        
        elif self.key_strategy == "composite" and self.key_field:
            # Combine multiple fields (comma-separated in key_field)
            fields = [f.strip() for f in self.key_field.split(',')]
            key_parts = []
            for field in fields:
                if field in data:
                    key_parts.append(str(data[field]))
                else:
                    print(f"Warning: Composite key field '{field}' not found in data")
                    return None
            return "_".join(key_parts)
        
        return None
    
    def send(self, data: Dict[str, Any]) -> bool:
        """
        Send data to Kafka topic.
        Returns True if successful, False otherwise.
        """
        if not self.producer:
            return False
        
        try:
            # Convert data to JSON
            json_data = json.dumps(data, ensure_ascii=False)
            
            # Generate message key
            message_key = self._generate_key(data)
            
            # Prepare producer arguments
            produce_kwargs = {
                'topic': self.topic,
                'value': json_data.encode('utf-8'),
                'callback': self._delivery_callback
            }
            
            # Add key if present
            if message_key is not None:
                produce_kwargs['key'] = message_key.encode('utf-8')
            
            # Produce message asynchronously
            self.producer.produce(**produce_kwargs)
            
            # Poll for delivery reports
            self.producer.poll(0)
            return True
            
        except KafkaException as e:
            print(f"Kafka error: {e}")
            return False
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
            return False
    
    def _delivery_callback(self, err, msg) -> None:
        """Callback for message delivery confirmation"""
        if err:
            print(f"Message delivery failed: {err}")
        else:
            # Message delivered successfully
            pass
    
    def flush(self, timeout: float = 10.0) -> int:
        """
        Wait for all messages to be delivered.
        Returns number of outstanding messages.
        """
        if self.producer:
            return self.producer.flush(timeout)
        return 0
    
    def close(self) -> None:
        """Close the Kafka producer"""
        if self.producer:
            try:
                # Flush any remaining messages
                remaining = self.producer.flush(10.0)
                if remaining > 0:
                    print(f"Warning: {remaining} messages unsent")
            except Exception as e:
                print(f"Error flushing Kafka producer: {e}")
            finally:
                self.producer = None


class KafkaHealthChecker:
    """Utility class for checking Kafka connectivity"""
    
    @staticmethod
    def check_connection(bootstrap_servers: str,
                        security_protocol: str = "PLAINTEXT",
                        sasl_mechanism: Optional[str] = None,
                        sasl_username: Optional[str] = None,
                        sasl_password: Optional[str] = None) -> bool:
        """Check if Kafka cluster is reachable"""
        try:
            # Simple connection test using metadata request
            config = {
                'bootstrap.servers': bootstrap_servers,
                'security.protocol': security_protocol,
                'socket.connection.setup.timeout.ms': 5000,  # 5 seconds
                'metadata.max.age.ms': 30000,
            }
            
            if sasl_mechanism and sasl_username and sasl_password:
                config.update({
                    'sasl.mechanism': sasl_mechanism,
                    'sasl.username': sasl_username,
                    'sasl.password': sasl_password,
                })
            
            producer = Producer(config)
            
            # Try to get metadata
            metadata = producer.list_topics(timeout=10.0)
            producer.flush(1.0)
            
            return metadata is not None
            
        except Exception as e:
            print(f"Kafka connection check failed: {e}")
            return False