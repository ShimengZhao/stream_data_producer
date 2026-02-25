"""Single producer manager for simplified data generation"""

import time
import signal
import threading
from typing import Optional
from datetime import datetime

from ..core.config import AppConfig
from ..core.generator import DataGenerator
from ..core.rate_controller import RateController
from ..core.dictionary import DictionaryLoader
from ..output.console import ConsoleOutput
from ..output.file import FileOutput
from ..output.kafka import KafkaOutput
from ..utils.error_logger import ErrorTracker


class SingleProducerManager:
    """Manages a single data producer with simplified lifecycle"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.producer_config = config.producer
        self.data_generator = None
        self.rate_controller = None
        self.output_handler = None
        self.error_tracker = ErrorTracker()
        self._running = False
        self._thread = None
        self._stats = {
            "start_time": None,
            "messages_sent": 0,
            "last_message_time": None
        }
    
    def initialize(self) -> bool:
        """Initialize all components"""
        if not self.producer_config:
            print("❌ No producer configuration found")
            return False
        
        try:
            # Initialize dictionary loader
            self.dictionary_loader = DictionaryLoader()
            self.dictionary_loader.load_all_dictionaries(self.config.dictionaries)
            
            # Initialize data generator
            self.data_generator = DataGenerator(self.dictionary_loader)
            
            # Initialize rate controller
            self.rate_controller = RateController(
                rate=self.producer_config.rate,
                interval=self.producer_config.interval
            )
            
            # Initialize output handler based on type
            if self.producer_config.output.name == "CONSOLE":
                self.output_handler = ConsoleOutput()
            elif self.producer_config.output.name == "FILE":
                self.output_handler = FileOutput(
                    base_path=self.producer_config.file_path or f"./data/{self.producer_config.name}.json",
                    rolling_config=self.config.file_output
                )
            elif self.producer_config.output.name == "KAFKA":
                if not self.config.kafka:
                    raise ValueError("Kafka configuration required for Kafka output")
                self.output_handler = KafkaOutput(
                    bootstrap_servers=self.config.kafka.bootstrap_servers,
                    topic=self.producer_config.kafka_topic or self.config.kafka.default_topic,
                    security_protocol=self.config.kafka.security_protocol,
                    sasl_mechanism=self.config.kafka.sasl_mechanism,
                    sasl_username=self.config.kafka.sasl_username,
                    sasl_password=self.config.kafka.sasl_password,
                    key_field=getattr(self.config.kafka, 'key_field', None),
                    key_strategy=getattr(self.config.kafka, 'key_strategy', 'field')
                )
            
            print(f"✅ Initialized single producer: {self.producer_config.name}")
            print(f"   Output: {self.producer_config.output.value}")
            print(f"   Rate: {self.producer_config.rate} msg/s" if self.producer_config.rate else f"   Interval: {self.producer_config.interval}")
            print(f"   Fields: {len(self.producer_config.fields)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize producer: {e}")
            self.error_tracker.increment_error_count(self.producer_config.name)
            self.error_tracker.set_last_error(self.producer_config.name, str(e))
            return False
    
    def start(self) -> bool:
        """Start the producer"""
        if not self.initialize():
            return False
        
        if self._running:
            print("⚠️  Producer is already running")
            return True
        
        self._running = True
        self._stats["start_time"] = time.time()
        self._stats["messages_sent"] = 0
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # Start producer thread
        self._thread = threading.Thread(target=self._produce_loop, daemon=True)
        self._thread.start()
        
        print(f"🚀 Started producer '{self.producer_config.name}'")
        return True
    
    def stop(self) -> None:
        """Stop the producer gracefully"""
        if not self._running:
            return
        
        print(f"🛑 Stopping producer '{self.producer_config.name}'...")
        self._running = False
        
        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                print("⚠️  Producer thread did not terminate cleanly")
        
        # Don't cleanup output handler completely - keep it for potential restart
        # Just flush any pending data
        if self.output_handler:
            try:
                # Flush but don't close
                if hasattr(self.output_handler, 'flush'):
                    self.output_handler.flush()
            except Exception as e:
                print(f"⚠️  Error flushing output handler: {e}")
        
        print(f"✅ Producer '{self.producer_config.name}' stopped")
    
    def restart(self) -> bool:
        """Restart the producer with current configuration"""
        try:
            # If already running, stop first
            if self._running:
                self.stop()
            
            # Reset stats
            self._stats["start_time"] = time.time()
            self._stats["messages_sent"] = 0
            self._stats["last_message_time"] = None
            
            # Reset error tracker
            self.error_tracker.reset_error_count(self.producer_config.name)
            
            # Restart the production loop
            self._running = True
            self._thread = threading.Thread(target=self._produce_loop, daemon=True)
            self._thread.start()
            
            print(f"🔄 Restarted producer '{self.producer_config.name}'")
            return True
            
        except Exception as e:
            print(f"❌ Error restarting producer: {e}")
            return False
    
    def _produce_loop(self) -> None:
        """Main production loop"""
        try:
            while self._running:
                # Control rate/interval using wait_for_next_message
                if not self.rate_controller.wait_for_next_message():
                    break
                
                # Generate data
                try:
                    data = self.data_generator.generate_record(self.producer_config.fields)
                    self._stats["last_message_time"] = time.time()
                    
                    # Send to output
                    success = self.output_handler.send(data)
                    if success:
                        self._stats["messages_sent"] += 1
                    else:
                        self.error_tracker.increment_error_count(self.producer_config.name)
                        self.error_tracker.set_last_error(self.producer_config.name, "Output send failed")
                
                except Exception as e:
                    print(f"❌ Error generating/sending data: {e}")
                    self.error_tracker.increment_error_count(self.producer_config.name)
                    self.error_tracker.set_last_error(self.producer_config.name, str(e))
                    time.sleep(1)  # Brief pause on error
                    
        except Exception as e:
            print(f"❌ Fatal error in producer loop: {e}")
            self.error_tracker.increment_error_count(self.producer_config.name)
            self.error_tracker.set_last_error(self.producer_config.name, f"Fatal error: {e}")
        finally:
            self._running = False
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\nReceived signal {signum}, shutting down...")
        self.stop()
        import sys
        sys.exit(0)
    
    def get_status(self) -> dict:
        """Get current producer status"""
        if not self.producer_config:
            return {"status": "no_config"}
        
        uptime = 0
        if self._stats["start_time"]:
            uptime = time.time() - self._stats["start_time"]
        
        current_rate = 0.0
        if self.rate_controller:
            # Estimate current rate based on configuration
            if self.rate_controller.rate:
                current_rate = float(self.rate_controller.rate)
            elif hasattr(self.rate_controller, '_interval_seconds') and self.rate_controller._interval_seconds > 0:
                current_rate = 1.0 / self.rate_controller._interval_seconds
        
        error_stats = self.error_tracker.get_error_stats(self.producer_config.name)
        
        return {
            "name": self.producer_config.name,
            "status": "running" if self._running else "stopped",
            "output": self.producer_config.output.value,
            "rate": self.producer_config.rate,
            "interval": self.producer_config.interval,
            "uptime_seconds": int(uptime),
            "messages_sent": self._stats["messages_sent"],
            "current_rate": round(current_rate, 2),
            "error_count": error_stats["error_count"],
            "last_error": error_stats["last_error"],
            "last_message_time": self._stats["last_message_time"]
        }
    
    def is_running(self) -> bool:
        """Check if producer is currently running"""
        return self._running and self._thread and self._thread.is_alive()
    
    def update_rate(self, rate: Optional[int] = None, interval: Optional[str] = None) -> bool:
        """Update the producer rate or interval"""
        try:
            if not self.rate_controller:
                return False
            
            if rate is not None:
                self.rate_controller.set_rate(rate)
                # Update config to persist the change
                if self.producer_config:
                    self.producer_config.rate = rate
                    self.producer_config.interval = None
                return True
            elif interval is not None:
                self.rate_controller.set_interval(interval)
                # Update config to persist the change
                if self.producer_config:
                    self.producer_config.interval = interval
                    self.producer_config.rate = None
                return True
            
            return False
        except Exception as e:
            print(f"❌ Error updating rate: {e}")
            return False
