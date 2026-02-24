"""Rate control mechanisms for data generation timing"""

import time
from typing import Optional, Callable
import threading
from datetime import timedelta


class RateController:
    """Controls the rate of data generation"""
    
    def __init__(self, rate: Optional[int] = None, interval: Optional[str] = None):
        self.rate = rate
        self.interval = interval
        self._paused = False
        self._pause_condition = threading.Condition()
        self._should_stop = False
        
        # Parse interval if provided
        self._interval_seconds = 0.0
        if interval:
            self._parse_interval(interval)
    
    def _parse_interval(self, interval_str: str) -> None:
        """Parse interval string like '5s', '1m', '2h'"""
        if not interval_str:
            return
            
        interval_str = interval_str.strip().lower()
        
        if interval_str.endswith('ms'):
            self._interval_seconds = float(interval_str[:-2]) / 1000
        elif interval_str.endswith('s'):
            self._interval_seconds = float(interval_str[:-1])
        elif interval_str.endswith('m'):
            self._interval_seconds = float(interval_str[:-1]) * 60
        elif interval_str.endswith('h'):
            self._interval_seconds = float(interval_str[:-1]) * 3600
        else:
            raise ValueError(f"Invalid interval format: {interval_str}")
    
    def wait_for_next_message(self) -> bool:
        """
        Wait for appropriate time before next message.
        Returns False if should stop, True otherwise.
        """
        if self._should_stop:
            return False
            
        with self._pause_condition:
            while self._paused and not self._should_stop:
                self._pause_condition.wait()
            
            if self._should_stop:
                return False
        
        if self.rate:
            # Rate-based control: sleep to maintain messages per second
            sleep_time = 1.0 / self.rate
            time.sleep(sleep_time)
        elif self.interval:
            # Interval-based control: fixed sleep time
            time.sleep(self._interval_seconds)
        else:
            # No rate control - minimal sleep to prevent busy loop
            time.sleep(0.001)  # 1ms
        
        return True
    
    def pause(self) -> None:
        """Pause the rate controller"""
        with self._pause_condition:
            self._paused = True
    
    def resume(self) -> None:
        """Resume the rate controller"""
        with self._pause_condition:
            self._paused = False
            self._pause_condition.notify_all()
    
    def stop(self) -> None:
        """Signal the rate controller to stop"""
        self._should_stop = True
        with self._pause_condition:
            self._pause_condition.notify_all()
    
    def set_rate(self, rate: Optional[int]) -> None:
        """Dynamically change the rate"""
        self.rate = rate
        if rate is not None:
            self.interval = None  # Clear interval when rate is set
    
    def set_interval(self, interval: Optional[str]) -> None:
        """Dynamically change the interval"""
        self.interval = interval
        if interval is not None:
            self._parse_interval(interval)
            self.rate = None  # Clear rate when interval is set
    
    def is_paused(self) -> bool:
        """Check if currently paused"""
        return self._paused
    
    def get_current_settings(self) -> dict:
        """Get current rate control settings"""
        return {
            'rate': self.rate,
            'interval': self.interval,
            'paused': self._paused
        }


class AdaptiveRateController(RateController):
    """Rate controller with adaptive capabilities"""
    
    def __init__(self, rate: Optional[int] = None, interval: Optional[str] = None):
        super().__init__(rate, interval)
        self._actual_rate_history = []
        self._last_calculation_time = time.time()
    
    def update_actual_rate(self, actual_rate: float) -> None:
        """Update with actual measured rate for monitoring"""
        current_time = time.time()
        self._actual_rate_history.append((current_time, actual_rate))
        
        # Keep only recent history (last 60 seconds)
        cutoff_time = current_time - 60
        self._actual_rate_history = [
            (t, rate) for t, rate in self._actual_rate_history 
            if t > cutoff_time
        ]
    
    def get_average_actual_rate(self) -> float:
        """Get average actual rate from recent history"""
        if not self._actual_rate_history:
            return 0.0
        
        rates = [rate for _, rate in self._actual_rate_history]
        return sum(rates) / len(rates)