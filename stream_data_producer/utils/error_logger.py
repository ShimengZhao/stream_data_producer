"""Error logging system for dropped data with time-based rotation"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from ..output.file import RotatingFileHandler


class ErrorLogger:
    """Logs dropped data and errors with time-based rotation"""
    
    def __init__(self, log_directory: str = "./logs", 
                 rolling: str = "daily", 
                 max_age_days: int = 7):
        self.log_directory = log_directory
        self.rolling = rolling.lower()
        self.max_age_days = max_age_days
        self._rotating_handler = RotatingFileHandler(log_directory, max_age_days)
        self._ensure_directory_exists()
    
    def _ensure_directory_exists(self) -> None:
        """Ensure the log directory exists"""
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory, exist_ok=True)
    
    def _get_log_filename(self) -> str:
        """Get current log filename based on rolling configuration"""
        now = datetime.now()
        if self.rolling == "hourly":
            timestamp = now.strftime("%Y%m%d_%H")
        elif self.rolling == "daily":
            timestamp = now.strftime("%Y%m%d")
        else:
            raise ValueError(f"Unsupported rolling type: {self.rolling}")
        
        return os.path.join(self.log_directory, f"errors_{timestamp}.json")
    
    def log_dropped_data(self, producer_name: str, data: Dict[str, Any], 
                        reason: str) -> bool:
        """
        Log dropped data with error information.
        Returns True if successful, False otherwise.
        """
        try:
            error_record = {
                "timestamp": datetime.now().isoformat(),
                "producer": producer_name,
                "reason": reason,
                "data": data
            }
            
            log_file = self._get_log_filename()
            
            with open(log_file, 'a', encoding='utf-8') as f:
                json_line = json.dumps(error_record, ensure_ascii=False)
                f.write(json_line + '\n')
            
            return True
            
        except Exception as e:
            print(f"Error logging dropped data: {e}")
            return False
    
    def log_error(self, producer_name: str, error_message: str,
                  error_details: Optional[Dict[str, Any]] = None) -> bool:
        """
        Log general error information.
        Returns True if successful, False otherwise.
        """
        try:
            error_record = {
                "timestamp": datetime.now().isoformat(),
                "producer": producer_name,
                "error": error_message,
                "details": error_details or {}
            }
            
            log_file = self._get_log_filename()
            
            with open(log_file, 'a', encoding='utf-8') as f:
                json_line = json.dumps(error_record, ensure_ascii=False)
                f.write(json_line + '\n')
            
            return True
            
        except Exception as e:
            print(f"Error logging general error: {e}")
            return False
    
    def cleanup_old_logs(self) -> None:
        """Clean up log files older than max_age_days"""
        self._rotating_handler.cleanup_old_files()


class ErrorTracker:
    """Tracks error statistics for producers"""
    
    def __init__(self):
        self._error_counts: Dict[str, int] = {}
        self._last_errors: Dict[str, str] = {}
    
    def increment_error_count(self, producer_name: str) -> None:
        """Increment error count for a producer"""
        self._error_counts[producer_name] = self._error_counts.get(producer_name, 0) + 1
    
    def set_last_error(self, producer_name: str, error_message: str) -> None:
        """Set the last error message for a producer"""
        self._last_errors[producer_name] = error_message
    
    def get_error_stats(self, producer_name: str) -> Dict[str, Any]:
        """Get error statistics for a producer"""
        return {
            "error_count": self._error_counts.get(producer_name, 0),
            "last_error": self._last_errors.get(producer_name, None)
        }
    
    def reset_error_count(self, producer_name: str) -> None:
        """Reset error count for a producer"""
        self._error_counts[producer_name] = 0