"""Data generation engine for producing realistic test data"""

import random
import time
from typing import Any, Dict, List, Union
from datetime import datetime
from ..core.config import FieldConfig, FieldType, RuleType
from ..core.dictionary import DictionaryLoader


class DataGenerator:
    """Generates data records based on field configurations"""
    
    def __init__(self, dictionary_loader: DictionaryLoader):
        self.dictionary_loader = dictionary_loader
    
    def generate_record(self, fields: List[FieldConfig]) -> Dict[str, Any]:
        """Generate a single data record based on field configurations"""
        record = {}
        
        for field in fields:
            value = self._generate_field_value(field)
            record[field.name] = value
        
        return record
    
    def _generate_field_value(self, field: FieldConfig) -> Any:
        """Generate value for a single field based on its configuration"""
        
        if field.rule == RuleType.RANDOM_RANGE:
            return self._generate_random_range(field)
        elif field.rule == RuleType.RANDOM_FROM_LIST:
            return self._generate_random_from_list(field)
        elif field.rule == RuleType.RANDOM_FROM_DICTIONARY:
            return self._generate_random_from_dictionary(field)
        elif field.rule == RuleType.NOW:
            return self._generate_now(field)
        elif field.rule == RuleType.CONSTANT:
            return self._generate_constant(field)
        else:
            raise ValueError(f"Unsupported rule type: {field.rule}")
    
    def _generate_random_range(self, field: FieldConfig) -> Union[int, float]:
        """Generate random value within specified range"""
        if field.min is None or field.max is None:
            raise ValueError("min and max must be specified for random_range rule")
        
        if field.type in [FieldType.INT, FieldType.LONG]:
            return random.randint(int(field.min), int(field.max))
        elif field.type == FieldType.DOUBLE:
            return round(random.uniform(float(field.min), float(field.max)), 2)
        else:
            raise ValueError(f"random_range not supported for type: {field.type}")
    
    def _generate_random_from_list(self, field: FieldConfig) -> Any:
        """Generate random value from predefined list"""
        if not field.list:
            raise ValueError("list must be specified for random_from_list rule")
        
        value = random.choice(field.list)
        
        # Convert to appropriate type if needed
        if field.type == FieldType.INT:
            return int(value)
        elif field.type == FieldType.LONG:
            return int(value)
        elif field.type == FieldType.DOUBLE:
            return float(value)
        elif field.type == FieldType.BOOLEAN:
            return bool(value)
        else:  # STRING
            return str(value)
    
    def _generate_random_from_dictionary(self, field: FieldConfig) -> str:
        """Generate random value from loaded dictionary"""
        if not field.dictionary:
            raise ValueError("dictionary must be specified for random_from_dictionary rule")
        
        if not field.dictionary_column:
            raise ValueError("dictionary_column must be specified for random_from_dictionary rule")
        
        if not self.dictionary_loader.is_loaded(field.dictionary):
            raise ValueError(f"Dictionary '{field.dictionary}' not loaded")
        
        return self.dictionary_loader.get_random_value(field.dictionary, field.dictionary_column)
    
    def _generate_now(self, field: FieldConfig) -> Union[int, str]:
        """Generate current timestamp"""
        now = datetime.now()
        
        if field.type in [FieldType.LONG, FieldType.INT]:
            return int(now.timestamp() * 1000)  # milliseconds since epoch
        else:  # STRING
            return now.isoformat()
    
    def _generate_constant(self, field: FieldConfig) -> Any:
        """Generate constant value"""
        if field.value is None:
            raise ValueError("value must be specified for constant rule")
        
        # Convert to appropriate type
        if field.type == FieldType.INT:
            return int(field.value)
        elif field.type == FieldType.LONG:
            return int(field.value)
        elif field.type == FieldType.DOUBLE:
            return float(field.value)
        elif field.type == FieldType.BOOLEAN:
            return bool(field.value)
        else:  # STRING
            return str(field.value)