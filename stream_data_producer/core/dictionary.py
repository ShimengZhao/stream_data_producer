"""Dictionary loader for CSV-based data dictionaries"""

import csv
import os
from typing import Dict, List, Union
from ..core.config import DictionaryConfig


class DictionaryLoader:
    """Loads and manages CSV-based data dictionaries"""
    
    def __init__(self):
        self._dictionaries: Dict[str, List[Dict[str, str]]] = {}
        self._loaded_configs: Dict[str, DictionaryConfig] = {}
    
    def load_dictionary(self, name: str, config: DictionaryConfig) -> None:
        """Load a dictionary from CSV file"""
        if not os.path.exists(config.file):
            raise FileNotFoundError(f"Dictionary file not found: {config.file}")
        
        data = []
        with open(config.file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                entry = {}
                for column_name, column_index in config.columns.items():
                    if isinstance(column_index, str):
                        # Handle named columns (future extension)
                        entry[column_name] = row[0]  # fallback to first column
                    else:
                        # Handle numeric index
                        if column_index < len(row):
                            entry[column_name] = row[column_index]
                        else:
                            entry[column_name] = ""
                data.append(entry)
        
        self._dictionaries[name] = data
        self._loaded_configs[name] = config
    
    def get_random_value(self, dictionary_name: str, column: Union[str, int]) -> str:
        """Get a random value from the specified dictionary column"""
        if dictionary_name not in self._dictionaries:
            raise ValueError(f"Dictionary '{dictionary_name}' not loaded")
        
        import random
        entries = self._dictionaries[dictionary_name]
        if not entries:
            raise ValueError(f"Dictionary '{dictionary_name}' is empty")
        
        entry = random.choice(entries)
        
        if isinstance(column, int):
            # Get by index - convert to string key
            keys = list(entry.keys())
            if column < len(keys):
                return entry[keys[column]]
            else:
                raise IndexError(f"Column index {column} out of range")
        else:
            # Get by name
            if column in entry:
                return entry[column]
            else:
                raise KeyError(f"Column '{column}' not found in dictionary")
    
    def load_all_dictionaries(self, dictionaries: Dict[str, DictionaryConfig]) -> None:
        """Load all dictionaries from configuration"""
        for name, config in dictionaries.items():
            self.load_dictionary(name, config)
    
    def get_dictionary_names(self) -> List[str]:
        """Get list of loaded dictionary names"""
        return list(self._dictionaries.keys())
    
    def is_loaded(self, name: str) -> bool:
        """Check if a dictionary is loaded"""
        return name in self._dictionaries