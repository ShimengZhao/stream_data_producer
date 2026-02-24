"""Unit tests for dictionary module"""

import pytest
import tempfile
import os
from pathlib import Path

from stream_data_producer.core.dictionary import DictionaryLoader
from stream_data_producer.core.config import DictionaryConfig


class TestDictionaryLoader:
    """Test DictionaryLoader class"""
    
    @pytest.fixture
    def sample_csv_content(self):
        """Sample CSV content for testing"""
        return """1,Alice,admin
2,Bob,user
3,Charlie,moderator
4,David,guest
5,Eve,admin"""
    
    @pytest.fixture
    def temp_csv_file(self, sample_csv_content):
        """Create temporary CSV file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(sample_csv_content)
            file_path = f.name
        yield file_path
        os.unlink(file_path)
    
    @pytest.fixture
    def dictionary_config(self, temp_csv_file):
        """Sample dictionary configuration"""
        return DictionaryConfig(
            file=temp_csv_file,
            columns={"id": 0, "name": 1, "role": 2}
        )
    
    def test_loader_initialization(self):
        """Test dictionary loader initialization"""
        loader = DictionaryLoader()
        assert loader._dictionaries == {}
        assert loader._loaded_configs == {}
    
    def test_load_single_dictionary(self, dictionary_config):
        """Test loading a single dictionary"""
        loader = DictionaryLoader()
        loader.load_dictionary("test_dict", dictionary_config)
        
        assert "test_dict" in loader._dictionaries
        assert len(loader._dictionaries["test_dict"]) == 5  # 5 rows
        assert "test_dict" in loader._loaded_configs
    
    def test_load_multiple_dictionaries(self, dictionary_config, temp_csv_file):
        """Test loading multiple dictionaries"""
        loader = DictionaryLoader()
        
        # Load first dictionary
        loader.load_dictionary("dict1", dictionary_config)
        
        # Load second dictionary (same file, different name)
        loader.load_dictionary("dict2", dictionary_config)
        
        assert "dict1" in loader._dictionaries
        assert "dict2" in loader._dictionaries
        assert len(loader._dictionaries["dict1"]) == 5
        assert len(loader._dictionaries["dict2"]) == 5
        assert "dict1" in loader._loaded_configs
        assert "dict2" in loader._loaded_configs
    
    def test_load_all_dictionaries(self, dictionary_config, temp_csv_file):
        """Test loading all dictionaries at once"""
        loader = DictionaryLoader()
        
        dict_configs = {
            "users": dictionary_config,
            "employees": DictionaryConfig(file=temp_csv_file, columns={"emp_id": 0, "emp_name": 1})
        }
        
        loader.load_all_dictionaries(dict_configs)
        
        assert "users" in loader._dictionaries
        assert "employees" in loader._dictionaries
        assert len(loader._dictionaries["users"]) == 5
        assert len(loader._dictionaries["employees"]) == 5
    
    def test_get_random_value_by_name(self, dictionary_config):
        """Test getting random values by column name"""
        loader = DictionaryLoader()
        loader.load_dictionary("test_dict", dictionary_config)
        
        # Test getting values from 'id' column
        id_value = loader.get_random_value("test_dict", "id")
        assert id_value in ["1", "2", "3", "4", "5"]
        
        # Test getting values from 'name' column
        name_value = loader.get_random_value("test_dict", "name")
        assert name_value in ["Alice", "Bob", "Charlie", "David", "Eve"]
        
        # Test getting values from 'role' column
        role_value = loader.get_random_value("test_dict", "role")
        assert role_value in ["admin", "user", "moderator", "guest"]
    
    def test_get_random_value_by_index(self, dictionary_config):
        """Test getting random values by column index"""
        loader = DictionaryLoader()
        loader.load_dictionary("test_dict", dictionary_config)
        
        # Test getting values by index (0 = id, 1 = name, 2 = role)
        id_value = loader.get_random_value("test_dict", 0)
        assert id_value in ["1", "2", "3", "4", "5"]
        
        name_value = loader.get_random_value("test_dict", 1)
        assert name_value in ["Alice", "Bob", "Charlie", "David", "Eve"]
        
        role_value = loader.get_random_value("test_dict", 2)
        assert role_value in ["admin", "user", "moderator", "guest"]
    
    def test_get_random_value_nonexistent_dict(self):
        """Test getting values from non-existent dictionary"""
        loader = DictionaryLoader()
        
        with pytest.raises(ValueError, match="not loaded"):
            loader.get_random_value("nonexistent", "column")
    
    def test_get_random_value_nonexistent_column(self, dictionary_config):
        """Test getting values from non-existent column"""
        loader = DictionaryLoader()
        loader.load_dictionary("test_dict", dictionary_config)
        
        # Non-existent column name
        with pytest.raises(KeyError):
            loader.get_random_value("test_dict", "nonexistent")
        
        # Non-existent column index
        with pytest.raises(IndexError):
            loader.get_random_value("test_dict", 999)
    
    def test_is_loaded(self, dictionary_config):
        """Test is_loaded method"""
        loader = DictionaryLoader()
        
        assert not loader.is_loaded("test_dict")
        
        loader.load_dictionary("test_dict", dictionary_config)
        assert loader.is_loaded("test_dict")
    
    def test_dictionary_names(self, dictionary_config):
        """Test getting dictionary names"""
        loader = DictionaryLoader()
        loader.load_dictionary("test_dict", dictionary_config)
        
        names = loader.get_dictionary_names()
        assert names == ["test_dict"]
    
    def test_dictionary_with_empty_file(self):
        """Test loading dictionary from empty file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            file_path = f.name
        
        try:
            loader = DictionaryLoader()
            dict_config = DictionaryConfig(file=file_path, columns={"id": 0, "name": 1})
            
            loader.load_dictionary("empty_dict", dict_config)
            
            # Should have empty dictionary
            assert "empty_dict" in loader._dictionaries
            assert len(loader._dictionaries["empty_dict"]) == 0
            
            # Getting random value should raise exception
            with pytest.raises(ValueError, match="empty"):
                loader.get_random_value("empty_dict", "id")
        finally:
            os.unlink(file_path)
    
    def test_dictionary_with_malformed_csv(self):
        """Test loading dictionary with malformed CSV"""
        malformed_content = """1,Alice
2,Bob,extra_field
3,Charlie
4,David,another_extra"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(malformed_content)
            file_path = f.name
        
        try:
            loader = DictionaryLoader()
            dict_config = DictionaryConfig(file=file_path, columns={"id": 0, "name": 1})
            
            loader.load_dictionary("malformed_dict", dict_config)
            
            # Should still load, but rows with wrong number of fields are skipped
            assert "malformed_dict" in loader._dictionaries
            # All rows should be loaded since CSV reader handles variable fields
            assert len(loader._dictionaries["malformed_dict"]) == 4
            
            # Check that we can get values from valid rows
            id_value = loader.get_random_value("malformed_dict", "id")
            assert id_value in ["1", "2", "3", "4"]
        finally:
            os.unlink(file_path)
    
    def test_multiple_loads_same_file(self, dictionary_config, temp_csv_file):
        """Test loading the same file multiple times"""
        loader = DictionaryLoader()
        
        # Load same file twice with different dictionary names
        loader.load_dictionary("first_dict", dictionary_config)
        loader.load_dictionary("second_dict", dictionary_config)
        
        # Both dictionaries should have the same data
        assert len(loader._dictionaries["first_dict"]) == 5
        assert len(loader._dictionaries["second_dict"]) == 5
        
        # Getting values from both should work
        first_id = loader.get_random_value("first_dict", "id")
        second_id = loader.get_random_value("second_dict", "id")
        
        assert first_id in ["1", "2", "3", "4", "5"]
        assert second_id in ["1", "2", "3", "4", "5"]
    
    def test_concurrent_access_simulation(self, dictionary_config):
        """Test concurrent-like access to dictionary values"""
        loader = DictionaryLoader()
        loader.load_dictionary("test_dict", dictionary_config)
        
        # Simulate multiple accesses
        values = set()
        for _ in range(20):
            value = loader.get_random_value("test_dict", "id")
            values.add(value)
        
        # All values should be valid
        assert values.issubset({"1", "2", "3", "4", "5"})
        assert len(values) > 0  # Should have gotten at least one value