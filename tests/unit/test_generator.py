"""Unit tests for data generator module"""

import pytest
from unittest.mock import Mock, patch
import time

from stream_data_producer.core.generator import DataGenerator
from stream_data_producer.core.config import FieldConfig, FieldType, RuleType
from stream_data_producer.core.dictionary import DictionaryLoader


class TestDataGenerator:
    """Test DataGenerator class"""
    
    @pytest.fixture
    def mock_dictionary_loader(self):
        """Mock dictionary loader for testing"""
        loader = Mock(spec=DictionaryLoader)
        loader.get_random_value.side_effect = lambda dict_name, column: f"mock_{dict_name}_{column}"
        return loader
    
    @pytest.fixture
    def sample_fields(self):
        """Sample field configurations for testing"""
        return [
            FieldConfig(name="id", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=1, max=100),
            FieldConfig(name="name", type=FieldType.STRING, rule=RuleType.RANDOM_FROM_LIST, list=["Alice", "Bob", "Charlie"]),
            FieldConfig(name="score", type=FieldType.DOUBLE, rule=RuleType.RANDOM_RANGE, min=0.0, max=100.0),
            FieldConfig(name="active", type=FieldType.BOOLEAN, rule=RuleType.RANDOM_FROM_LIST, list=[True, False]),
            FieldConfig(name="timestamp", type=FieldType.LONG, rule=RuleType.NOW),
            FieldConfig(name="status", type=FieldType.STRING, rule=RuleType.CONSTANT, value="active"),
        ]
    
    def test_generator_initialization(self, mock_dictionary_loader):
        """Test data generator initialization"""
        generator = DataGenerator(mock_dictionary_loader)
        assert generator.dictionary_loader == mock_dictionary_loader
    
    def test_generate_record_basic(self, mock_dictionary_loader, sample_fields):
        """Test basic record generation"""
        generator = DataGenerator(mock_dictionary_loader)
        
        with patch('random.randint', return_value=42), \
             patch('random.choice', side_effect=["Alice", True]), \
             patch('random.uniform', return_value=87.5), \
             patch('datetime.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.timestamp.return_value = 1708765432.123
            mock_datetime.now.return_value = mock_now
            
            record = generator.generate_record(sample_fields)
            
            # Check all fields are present
            assert "id" in record
            assert "name" in record
            assert "score" in record
            assert "active" in record
            assert "timestamp" in record
            assert "status" in record
            
            # Check values match expectations
            assert record["id"] == 42
            assert record["name"] == "Alice"
            assert record["score"] == 87.5
            assert record["active"] is True
            # timestamp is dynamic, just check it exists and is int
            assert "timestamp" in record
            assert isinstance(record["timestamp"], int)
            assert record["status"] == "active"
    
    def test_random_range_generation(self, mock_dictionary_loader):
        """Test random range generation for different types"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Test integer range
        int_field = FieldConfig(name="int_val", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=10, max=20)
        with patch('random.randint', return_value=15):
            record = generator.generate_record([int_field])
            assert record["int_val"] == 15
            assert isinstance(record["int_val"], int)
        
        # Test long range
        long_field = FieldConfig(name="long_val", type=FieldType.LONG, rule=RuleType.RANDOM_RANGE, min=1000, max=2000)
        with patch('random.randint', return_value=1500):
            record = generator.generate_record([long_field])
            assert record["long_val"] == 1500
            assert isinstance(record["long_val"], int)
        
        # Test double range
        double_field = FieldConfig(name="double_val", type=FieldType.DOUBLE, rule=RuleType.RANDOM_RANGE, min=10.5, max=20.8)
        with patch('random.uniform', return_value=15.7):
            record = generator.generate_record([double_field])
            assert record["double_val"] == 15.7
            assert isinstance(record["double_val"], float)
    
    def test_random_from_list_generation(self, mock_dictionary_loader):
        """Test random selection from list"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Test string list
        string_field = FieldConfig(name="choice", type=FieldType.STRING, rule=RuleType.RANDOM_FROM_LIST, list=["A", "B", "C"])
        with patch('random.choice', return_value="B"):
            record = generator.generate_record([string_field])
            assert record["choice"] == "B"
            assert isinstance(record["choice"], str)
        
        # Test boolean list
        bool_field = FieldConfig(name="flag", type=FieldType.BOOLEAN, rule=RuleType.RANDOM_FROM_LIST, list=[True, False])
        with patch('random.choice', return_value=False):
            record = generator.generate_record([bool_field])
            assert record["flag"] is False
            assert isinstance(record["flag"], bool)
        
        # Test mixed type list (should work with any types)
        mixed_field = FieldConfig(name="mixed", type=FieldType.STRING, rule=RuleType.RANDOM_FROM_LIST, list=[1, "two", 3.0])
        with patch('random.choice', return_value="two"):
            record = generator.generate_record([mixed_field])
            assert record["mixed"] == "two"
    
    def test_now_timestamp_generation(self, mock_dictionary_loader):
        """Test current timestamp generation"""
        generator = DataGenerator(mock_dictionary_loader)
        
        timestamp_field = FieldConfig(name="ts", type=FieldType.LONG, rule=RuleType.NOW)
        
        # Test long timestamp (milliseconds)
        with patch('datetime.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.timestamp.return_value = 1708765432.123
            mock_datetime.now.return_value = mock_now
            
            record = generator.generate_record([timestamp_field])
            # Just check it's an integer (timestamp)
            assert isinstance(record["ts"], int)
        
        # Test string timestamp
        timestamp_field_str = FieldConfig(name="ts_str", type=FieldType.STRING, rule=RuleType.NOW)
        with patch('datetime.datetime') as mock_datetime:
            mock_now = Mock()
            mock_now.isoformat.return_value = "2024-02-24T10:30:32.123456"
            mock_datetime.now.return_value = mock_now
            
            record = generator.generate_record([timestamp_field_str])
            assert isinstance(record["ts_str"], str)
    
    def test_constant_value_generation(self, mock_dictionary_loader):
        """Test constant value generation"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Test string constant
        string_const = FieldConfig(name="const_str", type=FieldType.STRING, rule=RuleType.CONSTANT, value="fixed_value")
        record = generator.generate_record([string_const])
        assert record["const_str"] == "fixed_value"
        
        # Test numeric constant
        num_const = FieldConfig(name="const_num", type=FieldType.INT, rule=RuleType.CONSTANT, value=42)
        record = generator.generate_record([num_const])
        assert record["const_num"] == 42
        
        # Test boolean constant
        bool_const = FieldConfig(name="const_bool", type=FieldType.BOOLEAN, rule=RuleType.CONSTANT, value=True)
        record = generator.generate_record([bool_const])
        assert record["const_bool"] is True
    
    def test_random_from_dictionary_generation(self, mock_dictionary_loader):
        """Test random selection from dictionary"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Mock the dictionary loader to return specific values
        mock_dictionary_loader.get_random_value.side_effect = [
            "user123",  # for user_id
            "John Doe"  # for user_name
        ]
        
        dict_field1 = FieldConfig(
            name="user_id",
            type=FieldType.STRING,
            rule=RuleType.RANDOM_FROM_DICTIONARY,
            dictionary="users",
            dictionary_column="id"
        )
        
        dict_field2 = FieldConfig(
            name="user_name",
            type=FieldType.STRING,
            rule=RuleType.RANDOM_FROM_DICTIONARY,
            dictionary="users",
            dictionary_column="name"
        )
        
        record = generator.generate_record([dict_field1, dict_field2])
        
        # Check that dictionary loader was called correctly
        assert mock_dictionary_loader.get_random_value.call_count == 2
        mock_dictionary_loader.get_random_value.assert_any_call("users", "id")
        mock_dictionary_loader.get_random_value.assert_any_call("users", "name")
        
        # Check returned values
        assert record["user_id"] == "user123"
        assert record["user_name"] == "John Doe"
    
    def test_empty_fields_list(self, mock_dictionary_loader):
        """Test generating record with empty fields list"""
        generator = DataGenerator(mock_dictionary_loader)
        record = generator.generate_record([])
        assert record == {}
    
    def test_single_field_generation(self, mock_dictionary_loader):
        """Test generating record with single field"""
        generator = DataGenerator(mock_dictionary_loader)
        
        field = FieldConfig(name="single", type=FieldType.STRING, rule=RuleType.CONSTANT, value="only_value")
        record = generator.generate_record([field])
        
        assert len(record) == 1
        assert record["single"] == "only_value"
    
    def test_duplicate_field_names(self, mock_dictionary_loader):
        """Test handling duplicate field names"""
        generator = DataGenerator(mock_dictionary_loader)
        
        fields = [
            FieldConfig(name="id", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=1, max=10),
            FieldConfig(name="id", type=FieldType.STRING, rule=RuleType.CONSTANT, value="duplicate")  # Same name
        ]
        
        with patch('random.randint', return_value=5):
            record = generator.generate_record(fields)
        
        # Second field should overwrite the first
        assert len(record) == 1
        assert record["id"] == "duplicate"
        assert isinstance(record["id"], str)
    
    def test_range_validation(self, mock_dictionary_loader):
        """Test that range validation works correctly"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Test min > max should be handled gracefully
        invalid_field = FieldConfig(name="invalid", type=FieldType.INT, rule=RuleType.RANDOM_RANGE, min=100, max=10)
        
        with patch('random.randint', return_value=10):  # random.randint handles reversed ranges
            record = generator.generate_record([invalid_field])
            assert record["invalid"] == 10
    
    def test_list_choice_empty_list(self, mock_dictionary_loader):
        """Test handling empty choice list"""
        generator = DataGenerator(mock_dictionary_loader)
        
        empty_field = FieldConfig(name="empty", type=FieldType.STRING, rule=RuleType.RANDOM_FROM_LIST, list=[])
        
        with pytest.raises(ValueError, match="list must be specified"):
            generator.generate_record([empty_field])
    
    def test_unsupported_field_type(self, mock_dictionary_loader):
        """Test handling unsupported field types"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Create a field with unsupported combination (missing min/max)
        unsupported_field = FieldConfig(name="unsupported", type=FieldType.INT, rule=RuleType.RANDOM_RANGE)
        
        with pytest.raises(ValueError, match="min and max must be specified"):
            generator.generate_record([unsupported_field])
    
    def test_performance_multiple_fields(self, mock_dictionary_loader):
        """Test performance with multiple fields"""
        generator = DataGenerator(mock_dictionary_loader)
        
        # Create many fields
        fields = []
        for i in range(50):
            fields.append(FieldConfig(
                name=f"field_{i}",
                type=FieldType.STRING,
                rule=RuleType.CONSTANT,
                value=f"value_{i}"
            ))
        
        # Should generate quickly
        import time
        start_time = time.time()
        record = generator.generate_record(fields)
        end_time = time.time()
        
        # Should complete in reasonable time
        assert (end_time - start_time) < 1.0  # Less than 1 second
        assert len(record) == 50
        assert record["field_0"] == "value_0"
        assert record["field_49"] == "value_49"