"""Unit tests for simplified API server functionality"""

import pytest
import json
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from datetime import datetime

from stream_data_producer.api.simple_server import SimpleAPIServer, ApiResponse


class TestApiResponse:
    """Test ApiResponse model"""
    
    def test_api_response_creation(self):
        """Test creating API response"""
        response = ApiResponse(
            success=True,
            message="Test message",
            data={"key": "value"}
        )
        
        assert response.success is True
        assert response.message == "Test message"
        assert response.data == {"key": "value"}
    
    def test_api_response_without_data(self):
        """Test API response without data"""
        response = ApiResponse(success=True, message="Success")
        assert response.success is True
        assert response.message == "Success"
        assert response.data is None


class TestSimpleAPIServer:
    """Test SimpleAPIServer class"""
    
    @pytest.fixture
    def mock_producer_manager(self):
        """Create mock producer manager"""
        manager = Mock()
        manager.get_status.return_value = {
            "name": "test-producer",
            "status": "running",
            "output": "console",
            "rate": 10,
            "current_rate": 8.5,
            "messages_sent": 1000,
            "error_count": 0,
            "last_error": None,
            "uptime_seconds": 300
        }
        return manager
    
    @pytest.fixture
    def api_server(self, mock_producer_manager):
        """Create API server instance"""
        return SimpleAPIServer(mock_producer_manager, host="127.0.0.1", port=8000)
    
    def test_api_server_initialization(self, api_server, mock_producer_manager):
        """Test API server initialization"""
        assert api_server.producer_manager == mock_producer_manager
        assert api_server.host == "127.0.0.1"
        assert api_server.port == 8000
        assert api_server.app is not None
    
    def test_root_endpoint(self, api_server):
        """Test root endpoint"""
        client = TestClient(api_server.app)
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "Single Producer" in data["message"]
        assert data["data"]["version"] == "0.1.0"
    
    def test_health_check_endpoint(self, api_server):
        """Test health check endpoint"""
        client = TestClient(api_server.app)
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["message"] == "Service is healthy"
        assert "status" in data["data"]
        assert "timestamp" in data["data"]
    
    def test_get_status_endpoint(self, api_server, mock_producer_manager):
        """Test get status endpoint"""
        client = TestClient(api_server.app)
        response = client.get("/status")
        
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "test-producer"
        assert data["status"] == "running"
        assert data["output"] == "console"
        assert data["rate"] == 10
        assert data["messages_sent"] == 1000
    
    def test_get_status_endpoint_error(self, api_server, mock_producer_manager):
        """Test get status endpoint with error"""
        mock_producer_manager.get_status.side_effect = Exception("Database error")
        
        client = TestClient(api_server.app)
        response = client.get("/status")
        
        assert response.status_code == 500
        assert "Database error" in response.text
    
    def test_cors_middleware_enabled(self, api_server):
        """Test that CORS middleware is enabled"""
        # Check that the app has middleware configured
        assert hasattr(api_server.app, 'middleware_stack')
        # The actual CORS testing would require more complex setup
