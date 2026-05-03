import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

# Needs to be patched before import so it doesn't try to connect
with patch('src.api.rest_api.client_pool.connect'):
    from src.api.rest_api import app, client_pool

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": "1.0.0"}

def test_acquire_lock_missing_auth():
    response = client.post("/api/lock/acquire", json={
        "lock_id": "res-1",
        "client_id": "test-client"
    })
    # Should fail without auth header
    assert response.status_code == 401

@patch.object(client_pool, 'get_stub')
def test_acquire_lock_success(mock_get_stub):
    # Mock the gRPC LockStub
    mock_stub = MagicMock()
    # Mock the AcquireLock method to return an awaitable object
    async def mock_acquire(*args, **kwargs):
        class MockResponse:
            success = True
            lease_token = "mock-token-123"
            message = "Lock acquired"
        return MockResponse()
    
    mock_stub.AcquireLock = mock_acquire
    mock_get_stub.return_value = mock_stub
    
    response = client.post(
        "/api/lock/acquire", 
        json={
            "lock_id": "res-1",
            "client_id": "test-client"
        },
        headers={"Authorization": "Bearer some-token"}
    )
    
    assert response.status_code == 200
    assert response.json() == {"lease_token": "mock-token-123", "message": "Lock acquired"}

@patch.object(client_pool, 'get_stub')
def test_acquire_lock_conflict(mock_get_stub):
    # Mock the gRPC LockStub to simulate lock unavailable
    mock_stub = MagicMock()
    async def mock_acquire(*args, **kwargs):
        class MockResponse:
            success = False
            message = "Lock already held"
        return MockResponse()
    
    mock_stub.AcquireLock = mock_acquire
    mock_get_stub.return_value = mock_stub
    
    response = client.post(
        "/api/lock/acquire", 
        json={
            "lock_id": "res-1",
            "client_id": "test-client"
        },
        headers={"Authorization": "Bearer some-token"}
    )
    
    # 409 Conflict is mapped in the FastAPI code for lock acquisition failures
    assert response.status_code == 409
    assert response.json()["detail"] == "Lock already held"
