import pytest
from unittest.mock import patch, MagicMock
from src.nodes.cache_node import CacheNode, MOESIState, CacheLine

@pytest.fixture(autouse=True)
def mock_env_vars():
    with patch('os.environ', {
        'NODE_ID': 'test-node-1',
        'NODE_HOST': '127.0.0.1',
        'NODE_PORT': '50051',
        'METRICS_PORT': '9091',
        'CLUSTER_NODES': 'test-node-1:127.0.0.1:50051'
    }):
        yield

@pytest.fixture
def cache_node():
    # Bypass engine/redis initialization
    with patch('src.nodes.base_node.create_async_engine'):
        with patch('src.nodes.base_node.sessionmaker'):
            node = CacheNode()
            node.node_id = "test-node-1"
            return node

@pytest.mark.asyncio
async def test_read_miss(cache_node):
    # Mock network fetch to return nothing (fetch from DB simulated miss)
    with patch.object(cache_node, '_fetch_from_peers', return_value=(None, None, None)):
        hit, value, state = await cache_node.read("key1")
        assert hit is False
        assert value is None
        assert state == MOESIState.INVALID

@pytest.mark.asyncio
async def test_read_hit_local(cache_node):
    # Populate local cache
    cache_node._cache["key1"] = CacheLine(
        key="key1", value=b"data", state=MOESIState.MODIFIED, version=1
    )
    
    hit, value, state = await cache_node.read("key1")
    assert hit is True
    assert value == b"data"
    assert state == MOESIState.MODIFIED
    assert cache_node._hits == 1

@pytest.mark.asyncio
async def test_write_hit_upgrade(cache_node):
    # Transition S -> M
    cache_node._cache["key1"] = CacheLine(
        key="key1", value=b"old_data", state=MOESIState.SHARED, version=1
    )
    
    with patch.object(cache_node, '_broadcast_invalidation') as mock_broadcast:
        success, version = await cache_node.write("key1", b"new_data")
        
        assert success is True
        assert version == 2
        assert cache_node._cache["key1"].state == MOESIState.MODIFIED
        assert cache_node._cache["key1"].value == b"new_data"
        mock_broadcast.assert_called_once_with("key1", 2)

@pytest.mark.asyncio
async def test_handle_read_request_intervention(cache_node):
    # Intervention: M -> O
    cache_node._cache["key1"] = CacheLine(
        key="key1", value=b"data", state=MOESIState.MODIFIED, version=2
    )
    
    value, state, version = await cache_node.handle_read_request("key1", "requester-node")
    
    assert value == b"data"
    assert version == 2
    assert cache_node._cache["key1"].state == MOESIState.OWNED  # Transitioned to OWNED
    assert "requester-node" in cache_node._directory["key1"]

@pytest.mark.asyncio
async def test_handle_invalidate(cache_node):
    # We have an OWNED copy, someone else writes and invalidates us
    cache_node._cache["key1"] = CacheLine(
        key="key1", value=b"data", state=MOESIState.OWNED, version=2
    )
    cache_node._directory["key1"] = {"test-node-1", "other-node"}
    
    with patch.object(cache_node, '_write_back') as mock_write_back:
        success, had_dirty = await cache_node.handle_invalidate("key1", version=3, sender_id="other-node")
        
        assert success is True
        assert had_dirty is True
        assert cache_node._cache["key1"].state == MOESIState.INVALID
        mock_write_back.assert_called_once()
        assert "test-node-1" not in cache_node._directory["key1"]
