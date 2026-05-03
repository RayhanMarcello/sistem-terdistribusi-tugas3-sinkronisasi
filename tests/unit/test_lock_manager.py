import pytest
import asyncio
from unittest.mock import patch, MagicMock
from src.nodes.lock_manager import LockManager, LockType, LockWaiter

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
def lock_manager():
    with patch('src.nodes.base_node.create_async_engine'):
        with patch('src.nodes.base_node.sessionmaker'):
            with patch('src.nodes.lock_manager.RaftNode') as MockRaft:
                manager = LockManager()
                manager.node_id = "test-node-1"
                
                # Mock Raft and Persistence
                manager.raft.is_leader = True
                manager.raft.propose = AsyncMock()
                manager._persist_lock = AsyncMock()
                manager._remove_lock_from_db = AsyncMock()
                manager._log_lock_history = AsyncMock()
                
                return manager

class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)

@pytest.mark.asyncio
async def test_acquire_and_release_lock_exclusive(lock_manager):
    # 1. Acquire Lock
    success, token, msg = await lock_manager.acquire_lock(
        lock_id="resource-1",
        lock_type=LockType.EXCLUSIVE,
        client_id="client-A",
        timeout_ms=100,
        lease_ms=1000
    )
    
    assert success is True
    assert token is not None
    assert lock_manager._locks["resource-1"].owner_client == "client-A"
    
    # 2. Another client tries to acquire (should fail/timeout)
    success2, token2, msg2 = await lock_manager.acquire_lock(
        lock_id="resource-1",
        lock_type=LockType.EXCLUSIVE,
        client_id="client-B",
        timeout_ms=50,
        lease_ms=1000
    )
    assert success2 is False
    assert token2 is None
    
    # 3. Release Lock
    rel_success, rel_msg = await lock_manager.release_lock(
        lock_id="resource-1",
        client_id="client-A",
        lease_token=token
    )
    assert rel_success is True
    assert "resource-1" not in lock_manager._locks

@pytest.mark.asyncio
async def test_deadlock_detection(lock_manager):
    # Simulate a Wait-For Graph with a cycle:
    # A waits for B, B waits for C, C waits for A
    
    lock_manager._wait_for_graph = {
        "client-A": {"client-B"},
        "client-B": {"client-C"},
        "client-C": {"client-A"}
    }
    
    # Add dummy waiters so resolution knows their age
    fA = asyncio.Future()
    fB = asyncio.Future()
    fC = asyncio.Future()
    
    lock_manager._wait_queues["lock-B"].append(LockWaiter("client-A", LockType.EXCLUSIVE, 100, fA))
    lock_manager._wait_queues["lock-C"].append(LockWaiter("client-B", LockType.EXCLUSIVE, 110, fB))
    lock_manager._wait_queues["lock-A"].append(LockWaiter("client-C", LockType.EXCLUSIVE, 120, fC)) # Youngest
    
    # Run detector
    cycle = await lock_manager._detect_deadlock()
    assert cycle is not None
    assert len(cycle) >= 3
    
    # Resolve deadlock
    await lock_manager._resolve_deadlock(cycle)
    
    # The youngest (client-C at time 120) should have its future aborted
    assert fC.done()
    assert isinstance(fC.exception(), Exception)
    assert "Deadlock detected" in str(fC.exception())
    
    # Graph should be cleaned
    assert "client-C" not in lock_manager._wait_for_graph
