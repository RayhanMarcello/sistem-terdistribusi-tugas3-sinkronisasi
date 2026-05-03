import pytest
import time
from unittest.mock import patch, MagicMock

# Kita perlu meng-mock config dan os environment untuk menghindari error inisialisasi gRPC sungguhan
@pytest.fixture(autouse=True)
def mock_env_vars():
    with patch.dict('os.environ', {
        'NODE_ID': 'test-node-1',
        'NODE_HOST': '127.0.0.1',
        'NODE_PORT': '50051',
        'METRICS_PORT': '9091',
        'CLUSTER_NODES': 'test-node-1:127.0.0.1:50051'
    }):
        yield

from src.nodes.base_node import BaseNode
from src.utils.config import Config

@pytest.fixture
def mock_config():
    with patch('src.nodes.base_node.config', new_callable=Config) as mock_cfg:
        mock_cfg.node_id = 'test-node-1'
        yield mock_cfg

@pytest.mark.asyncio
async def test_base_node_initialization(mock_config):
    """Test apakah BaseNode bisa diinisialisasi dengan state yang benar"""
    node = BaseNode()
    
    assert node.node_id == "test-node-1"
    assert node.peer_channels == {}
    assert isinstance(node.start_time, float)

@pytest.mark.asyncio
async def test_get_node_info(mock_config):
    """Test output informasi node dasar dari BaseNode"""
    node = BaseNode()
    
    info = node.get_node_info()
    
    assert "node_id" in info
    assert info["node_id"] == "test-node-1"
    assert "uptime_seconds" in info
    assert "peer_count" in info
    assert info["peer_count"] == 0
