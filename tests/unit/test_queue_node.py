import pytest
from src.nodes.queue_node import ConsistentHashRing

def test_consistent_hash_ring_add_remove():
    ring = ConsistentHashRing()
    
    # Test empty ring
    assert ring.get_node("any_key") is None
    
    # Add nodes
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")
    
    assert len(ring.get_all_nodes()) == 3
    assert set(ring.get_all_nodes()) == {"node-1", "node-2", "node-3"}
    
    # Check ring size (150 vnodes per node)
    assert len(ring._ring) == 3 * 150
    assert len(ring._map) == 3 * 150
    
    # Remove node
    ring.remove_node("node-2")
    assert len(ring.get_all_nodes()) == 2
    assert "node-2" not in ring.get_all_nodes()
    assert len(ring._ring) == 2 * 150

def test_consistent_hash_ring_distribution():
    ring = ConsistentHashRing()
    
    for i in range(5):
        ring.add_node(f"node-{i}")
        
    # Check that a specific key always hashes to the same node
    key1 = "user_123"
    node_for_key1 = ring.get_node(key1)
    
    assert node_for_key1 is not None
    assert ring.get_node(key1) == node_for_key1
    
    # Add a new node (simulate scale up)
    ring.add_node("node-5")
    
    # It might stay the same or change, but consistency dictates that 
    # most keys should NOT move. Let's just ensure it still returns a valid node.
    new_node_for_key1 = ring.get_node(key1)
    assert new_node_for_key1 in ring.get_all_nodes()
    
def test_consistent_hash_ring_partition():
    ring = ConsistentHashRing()
    partition = ring.get_partition("test_key", num_partitions=12)
    assert 0 <= partition < 12
