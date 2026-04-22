"""
Unified Node - Combines all distributed managers
"""
import asyncio
import logging

from src.nodes.base_node import BaseNode
from src.nodes.lock_manager import LockManager
from src.nodes.cache_node import CacheNode
from src.nodes.queue_node import QueueNode

from src.consensus.pbft import PBFTNode
from src.geo.region_manager import RegionManager
from src.ml.load_balancer import MLLoadBalancer
from src.communication.failure_detector import FailureDetector
from src.security.rbac import RBACManager, AuditLogger

from src.utils.config import config

logger = logging.getLogger(__name__)


class SyncNode(LockManager, CacheNode, QueueNode):
    """
    Unified node combining Lock, Cache, and Queue functionalities.
    Multiple inheritance relies on Python's MRO.
    """
    
    def __init__(self):
        # We call BaseNode init first to set up common properties
        BaseNode.__init__(self)
        
        # Initialize specific managers
        LockManager.__init__(self)
        CacheNode.__init__(self)
        QueueNode.__init__(self)
        
        # PBFT
        self.pbft = PBFTNode(
            node_id=self.node_id,
            all_nodes=[nid for nid, _, _ in config.cluster_nodes],
            peer_stubs=self.peer_stubs
        )
        
        # Geo-Distributed Region Manager
        self.geo_manager = RegionManager(
            node_id=self.node_id,
            region=config.geo_region,
            peer_stubs=self.peer_stubs,
            kafka_producer=None  # will be set after queue starts
        )
        
        # ML Load Balancer
        self.load_balancer = MLLoadBalancer(
            node_ids=[nid for nid, _, _ in config.cluster_nodes]
        )
        
        # Failure Detector
        self.failure_detector = FailureDetector(
            node_id=self.node_id,
            peer_stubs=self.peer_stubs
        )
        
        # Security
        self.rbac = RBACManager(self._db_session)
        self.audit_logger = AuditLogger(None)  # producer will be set later
        
    async def start(self):
        """Start all subsystems"""
        # 1. Base Node (gRPC, DB, Redis)
        await BaseNode.start(self)
        
        # 2. Lock Manager
        await LockManager.start(self)
        
        # 3. Cache Node
        await CacheNode.start(self)
        
        # 4. Queue Node
        await QueueNode.start(self)
        
        # Bind Kafka producer to Geo and Audit
        self.geo_manager.kafka_producer = self._producer
        self.audit_logger._producer = self._producer
        
        # 5. Security
        await self.rbac.initialize()
        
        # 6. ML Load Balancer
        await self.load_balancer.initialize()
        
        # 7. Failure Detector
        await self.failure_detector.start()
        
        logger.info(f"[SyncNode:{self.node_id}] 🚀 All Subsystems STARTED!")

    async def stop(self):
        await self.failure_detector.stop()
        await QueueNode.stop(self)
        await CacheNode.stop(self)
        await LockManager.stop(self)
        await BaseNode.stop(self)
        logger.info(f"[SyncNode:{self.node_id}] Stopped.")
