"""
Distributed Sync System - Base Node
Foundation class for all node types. Manages:
  - gRPC server startup with optional mTLS
  - Peer connections management
  - Prometheus metrics server
  - Redis + MySQL connection pools
  - Heartbeat / failure detection
"""
import asyncio
import logging
import time
from typing import Dict, Optional, Any

import grpc
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from src.utils.config import config
from src.utils.metrics import (
    NODE_UPTIME, NODE_CONNECTED_PEERS,
    GRPC_REQUESTS, GRPC_LATENCY,
    start_metrics_server
)

logger = logging.getLogger(__name__)


class BaseNode:
    """
    Base class for all distributed system nodes.
    Provides: gRPC server, peer connections, Redis, MySQL, metrics.
    """

    def __init__(self, node_id: str = None):
        self.node_id = node_id or config.node_id
        self.start_time = time.monotonic()

        # gRPC server
        self._grpc_server: Optional[grpc.aio.Server] = None

        # Peer gRPC stubs: {node_id: stub_dict}
        self.peer_stubs: Dict[str, Dict[str, Any]] = {}
        self.peer_channels: Dict[str, grpc.aio.Channel] = {}

        # Redis
        self._redis: Optional[aioredis.Redis] = None

        # MySQL
        self._engine = create_async_engine(
            config.mysql_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        self._db_session = sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

        # Node registry (known peers)
        self._known_nodes: Dict[str, Dict] = {}

        logger.info(f"[Node:{self.node_id}] BaseNode initialized")

    # --------------------------------------------------
    # Startup / Shutdown
    # --------------------------------------------------
    async def start(self):
        """Start all node services"""
        # Start Prometheus metrics
        start_metrics_server(config.metrics_port, self.node_id)
        logger.info(f"[Node:{self.node_id}] Metrics server on :{config.metrics_port}")

        # Connect to Redis
        await self._connect_redis()

        # Start gRPC server
        await self._start_grpc_server()

        # Connect to peers
        await self._connect_to_peers()

        # Start uptime updater
        asyncio.create_task(self._update_uptime())

        logger.info(f"[Node:{self.node_id}] ✅ Node started successfully")

    async def stop(self):
        """Graceful shutdown"""
        logger.info(f"[Node:{self.node_id}] Shutting down...")

        # Stop gRPC
        if self._grpc_server:
            await self._grpc_server.stop(grace=5)

        # Close peer channels
        for channel in self.peer_channels.values():
            await channel.close()

        # Close Redis
        if self._redis:
            await self._redis.aclose()

        # Close MySQL
        await self._engine.dispose()

        logger.info(f"[Node:{self.node_id}] Shutdown complete")

    # --------------------------------------------------
    # gRPC Server
    # --------------------------------------------------
    async def _start_grpc_server(self):
        """Start gRPC server with optional mTLS"""
        from src.proto.generated import node_pb2_grpc
        from src.communication.message_passing import (
            RaftServicer, LockServicer, CacheServicer,
            QueueServicer, PBFTServicer, NodeServicer, GeoServicer
        )

        self._grpc_server = grpc.aio.server(
            options=[
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
            ]
        )

        # Register all servicers
        node_pb2_grpc.add_RaftServiceServicer_to_server(
            RaftServicer(self), self._grpc_server
        )
        node_pb2_grpc.add_LockServiceServicer_to_server(
            LockServicer(self), self._grpc_server
        )
        node_pb2_grpc.add_CacheServiceServicer_to_server(
            CacheServicer(self), self._grpc_server
        )
        node_pb2_grpc.add_QueueServiceServicer_to_server(
            QueueServicer(self), self._grpc_server
        )
        node_pb2_grpc.add_PBFTServiceServicer_to_server(
            PBFTServicer(self), self._grpc_server
        )
        node_pb2_grpc.add_NodeServiceServicer_to_server(
            NodeServicer(self), self._grpc_server
        )
        node_pb2_grpc.add_GeoServiceServicer_to_server(
            GeoServicer(self), self._grpc_server
        )

        # TLS credentials
        credentials = await self._get_server_credentials()

        listen_addr = f"{config.node_host}:{config.grpc_port}"
        if credentials:
            self._grpc_server.add_secure_port(listen_addr, credentials)
            logger.info(f"[Node:{self.node_id}] gRPC server with mTLS on {listen_addr}")
        else:
            self._grpc_server.add_insecure_port(listen_addr)
            logger.info(f"[Node:{self.node_id}] gRPC server (insecure) on {listen_addr}")

        await self._grpc_server.start()

    async def _get_server_credentials(self) -> Optional[grpc.ServerCredentials]:
        """Load mTLS server credentials if TLS is enabled"""
        if not config.tls_enabled:
            return None
        try:
            with open(config.tls_cert_file, 'rb') as f:
                cert = f.read()
            with open(config.tls_key_file, 'rb') as f:
                key = f.read()
            with open(config.tls_ca_file, 'rb') as f:
                ca = f.read()

            return grpc.ssl_server_credentials(
                [(key, cert)],
                root_certificates=ca,
                require_client_auth=True  # mTLS
            )
        except FileNotFoundError as e:
            logger.warning(f"[Node:{self.node_id}] TLS cert not found ({e}), using insecure mode")
            return None

    # --------------------------------------------------
    # Peer Connections
    # --------------------------------------------------
    async def _connect_to_peers(self):
        """Establish gRPC channels + stubs to all peer nodes"""
        from src.proto.generated import node_pb2_grpc

        for peer_id, peer_host, peer_port in config.peer_nodes:
            await self._connect_to_peer(peer_id, peer_host, peer_port)

        NODE_CONNECTED_PEERS.labels(node_id=self.node_id).set(len(self.peer_channels))

    async def _connect_to_peer(self, peer_id: str, host: str, port: int):
        """Connect to a single peer node"""
        from src.proto.generated import node_pb2_grpc

        target = f"{host}:{port}"
        try:
            credentials = await self._get_client_credentials()
            if credentials:
                channel = grpc.aio.secure_channel(target, credentials)
            else:
                channel = grpc.aio.insecure_channel(
                    target,
                    options=[
                        ('grpc.keepalive_time_ms', 10000),
                        ('grpc.keepalive_timeout_ms', 5000),
                    ]
                )

            self.peer_channels[peer_id] = channel
            self.peer_stubs[peer_id] = {
                'raft':   node_pb2_grpc.RaftServiceStub(channel),
                'lock':   node_pb2_grpc.LockServiceStub(channel),
                'cache':  node_pb2_grpc.CacheServiceStub(channel),
                'queue':  node_pb2_grpc.QueueServiceStub(channel),
                'pbft':   node_pb2_grpc.PBFTServiceStub(channel),
                'node':   node_pb2_grpc.NodeServiceStub(channel),
                'geo':    node_pb2_grpc.GeoServiceStub(channel),
            }
            logger.info(f"[Node:{self.node_id}] Connected to peer {peer_id} at {target}")
        except Exception as e:
            logger.error(f"[Node:{self.node_id}] Failed to connect to {peer_id}: {e}")

    async def _get_client_credentials(self) -> Optional[grpc.ChannelCredentials]:
        """Load mTLS client credentials"""
        if not config.tls_enabled:
            return None
        try:
            with open(config.tls_cert_file, 'rb') as f:
                cert = f.read()
            with open(config.tls_key_file, 'rb') as f:
                key = f.read()
            with open(config.tls_ca_file, 'rb') as f:
                ca = f.read()
            return grpc.ssl_channel_credentials(ca, key, cert)
        except FileNotFoundError:
            return None

    def get_peer_stub(self, peer_id: str, service: str) -> Optional[Any]:
        """Get a specific gRPC stub for a peer"""
        return self.peer_stubs.get(peer_id, {}).get(service)

    def get_all_peer_stubs(self, service: str) -> Dict[str, Any]:
        """Get stubs for a service from all peers"""
        return {
            pid: stubs[service]
            for pid, stubs in self.peer_stubs.items()
            if service in stubs
        }

    # --------------------------------------------------
    # Redis
    # --------------------------------------------------
    async def _connect_redis(self):
        """Connect to Redis"""
        self._redis = aioredis.from_url(
            config.redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        await self._redis.ping()
        logger.info(f"[Node:{self.node_id}] Redis connected at {config.redis_host}:{config.redis_port}")

    async def redis_set(self, key: str, value: str, ttl: int = None):
        await self._redis.set(key, value, ex=ttl)

    async def redis_get(self, key: str) -> Optional[str]:
        return await self._redis.get(key)

    async def redis_delete(self, key: str):
        await self._redis.delete(key)

    async def redis_exists(self, key: str) -> bool:
        return bool(await self._redis.exists(key))

    # --------------------------------------------------
    # Database
    # --------------------------------------------------
    def db_session(self) -> AsyncSession:
        return self._db_session()

    # --------------------------------------------------
    # Node Info
    # --------------------------------------------------
    def get_node_info(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": config.node_host,
            "grpc_port": config.grpc_port,
            "rest_port": config.rest_port,
            "region": config.geo_region,
            "uptime_seconds": time.monotonic() - self.start_time,
            "connected_peers": list(self.peer_stubs.keys()),
            "peer_count": len(self.peer_stubs),
        }

    async def _update_uptime(self):
        """Update uptime metric every 10 seconds"""
        while True:
            uptime = time.monotonic() - self.start_time
            NODE_UPTIME.labels(node_id=self.node_id).set(uptime)
            await asyncio.sleep(10)
