"""
API Gateway - FastAPI Application
Acts as a frontend to the gRPC distributed synchronization system.
"""
import asyncio
import logging
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Depends, Header, Request
from pydantic import BaseModel

import grpc
from src.proto.generated import node_pb2, node_pb2_grpc
from src.utils.config import config
from src.security.rbac import Role

logger = logging.getLogger(__name__)

app = FastAPI(title="Distributed Sync System API", version="1.0.0")

# Simple round-robin client for the gateway to connect to nodes
class GRPCClientPool:
    def __init__(self):
        self.channels = []
        self.stubs = []
        self.index = 0

    def connect(self):
        for node_id, host, port in config.cluster_nodes:
            # Connect insecure for API to internal nodes for simplicity in demo
            channel = grpc.aio.insecure_channel(f"{host}:{port}")
            self.channels.append(channel)
            self.stubs.append({
                'lock': node_pb2_grpc.LockServiceStub(channel),
                'cache': node_pb2_grpc.CacheServiceStub(channel),
                'queue': node_pb2_grpc.QueueServiceStub(channel),
                'pbft': node_pb2_grpc.PBFTServiceStub(channel),
                'node': node_pb2_grpc.NodeServiceStub(channel),
                'geo': node_pb2_grpc.GeoServiceStub(channel),
            })

    async def close(self):
        for channel in self.channels:
            await channel.close()

    def get_stub(self, service: str):
        if not self.stubs:
            raise Exception("No nodes available")
        stubs = self.stubs[self.index % len(self.stubs)]
        self.index += 1
        return stubs[service]

client_pool = GRPCClientPool()

@app.on_event("startup")
async def startup_event():
    logger.info("Initializing API Gateway gRPC pool")
    client_pool.connect()

@app.on_event("shutdown")
async def shutdown_event():
    await client_pool.close()

# --------------------------------------------------
# Auth Dependency
# --------------------------------------------------
def get_auth_token(authorization: Optional[str] = Header(None)) -> str:
    """In a real app, verify JWT. Here we just expect a raw token."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization Header")
    if authorization.startswith("Bearer "):
        authorization = authorization.split(" ")[1]
    return authorization

# --------------------------------------------------
# Lock API
# --------------------------------------------------
class AcquireRequest(BaseModel):
    lock_id: str
    client_id: str
    lock_type: str = "exclusive"
    timeout_ms: int = 5000
    lease_ms: int = 15000

class ReleaseRequest(BaseModel):
    lock_id: str
    client_id: str
    lease_token: str

@app.post("/api/lock/acquire")
async def api_acquire_lock(req: AcquireRequest, token: str = Depends(get_auth_token)):
    stub = client_pool.get_stub('lock')
    try:
        response = await stub.AcquireLock(node_pb2.LockRequest(
            lock_id=req.lock_id,
            lock_type=node_pb2.LOCK_EXCLUSIVE if req.lock_type == "exclusive" else node_pb2.LOCK_SHARED,
            client_id=req.client_id,
            timeout_ms=req.timeout_ms,
            lease_ms=req.lease_ms
        ), timeout=req.timeout_ms/1000 + 1)
        if not response.success:
            raise HTTPException(status_code=409, detail=response.message)
        return {"lease_token": response.lease_token, "message": response.message}
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/lock/release")
async def api_release_lock(req: ReleaseRequest, token: str = Depends(get_auth_token)):
    stub = client_pool.get_stub('lock')
    try:
        response = await stub.ReleaseLock(node_pb2.ReleaseRequest(
            lock_id=req.lock_id,
            client_id=req.client_id,
            lease_token=req.lease_token
        ))
        if not response.success:
            raise HTTPException(status_code=400, detail=response.message)
        return {"success": True, "message": response.message}
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------------------------------
# Cache API
# --------------------------------------------------
class CacheReadRequest(BaseModel):
    key: str
    requester_id: str

@app.post("/api/cache/read")
async def api_read_cache(req: CacheReadRequest, token: str = Depends(get_auth_token)):
    stub = client_pool.get_stub('cache')
    try:
        response = await stub.ReadCache(node_pb2.CacheReadRequest(
            requester_id=req.requester_id,
            cache_key=req.key
        ))
        if not response.success:
            raise HTTPException(status_code=404, detail="Cache miss")
        # In a real app we'd decode bytes based on content type
        return {"value": response.value.decode(errors="replace"), "state": response.state, "version": response.version}
    except grpc.RpcError as e:
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------------------------------
# System Status
# --------------------------------------------------
@app.get("/api/health")
async def api_health():
    """Check API Gateway health"""
    return {"status": "ok", "version": "1.0.0"}
