"""
gRPC Servicers (Message Passing)
Maps incoming gRPC requests to internal node logic.
"""
import asyncio
import logging

import grpc
from src.proto.generated import node_pb2, node_pb2_grpc

logger = logging.getLogger(__name__)


class RaftServicer(node_pb2_grpc.RaftServiceServicer):
    def __init__(self, node):
        self.node = node

    async def RequestVote(self, request, context):
        if not hasattr(self.node, 'raft'):
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Raft not enabled")
        vote_granted, term = await self.node.raft.handle_request_vote(
            request.candidate_id, request.term,
            request.last_log_index, request.last_log_term
        )
        return node_pb2.VoteResponse(term=term, vote_granted=vote_granted)

    async def AppendEntries(self, request, context):
        if not hasattr(self.node, 'raft'):
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Raft not enabled")
        
        entries = []
        for e in request.entries:
            # Reconstruct dictionary from protobuf message
            import json
            cmd = json.loads(e.command) if e.command else {}
            # We mock the LogEntry class for simplicity in mapping
            from src.consensus.raft import LogEntry
            entries.append(LogEntry(term=e.term, command=cmd, entry_type=e.entry_type))
            
        success, term = await self.node.raft.handle_append_entries(
            request.leader_id, request.term,
            request.prev_log_index, request.prev_log_term,
            entries, request.leader_commit
        )
        return node_pb2.AppendResponse(term=term, success=success)


class LockServicer(node_pb2_grpc.LockServiceServicer):
    def __init__(self, node):
        self.node = node

    async def AcquireLock(self, request, context):
        from src.nodes.lock_manager import LockType
        ltype = LockType.EXCLUSIVE if request.lock_type == node_pb2.LockType.EXCLUSIVE else LockType.SHARED
        success, lease_token, msg = await self.node.acquire_lock(
            request.lock_id, ltype, request.client_id,
            request.timeout_ms, request.lease_ms
        )
        return node_pb2.LockResponse(success=success, lease_token=lease_token or "", message=msg)

    async def ReleaseLock(self, request, context):
        success, msg = await self.node.release_lock(
            request.lock_id, request.client_id, request.lease_token
        )
        return node_pb2.LockResponse(success=success, lease_token="", message=msg)

    async def RenewLock(self, request, context):
        success, expires_at, msg = await self.node.renew_lock(
            request.lock_id, request.client_id, request.lease_token, request.extend_ms
        )
        return node_pb2.LockResponse(success=success, lease_token="", message=f"{msg} (expires: {expires_at})")


class CacheServicer(node_pb2_grpc.CacheServiceServicer):
    def __init__(self, node):
        self.node = node

    async def ReadCache(self, request, context):
        val, state, version = await self.node.handle_read_request(request.cache_key, request.requester_id)
        if val is None:
            return node_pb2.CacheReadResponse(success=False)
        return node_pb2.CacheReadResponse(
            success=True, value=val,
            state=node_pb2.CacheState.Value(state.name),
            version=version
        )

    async def InvalidateCache(self, request, context):
        success, had_dirty = await self.node.handle_invalidate(
            request.cache_key, request.version, request.sender_id
        )
        return node_pb2.CacheInvalidateResponse(success=success, acknowledged=True)


class QueueServicer(node_pb2_grpc.QueueServiceServicer):
    def __init__(self, node):
        self.node = node

    async def Enqueue(self, request, context):
        headers = {k: v for k, v in request.headers.items()}
        success, msg_id, responsible_node, partition = await self.node.enqueue(
            request.topic, request.partition_key, request.payload, headers, request.producer_id
        )
        return node_pb2.EnqueueResponse(
            success=success, message_id=msg_id,
            responsible_node=responsible_node, partition=partition
        )


class PBFTServicer(node_pb2_grpc.PBFTServiceServicer):
    def __init__(self, node):
        self.node = node

    async def PrePrepare(self, request, context):
        if hasattr(self.node, 'pbft'):
            success = await self.node.pbft.handle_pre_prepare(
                request.view, request.sequence, request.digest,
                request.request.operation, request.primary_id
            )
            return node_pb2.PBFTResponse(success=success)
        return node_pb2.PBFTResponse(success=False)

    async def Prepare(self, request, context):
        if hasattr(self.node, 'pbft'):
            await self.node.pbft.handle_prepare(
                request.view, request.sequence, request.digest, request.node_id
            )
        return node_pb2.PBFTResponse(success=True)

    async def Commit(self, request, context):
        if hasattr(self.node, 'pbft'):
            await self.node.pbft.handle_commit(
                request.view, request.sequence, request.digest, request.node_id
            )
        return node_pb2.PBFTResponse(success=True)

    async def ViewChange(self, request, context):
        if hasattr(self.node, 'pbft'):
            await self.node.pbft.handle_view_change(
                request.new_view, request.node_id
            )
        return node_pb2.PBFTResponse(success=True)


class NodeServicer(node_pb2_grpc.NodeServiceServicer):
    def __init__(self, node):
        self.node = node

    async def Heartbeat(self, request, context):
        if hasattr(self.node, 'failure_detector'):
            self.node.failure_detector.record_heartbeat(request.sender_id)
        return node_pb2.HeartbeatResponse(
            alive=True, timestamp=int(time.time() * 1000)
        )


class GeoServicer(node_pb2_grpc.GeoServiceServicer):
    def __init__(self, node):
        self.node = node

    async def RouteRequest(self, request, context):
        if hasattr(self.node, 'geo_manager'):
            success, result, target_region, est_latency = await self.node.geo_manager.route_request(
                request.client_region, request.operation_type, request.payload
            )
            return node_pb2.GeoRouteResponse(
                success=success, result=result,
                target_region=target_region, estimated_latency_ms=est_latency
            )
        return node_pb2.GeoRouteResponse(success=False)

    async def ReplicateData(self, request, context):
        if hasattr(self.node, 'geo_manager'):
            # Reconstruct vector clock dict
            vc = {request.source_region: request.vector_clock}
            import json
            try:
                val = json.loads(request.value)
            except:
                val = request.value
            await self.node.geo_manager.apply_replication(
                request.key, val, vc, request.source_region
            )
        return node_pb2.PBFTResponse(success=True)
