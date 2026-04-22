"""
Distributed Sync System - Distributed Lock Manager
Implements distributed locking using Raft consensus:
  - Shared locks (multiple readers)
  - Exclusive locks (single writer)
  - Deadlock detection via wait-for graph
  - Network partition handling via quorum
  - Lock lease renewal
  - Persistence in MySQL
"""
import asyncio
import json
import logging
import secrets
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from src.nodes.base_node import BaseNode
from src.consensus.raft import RaftNode, RaftRole
from src.utils.config import config
from src.utils.metrics import (
    LOCK_ACQUIRE_TOTAL, LOCK_RELEASE_TOTAL, LOCK_ACTIVE_COUNT,
    LOCK_WAIT_QUEUE, LOCK_DURATION, LOCK_ACQUIRE_LATENCY,
    DEADLOCK_DETECTIONS
)

logger = logging.getLogger(__name__)


class LockType(Enum):
    SHARED    = "shared"
    EXCLUSIVE = "exclusive"


@dataclass
class Lock:
    lock_id: str
    lock_type: LockType
    owner_client: str
    owner_node: str
    acquired_at: float
    expires_at: float
    lease_token: str
    # For shared locks: multiple holders
    shared_holders: Set[str] = field(default_factory=set)


@dataclass
class LockWaiter:
    client_id: str
    lock_type: LockType
    requested_at: float
    future: Optional[asyncio.Future] = None


class LockManager(BaseNode):
    """
    Distributed Lock Manager using Raft consensus for coordination.

    Lock acquisition flow:
    1. Client requests lock → Lock Manager receives request
    2. If leader: check lock state, acquire if possible, replicate via Raft
    3. If follower: forward to leader
    4. On commit: apply lock, return lease token to client

    Deadlock detection:
    - Wait-for graph: edge A→B means A is waiting for lock held by B
    - Cycle detection: DFS on the graph, if cycle found → deadlock
    - Resolution: abort youngest transaction in cycle
    """

    def __init__(self):
        super().__init__()

        # Active locks: lock_id → Lock
        self._locks: Dict[str, Lock] = {}

        # Wait queues: lock_id → [LockWaiter]
        self._wait_queues: Dict[str, List[LockWaiter]] = defaultdict(list)

        # Wait-for graph: client_id → set of client_ids holding blocking locks
        self._wait_for_graph: Dict[str, Set[str]] = defaultdict(set)

        # Raft node
        self.raft = RaftNode(self.node_id, apply_callback=self._apply_raft_command)

        # Background tasks
        self._lock_expiry_task: Optional[asyncio.Task] = None
        self._deadlock_task: Optional[asyncio.Task] = None

        # Lock for thread-safe access
        self._lock = asyncio.Lock()

    # --------------------------------------------------
    # Startup / Shutdown
    # --------------------------------------------------
    async def start(self):
        """Start lock manager with Raft"""
        await super().start()

        # Wire up Raft peer stubs
        self.raft.peer_stubs = {
            peer_id: stubs['raft']
            for peer_id, stubs in self.peer_stubs.items()
        }
        self.raft._on_become_leader = self._on_become_leader
        self.raft._on_lose_leadership = self._on_lose_leadership

        await self.raft.start()

        # Load active locks from DB
        await self._load_locks_from_db()

        # Start background tasks
        self._lock_expiry_task = asyncio.create_task(self._expire_locks())
        self._deadlock_task = asyncio.create_task(self._deadlock_detector())

        logger.info(f"[LockManager:{self.node_id}] ✅ Lock Manager started")

    async def stop(self):
        for t in [self._lock_expiry_task, self._deadlock_task]:
            if t:
                t.cancel()
        await self.raft.stop()
        await super().stop()

    # --------------------------------------------------
    # Public API
    # --------------------------------------------------
    async def acquire_lock(
        self,
        lock_id: str,
        lock_type: LockType,
        client_id: str,
        timeout_ms: int = None,
        lease_ms: int = None,
    ) -> Tuple[bool, Optional[str], str]:
        """
        Acquire a distributed lock.

        Returns:
            (success, lease_token, message)
        """
        start = time.monotonic()
        timeout_ms = timeout_ms or (config.lock_timeout * 1000)
        lease_ms = lease_ms or (config.lock_timeout * 1000)

        # If not leader, forward to leader
        if not self.raft.is_leader:
            return await self._forward_acquire_to_leader(lock_id, lock_type, client_id, timeout_ms, lease_ms)

        deadline = time.monotonic() + timeout_ms / 1000

        async with self._lock:
            result = await self._try_acquire(lock_id, lock_type, client_id, lease_ms)

        if result:
            lease_token, lock = result
            elapsed = time.monotonic() - start
            LOCK_ACQUIRE_LATENCY.labels(node_id=self.node_id, lock_type=lock_type.value).observe(elapsed)
            LOCK_ACQUIRE_TOTAL.labels(node_id=self.node_id, lock_type=lock_type.value, result="success").inc()
            LOCK_ACTIVE_COUNT.labels(node_id=self.node_id, lock_type=lock_type.value).inc()
            logger.info(f"[LockManager] Acquired {lock_type.value} lock '{lock_id}' for {client_id}")
            return True, lease_token, "Lock acquired"

        # Lock not available — wait
        future = asyncio.get_event_loop().create_future()
        waiter = LockWaiter(
            client_id=client_id,
            lock_type=lock_type,
            requested_at=time.monotonic(),
            future=future
        )

        async with self._lock:
            self._wait_queues[lock_id].append(waiter)
            # Update wait-for graph
            if lock_id in self._locks:
                existing = self._locks[lock_id]
                holders = existing.shared_holders if existing.lock_type == LockType.SHARED else {existing.owner_client}
                for holder in holders:
                    self._wait_for_graph[client_id].add(holder)

        LOCK_WAIT_QUEUE.labels(node_id=self.node_id).inc()

        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            await asyncio.wait_for(future, timeout=remaining)
            # Woke up — try to acquire again
            async with self._lock:
                result = await self._try_acquire(lock_id, lock_type, client_id, lease_ms)

            if result:
                lease_token, lock = result
                elapsed = time.monotonic() - start
                LOCK_ACQUIRE_LATENCY.labels(node_id=self.node_id, lock_type=lock_type.value).observe(elapsed)
                LOCK_ACQUIRE_TOTAL.labels(node_id=self.node_id, lock_type=lock_type.value, result="success").inc()
                LOCK_ACTIVE_COUNT.labels(node_id=self.node_id, lock_type=lock_type.value).inc()
                return True, lease_token, "Lock acquired"

            LOCK_ACQUIRE_TOTAL.labels(node_id=self.node_id, lock_type=lock_type.value, result="failed").inc()
            return False, None, "Failed to acquire after wake"

        except asyncio.TimeoutError:
            async with self._lock:
                self._wait_queues[lock_id] = [
                    w for w in self._wait_queues[lock_id] if w.client_id != client_id
                ]
                self._wait_for_graph.pop(client_id, None)
            LOCK_WAIT_QUEUE.labels(node_id=self.node_id).dec()
            LOCK_ACQUIRE_TOTAL.labels(node_id=self.node_id, lock_type=lock_type.value, result="timeout").inc()
            return False, None, f"Lock acquisition timeout after {timeout_ms}ms"

    async def release_lock(
        self, lock_id: str, client_id: str, lease_token: str
    ) -> Tuple[bool, str]:
        """Release a distributed lock"""
        if not self.raft.is_leader:
            return await self._forward_release_to_leader(lock_id, client_id, lease_token)

        async with self._lock:
            lock = self._locks.get(lock_id)
            if not lock:
                return False, "Lock not found"

            # Validate lease token
            if lock.lease_token != lease_token:
                return False, "Invalid lease token"

            # Remove from shared holders or release entirely
            if lock.lock_type == LockType.SHARED:
                lock.shared_holders.discard(client_id)
                if lock.shared_holders:
                    # Other holders remain
                    await self._persist_lock(lock)
                    return True, "Released shared lock"

            # Fully release the lock
            held_for = time.time() - lock.acquired_at
            LOCK_DURATION.labels(node_id=self.node_id, lock_type=lock.lock_type.value).observe(held_for)
            LOCK_RELEASE_TOTAL.labels(node_id=self.node_id, lock_type=lock.lock_type.value).inc()
            LOCK_ACTIVE_COUNT.labels(node_id=self.node_id, lock_type=lock.lock_type.value).dec()

            del self._locks[lock_id]
            await self._remove_lock_from_db(lock_id)
            await self._log_lock_history(lock_id, lock.lock_type, client_id, "released", held_for)

            # Replicate release via Raft
            await self.raft.propose({
                "op": "release_lock",
                "lock_id": lock_id,
                "client_id": client_id,
            }, entry_type="lock_release")

            # Notify waiters
            await self._notify_waiters(lock_id)
            logger.info(f"[LockManager] Released lock '{lock_id}' by {client_id}")
            return True, "Lock released"

    async def renew_lock(
        self, lock_id: str, client_id: str, lease_token: str, extend_ms: int
    ) -> Tuple[bool, int, str]:
        """Renew a lock lease"""
        async with self._lock:
            lock = self._locks.get(lock_id)
            if not lock or lock.lease_token != lease_token:
                return False, 0, "Lock not found or invalid token"

            lock.expires_at = time.time() + extend_ms / 1000
            await self._persist_lock(lock)
            return True, int(lock.expires_at * 1000), "Lease renewed"

    async def get_lock_status(self, lock_id: str) -> Dict:
        """Get current status of a lock"""
        lock = self._locks.get(lock_id)
        if not lock:
            return {"exists": False}

        return {
            "exists": True,
            "lock_id": lock_id,
            "lock_type": lock.lock_type.value,
            "owner": lock.owner_client,
            "expires_at": int(lock.expires_at * 1000),
            "waiter_count": len(self._wait_queues.get(lock_id, [])),
            "shared_holders": list(lock.shared_holders) if lock.lock_type == LockType.SHARED else [],
        }

    async def list_locks(self) -> List[Dict]:
        """List all active locks"""
        result = []
        for lock_id, lock in self._locks.items():
            result.append({
                "lock_id": lock_id,
                "lock_type": lock.lock_type.value,
                "owner": lock.owner_client,
                "acquired_at": int(lock.acquired_at * 1000),
                "expires_at": int(lock.expires_at * 1000),
                "waiter_count": len(self._wait_queues.get(lock_id, [])),
            })
        return result

    # --------------------------------------------------
    # Lock Acquisition Logic
    # --------------------------------------------------
    async def _try_acquire(
        self,
        lock_id: str,
        lock_type: LockType,
        client_id: str,
        lease_ms: int
    ) -> Optional[Tuple[str, Lock]]:
        """Try to acquire lock immediately. Returns (lease_token, lock) or None."""
        existing = self._locks.get(lock_id)
        now = time.time()

        if existing:
            # Check if expired
            if existing.expires_at < now:
                await self._expire_lock(lock_id, existing)
                existing = None

        if not existing:
            # Lock is free — create it
            lease_token = secrets.token_hex(32)
            lock = Lock(
                lock_id=lock_id,
                lock_type=lock_type,
                owner_client=client_id,
                owner_node=self.node_id,
                acquired_at=now,
                expires_at=now + lease_ms / 1000,
                lease_token=lease_token,
                shared_holders={client_id} if lock_type == LockType.SHARED else set()
            )
            self._locks[lock_id] = lock
            await self._persist_lock(lock)

            # Replicate via Raft
            await self.raft.propose({
                "op": "acquire_lock",
                "lock_id": lock_id,
                "lock_type": lock_type.value,
                "client_id": client_id,
                "expires_at": lock.expires_at,
                "lease_token": lease_token,
            }, entry_type="lock_acquire")

            return lease_token, lock

        # Lock exists
        if existing.lock_type == LockType.SHARED and lock_type == LockType.SHARED:
            # Shared-compatible: add to holders
            existing.shared_holders.add(client_id)
            existing.expires_at = max(existing.expires_at, now + lease_ms / 1000)
            await self._persist_lock(existing)
            return existing.lease_token, existing

        # Not compatible
        return None

    # --------------------------------------------------
    # Deadlock Detection
    # --------------------------------------------------
    async def _deadlock_detector(self):
        """Periodically run deadlock detection"""
        interval = config.deadlock_detection_interval
        while True:
            await asyncio.sleep(interval)
            try:
                cycle = await self._detect_deadlock()
                if cycle:
                    DEADLOCK_DETECTIONS.labels(node_id=self.node_id, result="detected").inc()
                    logger.warning(f"[LockManager] 🔴 Deadlock detected! Cycle: {cycle}")
                    await self._resolve_deadlock(cycle)
                else:
                    DEADLOCK_DETECTIONS.labels(node_id=self.node_id, result="none").inc()
            except Exception as e:
                logger.error(f"[LockManager] Deadlock detector error: {e}")

    async def _detect_deadlock(self) -> Optional[List[str]]:
        """
        Detect deadlock using DFS on wait-for graph.
        Returns cycle path if found, None otherwise.
        """
        async with self._lock:
            graph = dict(self._wait_for_graph)

        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        path: List[str] = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    result = dfs(neighbor)
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # Found cycle
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:]

            path.pop()
            rec_stack.discard(node)
            return None

        for node in list(graph.keys()):
            if node not in visited:
                cycle = dfs(node)
                if cycle:
                    return cycle

        return None

    async def _resolve_deadlock(self, cycle: List[str]):
        """
        Resolve deadlock by aborting the youngest transaction in the cycle.
        Strategy: abort the client that requested the lock most recently.
        """
        # Find the youngest waiter in the cycle
        youngest_client = None
        youngest_time = float('-inf')

        for client_id in cycle:
            for lock_id, waiters in self._wait_queues.items():
                for waiter in waiters:
                    if waiter.client_id == client_id and waiter.requested_at > youngest_time:
                        youngest_time = waiter.requested_at
                        youngest_client = client_id
                        youngest_waiter = waiter

        if youngest_client and hasattr(youngest_waiter, 'future'):
            if not youngest_waiter.future.done():
                youngest_waiter.future.set_exception(
                    Exception(f"Deadlock detected and resolved. Cycle: {' → '.join(cycle)}")
                )
            # Remove from wait queue
            for lock_id in list(self._wait_queues.keys()):
                self._wait_queues[lock_id] = [
                    w for w in self._wait_queues[lock_id]
                    if w.client_id != youngest_client
                ]
            self._wait_for_graph.pop(youngest_client, None)
            logger.info(f"[LockManager] Resolved deadlock by aborting {youngest_client}")

    # --------------------------------------------------
    # Lock Expiry
    # --------------------------------------------------
    async def _expire_locks(self):
        """Background task: expire timed-out locks"""
        while True:
            await asyncio.sleep(1)
            try:
                now = time.time()
                expired = [
                    (lid, lock) for lid, lock in self._locks.items()
                    if lock.expires_at < now
                ]
                for lock_id, lock in expired:
                    async with self._lock:
                        await self._expire_lock(lock_id, lock)
            except Exception as e:
                logger.error(f"[LockManager] Expiry task error: {e}")

    async def _expire_lock(self, lock_id: str, lock: Lock):
        """Expire a specific lock"""
        held_for = time.time() - lock.acquired_at
        LOCK_ACTIVE_COUNT.labels(node_id=self.node_id, lock_type=lock.lock_type.value).dec()
        del self._locks[lock_id]
        await self._remove_lock_from_db(lock_id)
        await self._log_lock_history(lock_id, lock.lock_type, lock.owner_client, "expired", held_for)
        await self._notify_waiters(lock_id)
        logger.info(f"[LockManager] Lock '{lock_id}' expired (held {held_for:.2f}s)")

    # --------------------------------------------------
    # Waiter Notification
    # --------------------------------------------------
    async def _notify_waiters(self, lock_id: str):
        """Wake up the next waiter(s) for a released lock"""
        waiters = self._wait_queues.get(lock_id, [])
        if not waiters:
            return

        # Wake exclusive: 1 waiter. Wake shared: all consecutive shared waiters
        to_wake = []
        if waiters[0].lock_type == LockType.EXCLUSIVE:
            to_wake = [waiters.pop(0)]
        else:
            while waiters and waiters[0].lock_type == LockType.SHARED:
                to_wake.append(waiters.pop(0))

        for waiter in to_wake:
            LOCK_WAIT_QUEUE.labels(node_id=self.node_id).dec()
            self._wait_for_graph.pop(waiter.client_id, None)
            if waiter.future and not waiter.future.done():
                waiter.future.set_result(True)

    # --------------------------------------------------
    # Raft Apply Callback
    # --------------------------------------------------
    async def _apply_raft_command(self, entry):
        """Apply committed Raft log entries to lock state"""
        cmd = entry.command
        op = cmd.get("op")

        if op == "acquire_lock":
            lock_id = cmd["lock_id"]
            if lock_id not in self._locks:
                lock = Lock(
                    lock_id=lock_id,
                    lock_type=LockType(cmd["lock_type"]),
                    owner_client=cmd["client_id"],
                    owner_node=self.node_id,
                    acquired_at=time.time(),
                    expires_at=cmd["expires_at"],
                    lease_token=cmd["lease_token"],
                )
                self._locks[lock_id] = lock

        elif op == "release_lock":
            self._locks.pop(cmd["lock_id"], None)
            await self._notify_waiters(cmd["lock_id"])

    # --------------------------------------------------
    # Leader Callbacks
    # --------------------------------------------------
    async def _on_become_leader(self):
        logger.info(f"[LockManager:{self.node_id}] Became Raft leader")

    async def _on_lose_leadership(self):
        logger.info(f"[LockManager:{self.node_id}] Lost Raft leadership")

    # --------------------------------------------------
    # Persistence
    # --------------------------------------------------
    async def _persist_lock(self, lock: Lock):
        """Save lock to MySQL"""
        try:
            async with self.db_session() as session:
                await session.execute(
                    text("""
                        INSERT INTO distributed_locks
                            (lock_id, lock_type, owner_node, owner_client, expires_at, lease_token)
                        VALUES
                            (:lid, :ltype, :node, :client, FROM_UNIXTIME(:exp), :token)
                        ON DUPLICATE KEY UPDATE
                            lock_type=VALUES(lock_type),
                            owner_node=VALUES(owner_node),
                            owner_client=VALUES(owner_client),
                            expires_at=VALUES(expires_at),
                            lease_token=VALUES(lease_token)
                    """),
                    {
                        "lid": lock.lock_id,
                        "ltype": lock.lock_type.value,
                        "node": lock.owner_node,
                        "client": lock.owner_client,
                        "exp": lock.expires_at,
                        "token": lock.lease_token,
                    }
                )
                await session.commit()
        except Exception as e:
            logger.error(f"[LockManager] Persist lock error: {e}")

    async def _remove_lock_from_db(self, lock_id: str):
        try:
            async with self.db_session() as session:
                await session.execute(
                    text("DELETE FROM distributed_locks WHERE lock_id = :lid"),
                    {"lid": lock_id}
                )
                await session.commit()
        except Exception as e:
            logger.error(f"[LockManager] Remove lock error: {e}")

    async def _log_lock_history(
        self, lock_id: str, lock_type: LockType,
        client_id: str, action: str, duration_s: float = None
    ):
        try:
            async with self.db_session() as session:
                await session.execute(
                    text("""
                        INSERT INTO lock_history
                            (lock_id, lock_type, owner_client, node_id, action, duration_ms)
                        VALUES (:lid, :ltype, :client, :node, :action, :dur)
                    """),
                    {
                        "lid": lock_id,
                        "ltype": lock_type.value,
                        "client": client_id,
                        "node": self.node_id,
                        "action": action,
                        "dur": int(duration_s * 1000) if duration_s else None,
                    }
                )
                await session.commit()
        except Exception as e:
            logger.error(f"[LockManager] Log history error: {e}")

    async def _load_locks_from_db(self):
        """Load active (non-expired) locks from MySQL on startup"""
        try:
            async with self.db_session() as session:
                result = await session.execute(
                    text("""
                        SELECT lock_id, lock_type, owner_node, owner_client,
                               UNIX_TIMESTAMP(acquired_at), UNIX_TIMESTAMP(expires_at), lease_token
                        FROM distributed_locks
                        WHERE expires_at > NOW()
                    """)
                )
                count = 0
                for row in result.fetchall():
                    lock = Lock(
                        lock_id=row[0],
                        lock_type=LockType(row[1]),
                        owner_node=row[2],
                        owner_client=row[3],
                        acquired_at=float(row[4]),
                        expires_at=float(row[5]),
                        lease_token=row[6],
                    )
                    self._locks[lock.lock_id] = lock
                    count += 1
                logger.info(f"[LockManager] Loaded {count} active locks from DB")
        except Exception as e:
            logger.warning(f"[LockManager] Could not load locks from DB: {e}")

    # --------------------------------------------------
    # Leader Forwarding
    # --------------------------------------------------
    async def _forward_acquire_to_leader(
        self, lock_id: str, lock_type: LockType,
        client_id: str, timeout_ms: int, lease_ms: int
    ) -> Tuple[bool, Optional[str], str]:
        """Forward lock acquisition to current Raft leader"""
        leader = self.raft.leader_id
        if not leader or leader not in self.peer_stubs:
            return False, None, "No leader available (network partition?)"

        try:
            from src.proto.generated import node_pb2
            stub = self.peer_stubs[leader]['lock']
            response = await stub.AcquireLock(
                node_pb2.LockRequest(
                    lock_id=lock_id,
                    lock_type=node_pb2.LockType.EXCLUSIVE if lock_type == LockType.EXCLUSIVE else node_pb2.LockType.SHARED,
                    client_id=client_id,
                    timeout_ms=timeout_ms,
                    lease_ms=lease_ms,
                ),
                timeout=timeout_ms / 1000 + 1
            )
            return response.success, response.lease_token if response.success else None, response.message
        except Exception as e:
            return False, None, f"Failed to forward to leader: {e}"

    async def _forward_release_to_leader(
        self, lock_id: str, client_id: str, lease_token: str
    ) -> Tuple[bool, str]:
        leader = self.raft.leader_id
        if not leader or leader not in self.peer_stubs:
            return False, "No leader available"

        try:
            from src.proto.generated import node_pb2
            stub = self.peer_stubs[leader]['lock']
            response = await stub.ReleaseLock(
                node_pb2.ReleaseRequest(
                    lock_id=lock_id,
                    client_id=client_id,
                    lease_token=lease_token,
                ),
                timeout=5
            )
            return response.success, response.message
        except Exception as e:
            return False, f"Forward to leader failed: {e}"
