"""
Distributed Cache Coherence - MOESI Protocol
States: Modified, Owned, Exclusive, Shared, Invalid
  - Full state machine transitions
  - Invalidation broadcast via gRPC
  - LRU + LFU replacement policies (configurable)
  - Intervention protocol (O → S transition)
  - Performance metrics collection
"""
import asyncio
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text

from src.nodes.base_node import BaseNode
from src.utils.config import config
from src.utils.metrics import (
    CACHE_HITS, CACHE_MISSES, CACHE_INVALIDATIONS, CACHE_EVICTIONS,
    CACHE_SIZE, CACHE_HIT_RATE, CACHE_OPERATION_LATENCY,
    CACHE_STATE_DISTRIBUTION, CACHE_STATE_TRANSITIONS
)

logger = logging.getLogger(__name__)


class MOESIState(Enum):
    MODIFIED  = "Modified"
    OWNED     = "Owned"
    EXCLUSIVE = "Exclusive"
    SHARED    = "Shared"
    INVALID   = "Invalid"


@dataclass
class CacheLine:
    key: str
    value: bytes
    state: MOESIState
    version: int = 1
    access_count: int = 0
    last_access: float = field(default_factory=time.monotonic)
    created_at: float = field(default_factory=time.monotonic)
    owner_node: Optional[str] = None  # node that has Modified/Owned copy


class CacheNode(BaseNode):
    """
    MOESI Cache Coherence Protocol Implementation.

    State Transitions:
    ─────────────────────────────────────────────────────────────
    READ HIT  (local):
      M → M  (serve locally)
      O → O  (serve locally, still dirty)
      E → S  (downgrade to Shared, notify no other copies)
      S → S  (serve locally)

    READ MISS (no local copy or Invalid):
      → Check directory/other nodes
      If another has M/O: Intervention → O state for owner, S for both
      If another has E:   E → S, we get S
      If no one has it:   fetch from backing store → E (exclusive)

    WRITE HIT (local):
      M → M  (update locally)
      O → M  (upgrade to Modified, invalidate all S copies)
      E → M  (upgrade to Modified)
      S → M  (upgrade: BusUpgr → invalidate all other S copies)

    WRITE MISS:
      I → M  (fetch + invalidate all other copies)
      Any → M (RequestOwnership → invalidate others)
    ─────────────────────────────────────────────────────────────
    """

    def __init__(self):
        super().__init__()
        # Local cache storage: key → CacheLine
        self._cache: OrderedDict[str, CacheLine] = OrderedDict()
        # For LFU: access frequency
        self._freq: Dict[str, int] = {}
        # Directory: key → set of node_ids that have a copy
        self._directory: Dict[str, Set[str]] = {}
        # Cache lock
        self._cache_lock = asyncio.Lock()
        # Stats
        self._hits = 0
        self._misses = 0
        self._invalidations_sent = 0
        self._invalidations_recv = 0
        self._evictions = 0

    # --------------------------------------------------
    # Startup
    # --------------------------------------------------
    async def start(self):
        await super().start()
        asyncio.create_task(self._metrics_reporter())
        logger.info(f"[CacheNode:{self.node_id}] ✅ MOESI cache started (capacity={config.cache_max_size})")

    # --------------------------------------------------
    # READ Operation
    # --------------------------------------------------
    async def read(self, key: str, requester_id: str = None) -> Tuple[bool, Optional[bytes], MOESIState]:
        """
        Read a cache entry. Implements MOESI read protocol.
        Returns: (hit, value, state)
        """
        start = time.monotonic()
        requester = requester_id or self.node_id

        async with self._cache_lock:
            line = self._cache.get(key)

            # ── Local HIT ──────────────────────────────────────
            if line and line.state != MOESIState.INVALID:
                self._touch(key, line)
                self._hits += 1
                CACHE_HITS.labels(node_id=self.node_id).inc()
                CACHE_OPERATION_LATENCY.labels(node_id=self.node_id, operation="read_hit").observe(
                    time.monotonic() - start
                )
                return True, line.value, line.state

        # ── MISS ───────────────────────────────────────────────
        self._misses += 1
        CACHE_MISSES.labels(node_id=self.node_id).inc()

        # Try to get from other nodes
        value, state, owner = await self._fetch_from_peers(key)

        if value is not None:
            # Got from peer — store as Shared
            async with self._cache_lock:
                await self._store_line(key, value, MOESIState.SHARED, owner_node=owner)

            CACHE_OPERATION_LATENCY.labels(node_id=self.node_id, operation="read_miss").observe(
                time.monotonic() - start
            )
            return True, value, MOESIState.SHARED

        CACHE_OPERATION_LATENCY.labels(node_id=self.node_id, operation="read_miss").observe(
            time.monotonic() - start
        )
        return False, None, MOESIState.INVALID

    # --------------------------------------------------
    # WRITE Operation
    # --------------------------------------------------
    async def write(self, key: str, value: bytes, ttl: int = None) -> Tuple[bool, int]:
        """
        Write a cache entry. Implements MOESI write protocol.
        Returns: (success, new_version)
        """
        start = time.monotonic()

        async with self._cache_lock:
            line = self._cache.get(key)
            old_state = line.state if line else MOESIState.INVALID

            # Determine new version
            new_version = (line.version + 1) if line else 1

            # For M/O/E states we own it — can write directly
            # For S/I states — must invalidate all other copies first
            if old_state in (MOESIState.SHARED, MOESIState.INVALID):
                # Release lock briefly to invalidate peers
                pass

        # Invalidate all other copies (outside lock to avoid deadlock)
        if old_state in (MOESIState.SHARED, MOESIState.INVALID):
            await self._broadcast_invalidation(key, new_version)

        async with self._cache_lock:
            old_state2 = MOESIState.INVALID
            if key in self._cache:
                old_state2 = self._cache[key].state

            # Transition to Modified (we now have the dirty exclusive copy)
            await self._store_line(key, value, MOESIState.MODIFIED, version=new_version)

            CACHE_STATE_TRANSITIONS.labels(
                node_id=self.node_id,
                from_state=old_state2.value,
                to_state=MOESIState.MODIFIED.value
            ).inc()

        CACHE_OPERATION_LATENCY.labels(node_id=self.node_id, operation="write").observe(
            time.monotonic() - start
        )
        return True, new_version

    # --------------------------------------------------
    # MOESI State Handlers (called by gRPC servicer)
    # --------------------------------------------------
    async def handle_invalidate(self, key: str, version: int, sender_id: str) -> Tuple[bool, bool]:
        """
        Handle invalidation request from another node.
        Returns: (success, had_dirty)
        """
        had_dirty = False
        async with self._cache_lock:
            line = self._cache.get(key)
            if line:
                had_dirty = line.state in (MOESIState.MODIFIED, MOESIState.OWNED)
                if had_dirty:
                    # Write back dirty data before invalidating
                    await self._write_back(key, line)

                old_state = line.state
                line.state = MOESIState.INVALID
                CACHE_STATE_TRANSITIONS.labels(
                    node_id=self.node_id,
                    from_state=old_state.value,
                    to_state=MOESIState.INVALID.value
                ).inc()

                self._invalidations_recv += 1
                CACHE_INVALIDATIONS.labels(node_id=self.node_id, direction="received").inc()

        # Remove from directory
        if key in self._directory:
            self._directory[key].discard(self.node_id)

        return True, had_dirty

    async def handle_read_request(self, key: str, requester_id: str) -> Tuple[Optional[bytes], MOESIState, int]:
        """
        Handle read request from another node.
        If we have M/O, we supply the data (intervention).
        """
        async with self._cache_lock:
            line = self._cache.get(key)
            if not line or line.state == MOESIState.INVALID:
                return None, MOESIState.INVALID, 0

            # Intervention: M → O (we remain owner but share)
            if line.state == MOESIState.MODIFIED:
                old_state = line.state
                line.state = MOESIState.OWNED
                CACHE_STATE_TRANSITIONS.labels(
                    node_id=self.node_id,
                    from_state=old_state.value,
                    to_state=MOESIState.OWNED.value
                ).inc()

            # E → S (we downgrade since another has a copy now)
            elif line.state == MOESIState.EXCLUSIVE:
                old_state = line.state
                line.state = MOESIState.SHARED
                CACHE_STATE_TRANSITIONS.labels(
                    node_id=self.node_id,
                    from_state=old_state.value,
                    to_state=MOESIState.SHARED.value
                ).inc()

            # Track directory
            if key not in self._directory:
                self._directory[key] = set()
            self._directory[key].add(requester_id)

            return line.value, line.state, line.version

    async def handle_ownership_request(self, key: str, requester_id: str) -> Tuple[Optional[bytes], int, MOESIState]:
        """
        Handle write ownership request. Give up our copy, transition to I.
        """
        async with self._cache_lock:
            line = self._cache.get(key)
            if not line or line.state == MOESIState.INVALID:
                return None, 0, MOESIState.INVALID

            value = line.value
            version = line.version
            old_state = line.state

            # Write back if dirty
            if old_state in (MOESIState.MODIFIED, MOESIState.OWNED):
                await self._write_back(key, line)

            line.state = MOESIState.INVALID
            CACHE_STATE_TRANSITIONS.labels(
                node_id=self.node_id,
                from_state=old_state.value,
                to_state=MOESIState.INVALID.value
            ).inc()

            return value, version, old_state

    # --------------------------------------------------
    # Peer Communication
    # --------------------------------------------------
    async def _fetch_from_peers(self, key: str) -> Tuple[Optional[bytes], Optional[MOESIState], Optional[str]]:
        """Fetch a cache line from peer nodes"""
        from src.proto.generated import node_pb2

        tasks = []
        peer_ids = list(self.peer_stubs.keys())

        for peer_id in peer_ids:
            stub = self.peer_stubs[peer_id].get('cache')
            if stub:
                tasks.append((peer_id, stub.ReadCache(
                    node_pb2.CacheReadRequest(
                        requester_id=self.node_id,
                        cache_key=key
                    ),
                    timeout=1.0
                )))

        for peer_id, coro in tasks:
            try:
                response = await coro
                if response.success and response.value:
                    state_map = {
                        0: MOESIState.MODIFIED,
                        1: MOESIState.OWNED,
                        2: MOESIState.EXCLUSIVE,
                        3: MOESIState.SHARED,
                        4: MOESIState.INVALID,
                    }
                    return response.value, state_map.get(response.state, MOESIState.SHARED), peer_id
            except Exception:
                continue

        return None, None, None

    async def _broadcast_invalidation(self, key: str, version: int):
        """Send invalidation to all peers that might have a copy"""
        from src.proto.generated import node_pb2

        peers_with_copy = self._directory.get(key, set(self.peer_stubs.keys()))

        tasks = []
        for peer_id in peers_with_copy:
            if peer_id == self.node_id:
                continue
            stub = self.peer_stubs.get(peer_id, {}).get('cache')
            if stub:
                tasks.append(stub.InvalidateCache(
                    node_pb2.InvalidateRequest(
                        sender_id=self.node_id,
                        cache_key=key,
                        version=version,
                        reason="write"
                    ),
                    timeout=1.0
                ))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            sent = sum(1 for r in results if not isinstance(r, Exception))
            self._invalidations_sent += sent
            CACHE_INVALIDATIONS.labels(node_id=self.node_id, direction="sent").inc(sent)

        # Clear directory for this key (others now invalid)
        self._directory[key] = {self.node_id}

    async def _write_back(self, key: str, line: CacheLine):
        """Write dirty data back to Redis backing store"""
        try:
            await self.redis_set(
                f"cache:{key}",
                json.dumps({"value": line.value.hex(), "version": line.version}),
                ttl=3600
            )
        except Exception as e:
            logger.error(f"[CacheNode] Write-back error for {key}: {e}")

    # --------------------------------------------------
    # Cache Storage & Eviction
    # --------------------------------------------------
    async def _store_line(
        self, key: str, value: bytes, state: MOESIState,
        version: int = None, owner_node: str = None
    ):
        """Store a cache line, evicting if necessary (called under lock)"""
        if len(self._cache) >= config.cache_max_size and key not in self._cache:
            await self._evict_one()

        if key in self._cache:
            line = self._cache[key]
            line.value = value
            line.state = state
            line.version = version or (line.version + 1)
            line.last_access = time.monotonic()
            line.access_count += 1
            if owner_node:
                line.owner_node = owner_node
        else:
            self._cache[key] = CacheLine(
                key=key,
                value=value,
                state=state,
                version=version or 1,
                owner_node=owner_node,
                access_count=1,
            )
            self._freq[key] = 1

        # Move to end (most recently used) for LRU
        self._cache.move_to_end(key)
        CACHE_SIZE.labels(node_id=self.node_id).set(len(self._cache))

    async def _evict_one(self):
        """Evict one entry based on configured policy (LRU or LFU)"""
        if not self._cache:
            return

        if config.cache_policy == "lfu":
            # LFU: evict least frequently used
            victim_key = min(self._cache.keys(), key=lambda k: self._cache[k].access_count)
        else:
            # LRU: evict oldest (first item in OrderedDict)
            victim_key = next(iter(self._cache))

        victim = self._cache[victim_key]

        # Write back if dirty
        if victim.state in (MOESIState.MODIFIED, MOESIState.OWNED):
            await self._write_back(victim_key, victim)

        del self._cache[victim_key]
        self._freq.pop(victim_key, None)
        self._evictions += 1
        CACHE_EVICTIONS.labels(node_id=self.node_id, policy=config.cache_policy).inc()

    def _touch(self, key: str, line: CacheLine):
        """Update access stats for LRU/LFU"""
        line.last_access = time.monotonic()
        line.access_count += 1
        self._freq[key] = self._freq.get(key, 0) + 1
        self._cache.move_to_end(key)

    # --------------------------------------------------
    # Stats & Metrics
    # --------------------------------------------------
    async def get_stats(self) -> Dict:
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0

        # Count MOESI state distribution
        state_counts: Dict[str, int] = {s.value: 0 for s in MOESIState}
        for line in self._cache.values():
            state_counts[line.state.value] += 1

        return {
            "node_id": self.node_id,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(hit_rate, 4),
            "size": len(self._cache),
            "capacity": config.cache_max_size,
            "invalidations_sent": self._invalidations_sent,
            "invalidations_received": self._invalidations_recv,
            "evictions": self._evictions,
            "policy": config.cache_policy,
            "state_distribution": state_counts,
        }

    async def _metrics_reporter(self):
        """Periodically update Prometheus metrics"""
        while True:
            await asyncio.sleep(15)
            stats = await self.get_stats()
            CACHE_HIT_RATE.labels(node_id=self.node_id).set(stats["hit_rate"])
            CACHE_SIZE.labels(node_id=self.node_id).set(stats["size"])
            for state, count in stats["state_distribution"].items():
                CACHE_STATE_DISTRIBUTION.labels(node_id=self.node_id, state=state).set(count)
