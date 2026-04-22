"""
Geo-Distributed System (Bonus B)
  - Multi-region simulation (US, EU, ASIA)
  - Latency-aware routing
  - Eventual consistency with vector clocks
  - Cross-region data replication via Kafka
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from src.utils.config import config
from src.utils.metrics import GEO_REPLICATION_LATENCY, GEO_CONFLICTS

logger = logging.getLogger(__name__)


@dataclass
class VectorClock:
    """Vector clock for tracking causal ordering across regions"""
    clock: Dict[str, int] = field(default_factory=dict)

    def increment(self, node_id: str):
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def merge(self, other: "VectorClock") -> "VectorClock":
        merged = dict(self.clock)
        for node, ts in other.clock.items():
            merged[node] = max(merged.get(node, 0), ts)
        return VectorClock(clock=merged)

    def happens_before(self, other: "VectorClock") -> bool:
        """Returns True if self → other (self happened before other)"""
        return (
            all(self.clock.get(k, 0) <= other.clock.get(k, 0) for k in self.clock)
            and any(self.clock.get(k, 0) < other.clock.get(k, 0) for k in self.clock)
        )

    def concurrent_with(self, other: "VectorClock") -> bool:
        return not self.happens_before(other) and not other.happens_before(self)

    def to_dict(self) -> Dict[str, int]:
        return dict(self.clock)


@dataclass
class DataEntry:
    key: str
    value: Any
    vector_clock: VectorClock
    region: str
    timestamp: float = field(default_factory=time.time)


# Region latency matrix (ms)
REGION_LATENCY = {
    ("us", "eu"):   100,
    ("eu", "us"):   100,
    ("us", "asia"): 150,
    ("asia", "us"): 150,
    ("eu", "asia"): 120,
    ("asia", "eu"): 120,
    ("us", "us"):   5,
    ("eu", "eu"):   5,
    ("asia", "asia"): 5,
}


class RegionManager:
    """
    Geo-Distributed Region Manager.

    Features:
    - Latency-aware routing: direct requests to nearest region
    - Eventual consistency using vector clocks
    - Cross-region replication via Kafka topics
    - Conflict detection & resolution (last-writer-wins by timestamp)
    """

    REGIONS = ["us", "eu", "asia"]

    def __init__(self, node_id: str, region: str, peer_stubs: Dict, kafka_producer=None):
        self.node_id = node_id
        self.region = region
        self.peer_stubs = peer_stubs
        self.kafka_producer = kafka_producer

        # Local data store with vector clocks
        self._store: Dict[str, DataEntry] = {}
        self._vector_clock = VectorClock()
        self._pending_replication: List[DataEntry] = []

        logger.info(f"[GeoManager:{node_id}] Initialized in region '{region}'")

    # --------------------------------------------------
    # Routing
    # --------------------------------------------------
    def get_nearest_node(self, client_region: str, operation: str = "read") -> Optional[str]:
        """
        Find the nearest node for a client in the given region.
        For writes: prefer leader node. For reads: prefer local region.
        """
        # Find peers in the same region
        same_region_peers = [
            pid for pid, stubs in self.peer_stubs.items()
            if self._get_peer_region(pid) == client_region
        ]
        if same_region_peers:
            return same_region_peers[0]

        # Otherwise find lowest-latency neighbor
        min_latency = float('inf')
        best_peer = None
        for pid in self.peer_stubs:
            peer_region = self._get_peer_region(pid)
            latency = REGION_LATENCY.get((client_region, peer_region), 999)
            if latency < min_latency:
                min_latency = latency
                best_peer = pid

        return best_peer

    def _get_peer_region(self, peer_id: str) -> str:
        """Infer region from node naming convention (geo-node-us → us)"""
        for region in self.REGIONS:
            if peer_id.endswith(f"-{region}") or peer_id.endswith(f"_{region}"):
                return region
        return "us"  # default

    async def route_request(
        self,
        client_region: str,
        operation: str,
        payload: bytes
    ) -> Tuple[bool, bytes, str, int]:
        """
        Route a client request to the optimal node.
        Returns: (success, result, target_region, estimated_latency_ms)
        """
        target_node = self.get_nearest_node(client_region, operation)
        target_region = self._get_peer_region(target_node) if target_node else self.region
        estimated_latency = REGION_LATENCY.get((client_region, target_region), 10)

        # Simulate network latency
        await asyncio.sleep(estimated_latency / 1000)

        if not target_node or target_node not in self.peer_stubs:
            # Handle locally
            return True, b'{"handled": "locally"}', self.region, 5

        try:
            geo_stub = self.peer_stubs[target_node].get('geo')
            if not geo_stub:
                return False, b'', target_region, estimated_latency

            from src.proto.generated import node_pb2
            response = await geo_stub.RouteRequest(
                node_pb2.GeoRouteRequest(
                    client_region=client_region,
                    operation_type=operation,
                    payload=payload,
                ),
                timeout=5.0
            )
            return response.success, response.result, target_region, estimated_latency

        except Exception as e:
            logger.error(f"[GeoManager:{self.node_id}] Route error: {e}")
            return False, b'', target_region, estimated_latency

    # --------------------------------------------------
    # Data Operations (Eventual Consistency)
    # --------------------------------------------------
    async def write(self, key: str, value: Any) -> VectorClock:
        """Write data with vector clock (eventual consistency)"""
        self._vector_clock.increment(self.node_id)
        entry = DataEntry(
            key=key,
            value=value,
            vector_clock=VectorClock(clock=dict(self._vector_clock.clock)),
            region=self.region,
        )
        self._store[key] = entry

        # Async replication to other regions
        asyncio.create_task(self._replicate_to_regions(entry))
        return entry.vector_clock

    async def read(self, key: str) -> Optional[DataEntry]:
        """Read data from local store (may be stale — eventual consistency)"""
        return self._store.get(key)

    async def apply_replication(self, key: str, value: Any, vc_dict: Dict[str, int], source_region: str):
        """
        Apply a replicated entry from another region.
        Conflict resolution: last-writer-wins by timestamp.
        """
        incoming_vc = VectorClock(clock=vc_dict)
        local = self._store.get(key)

        if local is None:
            # No local copy — accept
            self._store[key] = DataEntry(
                key=key, value=value,
                vector_clock=incoming_vc,
                region=source_region,
            )
            self._vector_clock = self._vector_clock.merge(incoming_vc)
            return

        if local.vector_clock.happens_before(incoming_vc):
            # Incoming is newer — accept
            self._store[key] = DataEntry(
                key=key, value=value,
                vector_clock=incoming_vc,
                region=source_region,
            )
            self._vector_clock = self._vector_clock.merge(incoming_vc)

        elif incoming_vc.happens_before(local.vector_clock):
            # Local is newer — discard
            pass

        else:
            # Concurrent writes — CONFLICT
            GEO_CONFLICTS.labels(region=self.region).inc()
            logger.warning(
                f"[GeoManager:{self.node_id}] Conflict on '{key}' "
                f"(local={self.region}, remote={source_region}) → LWW resolution"
            )
            # Last-writer-wins: keep newest by wall clock
            # (in production: use application-level merge)
            incoming_entry = DataEntry(key=key, value=value, vector_clock=incoming_vc, region=source_region)
            if incoming_entry.timestamp > local.timestamp:
                self._store[key] = incoming_entry
            self._vector_clock = self._vector_clock.merge(incoming_vc)

    async def _replicate_to_regions(self, entry: DataEntry):
        """Replicate data to all peer nodes in other regions"""
        start = time.monotonic()

        tasks = []
        for peer_id, stubs in self.peer_stubs.items():
            peer_region = self._get_peer_region(peer_id)
            if peer_region == self.region:
                continue  # Skip same region

            geo_stub = stubs.get('geo')
            if not geo_stub:
                continue

            tasks.append(self._send_replication(
                geo_stub, peer_id, peer_region, entry
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (peer_region,), result in zip(
            [(self._get_peer_region(pid),) for pid in self.peer_stubs if self._get_peer_region(pid) != self.region],
            results
        ):
            if not isinstance(result, Exception):
                latency = time.monotonic() - start
                GEO_REPLICATION_LATENCY.labels(
                    source_region=self.region,
                    target_region=peer_region
                ).observe(latency)

    async def _send_replication(self, geo_stub, peer_id: str, peer_region: str, entry: DataEntry):
        """Send replication request to a peer"""
        try:
            # Simulate cross-region latency
            latency_ms = config.get_geo_latency(self.region, peer_region)
            await asyncio.sleep(latency_ms / 1000)

            from src.proto.generated import node_pb2
            await geo_stub.ReplicateData(
                node_pb2.ReplicationRequest(
                    source_region=self.region,
                    key=entry.key,
                    value=json.dumps(entry.value).encode() if not isinstance(entry.value, bytes) else entry.value,
                    vector_clock=entry.vector_clock.clock.get(self.node_id, 0),
                    operation="write",
                ),
                timeout=5.0
            )
        except Exception as e:
            logger.debug(f"[GeoManager:{self.node_id}] Replication to {peer_id} failed: {e}")

    # --------------------------------------------------
    # Status
    # --------------------------------------------------
    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "region": self.region,
            "vector_clock": self._vector_clock.to_dict(),
            "store_size": len(self._store),
            "regions": self.REGIONS,
            "latency_matrix": {
                f"{r1}→{r2}": REGION_LATENCY.get((r1, r2), 0)
                for r1 in self.REGIONS for r2 in self.REGIONS
            },
        }
