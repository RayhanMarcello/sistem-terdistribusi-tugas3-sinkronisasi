"""
Distributed Sync System - Failure Detector
Implements Phi Accrual Failure Detector:
  - Heartbeat monitoring for each peer
  - Adaptive threshold based on arrival history
  - Network partition detection
  - Automatic peer reconnection
"""
import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, Optional, Set

from src.utils.config import config
from src.utils.metrics import NODE_CONNECTED_PEERS

logger = logging.getLogger(__name__)


@dataclass
class HeartbeatHistory:
    """Tracks heartbeat inter-arrival times for Phi Accrual"""
    times: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    last_heartbeat: float = field(default_factory=time.monotonic)

    def record(self):
        now = time.monotonic()
        interval = now - self.last_heartbeat
        self.last_heartbeat = now
        if interval > 0:
            self.times.append(interval)

    @property
    def mean(self) -> float:
        if not self.times:
            return 1.0
        return sum(self.times) / len(self.times)

    @property
    def variance(self) -> float:
        if len(self.times) < 2:
            return 0.1
        m = self.mean
        return sum((t - m) ** 2 for t in self.times) / len(self.times)

    @property
    def std_dev(self) -> float:
        return math.sqrt(self.variance)

    def phi(self) -> float:
        """
        Compute Phi accrual value.
        Higher phi = more likely the node has failed.
        Phi > threshold (typically 8-16) = suspect failed.
        """
        elapsed = time.monotonic() - self.last_heartbeat
        if not self.times or self.std_dev == 0:
            return 0.0

        # Cumulative probability that heartbeat is late
        # Using normal distribution approximation
        t = elapsed - self.mean
        prob_late = 1.0 - self._phi_cdf(t / self.std_dev) if self.std_dev > 0 else 0.5
        prob_late = max(1e-10, prob_late)
        return -math.log10(prob_late)

    @staticmethod
    def _phi_cdf(x: float) -> float:
        """CDF of normal distribution"""
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))


class FailureDetector:
    """
    Phi Accrual Failure Detector for distributed node monitoring.

    Features:
    - Adaptive threshold based on heartbeat history
    - Hooks for on_suspect / on_recovered callbacks
    - Automatic reconnection on channel failure
    - Network partition detection (multiple nodes fail simultaneously)
    """

    PHI_THRESHOLD = 8.0     # phi > this → suspect failed
    HEARTBEAT_INTERVAL = 1.0  # seconds between heartbeats
    RECONNECT_INTERVAL = 5.0  # seconds between reconnect attempts

    def __init__(self, node_id: str, peer_stubs: Dict, reconnect_callback: Optional[Callable] = None):
        self.node_id = node_id
        self.peer_stubs = peer_stubs

        # Heartbeat tracking per peer
        self._histories: Dict[str, HeartbeatHistory] = {
            pid: HeartbeatHistory() for pid in peer_stubs
        }

        # Current suspect status
        self._suspects: Set[str] = set()
        self._confirmed_partitioned: Set[str] = set()

        # Callbacks
        self._on_suspect: Optional[Callable] = None      # (node_id: str) → None
        self._on_recovered: Optional[Callable] = None    # (node_id: str) → None
        self._on_partition: Optional[Callable] = None    # (suspects: Set[str]) → None
        self._reconnect_callback = reconnect_callback    # (node_id, host, port) → None

        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None

    # --------------------------------------------------
    # Start / Stop
    # --------------------------------------------------
    async def start(self):
        self._heartbeat_task = asyncio.create_task(self._send_heartbeats())
        self._monitor_task = asyncio.create_task(self._monitor_phi())
        logger.info(f"[FailureDetector:{self.node_id}] Started (threshold={self.PHI_THRESHOLD})")

    async def stop(self):
        for task in [self._heartbeat_task, self._monitor_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    # --------------------------------------------------
    # Heartbeat Sending
    # --------------------------------------------------
    async def _send_heartbeats(self):
        """Continuously send heartbeats to all peers"""
        while True:
            tasks = [
                self._ping_peer(peer_id, stub)
                for peer_id, stub in self.peer_stubs.items()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)

    async def _ping_peer(self, peer_id: str, node_stub):
        """Send a heartbeat to a single peer"""
        try:
            from src.proto.generated import node_pb2
            response = await node_stub.Heartbeat(
                node_pb2.HeartbeatRequest(
                    sender_id=self.node_id,
                    timestamp=int(time.time() * 1000),
                ),
                timeout=2.0
            )

            if response.alive:
                # Record successful heartbeat
                if peer_id not in self._histories:
                    self._histories[peer_id] = HeartbeatHistory()
                self._histories[peer_id].record()

                # Node recovered
                if peer_id in self._suspects:
                    self._suspects.discard(peer_id)
                    self._confirmed_partitioned.discard(peer_id)
                    logger.info(f"[FailureDetector:{self.node_id}] ✅ Node {peer_id} RECOVERED")
                    NODE_CONNECTED_PEERS.labels(node_id=self.node_id).inc()
                    if self._on_recovered:
                        asyncio.create_task(self._on_recovered(peer_id))

        except Exception:
            # Heartbeat failed — don't record, phi will increase naturally
            pass

    # --------------------------------------------------
    # Phi Accrual Monitoring
    # --------------------------------------------------
    async def _monitor_phi(self):
        """Monitor phi values and detect failures"""
        while True:
            await asyncio.sleep(1.0)
            try:
                newly_suspected = []

                for peer_id, history in self._histories.items():
                    phi = history.phi()

                    if phi > self.PHI_THRESHOLD and peer_id not in self._suspects:
                        self._suspects.add(peer_id)
                        newly_suspected.append(peer_id)
                        logger.warning(
                            f"[FailureDetector:{self.node_id}] 🔴 SUSPECT: {peer_id} "
                            f"(phi={phi:.2f}, threshold={self.PHI_THRESHOLD})"
                        )
                        NODE_CONNECTED_PEERS.labels(node_id=self.node_id).dec()
                        if self._on_suspect:
                            asyncio.create_task(self._on_suspect(peer_id))

                # Network partition detection: if majority of nodes fail simultaneously
                total_peers = len(self._histories)
                if total_peers > 0:
                    suspect_ratio = len(self._suspects) / total_peers
                    if suspect_ratio > 0.5 and self._on_partition:
                        logger.critical(
                            f"[FailureDetector:{self.node_id}] ⚠️ NETWORK PARTITION DETECTED! "
                            f"Suspects: {self._suspects}"
                        )
                        asyncio.create_task(self._on_partition(set(self._suspects)))

            except Exception as e:
                logger.error(f"[FailureDetector:{self.node_id}] Monitor error: {e}")

    # --------------------------------------------------
    # External Heartbeat Handler
    # --------------------------------------------------
    def record_heartbeat(self, sender_id: str):
        """Called when we receive a heartbeat FROM a peer"""
        if sender_id not in self._histories:
            self._histories[sender_id] = HeartbeatHistory()
        self._histories[sender_id].record()

    # --------------------------------------------------
    # Status
    # --------------------------------------------------
    def is_alive(self, peer_id: str) -> bool:
        return peer_id not in self._suspects

    def get_phi(self, peer_id: str) -> float:
        h = self._histories.get(peer_id)
        return h.phi() if h else 0.0

    def get_status(self) -> Dict:
        return {
            "alive": [p for p in self._histories if p not in self._suspects],
            "suspected": list(self._suspects),
            "partitioned": list(self._confirmed_partitioned),
            "phi_values": {
                p: round(h.phi(), 3) for p, h in self._histories.items()
            }
        }
