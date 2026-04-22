"""
PBFT - Practical Byzantine Fault Tolerance (Bonus A)
3-phase protocol: Pre-Prepare → Prepare → Commit
  - Tolerates f = (n-1)/3 faulty nodes
  - View Change for faulty primary replacement
  - Malicious node simulation for demo
"""
import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Any

from src.utils.config import config
from src.utils.metrics import (
    PBFT_ROUNDS, PBFT_VIEW, PBFT_IS_PRIMARY, PBFT_PHASE_DURATION
)

logger = logging.getLogger(__name__)


class PBFTPhase(Enum):
    PRE_PREPARE  = "pre_prepare"
    PREPARE      = "prepare"
    COMMIT       = "commit"
    VIEW_CHANGE  = "view_change"


@dataclass
class PBFTMessage:
    view: int
    sequence: int
    digest: str
    node_id: str
    phase: PBFTPhase
    payload: bytes = b""
    timestamp: float = field(default_factory=time.time)


@dataclass
class RequestEntry:
    """Tracks state for a single PBFT consensus round"""
    view: int
    sequence: int
    digest: str
    request: bytes
    pre_prepared: bool = False
    prepare_votes: Set[str] = field(default_factory=set)   # node_ids that sent Prepare
    commit_votes: Set[str] = field(default_factory=set)    # node_ids that sent Commit
    committed: bool = False
    result: Optional[bytes] = None
    future: Optional[asyncio.Future] = None


class PBFTNode:
    """
    Practical Byzantine Fault Tolerance (PBFT) Implementation.

    Tolerates up to f = floor((n-1)/3) Byzantine (malicious) nodes.
    Requires n >= 3f + 1 nodes (minimum 4 nodes for f=1).

    Protocol phases:
    ┌─────────────────────────────────────────┐
    │ Client → Primary: REQUEST               │
    │ Primary → All:    PRE-PREPARE(v, n, d)  │
    │ All → All:        PREPARE(v, n, d, i)   │
    │ All → All:        COMMIT(v, n, d, i)    │
    │ All → Client:     REPLY(v, t, c, i, r)  │
    └─────────────────────────────────────────┘
    """

    VIEW_CHANGE_TIMEOUT = 5.0  # seconds before triggering view change

    def __init__(self, node_id: str, all_nodes: List[str], peer_stubs: Dict):
        self.node_id = node_id
        self.all_nodes = sorted(all_nodes)
        self.peer_stubs = peer_stubs
        self.n = len(all_nodes)
        self.f = (self.n - 1) // 3  # max faulty nodes
        self.quorum = 2 * self.f + 1  # quorum size

        self.view = 0
        self.sequence = 0
        self.log: Dict[str, RequestEntry] = {}  # digest → RequestEntry

        # View change tracking
        self._view_change_votes: Dict[int, Set[str]] = {}  # new_view → set of nodes
        self._view_change_timer: Optional[asyncio.Task] = None

        # Is this node simulated as malicious (for demo)?
        self._is_malicious = False
        self._malicious_behavior = "silent"  # silent | equivocate | delay

        logger.info(
            f"[PBFT:{self.node_id}] Init: n={self.n}, f={self.f}, quorum={self.quorum}, "
            f"primary={self.primary_id}"
        )

    # --------------------------------------------------
    # Properties
    # --------------------------------------------------
    @property
    def primary_id(self) -> str:
        return self.all_nodes[self.view % self.n]

    @property
    def is_primary(self) -> bool:
        return self.node_id == self.primary_id

    # --------------------------------------------------
    # Client Request (sent to primary)
    # --------------------------------------------------
    async def submit_request(self, operation: bytes, client_id: str) -> Optional[bytes]:
        """
        Submit a client request to the PBFT cluster.
        Returns the agreed result or None if consensus fails.
        """
        if not self.is_primary:
            # Forward to primary
            primary_stub = self.peer_stubs.get(self.primary_id, {}).get('pbft')
            if primary_stub:
                try:
                    from src.proto.generated import node_pb2
                    resp = await primary_stub.PrePrepare(
                        self._make_pre_prepare_proto(operation),
                        timeout=self.VIEW_CHANGE_TIMEOUT
                    )
                    return resp.result if resp.success else None
                except Exception as e:
                    logger.warning(f"[PBFT:{self.node_id}] Primary forward failed: {e}")
            return None

        # We ARE the primary — start pre-prepare
        return await self._do_pre_prepare(operation, client_id)

    async def _do_pre_prepare(self, operation: bytes, client_id: str) -> Optional[bytes]:
        """Primary: initiate Pre-Prepare phase"""
        if self._is_malicious and self._malicious_behavior == "silent":
            logger.warning(f"[PBFT:{self.node_id}] ⚠️ Malicious primary: dropping request")
            return None

        self.sequence += 1
        digest = self._digest(operation)
        entry = RequestEntry(
            view=self.view,
            sequence=self.sequence,
            digest=digest,
            request=operation,
            pre_prepared=True,
            prepare_votes={self.node_id},
        )
        entry.future = asyncio.get_event_loop().create_future()
        self.log[digest] = entry

        phase_start = time.monotonic()

        # Broadcast PRE-PREPARE to all replicas
        tasks = []
        for peer_id, stubs in self.peer_stubs.items():
            pbft_stub = stubs.get('pbft')
            if pbft_stub:
                tasks.append(self._send_pre_prepare(peer_id, pbft_stub, operation, digest))

        await asyncio.gather(*tasks, return_exceptions=True)

        # Also prepare ourselves
        await self._on_prepare(self.node_id, self.view, self.sequence, digest)

        PBFT_PHASE_DURATION.labels(node_id=self.node_id, phase="pre_prepare").observe(
            time.monotonic() - phase_start
        )

        # Wait for commit with timeout
        try:
            result = await asyncio.wait_for(entry.future, timeout=self.VIEW_CHANGE_TIMEOUT)
            PBFT_ROUNDS.labels(node_id=self.node_id, result="success").inc()
            return result
        except asyncio.TimeoutError:
            logger.warning(f"[PBFT:{self.node_id}] Consensus timeout for seq={self.sequence}")
            PBFT_ROUNDS.labels(node_id=self.node_id, result="timeout").inc()
            await self._trigger_view_change()
            return None

    async def _send_pre_prepare(self, peer_id: str, stub, operation: bytes, digest: str):
        """Send PRE-PREPARE to a specific peer"""
        try:
            from src.proto.generated import node_pb2
            await stub.PrePrepare(
                node_pb2.PrePrepareRequest(
                    view=self.view,
                    sequence=self.sequence,
                    digest=digest,
                    request=node_pb2.PBFTRequest(
                        operation=operation,
                        client_id=self.node_id,
                        timestamp=int(time.time() * 1000),
                    ),
                    primary_id=self.node_id,
                ),
                timeout=2.0
            )
        except Exception as e:
            logger.debug(f"[PBFT:{self.node_id}] Pre-prepare to {peer_id} failed: {e}")

    # --------------------------------------------------
    # Phase 1: Handle PRE-PREPARE (replica)
    # --------------------------------------------------
    async def handle_pre_prepare(
        self, view: int, sequence: int, digest: str,
        request: bytes, primary_id: str
    ) -> bool:
        """Replica receives PRE-PREPARE from primary"""
        if view != self.view:
            return False
        if primary_id != self.primary_id:
            return False
        if self._digest(request) != digest:
            logger.warning(f"[PBFT:{self.node_id}] Digest mismatch in pre-prepare!")
            return False

        entry = RequestEntry(
            view=view,
            sequence=sequence,
            digest=digest,
            request=request,
            pre_prepared=True,
            prepare_votes={self.node_id},
        )
        self.log[digest] = entry

        # Broadcast PREPARE to all
        await self._broadcast_prepare(view, sequence, digest)
        return True

    # --------------------------------------------------
    # Phase 2: PREPARE
    # --------------------------------------------------
    async def _broadcast_prepare(self, view: int, sequence: int, digest: str):
        """Broadcast PREPARE message to all nodes"""
        phase_start = time.monotonic()
        tasks = []
        for peer_id, stubs in self.peer_stubs.items():
            pbft_stub = stubs.get('pbft')
            if pbft_stub:
                tasks.append(self._send_prepare(peer_id, pbft_stub, view, sequence, digest))

        await asyncio.gather(*tasks, return_exceptions=True)
        # Count our own prepare
        await self._on_prepare(self.node_id, view, sequence, digest)

        PBFT_PHASE_DURATION.labels(node_id=self.node_id, phase="prepare").observe(
            time.monotonic() - phase_start
        )

    async def _send_prepare(self, peer_id: str, stub, view: int, sequence: int, digest: str):
        try:
            from src.proto.generated import node_pb2
            await stub.Prepare(
                node_pb2.PrepareRequest(
                    view=view, sequence=sequence,
                    digest=digest, node_id=self.node_id,
                ),
                timeout=2.0
            )
        except Exception:
            pass

    async def handle_prepare(self, view: int, sequence: int, digest: str, sender_id: str):
        """Handle incoming PREPARE message"""
        await self._on_prepare(sender_id, view, sequence, digest)

    async def _on_prepare(self, sender_id: str, view: int, sequence: int, digest: str):
        """Record prepare vote and check if prepared"""
        entry = self.log.get(digest)
        if not entry or not entry.pre_prepared:
            return

        entry.prepare_votes.add(sender_id)

        # Prepared = received 2f prepare messages (quorum - 1, excluding pre-prepare)
        if len(entry.prepare_votes) >= self.quorum and not entry.committed:
            await self._broadcast_commit(view, sequence, digest)

    # --------------------------------------------------
    # Phase 3: COMMIT
    # --------------------------------------------------
    async def _broadcast_commit(self, view: int, sequence: int, digest: str):
        """Broadcast COMMIT once prepared"""
        phase_start = time.monotonic()
        tasks = []
        for peer_id, stubs in self.peer_stubs.items():
            pbft_stub = stubs.get('pbft')
            if pbft_stub:
                tasks.append(self._send_commit(peer_id, pbft_stub, view, sequence, digest))

        await asyncio.gather(*tasks, return_exceptions=True)
        await self._on_commit(self.node_id, view, sequence, digest)

        PBFT_PHASE_DURATION.labels(node_id=self.node_id, phase="commit").observe(
            time.monotonic() - phase_start
        )

    async def _send_commit(self, peer_id, stub, view, sequence, digest):
        try:
            from src.proto.generated import node_pb2
            await stub.Commit(
                node_pb2.CommitRequest(
                    view=view, sequence=sequence,
                    digest=digest, node_id=self.node_id,
                ),
                timeout=2.0
            )
        except Exception:
            pass

    async def handle_commit(self, view: int, sequence: int, digest: str, sender_id: str):
        await self._on_commit(sender_id, view, sequence, digest)

    async def _on_commit(self, sender_id: str, view: int, sequence: int, digest: str):
        """Record commit vote and execute if committed"""
        entry = self.log.get(digest)
        if not entry or entry.committed:
            return

        entry.commit_votes.add(sender_id)

        # Committed = 2f+1 commit messages
        if len(entry.commit_votes) >= self.quorum:
            entry.committed = True
            result = await self._execute(entry)
            entry.result = result
            if entry.future and not entry.future.done():
                entry.future.set_result(result)
            PBFT_ROUNDS.labels(node_id=self.node_id, result="success").inc()
            logger.info(f"[PBFT:{self.node_id}] ✅ Committed seq={sequence}, view={view}")

    async def _execute(self, entry: RequestEntry) -> bytes:
        """Execute the committed operation"""
        try:
            op = json.loads(entry.request.decode())
            return json.dumps({"status": "ok", "result": op}).encode()
        except Exception:
            return b'{"status":"ok"}'

    # --------------------------------------------------
    # View Change
    # --------------------------------------------------
    async def _trigger_view_change(self):
        """Trigger a view change when primary is suspected faulty"""
        new_view = self.view + 1
        logger.warning(
            f"[PBFT:{self.node_id}] Triggering view change: {self.view} → {new_view}"
        )
        PBFT_ROUNDS.labels(node_id=self.node_id, result="view_change").inc()

        # Broadcast VIEW-CHANGE
        tasks = []
        for peer_id, stubs in self.peer_stubs.items():
            pbft_stub = stubs.get('pbft')
            if pbft_stub:
                tasks.append(self._send_view_change(pbft_stub, new_view))

        await asyncio.gather(*tasks, return_exceptions=True)
        await self.handle_view_change(new_view, self.node_id)

    async def _send_view_change(self, stub, new_view: int):
        try:
            from src.proto.generated import node_pb2
            await stub.ViewChange(
                node_pb2.ViewChangeRequest(
                    new_view=new_view, node_id=self.node_id,
                    last_seq=self.sequence,
                ),
                timeout=2.0
            )
        except Exception:
            pass

    async def handle_view_change(self, new_view: int, sender_id: str):
        """Handle VIEW-CHANGE message"""
        if new_view not in self._view_change_votes:
            self._view_change_votes[new_view] = set()
        self._view_change_votes[new_view].add(sender_id)

        # Need f+1 view-change messages to adopt new view
        if len(self._view_change_votes[new_view]) >= self.f + 1:
            self.view = new_view
            self.sequence = 0
            PBFT_VIEW.labels(node_id=self.node_id).set(self.view)
            PBFT_IS_PRIMARY.labels(node_id=self.node_id).set(1 if self.is_primary else 0)
            logger.info(
                f"[PBFT:{self.node_id}] ✅ View change complete → view={self.view}, "
                f"new primary={self.primary_id}"
            )

    # --------------------------------------------------
    # Malicious Node Simulation (Demo)
    # --------------------------------------------------
    def set_malicious(self, behavior: str = "silent"):
        """Enable malicious behavior for demo purposes"""
        self._is_malicious = True
        self._malicious_behavior = behavior
        logger.warning(f"[PBFT:{self.node_id}] ⚠️ Node set as MALICIOUS (behavior={behavior})")

    def set_honest(self):
        self._is_malicious = False
        logger.info(f"[PBFT:{self.node_id}] Node restored to honest behavior")

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    @staticmethod
    def _digest(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def get_status(self) -> Dict:
        return {
            "node_id": self.node_id,
            "view": self.view,
            "sequence": self.sequence,
            "primary_id": self.primary_id,
            "is_primary": self.is_primary,
            "n": self.n,
            "f": self.f,
            "quorum": self.quorum,
            "is_malicious": self._is_malicious,
            "log_size": len(self.log),
        }

    def _make_pre_prepare_proto(self, operation: bytes):
        from src.proto.generated import node_pb2
        return node_pb2.PrePrepareRequest(
            view=self.view,
            sequence=self.sequence + 1,
            digest=self._digest(operation),
            request=node_pb2.PBFTRequest(
                operation=operation,
                client_id=self.node_id,
                timestamp=int(time.time() * 1000),
            ),
            primary_id=self.primary_id,
        )
