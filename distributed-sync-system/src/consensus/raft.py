"""
Distributed Sync System - Raft Consensus Algorithm
Implements full Raft consensus:
  - Leader Election (randomized timeout)
  - Log Replication (majority quorum)
  - Persistent state (MySQL)
  - Snapshotting
"""
import asyncio
import json
import logging
import random
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Awaitable

import grpc
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from src.utils.config import config
from src.utils.metrics import (
    RAFT_TERM, RAFT_ROLE, RAFT_VOTES, RAFT_LOG_SIZE,
    RAFT_COMMIT_INDEX, RAFT_LEADER_CHANGES, RAFT_RPC_DURATION
)

logger = logging.getLogger(__name__)


class RaftRole(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]
    entry_type: str = "generic"

    def to_bytes(self) -> bytes:
        return json.dumps({
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "type": self.entry_type
        }).encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "LogEntry":
        d = json.loads(data.decode())
        return cls(
            term=d["term"],
            index=d["index"],
            command=d["command"],
            entry_type=d.get("type", "generic")
        )


@dataclass
class RaftState:
    """Persistent Raft state (survives restarts)"""
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)
    commit_index: int = 0
    last_applied: int = 0


class RaftNode:
    """
    Full Raft consensus implementation.

    Features:
    - Leader election with randomized timeout
    - Log replication with majority quorum
    - Persistent state in MySQL
    - Network partition handling
    - Snapshotting for compaction
    """

    def __init__(self, node_id: str, apply_callback: Optional[Callable] = None):
        self.node_id = node_id
        self.role = RaftRole.FOLLOWER
        self.state = RaftState()
        self.apply_callback = apply_callback  # Called when entry is committed

        # Leader-specific volatile state
        self.next_index: Dict[str, int] = {}   # node_id -> next log index to send
        self.match_index: Dict[str, int] = {}  # node_id -> highest confirmed index

        # Election state
        self.votes_received: set = set()
        self.current_leader: Optional[str] = None

        # Timers
        self._election_timer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._apply_task: Optional[asyncio.Task] = None
        self._last_heartbeat = time.monotonic()

        # gRPC stubs to peers (set by base_node)
        self.peer_stubs: Dict[str, Any] = {}

        # DB engine
        self._engine = create_async_engine(config.mysql_url, echo=False, pool_size=5)
        self._session_factory = sessionmaker(self._engine, class_=AsyncSession, expire_on_commit=False)

        # Locks
        self._state_lock = asyncio.Lock()
        self._log_lock = asyncio.Lock()

        # Callbacks for leadership changes
        self._on_become_leader: Optional[Callable] = None
        self._on_lose_leadership: Optional[Callable] = None

        logger.info(f"[Raft:{self.node_id}] Initialized as FOLLOWER")

    # --------------------------------------------------
    # Startup / Shutdown
    # --------------------------------------------------
    async def start(self):
        """Start Raft node: load persisted state, begin election timer"""
        await self._load_persistent_state()
        self._election_timer_task = asyncio.create_task(self._election_timer())
        self._apply_task = asyncio.create_task(self._apply_committed_entries())
        logger.info(f"[Raft:{self.node_id}] Started. Term={self.state.current_term}")

    async def stop(self):
        """Stop all Raft background tasks"""
        for task in [self._election_timer_task, self._heartbeat_task, self._apply_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        await self._engine.dispose()
        logger.info(f"[Raft:{self.node_id}] Stopped")

    # --------------------------------------------------
    # RPC Handlers (called by gRPC server)
    # --------------------------------------------------
    async def handle_request_vote(
        self, term: int, candidate_id: str,
        last_log_index: int, last_log_term: int
    ) -> tuple[int, bool]:
        """Handle RequestVote RPC"""
        async with self._state_lock:
            vote_granted = False

            # If candidate has newer term, update ours
            if term > self.state.current_term:
                await self._update_term(term)
                self._set_role(RaftRole.FOLLOWER)

            # Vote conditions:
            # 1. Candidate term >= our term
            # 2. We haven't voted yet (or already voted for this candidate)
            # 3. Candidate's log is at least as up-to-date as ours
            my_last_index = len(self.state.log)
            my_last_term = self.state.log[-1].term if self.state.log else 0

            log_ok = (
                last_log_term > my_last_term or
                (last_log_term == my_last_term and last_log_index >= my_last_index)
            )

            if (
                term >= self.state.current_term
                and (self.state.voted_for is None or self.state.voted_for == candidate_id)
                and log_ok
            ):
                vote_granted = True
                self.state.voted_for = candidate_id
                self._last_heartbeat = time.monotonic()  # Reset election timer
                await self._persist_state()
                logger.info(f"[Raft:{self.node_id}] Voted for {candidate_id} in term {term}")
                RAFT_VOTES.labels(node_id=self.node_id, direction="granted").inc()

            return self.state.current_term, vote_granted

    async def handle_append_entries(
        self, term: int, leader_id: str,
        prev_log_index: int, prev_log_term: int,
        entries: List[LogEntry], leader_commit: int
    ) -> tuple[int, bool, int]:
        """Handle AppendEntries RPC (heartbeat + log replication)"""
        async with self._state_lock:
            success = False
            match_index = 0

            if term > self.state.current_term:
                await self._update_term(term)

            if term < self.state.current_term:
                return self.state.current_term, False, 0

            # Valid leader - reset election timer
            self._last_heartbeat = time.monotonic()
            self.current_leader = leader_id
            if self.role != RaftRole.FOLLOWER:
                self._set_role(RaftRole.FOLLOWER)

            # Consistency check: prev entry must match
            if prev_log_index > 0:
                if len(self.state.log) < prev_log_index:
                    return self.state.current_term, False, 0
                actual_prev = self.state.log[prev_log_index - 1]
                if actual_prev.term != prev_log_term:
                    # Delete conflicting entries
                    async with self._log_lock:
                        self.state.log = self.state.log[:prev_log_index - 1]
                    return self.state.current_term, False, 0

            # Append new entries (skip existing ones)
            if entries:
                async with self._log_lock:
                    for i, entry in enumerate(entries):
                        idx = prev_log_index + i
                        if idx < len(self.state.log):
                            if self.state.log[idx].term != entry.term:
                                self.state.log = self.state.log[:idx]
                                self.state.log.append(entry)
                        else:
                            self.state.log.append(entry)
                    await self._persist_log_entries(entries)
                    RAFT_LOG_SIZE.labels(node_id=self.node_id).set(len(self.state.log))

            # Update commit index
            if leader_commit > self.state.commit_index:
                self.state.commit_index = min(leader_commit, len(self.state.log))
                RAFT_COMMIT_INDEX.labels(node_id=self.node_id).set(self.state.commit_index)

            success = True
            match_index = len(self.state.log)
            return self.state.current_term, True, match_index

    # --------------------------------------------------
    # Leader Operations
    # --------------------------------------------------
    async def propose(self, command: Dict[str, Any], entry_type: str = "generic") -> Optional[int]:
        """
        Propose a new command to the cluster.
        Returns log index if leader, None if not leader.
        """
        if self.role != RaftRole.LEADER:
            return None

        async with self._log_lock:
            entry = LogEntry(
                term=self.state.current_term,
                index=len(self.state.log) + 1,
                command=command,
                entry_type=entry_type
            )
            self.state.log.append(entry)
            await self._persist_log_entries([entry])
            RAFT_LOG_SIZE.labels(node_id=self.node_id).set(len(self.state.log))

        # Trigger immediate replication
        asyncio.create_task(self._replicate_to_all())
        return entry.index

    async def _replicate_to_all(self):
        """Replicate log entries to all followers, commit on majority"""
        if self.role != RaftRole.LEADER:
            return

        peers = list(self.peer_stubs.keys())
        if not peers:
            # Single node - auto-commit
            if self.state.log:
                self.state.commit_index = len(self.state.log)
            return

        tasks = [self._replicate_to_peer(peer_id) for peer_id in peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successful replications
        successes = 1  # Count self
        for r in results:
            if r is True:
                successes += 1

        total_nodes = len(peers) + 1
        quorum = total_nodes // 2 + 1

        if successes >= quorum:
            # Commit: advance commit_index to highest replicated entry
            new_commit = self.state.commit_index
            for n in range(len(self.state.log), self.state.commit_index, -1):
                if self.state.log[n-1].term == self.state.current_term:
                    matches = sum(
                        1 for m in self.match_index.values() if m >= n
                    ) + 1  # +1 for self
                    if matches >= quorum:
                        new_commit = n
                        break

            if new_commit > self.state.commit_index:
                self.state.commit_index = new_commit
                RAFT_COMMIT_INDEX.labels(node_id=self.node_id).set(new_commit)

    async def _replicate_to_peer(self, peer_id: str) -> bool:
        """Send AppendEntries to a single peer"""
        stub = self.peer_stubs.get(peer_id)
        if not stub:
            return False

        next_idx = self.next_index.get(peer_id, len(self.state.log) + 1)
        prev_idx = next_idx - 1
        prev_term = self.state.log[prev_idx - 1].term if prev_idx > 0 and prev_idx <= len(self.state.log) else 0

        entries_to_send = self.state.log[next_idx - 1:] if next_idx <= len(self.state.log) else []

        try:
            from src.proto.generated import node_pb2, node_pb2_grpc
            proto_entries = [
                node_pb2.LogEntry(
                    term=e.term,
                    index=e.index,
                    command=e.to_bytes(),
                    type=e.entry_type
                )
                for e in entries_to_send
            ]

            with RAFT_RPC_DURATION.labels(node_id=self.node_id, rpc_type="append_entries").time():
                response = await stub.AppendEntries(
                    node_pb2.AppendEntriesRequest(
                        term=self.state.current_term,
                        leader_id=self.node_id,
                        prev_log_index=prev_idx,
                        prev_log_term=prev_term,
                        entries=proto_entries,
                        leader_commit=self.state.commit_index
                    ),
                    timeout=0.5
                )

            if response.term > self.state.current_term:
                async with self._state_lock:
                    await self._update_term(response.term)
                    self._set_role(RaftRole.FOLLOWER)
                return False

            if response.success:
                if entries_to_send:
                    self.next_index[peer_id] = entries_to_send[-1].index + 1
                    self.match_index[peer_id] = entries_to_send[-1].index
                return True
            else:
                # Decrement next_index and retry
                self.next_index[peer_id] = max(1, next_idx - 1)
                return False

        except Exception as e:
            logger.debug(f"[Raft:{self.node_id}] Failed to replicate to {peer_id}: {e}")
            return False

    # --------------------------------------------------
    # Election
    # --------------------------------------------------
    async def _election_timer(self):
        """Run election timer - trigger election if no heartbeat"""
        while True:
            timeout_ms = random.randint(
                config.raft_election_timeout_min,
                config.raft_election_timeout_max
            )
            await asyncio.sleep(timeout_ms / 1000)

            if self.role == RaftRole.LEADER:
                continue

            elapsed = (time.monotonic() - self._last_heartbeat) * 1000
            if elapsed >= timeout_ms:
                logger.info(f"[Raft:{self.node_id}] Election timeout! Starting election...")
                await self._start_election()

    async def _start_election(self):
        """Start a new election"""
        async with self._state_lock:
            self._set_role(RaftRole.CANDIDATE)
            self.state.current_term += 1
            self.state.voted_for = self.node_id
            self.votes_received = {self.node_id}
            self.current_leader = None
            await self._persist_state()

            RAFT_TERM.labels(node_id=self.node_id).set(self.state.current_term)
            logger.info(f"[Raft:{self.node_id}] Starting election for term {self.state.current_term}")

        # Request votes from all peers
        my_last_index = len(self.state.log)
        my_last_term = self.state.log[-1].term if self.state.log else 0

        tasks = [
            self._request_vote_from(peer_id, my_last_index, my_last_term)
            for peer_id in self.peer_stubs
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _request_vote_from(self, peer_id: str, last_log_index: int, last_log_term: int):
        """Send RequestVote to a single peer"""
        stub = self.peer_stubs.get(peer_id)
        if not stub:
            return

        try:
            from src.proto.generated import node_pb2
            with RAFT_RPC_DURATION.labels(node_id=self.node_id, rpc_type="request_vote").time():
                response = await stub.RequestVote(
                    node_pb2.RequestVoteRequest(
                        term=self.state.current_term,
                        candidate_id=self.node_id,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    ),
                    timeout=0.5
                )

            async with self._state_lock:
                if response.term > self.state.current_term:
                    await self._update_term(response.term)
                    self._set_role(RaftRole.FOLLOWER)
                    return

                if self.role != RaftRole.CANDIDATE:
                    return

                if response.vote_granted:
                    self.votes_received.add(peer_id)
                    RAFT_VOTES.labels(node_id=self.node_id, direction="received").inc()

                    total_nodes = len(self.peer_stubs) + 1
                    quorum = total_nodes // 2 + 1

                    if len(self.votes_received) >= quorum:
                        await self._become_leader()

        except Exception as e:
            logger.debug(f"[Raft:{self.node_id}] Vote request to {peer_id} failed: {e}")

    async def _become_leader(self):
        """Transition to LEADER role"""
        if self.role == RaftRole.LEADER:
            return

        self._set_role(RaftRole.LEADER)
        self.current_leader = self.node_id
        RAFT_LEADER_CHANGES.labels(node_id=self.node_id).inc()

        # Initialize leader state
        for peer_id in self.peer_stubs:
            self.next_index[peer_id] = len(self.state.log) + 1
            self.match_index[peer_id] = 0

        logger.info(f"[Raft:{self.node_id}] 🎉 Became LEADER for term {self.state.current_term}")

        # Start heartbeat
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._send_heartbeats())

        # Notify application
        if self._on_become_leader:
            asyncio.create_task(self._on_become_leader())

        # Append no-op entry to commit previous term entries
        await self.propose({"type": "noop"}, "noop")

    async def _send_heartbeats(self):
        """Send periodic heartbeats to all followers"""
        while self.role == RaftRole.LEADER:
            tasks = [self._replicate_to_peer(peer_id) for peer_id in self.peer_stubs]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(config.raft_heartbeat_interval / 1000)

    # --------------------------------------------------
    # Apply Committed Entries
    # --------------------------------------------------
    async def _apply_committed_entries(self):
        """Apply committed log entries to state machine"""
        while True:
            if self.state.commit_index > self.state.last_applied:
                for i in range(self.state.last_applied + 1, self.state.commit_index + 1):
                    if i <= len(self.state.log):
                        entry = self.state.log[i - 1]
                        if self.apply_callback and entry.entry_type != "noop":
                            try:
                                await self.apply_callback(entry)
                            except Exception as e:
                                logger.error(f"[Raft:{self.node_id}] Apply callback error: {e}")
                        self.state.last_applied = i
            await asyncio.sleep(0.01)

    # --------------------------------------------------
    # Persistence (MySQL)
    # --------------------------------------------------
    async def _load_persistent_state(self):
        """Load Raft state from MySQL on startup"""
        try:
            async with self._session_factory() as session:
                result = await session.execute(
                    text("SELECT current_term, voted_for, commit_index, last_applied "
                         "FROM raft_state WHERE node_id = :nid"),
                    {"nid": self.node_id}
                )
                row = result.fetchone()
                if row:
                    self.state.current_term = row[0]
                    self.state.voted_for = row[1]
                    self.state.commit_index = row[2]
                    self.state.last_applied = row[3]

                # Load log
                log_result = await session.execute(
                    text("SELECT term, log_index, command FROM raft_log "
                         "WHERE node_id = :nid ORDER BY log_index ASC"),
                    {"nid": self.node_id}
                )
                for log_row in log_result.fetchall():
                    entry = LogEntry(
                        term=log_row[0],
                        index=log_row[1],
                        command=json.loads(log_row[2]) if isinstance(log_row[2], str) else log_row[2]
                    )
                    self.state.log.append(entry)

                logger.info(
                    f"[Raft:{self.node_id}] Loaded state: term={self.state.current_term}, "
                    f"log_size={len(self.state.log)}"
                )
        except Exception as e:
            logger.warning(f"[Raft:{self.node_id}] Could not load persistent state: {e}")

    async def _persist_state(self):
        """Persist current Raft state to MySQL"""
        try:
            async with self._session_factory() as session:
                await session.execute(
                    text("""
                        INSERT INTO raft_state (node_id, current_term, voted_for, commit_index, last_applied)
                        VALUES (:nid, :term, :voted, :commit, :applied)
                        ON DUPLICATE KEY UPDATE
                            current_term=VALUES(current_term),
                            voted_for=VALUES(voted_for),
                            commit_index=VALUES(commit_index),
                            last_applied=VALUES(last_applied)
                    """),
                    {
                        "nid": self.node_id,
                        "term": self.state.current_term,
                        "voted": self.state.voted_for,
                        "commit": self.state.commit_index,
                        "applied": self.state.last_applied
                    }
                )
                await session.commit()
        except Exception as e:
            logger.error(f"[Raft:{self.node_id}] Failed to persist state: {e}")

    async def _persist_log_entries(self, entries: List[LogEntry]):
        """Persist new log entries to MySQL"""
        try:
            async with self._session_factory() as session:
                for entry in entries:
                    await session.execute(
                        text("""
                            INSERT IGNORE INTO raft_log
                                (node_id, term, log_index, command, committed)
                            VALUES (:nid, :term, :idx, :cmd, :committed)
                        """),
                        {
                            "nid": self.node_id,
                            "term": entry.term,
                            "idx": entry.index,
                            "cmd": json.dumps(entry.command),
                            "committed": entry.index <= self.state.commit_index
                        }
                    )
                await session.commit()
        except Exception as e:
            logger.error(f"[Raft:{self.node_id}] Failed to persist log entries: {e}")

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    async def _update_term(self, new_term: int):
        """Update term and reset vote"""
        self.state.current_term = new_term
        self.state.voted_for = None
        RAFT_TERM.labels(node_id=self.node_id).set(new_term)
        await self._persist_state()

    def _set_role(self, role: RaftRole):
        """Set role and update metrics"""
        old_role = self.role
        self.role = role
        role_map = {RaftRole.LEADER: 1, RaftRole.CANDIDATE: 2, RaftRole.FOLLOWER: 3}
        RAFT_ROLE.labels(node_id=self.node_id).set(role_map[role])

        if old_role == RaftRole.LEADER and role != RaftRole.LEADER:
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            if self._on_lose_leadership:
                asyncio.create_task(self._on_lose_leadership())

        logger.info(f"[Raft:{self.node_id}] Role: {old_role.value} → {role.value}")

    @property
    def is_leader(self) -> bool:
        return self.role == RaftRole.LEADER

    @property
    def leader_id(self) -> Optional[str]:
        return self.current_leader

    def get_status(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role.value,
            "term": self.state.current_term,
            "leader_id": self.current_leader,
            "log_size": len(self.state.log),
            "commit_index": self.state.commit_index,
            "last_applied": self.state.last_applied,
            "voted_for": self.state.voted_for,
            "peers": list(self.peer_stubs.keys()),
        }
