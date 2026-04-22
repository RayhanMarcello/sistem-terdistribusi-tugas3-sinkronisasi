"""
Distributed Queue System - Consistent Hashing + Kafka
  - Consistent hashing ring with virtual nodes
  - Kafka as persistence backend
  - At-least-once delivery guarantee
  - Multiple producers & consumers
  - Node failure → hash ring rebalancing
"""
import asyncio
import hashlib
import json
import logging
import time
import uuid
from bisect import bisect_right, insort
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlalchemy import text

from src.nodes.base_node import BaseNode
from src.utils.config import config
from src.utils.metrics import (
    QUEUE_ENQUEUE_TOTAL, QUEUE_DEQUEUE_TOTAL, QUEUE_SIZE,
    QUEUE_THROUGHPUT, QUEUE_MESSAGE_AGE, QUEUE_REBALANCES
)

logger = logging.getLogger(__name__)

# Kafka topics
TOPIC_TASK_QUEUE = "task-queue"
TOPIC_DLQ        = "dead-letter-queue"
TOPIC_AUDIT      = "audit-log"
TOPIC_METRICS    = "metrics-stream"


# -----------------------------------------------
# Consistent Hash Ring
# -----------------------------------------------
class ConsistentHashRing:
    """
    Consistent hash ring with virtual nodes (vnodes).
    Provides even distribution and minimal rebalancing on node add/remove.
    """

    VIRTUAL_NODES = 150  # vnodes per physical node

    def __init__(self):
        self._ring: List[int]     = []   # sorted hash points
        self._map: Dict[int, str] = {}   # hash → node_id
        self._nodes: set = set()

    def add_node(self, node_id: str):
        """Add a node with virtual node replicas"""
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self.VIRTUAL_NODES):
            h = self._hash(f"{node_id}#{i}")
            insort(self._ring, h)
            self._map[h] = node_id
        logger.debug(f"[HashRing] Added node {node_id} ({self.VIRTUAL_NODES} vnodes)")

    def remove_node(self, node_id: str):
        """Remove a node and its virtual replicas"""
        if node_id not in self._nodes:
            return
        self._nodes.discard(node_id)
        for i in range(self.VIRTUAL_NODES):
            h = self._hash(f"{node_id}#{i}")
            if h in self._map:
                self._ring.remove(h)
                del self._map[h]
        logger.info(f"[HashRing] Removed node {node_id}")

    def get_node(self, key: str) -> Optional[str]:
        """Get the node responsible for a given key"""
        if not self._ring:
            return None
        h = self._hash(key)
        idx = bisect_right(self._ring, h) % len(self._ring)
        return self._map[self._ring[idx]]

    def get_partition(self, key: str, num_partitions: int) -> int:
        """Get Kafka partition for a key"""
        h = self._hash(key)
        return h % num_partitions

    def get_all_nodes(self) -> List[str]:
        return list(self._nodes)

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)


@dataclass
class Message:
    message_id: str
    topic: str
    partition_key: str
    payload: bytes
    headers: Dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0


class QueueNode(BaseNode):
    """
    Distributed Queue System using Kafka + Consistent Hashing.

    Features:
    - Consistent hashing for partition assignment
    - Kafka for durable message storage
    - At-least-once delivery (manual ack + retry)
    - Dead letter queue for failed messages
    - Multi-producer / multi-consumer support
    - Node failure → automatic rebalancing
    """

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__()
        self.hash_ring = ConsistentHashRing()
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumers: Dict[str, AIOKafkaConsumer] = {}
        self._pending_acks: Dict[str, Message] = {}  # message_id → message
        self._throughput_counter = 0
        self._throughput_reset_time = time.monotonic()

    # --------------------------------------------------
    # Startup / Shutdown
    # --------------------------------------------------
    async def start(self):
        await super().start()
        await self._setup_kafka_topics()
        await self._start_producer()
        await self._build_hash_ring()
        asyncio.create_task(self._throughput_reporter())
        asyncio.create_task(self._rebalance_watcher())
        logger.info(f"[QueueNode:{self.node_id}] ✅ Queue node started")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
        for consumer in self._consumers.values():
            await consumer.stop()
        await super().stop()

    # --------------------------------------------------
    # Kafka Setup
    # --------------------------------------------------
    async def _setup_kafka_topics(self):
        """Create Kafka topics if they don't exist"""
        topics = [
            NewTopic(TOPIC_TASK_QUEUE, config.kafka_partitions,   config.kafka_replication_factor),
            NewTopic(TOPIC_DLQ,        config.kafka_partitions//2, config.kafka_replication_factor),
            NewTopic(TOPIC_AUDIT,      3,                          config.kafka_replication_factor),
            NewTopic(TOPIC_METRICS,    3,                          config.kafka_replication_factor),
        ]
        try:
            admin = AIOKafkaAdminClient(bootstrap_servers=config.kafka_bootstrap_servers)
            await admin.start()
            existing = await admin.list_topics()
            new_topics = [t for t in topics if t.name not in existing]
            if new_topics:
                await admin.create_topics(new_topics)
                logger.info(f"[QueueNode] Created Kafka topics: {[t.name for t in new_topics]}")
            await admin.close()
        except Exception as e:
            logger.warning(f"[QueueNode] Kafka topic setup warning: {e}")

    async def _start_producer(self):
        """Start Kafka producer"""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=config.kafka_bootstrap_servers,
            key_serializer=lambda k: k.encode() if k else None,
            value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode(),
            acks="all",             # Wait for all replicas (at-least-once)
            enable_idempotence=True,
            max_batch_size=16384,
            linger_ms=5,
            compression_type="snappy",
        )
        await self._producer.start()
        logger.info(f"[QueueNode] Kafka producer started → {config.kafka_bootstrap_servers}")

    # --------------------------------------------------
    # Consistent Hash Ring
    # --------------------------------------------------
    async def _build_hash_ring(self):
        """Build hash ring from all cluster nodes"""
        for node_id, _, _ in config.cluster_nodes:
            self.hash_ring.add_node(node_id)
        logger.info(
            f"[QueueNode:{self.node_id}] Hash ring built with {len(config.cluster_nodes)} nodes"
        )

    async def _rebalance_watcher(self):
        """Watch for peer failures and rebalance hash ring"""
        known_peers = set(nid for nid, _, _ in config.peer_nodes)
        while True:
            await asyncio.sleep(10)
            current_peers = set(self.peer_stubs.keys())
            # Detect removed nodes
            for lost in known_peers - current_peers:
                self.hash_ring.remove_node(lost)
                QUEUE_REBALANCES.labels(node_id=self.node_id).inc()
                logger.warning(f"[QueueNode:{self.node_id}] Rebalanced ring: removed {lost}")
            # Detect added nodes (rejoined)
            for joined in current_peers - known_peers:
                self.hash_ring.add_node(joined)
            known_peers = current_peers

    # --------------------------------------------------
    # Produce (Enqueue)
    # --------------------------------------------------
    async def enqueue(
        self,
        topic: str,
        partition_key: str,
        payload: Any,
        headers: Dict[str, str] = None,
        producer_id: str = "anonymous"
    ) -> Tuple[bool, str, str, int]:
        """
        Enqueue a message to the distributed queue.
        Returns: (success, message_id, responsible_node, partition)
        """
        message_id = str(uuid.uuid4())
        # Determine responsible node via consistent hashing
        responsible_node = self.hash_ring.get_node(partition_key or message_id)
        partition = self.hash_ring.get_partition(
            partition_key or message_id, config.kafka_partitions
        )

        # Serialize payload
        if isinstance(payload, (dict, list)):
            payload_bytes = json.dumps(payload).encode()
        elif isinstance(payload, str):
            payload_bytes = payload.encode()
        else:
            payload_bytes = payload

        # Build Kafka message
        kafka_headers = [
            ("message_id", message_id.encode()),
            ("producer_id", producer_id.encode()),
            ("partition_key", (partition_key or "").encode()),
            ("created_at", str(time.time()).encode()),
        ]
        if headers:
            kafka_headers += [(k, v.encode()) for k, v in headers.items()]

        try:
            await self._producer.send_and_wait(
                topic,
                key=partition_key or message_id,
                value=payload_bytes,
                partition=partition,
                headers=kafka_headers,
            )

            # Persist to MySQL
            await self._persist_message(message_id, topic, partition_key, responsible_node, payload_bytes)

            QUEUE_ENQUEUE_TOTAL.labels(node_id=self.node_id, topic=topic).inc()
            QUEUE_SIZE.labels(node_id=self.node_id, topic=topic, partition=str(partition)).inc()
            self._throughput_counter += 1

            logger.debug(
                f"[QueueNode:{self.node_id}] Enqueued {message_id} → "
                f"topic={topic}, partition={partition}, node={responsible_node}"
            )
            return True, message_id, responsible_node, partition

        except Exception as e:
            logger.error(f"[QueueNode:{self.node_id}] Enqueue error: {e}")
            return False, "", "", 0

    # --------------------------------------------------
    # Consume (Dequeue)
    # --------------------------------------------------
    async def create_consumer(
        self, topic: str, consumer_id: str, group_id: str = None
    ) -> AIOKafkaConsumer:
        """Create and return a Kafka consumer for a topic"""
        group = group_id or config.kafka_consumer_group
        consumer_key = f"{topic}:{consumer_id}"

        if consumer_key in self._consumers:
            return self._consumers[consumer_key]

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,   # Manual commit = at-least-once
            value_deserializer=lambda v: v,
            key_deserializer=lambda k: k.decode() if k else None,
            fetch_max_wait_ms=500,
            max_poll_records=100,
        )
        await consumer.start()
        self._consumers[consumer_key] = consumer
        logger.info(f"[QueueNode:{self.node_id}] Consumer created: {consumer_key} (group={group})")
        return consumer

    async def dequeue(
        self, topic: str, consumer_id: str, group_id: str = None, max_messages: int = 10
    ) -> List[Message]:
        """
        Dequeue messages from the queue.
        Messages must be acknowledged to advance the offset.
        """
        consumer = await self.create_consumer(topic, consumer_id, group_id)
        messages = []

        try:
            batch = await asyncio.wait_for(
                consumer.getmany(timeout_ms=1000, max_records=max_messages),
                timeout=2.0
            )
            for tp, records in batch.items():
                for record in records:
                    headers = {k: v.decode() for k, v in record.headers}
                    msg = Message(
                        message_id=headers.get("message_id", str(uuid.uuid4())),
                        topic=topic,
                        partition_key=headers.get("partition_key", ""),
                        payload=record.value,
                        headers=headers,
                        created_at=float(headers.get("created_at", time.time())),
                    )
                    messages.append(msg)
                    self._pending_acks[msg.message_id] = (consumer, record)

                    age = time.time() - msg.created_at
                    QUEUE_MESSAGE_AGE.labels(node_id=self.node_id, topic=topic).observe(age)
                    QUEUE_DEQUEUE_TOTAL.labels(
                        node_id=self.node_id, topic=topic, consumer_group=group_id or config.kafka_consumer_group
                    ).inc()
                    self._throughput_counter += 1

        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"[QueueNode:{self.node_id}] Dequeue error: {e}")

        return messages

    async def acknowledge(self, message_id: str, success: bool = True) -> bool:
        """
        Acknowledge (commit) or NACK (retry) a consumed message.
        At-least-once: on NACK, message will be re-delivered.
        """
        if message_id not in self._pending_acks:
            return False

        consumer, record = self._pending_acks.pop(message_id)
        QUEUE_SIZE.labels(
            node_id=self.node_id, topic=record.topic, partition=str(record.partition)
        ).dec()

        if success:
            # Commit offset — message is processed
            await consumer.commit({
                consumer.assignment(): record.offset + 1
            })
        else:
            # NACK: send to DLQ after max retries
            headers = {k: v.decode() for k, v in record.headers}
            retry_count = int(headers.get("retry_count", 0)) + 1
            if retry_count >= self.MAX_RETRIES:
                logger.warning(f"[QueueNode] Message {message_id} exceeded retries → DLQ")
                await self._send_to_dlq(record, retry_count)
            else:
                # Re-enqueue with incremented retry count
                await self._producer.send(
                    record.topic,
                    key=record.key,
                    value=record.value,
                    headers=record.headers + [("retry_count", str(retry_count).encode())]
                )
        return True

    async def _send_to_dlq(self, record, retry_count: int):
        """Send failed message to Dead Letter Queue"""
        await self._producer.send_and_wait(
            TOPIC_DLQ,
            key=record.key,
            value=record.value,
            headers=record.headers + [
                ("dlq_reason", b"max_retries_exceeded"),
                ("original_topic", record.topic.encode()),
                ("retry_count", str(retry_count).encode()),
            ]
        )

    # --------------------------------------------------
    # Queue Stats
    # --------------------------------------------------
    async def get_queue_stats(self, topic: str = TOPIC_TASK_QUEUE) -> Dict:
        """Get queue statistics"""
        try:
            async with self.db_session() as session:
                result = await session.execute(
                    text("""
                        SELECT status, COUNT(*) as cnt
                        FROM queue_messages
                        WHERE topic = :topic
                        GROUP BY status
                    """),
                    {"topic": topic}
                )
                stats = {row[0]: row[1] for row in result.fetchall()}

            elapsed = time.monotonic() - self._throughput_reset_time
            tps = self._throughput_counter / elapsed if elapsed > 0 else 0

            return {
                "topic": topic,
                "pending":    stats.get("pending", 0),
                "processing": stats.get("processing", 0),
                "completed":  stats.get("completed", 0),
                "failed":     stats.get("failed", 0),
                "dead_letter":stats.get("dead_letter", 0),
                "total":      sum(stats.values()),
                "throughput_rps": round(tps, 2),
                "responsible_node": self.hash_ring.get_node(topic),
                "ring_nodes": self.hash_ring.get_all_nodes(),
            }
        except Exception as e:
            logger.error(f"[QueueNode] Stats error: {e}")
            return {}

    def get_node_for_key(self, partition_key: str) -> Tuple[Optional[str], int]:
        """Get responsible node and partition for a key"""
        node = self.hash_ring.get_node(partition_key)
        partition = self.hash_ring.get_partition(partition_key, config.kafka_partitions)
        return node, partition

    # --------------------------------------------------
    # Persistence
    # --------------------------------------------------
    async def _persist_message(
        self, message_id: str, topic: str, partition_key: str,
        node_id: str, payload: bytes
    ):
        try:
            async with self.db_session() as session:
                await session.execute(
                    text("""
                        INSERT IGNORE INTO queue_messages
                            (message_id, topic, partition_key, node_id, payload, status)
                        VALUES (:mid, :topic, :pk, :node, :payload, 'pending')
                    """),
                    {
                        "mid": message_id, "topic": topic,
                        "pk": partition_key, "node": node_id,
                        "payload": json.dumps({"data": payload.decode(errors="replace")}),
                    }
                )
                await session.commit()
        except Exception as e:
            logger.warning(f"[QueueNode] Persist message error: {e}")

    # --------------------------------------------------
    # Throughput Reporter
    # --------------------------------------------------
    async def _throughput_reporter(self):
        while True:
            await asyncio.sleep(10)
            elapsed = time.monotonic() - self._throughput_reset_time
            tps = self._throughput_counter / elapsed if elapsed > 0 else 0
            QUEUE_THROUGHPUT.labels(node_id=self.node_id, topic=TOPIC_TASK_QUEUE).set(tps)
            self._throughput_counter = 0
            self._throughput_reset_time = time.monotonic()
