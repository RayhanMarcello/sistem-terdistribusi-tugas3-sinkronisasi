"""
Distributed Sync System - Prometheus Metrics Collection
Provides metrics for all components: lock, cache, queue, raft, PBFT
"""
import time
from functools import wraps
from typing import Optional
from prometheus_client import (
    Counter, Gauge, Histogram, Summary, Info,
    CollectorRegistry, push_to_gateway, start_http_server,
    CONTENT_TYPE_LATEST, generate_latest
)

# -----------------------------------------------
# Registry & Node Info
# -----------------------------------------------
REGISTRY = CollectorRegistry(auto_describe=True)

NODE_INFO = Info(
    'dist_node',
    'Distributed node information',
    registry=REGISTRY
)

# -----------------------------------------------
# Raft Consensus Metrics
# -----------------------------------------------
RAFT_TERM = Gauge(
    'raft_current_term',
    'Current Raft term number',
    ['node_id'],
    registry=REGISTRY
)

RAFT_ROLE = Gauge(
    'raft_role',
    'Current Raft role (1=leader, 2=candidate, 3=follower)',
    ['node_id'],
    registry=REGISTRY
)

RAFT_VOTES = Counter(
    'raft_votes_total',
    'Total votes granted/received',
    ['node_id', 'direction'],  # direction: granted | received
    registry=REGISTRY
)

RAFT_LOG_SIZE = Gauge(
    'raft_log_entries_total',
    'Total log entries in Raft log',
    ['node_id'],
    registry=REGISTRY
)

RAFT_COMMIT_INDEX = Gauge(
    'raft_commit_index',
    'Raft commit index',
    ['node_id'],
    registry=REGISTRY
)

RAFT_LEADER_CHANGES = Counter(
    'raft_leader_changes_total',
    'Total number of leader elections',
    ['node_id'],
    registry=REGISTRY
)

RAFT_RPC_DURATION = Histogram(
    'raft_rpc_duration_seconds',
    'Duration of Raft RPC calls',
    ['node_id', 'rpc_type'],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5],
    registry=REGISTRY
)

# -----------------------------------------------
# Lock Manager Metrics
# -----------------------------------------------
LOCK_ACQUIRE_TOTAL = Counter(
    'lock_acquire_total',
    'Total lock acquisition attempts',
    ['node_id', 'lock_type', 'result'],  # result: success | failed | timeout
    registry=REGISTRY
)

LOCK_RELEASE_TOTAL = Counter(
    'lock_release_total',
    'Total lock releases',
    ['node_id', 'lock_type'],
    registry=REGISTRY
)

LOCK_ACTIVE_COUNT = Gauge(
    'lock_active_count',
    'Number of currently active locks',
    ['node_id', 'lock_type'],
    registry=REGISTRY
)

LOCK_WAIT_QUEUE = Gauge(
    'lock_wait_queue_size',
    'Number of clients waiting for locks',
    ['node_id'],
    registry=REGISTRY
)

LOCK_DURATION = Histogram(
    'lock_held_duration_seconds',
    'Duration locks are held',
    ['node_id', 'lock_type'],
    buckets=[.1, .5, 1, 2.5, 5, 10, 30, 60, 120, 300],
    registry=REGISTRY
)

LOCK_ACQUIRE_LATENCY = Histogram(
    'lock_acquire_latency_seconds',
    'Time to acquire a lock',
    ['node_id', 'lock_type'],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5],
    registry=REGISTRY
)

DEADLOCK_DETECTIONS = Counter(
    'deadlock_detections_total',
    'Total deadlock detection runs and results',
    ['node_id', 'result'],  # result: detected | none
    registry=REGISTRY
)

# -----------------------------------------------
# Cache Coherence Metrics (MOESI)
# -----------------------------------------------
CACHE_HITS = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['node_id'],
    registry=REGISTRY
)

CACHE_MISSES = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['node_id'],
    registry=REGISTRY
)

CACHE_INVALIDATIONS = Counter(
    'cache_invalidations_total',
    'Total cache invalidations sent/received',
    ['node_id', 'direction'],  # sent | received
    registry=REGISTRY
)

CACHE_EVICTIONS = Counter(
    'cache_evictions_total',
    'Total cache evictions',
    ['node_id', 'policy'],
    registry=REGISTRY
)

CACHE_SIZE = Gauge(
    'cache_entries_count',
    'Current number of cache entries',
    ['node_id'],
    registry=REGISTRY
)

CACHE_STATE_DISTRIBUTION = Gauge(
    'cache_state_entries',
    'Distribution of cache line states (MOESI)',
    ['node_id', 'state'],
    registry=REGISTRY
)

CACHE_HIT_RATE = Gauge(
    'cache_hit_rate',
    'Cache hit rate (0-1)',
    ['node_id'],
    registry=REGISTRY
)

CACHE_OPERATION_LATENCY = Histogram(
    'cache_operation_duration_seconds',
    'Cache read/write latency',
    ['node_id', 'operation'],
    buckets=[.0001, .0005, .001, .005, .01, .025, .05, .1, .25],
    registry=REGISTRY
)

CACHE_STATE_TRANSITIONS = Counter(
    'cache_state_transitions_total',
    'MOESI state transitions',
    ['node_id', 'from_state', 'to_state'],
    registry=REGISTRY
)

# -----------------------------------------------
# Queue System Metrics
# -----------------------------------------------
QUEUE_ENQUEUE_TOTAL = Counter(
    'queue_enqueue_total',
    'Total messages enqueued',
    ['node_id', 'topic'],
    registry=REGISTRY
)

QUEUE_DEQUEUE_TOTAL = Counter(
    'queue_dequeue_total',
    'Total messages dequeued',
    ['node_id', 'topic', 'consumer_group'],
    registry=REGISTRY
)

QUEUE_SIZE = Gauge(
    'queue_size',
    'Current queue depth per topic/partition',
    ['node_id', 'topic', 'partition'],
    registry=REGISTRY
)

QUEUE_THROUGHPUT = Gauge(
    'queue_throughput_rps',
    'Queue messages processed per second',
    ['node_id', 'topic'],
    registry=REGISTRY
)

QUEUE_MESSAGE_AGE = Histogram(
    'queue_message_age_seconds',
    'Age of messages when consumed',
    ['node_id', 'topic'],
    buckets=[.1, .5, 1, 5, 10, 30, 60, 300, 600],
    registry=REGISTRY
)

QUEUE_REBALANCES = Counter(
    'queue_rebalances_total',
    'Total consistent hash ring rebalances',
    ['node_id'],
    registry=REGISTRY
)

# -----------------------------------------------
# PBFT Metrics (Bonus A)
# -----------------------------------------------
PBFT_ROUNDS = Counter(
    'pbft_rounds_total',
    'Total PBFT consensus rounds',
    ['node_id', 'result'],  # success | view_change | timeout
    registry=REGISTRY
)

PBFT_VIEW = Gauge(
    'pbft_current_view',
    'Current PBFT view number',
    ['node_id'],
    registry=REGISTRY
)

PBFT_IS_PRIMARY = Gauge(
    'pbft_is_primary',
    '1 if this node is PBFT primary, 0 otherwise',
    ['node_id'],
    registry=REGISTRY
)

PBFT_PHASE_DURATION = Histogram(
    'pbft_phase_duration_seconds',
    'Duration of each PBFT phase',
    ['node_id', 'phase'],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1],
    registry=REGISTRY
)

# -----------------------------------------------
# Network & Node Health Metrics
# -----------------------------------------------
GRPC_REQUESTS = Counter(
    'grpc_requests_total',
    'Total gRPC requests',
    ['node_id', 'service', 'method', 'status'],
    registry=REGISTRY
)

GRPC_LATENCY = Histogram(
    'grpc_request_duration_seconds',
    'gRPC request latency',
    ['node_id', 'service', 'method'],
    buckets=[.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5],
    registry=REGISTRY
)

NODE_UPTIME = Gauge(
    'node_uptime_seconds',
    'Node uptime in seconds',
    ['node_id'],
    registry=REGISTRY
)

NODE_CONNECTED_PEERS = Gauge(
    'node_connected_peers',
    'Number of connected peer nodes',
    ['node_id'],
    registry=REGISTRY
)

# -----------------------------------------------
# Geo-Distributed Metrics (Bonus B)
# -----------------------------------------------
GEO_REPLICATION_LATENCY = Histogram(
    'geo_replication_latency_seconds',
    'Cross-region replication latency',
    ['source_region', 'target_region'],
    buckets=[.01, .05, .1, .25, .5, 1, 2.5, 5],
    registry=REGISTRY
)

GEO_CONFLICTS = Counter(
    'geo_replication_conflicts_total',
    'Total cross-region data conflicts',
    ['region'],
    registry=REGISTRY
)

# -----------------------------------------------
# ML Metrics (Bonus C)
# -----------------------------------------------
ML_PREDICTIONS = Counter(
    'ml_predictions_total',
    'Total ML load balancer predictions',
    ['node_id', 'predicted_node'],
    registry=REGISTRY
)

ML_ACCURACY = Gauge(
    'ml_prediction_accuracy',
    'ML model prediction accuracy',
    ['node_id'],
    registry=REGISTRY
)

ML_MODEL_VERSION = Gauge(
    'ml_model_version',
    'Current ML model version/training iteration',
    ['node_id'],
    registry=REGISTRY
)


# -----------------------------------------------
# Helper: timing decorator
# -----------------------------------------------
def timed(histogram: Histogram, labels: dict):
    """Decorator to time function execution and record to histogram"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return await func(*args, **kwargs)
            finally:
                duration = time.monotonic() - start
                histogram.labels(**labels).observe(duration)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.monotonic() - start
                histogram.labels(**labels).observe(duration)

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator


def start_metrics_server(port: int, node_id: str):
    """Start Prometheus metrics HTTP server"""
    NODE_INFO.info({
        'node_id': node_id,
        'version': '1.0.0',
        'component': 'distributed-sync-node'
    })
    start_http_server(port, registry=REGISTRY)
