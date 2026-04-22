"""
Distributed Sync System - Configuration Management
Loads all settings from environment variables with sensible defaults.
"""
import os
from dataclasses import dataclass, field
from typing import List, Tuple
from dotenv import load_dotenv

load_dotenv()


def _parse_cluster_nodes(val: str) -> List[Tuple[str, str, int]]:
    """Parse CLUSTER_NODES=node1:host1:port1,node2:host2:port2"""
    nodes = []
    for item in val.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 3:
            node_id, host, port = parts
            nodes.append((node_id.strip(), host.strip(), int(port.strip())))
        elif len(parts) == 2:
            node_id, port = parts
            nodes.append((node_id.strip(), node_id.strip(), int(port.strip())))
    return nodes


@dataclass
class Config:
    # Node
    node_id: str = field(default_factory=lambda: os.getenv("NODE_ID", "node1"))
    node_host: str = field(default_factory=lambda: os.getenv("NODE_HOST", "0.0.0.0"))
    grpc_port: int = field(default_factory=lambda: int(os.getenv("GRPC_PORT", "50051")))
    rest_port: int = field(default_factory=lambda: int(os.getenv("REST_PORT", "8000")))
    metrics_port: int = field(default_factory=lambda: int(os.getenv("METRICS_PORT", "9091")))
    geo_region: str = field(default_factory=lambda: os.getenv("GEO_REGION", "us"))
    is_geo_node: bool = field(default_factory=lambda: os.getenv("IS_GEO_NODE", "false").lower() == "true")

    # Cluster
    cluster_nodes_raw: str = field(
        default_factory=lambda: os.getenv(
            "CLUSTER_NODES",
            "node1:node1:50051,node2:node2:50051,node3:node3:50051"
        )
    )

    @property
    def cluster_nodes(self) -> List[Tuple[str, str, int]]:
        return _parse_cluster_nodes(self.cluster_nodes_raw)

    @property
    def peer_nodes(self) -> List[Tuple[str, str, int]]:
        return [(nid, h, p) for nid, h, p in self.cluster_nodes if nid != self.node_id]

    # Redis
    redis_host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "redis"))
    redis_port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    redis_password: str = field(default_factory=lambda: os.getenv("REDIS_PASSWORD", "redispass123"))
    redis_db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # MySQL
    mysql_host: str = field(default_factory=lambda: os.getenv("MYSQL_HOST", "mysql"))
    mysql_port: int = field(default_factory=lambda: int(os.getenv("MYSQL_PORT", "3306")))
    mysql_database: str = field(default_factory=lambda: os.getenv("MYSQL_DATABASE", "distributed_sync"))
    mysql_user: str = field(default_factory=lambda: os.getenv("MYSQL_USER", "distuser"))
    mysql_password: str = field(default_factory=lambda: os.getenv("MYSQL_PASSWORD", "distpass123"))

    @property
    def mysql_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )

    @property
    def mysql_url_sync(self) -> str:
        return (
            f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )

    # Kafka
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    kafka_consumer_group: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "dist-sync-group")
    )
    kafka_partitions: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_PARTITIONS", "6"))
    )
    kafka_replication_factor: int = field(
        default_factory=lambda: int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    )

    # Security
    tls_enabled: bool = field(
        default_factory=lambda: os.getenv("TLS_ENABLED", "true").lower() == "true"
    )
    cert_dir: str = field(default_factory=lambda: os.getenv("CERT_DIR", "/app/certs"))
    jwt_secret: str = field(
        default_factory=lambda: os.getenv("JWT_SECRET", "dev-secret-key-change-in-prod")
    )
    jwt_expiry: int = field(default_factory=lambda: int(os.getenv("JWT_EXPIRY", "3600")))

    # Lock Manager
    lock_timeout: int = field(default_factory=lambda: int(os.getenv("LOCK_TIMEOUT", "30")))
    deadlock_detection_interval: int = field(
        default_factory=lambda: int(os.getenv("DEADLOCK_DETECTION_INTERVAL", "5"))
    )

    # Cache (MOESI)
    cache_max_size: int = field(
        default_factory=lambda: int(os.getenv("CACHE_MAX_SIZE", "1000"))
    )
    cache_policy: str = field(default_factory=lambda: os.getenv("CACHE_POLICY", "lru"))

    # Raft
    raft_election_timeout_min: int = field(
        default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150"))
    )
    raft_election_timeout_max: int = field(
        default_factory=lambda: int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300"))
    )
    raft_heartbeat_interval: int = field(
        default_factory=lambda: int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50"))
    )

    # ML
    ml_model_path: str = field(
        default_factory=lambda: os.getenv("ML_MODEL_PATH", "/app/models/load_balancer.pkl")
    )
    ml_retrain_interval: int = field(
        default_factory=lambda: int(os.getenv("ML_RETRAIN_INTERVAL", "100"))
    )
    ml_training_samples: int = field(
        default_factory=lambda: int(os.getenv("ML_TRAINING_SAMPLES", "1000"))
    )

    # Geo
    geo_latency_us_eu: int = field(
        default_factory=lambda: int(os.getenv("GEO_LATENCY_US_EU", "100"))
    )
    geo_latency_us_asia: int = field(
        default_factory=lambda: int(os.getenv("GEO_LATENCY_US_ASIA", "150"))
    )
    geo_latency_eu_asia: int = field(
        default_factory=lambda: int(os.getenv("GEO_LATENCY_EU_ASIA", "120"))
    )

    # Logging
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_format: str = field(default_factory=lambda: os.getenv("LOG_FORMAT", "json"))

    @property
    def tls_cert_file(self) -> str:
        return os.path.join(self.cert_dir, f"{self.node_id}.crt")

    @property
    def tls_key_file(self) -> str:
        return os.path.join(self.cert_dir, f"{self.node_id}.key")

    @property
    def tls_ca_file(self) -> str:
        return os.path.join(self.cert_dir, "ca.crt")

    def get_geo_latency(self, from_region: str, to_region: str) -> int:
        """Get simulated latency between regions in ms"""
        pair = tuple(sorted([from_region, to_region]))
        latency_map = {
            ("eu", "us"): self.geo_latency_us_eu,
            ("asia", "us"): self.geo_latency_us_asia,
            ("asia", "eu"): self.geo_latency_eu_asia,
        }
        return latency_map.get(pair, 10)  # 10ms default (same region)


# Singleton config instance
config = Config()
