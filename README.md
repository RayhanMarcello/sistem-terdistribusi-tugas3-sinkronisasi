# Distributed Synchronization System

Advanced project for *Sistem Paralel dan Terdistribusi* handling concurrent lock synchronization, queued processing, cache coherence, distributed consensus, and machine learning load routing.

## Technology Stack
- **Python 3.11** (Asyncio architecture)
- **gRPC + Protobuf** for high throughput cross-node message passing (mTLS secured)
- **FastAPI** for API Gateway representation
- **MySQL 8.0** for structured persistence (Raft logs, Lock history, Security)
- **Redis & Kafka** for cache storage backing and queue streams 
- **Docker Compose** orchestrating 5+ interconnected services
- **Scikit-Learn** for ML-based load balancer models
- **Prometheus + Grafana** for detailed application monitoring

## Core Features
1. **Distributed Lock Manager**: Employs Raft Consensus for shared/exclusive locks + DFS-based deadlock resolution graph parsing.
2. **Distributed Queue System**: Implements Consistent Hashing + virtual nodes with Kafka DLQ and at-least-once deliveries.
3. **Cache Coherence**: Implements full **MOESI** cache state transitions (Modified, Owned, Exclusive, Shared, Invalid).

## Bonus Features Embedded
1. **PBFT Consensus**: Handles extreme faulty state replication through 3-phase protocols (Bonus A).
2. **Geo-Distributed Architecture**: Simulates cross-region latency tracking with Vector Clocks / Last-Writer-Wins eventual consistency algorithms (Bonus B).
3. **ML-Based Adaptive Load Balancer**: RandomForest predict/route to capable nodes; IsolationForest for node anomalies (Bonus C).
4. **Security & Fault Tolerance**: Includes automatic Phi-Accrual Failure Detector + RBAC Access & automated certificate generating + Async Audit Logging (Bonus D).

## Quickstart (Zero-Config)

System expects Docker and Docker-Compose to be present.

```bash
# 1. Rename environment sample
cp .env.example .env

# 2. Build and start full ensemble (Background)
# This spins up MySQL, Redis, Kafka, Zookeeper, Prometheus, Grafana, API Gateway, 
# Core Nodes (1,2,3), and Geo Nodes (EU, ASIA)
docker-compose up -d --build

# 3. Access Monitoring (Anonynous)
# Grafana: http://localhost:3000
```

*Docs and Mermaid diagram components located in `docs/architecture.md`.*
