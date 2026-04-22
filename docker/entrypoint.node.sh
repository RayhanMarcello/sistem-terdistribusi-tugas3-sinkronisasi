#!/bin/bash
# =============================================================
# Node Entrypoint Script
# 1. Generate TLS certificates
# 2. Wait for dependencies
# 3. Start node service
# =============================================================
set -euo pipefail

echo "🚀 Starting Distributed Sync Node: ${NODE_ID:-node1}"

# Generate TLS certificates
if [ "${TLS_ENABLED:-true}" = "true" ]; then
    echo "🔐 Setting up TLS certificates..."
    bash /app/scripts/generate_certs.sh
fi

# Wait for MySQL to be ready
echo "⏳ Waiting for MySQL..."
until python -c "
import pymysql, os, sys
try:
    conn = pymysql.connect(
        host=os.environ.get('MYSQL_HOST', 'mysql'),
        port=int(os.environ.get('MYSQL_PORT', 3306)),
        user=os.environ.get('MYSQL_USER', 'distuser'),
        password=os.environ.get('MYSQL_PASSWORD', 'distpass123'),
        database=os.environ.get('MYSQL_DATABASE', 'distributed_sync'),
        connect_timeout=5
    )
    conn.close()
    sys.exit(0)
except Exception as e:
    print(f'MySQL not ready: {e}')
    sys.exit(1)
" 2>/dev/null; do
    echo "⏳ MySQL not ready, waiting 3s..."
    sleep 3
done
echo "✅ MySQL is ready"

# Wait for Redis
echo "⏳ Waiting for Redis..."
until python -c "
import redis, os, sys
try:
    r = redis.Redis(
        host=os.environ.get('REDIS_HOST', 'redis'),
        port=int(os.environ.get('REDIS_PORT', 6379)),
        password=os.environ.get('REDIS_PASSWORD', ''),
        socket_connect_timeout=5
    )
    r.ping()
    sys.exit(0)
except Exception as e:
    print(f'Redis not ready: {e}')
    sys.exit(1)
" 2>/dev/null; do
    echo "⏳ Redis not ready, waiting 3s..."
    sleep 3
done
echo "✅ Redis is ready"

# Wait for Kafka
echo "⏳ Waiting for Kafka..."
until python -c "
from kafka import KafkaAdminClient
import os, sys
try:
    admin = KafkaAdminClient(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
        request_timeout_ms=5000
    )
    admin.close()
    sys.exit(0)
except Exception as e:
    print(f'Kafka not ready: {e}')
    sys.exit(1)
" 2>/dev/null; do
    echo "⏳ Kafka not ready, waiting 3s..."
    sleep 3
done
echo "✅ Kafka is ready"

echo "🎯 Starting node service..."
exec python -m src.main
