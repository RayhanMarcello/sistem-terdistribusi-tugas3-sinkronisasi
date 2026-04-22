#!/bin/bash
# =============================================================
# TLS Certificate Generation Script
# Auto-generates self-signed CA and per-node certificates
# for mTLS gRPC communication
# =============================================================

set -euo pipefail

CERT_DIR="${CERT_DIR:-/app/certs}"
DAYS="${CERT_VALIDITY_DAYS:-3650}"
NODE_ID="${NODE_ID:-node1}"
CLUSTER_NODES="${CLUSTER_NODES:-node1:node1:50051,node2:node2:50051,node3:node3:50051}"

echo "🔐 Generating TLS certificates in: $CERT_DIR"
mkdir -p "$CERT_DIR"

# Skip if CA already exists
if [ -f "$CERT_DIR/ca.crt" ] && [ -f "$CERT_DIR/ca.key" ]; then
    echo "✅ CA certificate already exists, skipping generation"
else
    echo "📜 Generating Certificate Authority (CA)..."
    openssl genrsa -out "$CERT_DIR/ca.key" 4096 2>/dev/null
    openssl req -new -x509 -days "$DAYS" \
        -key "$CERT_DIR/ca.key" \
        -out "$CERT_DIR/ca.crt" \
        -subj "/C=ID/ST=Jakarta/O=DistributedSync/CN=DistributedSyncCA" 2>/dev/null
    echo "✅ CA certificate generated"
fi

# Generate node certificate
NODE_CERT="$CERT_DIR/${NODE_ID}.crt"
NODE_KEY="$CERT_DIR/${NODE_ID}.key"

if [ -f "$NODE_CERT" ] && [ -f "$NODE_KEY" ]; then
    echo "✅ Node certificate for $NODE_ID already exists"
else
    echo "📜 Generating certificate for node: $NODE_ID..."

    # Build SAN (Subject Alternative Names) from cluster nodes
    SAN_LIST="DNS:localhost,IP:127.0.0.1"
    IFS=',' read -ra NODES <<< "$CLUSTER_NODES"
    for NODE in "${NODES[@]}"; do
        NODE_NAME=$(echo "$NODE" | cut -d':' -f1)
        NODE_HOST=$(echo "$NODE" | cut -d':' -f2)
        SAN_LIST="$SAN_LIST,DNS:$NODE_NAME,DNS:$NODE_HOST"
    done

    # Create extension config
    cat > "$CERT_DIR/${NODE_ID}.ext" << EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name

[req_distinguished_name]

[v3_req]
subjectAltName = $SAN_LIST
keyUsage = keyEncipherment, dataEncipherment, digitalSignature
extendedKeyUsage = serverAuth, clientAuth
EOF

    openssl genrsa -out "$NODE_KEY" 2048 2>/dev/null
    openssl req -new \
        -key "$NODE_KEY" \
        -out "$CERT_DIR/${NODE_ID}.csr" \
        -subj "/C=ID/ST=Jakarta/O=DistributedSync/CN=$NODE_ID" 2>/dev/null

    openssl x509 -req -days "$DAYS" \
        -in "$CERT_DIR/${NODE_ID}.csr" \
        -CA "$CERT_DIR/ca.crt" \
        -CAkey "$CERT_DIR/ca.key" \
        -CAcreateserial \
        -out "$NODE_CERT" \
        -extfile "$CERT_DIR/${NODE_ID}.ext" \
        -extensions v3_req 2>/dev/null

    rm -f "$CERT_DIR/${NODE_ID}.csr" "$CERT_DIR/${NODE_ID}.ext"
    echo "✅ Certificate for $NODE_ID generated"
fi

# Set permissions
chmod 600 "$CERT_DIR"/*.key 2>/dev/null || true
chmod 644 "$CERT_DIR"/*.crt 2>/dev/null || true

echo "🎉 TLS setup complete!"
echo "   CA:   $CERT_DIR/ca.crt"
echo "   Node: $NODE_CERT"
echo "   Key:  $NODE_KEY"
