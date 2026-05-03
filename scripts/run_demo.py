import asyncio
import json
import logging
from time import time
import grpc
from src.proto.generated import node_pb2, node_pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(message)s")

async def run_demo():
    print("="*60)
    print("🚀 MEMULAI DEMO DISTRIBUTED SYNC SYSTEM")
    print("="*60)

    # Koneksi tanpa TLS (Untuk sekadar tes lokal cepat ke Node 1)
    channel = grpc.aio.insecure_channel("localhost:50051")
    
    lock_stub = node_pb2_grpc.LockServiceStub(channel)
    cache_stub = node_pb2_grpc.CacheServiceStub(channel)
    queue_stub = node_pb2_grpc.QueueServiceStub(channel)
    geo_stub = node_pb2_grpc.GeoServiceStub(channel)

    print("\n[1] MENGUJI DISTRIBUTED LOCK (RAFT)")
    try:
        resp = await lock_stub.AcquireLock(node_pb2.LockRequest(
            lock_id="resource-x", 
            lock_type=node_pb2.LOCK_EXCLUSIVE,
            client_id="demo_client_1",
            timeout_ms=5000, lease_ms=10000
        ))
        print(f"✅ Acquire Lock: {resp.message} | Token: {resp.lease_token}")
        
        # Test the Wait Queue (Should timeout or wait because it's exclusive)
        print("⏳ Mencoba lock yang sama dengan client lain (mensimulasikan antrean/deadlock defense)...")
        resp2 = await lock_stub.AcquireLock(node_pb2.LockRequest(
            lock_id="resource-x", 
            lock_type=node_pb2.LOCK_EXCLUSIVE,
            client_id="demo_client_2",
            timeout_ms=2000, lease_ms=10000
        ))
        print(f"⚠️ Respon Client 2: {resp2.message}")

        resp3 = await lock_stub.ReleaseLock(node_pb2.ReleaseRequest(
            lock_id="resource-x", client_id="demo_client_1", lease_token=resp.lease_token
        ))
        print(f"✅ Release Lock: {resp3.message}")
    except Exception as e:
        print(f"❌ Lock fail: {e}")

    print("\n[2] MENGUJI CACHE COHERENCE (MOESI)")
    try:
        resp = await cache_stub.ReadCache(node_pb2.CacheReadRequest(
            requester_id="demo_client", cache_key="user_123"
        ))
        print(f"✅ Cache Read (Awal): Miss -> Fetches Empty. State returned.")
    except Exception as e:
        print(f"❌ Cache fail: {e}")

    print("\n[3] MENGUJI DISTRIBUTED QUEUE (KAFKA Hashing)")
    try:
        resp = await queue_stub.Enqueue(node_pb2.EnqueueRequest(
            topic="task-queue",
            partition_key="user_88",
            payload=b'{"task_type": "send_email", "uid": 88}',
            producer_id="demo_client"
        ))
        print(f"✅ Enqueue sukses! Node Penanggung Jawab (Hash Ring): {resp.responsible_node} (Partisi: {resp.partition})")
    except Exception as e:
        print(f"❌ Queue fail: {e}")

    print("\n[4] MENGUJI GEO-DISTRIBUTED (LATENCY ROUTING)")
    try:
        req_start = time()
        resp = await geo_stub.RouteRequest(node_pb2.GeoRouteRequest(
            client_region="eu",  # Kita minta routing dari Eropa
            operation_type="read",
            payload=json.dumps({"data": "hello"}).encode()
        ))
        elapsed = time() - req_start
        print(f"✅ Route Request Selesai!")
        print(f"   - Wilayah Klien: EU")
        print(f"   - Wilayah Target Terpilih: {resp.target_region}")
        print(f"   - Simulasi Latensi (Via Vektor): {resp.estimated_latency_ms}ms")
        print(f"   - Waktu Respon Real: {elapsed*1000:.1f}ms")
    except Exception as e:
        print(f"❌ Geo fail: {e}")

    await channel.close()
    print("\n" + "="*60)
    print("✅ DEMO SELESAI")
    print("Melihat metrik ML & PBFT dapat dilakukan melalui:")
    print("docker logs sister-tugas3-node1-1")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(run_demo())
