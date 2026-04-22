"""
Main Execution Entrypoint for Distributed Node
"""
import asyncio
import logging
import signal
import sys

from src.nodes.unified_node import SyncNode
from src.utils.config import config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def run_node():
    logger.info(f"Starting Node ID: {config.node_id} (Region: {config.geo_region})")
    
    node = SyncNode()
    
    # Graceful shutdown handler
    def handle_sigint():
        logger.info("Received SIGINT, initiating shutdown...")
        # Add shutdown task
        asyncio.create_task(shutdown(node))

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_sigint)
        
    try:
        await node.start()
        # Keep running
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Node crashed: {e}", exc_info=True)
    finally:
        await node.stop()

async def shutdown(node):
    await node.stop()
    asyncio.get_event_loop().stop()

if __name__ == "__main__":
    try:
        asyncio.run(run_node())
    except KeyboardInterrupt:
        pass
