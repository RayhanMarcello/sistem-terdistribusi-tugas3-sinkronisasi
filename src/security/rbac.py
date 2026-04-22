"""
Security - Role-Based Access Control and Audit Logging (Bonus D)
  - Pre-defined roles: Admin, ReadWrite, ReadOnly
  - Token-based verification
  - Async Audit logging to MySQL via Kafka
"""
import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

from aiokafka import AIOKafkaProducer
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.config import config

logger = logging.getLogger(__name__)


class Role(str, Enum):
    ADMIN = "admin"
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"


@dataclass
class User:
    username: str
    role: Role
    token: str
    active: bool = True


class RBACManager:
    """Role-Based Access Control Manager"""
    
    def __init__(self, db_session_maker):
        self._db_session = db_session_maker
        self._users: Dict[str, User] = {}
        # Pre-populate built-in admin for initial access
        self._users["admin-token-123"] = User(
            username="system_admin",
            role=Role.ADMIN,
            token="admin-token-123",
        )

    async def initialize(self):
        """Load users from database"""
        try:
            async with self._db_session() as session:
                result = await session.execute(text("SELECT username, role, token FROM rbac_users WHERE is_active = 1"))
                for row in result.fetchall():
                    self._users[row[2]] = User(username=row[0], role=Role(row[1]), token=row[2])
            logger.info(f"[RBAC] Initialized with {len(self._users)} users from DB")
        except Exception as e:
            logger.warning(f"[RBAC] DB init failed (using defaults): {e}")

    def verify_token(self, token: str) -> Optional[User]:
        return self._users.get(token)

    def check_permission(self, user: User, required_role: Role) -> bool:
        if not user.active:
            return False
        if user.role == Role.ADMIN:
            return True
        if required_role == Role.READ_ONLY and user.role in (Role.READ_WRITE, Role.READ_ONLY):
            return True
        if required_role == Role.READ_WRITE and user.role == Role.READ_WRITE:
            return True
        return False


class AuditLogger:
    """Async Audit Logger leveraging Kafka"""
    
    def __init__(self, kafka_producer: Optional[AIOKafkaProducer] = None):
        self._producer = kafka_producer
        
    async def log_event(
        self,
        node_id: str,
        event_type: str,
        user_id: str,
        resource: str,
        action: str,
        status: str,
        details: Dict = None
    ):
        """Log event to Kafka audit topic for persistence"""
        if not self._producer:
            return
            
        payload = {
            "log_id": str(uuid.uuid4()),
            "node_id": node_id,
            "timestamp": time.time(),
            "event_type": event_type,
            "user_id": user_id,
            "resource": resource,
            "action": action,
            "status": status,
            "details": details or {}
        }
        
        try:
            await self._producer.send(
                "audit-log",
                value=json.dumps(payload).encode(),
                key=node_id.encode()
            )
        except Exception as e:
            logger.error(f"[AuditLogger] Failed to send audit log: {e}")
