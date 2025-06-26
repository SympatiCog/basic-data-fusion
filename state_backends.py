"""
State backend implementations for the StateManager system.
Provides different storage backends for scalable state management.
"""

import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
from dataclasses import dataclass

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class StateBackendConfig:
    """Configuration for state backends"""
    ttl_default: int = 3600  # 1 hour default TTL
    max_key_size: int = 1000
    max_value_size: int = 10 * 1024 * 1024  # 10MB
    enable_compression: bool = True


class StateBackend(ABC):
    """Abstract base class for state storage backends"""
    
    def __init__(self, config: Optional[StateBackendConfig] = None):
        self.config = config or StateBackendConfig()
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Retrieve value by key"""
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Store value with optional TTL"""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete key"""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        pass
    
    @abstractmethod
    def clear(self) -> bool:
        """Clear all data (for testing)"""
        pass
    
    def _validate_key(self, key: str) -> bool:
        """Validate key format and size"""
        if not key or len(key) > self.config.max_key_size:
            return False
        return True
    
    def _serialize_value(self, value: Any) -> str:
        """Serialize value to JSON string"""
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize value: {e}")
            raise
    
    def _deserialize_value(self, value_str: str) -> Any:
        """Deserialize JSON string to value"""
        try:
            return json.loads(value_str)
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to deserialize value: {e}")
            return None


class ClientStateBackend(StateBackend):
    """
    Client-side state backend that preserves existing dcc.Store behavior.
    Returns special signals to indicate client-side management.
    """
    
    def get(self, key: str) -> str:
        """Return signal indicating client-side management"""
        return "CLIENT_MANAGED"
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Client-side storage is handled by Dash components"""
        return True
    
    def delete(self, key: str) -> bool:
        """Client-side deletion handled by Dash"""
        return True
    
    def exists(self, key: str) -> bool:
        """Assume client-side keys exist"""
        return True
    
    def clear(self) -> bool:
        """Client-side clear not applicable"""
        return True


class MemoryStateBackend(StateBackend):
    """
    In-memory state backend for development and testing.
    Thread-safe with automatic TTL cleanup.
    """
    
    def __init__(self, config: Optional[StateBackendConfig] = None):
        super().__init__(config)
        self._store: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._cleanup_thread = None
        self._start_cleanup_thread()
    
    def get(self, key: str) -> Optional[Any]:
        if not self._validate_key(key):
            logger.warning(f"Invalid key: {key}")
            return None
        
        with self._lock:
            if key not in self._store:
                return None
            
            entry = self._store[key]
            
            # Check TTL
            if entry.get('expires_at') and time.time() > entry['expires_at']:
                del self._store[key]
                return None
            
            return entry['value']
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self._validate_key(key):
            logger.warning(f"Invalid key: {key}")
            return False
        
        # Validate value size
        try:
            serialized = self._serialize_value(value)
            if len(serialized) > self.config.max_value_size:
                logger.warning(f"Value too large for key {key}")
                return False
        except Exception as e:
            logger.error(f"Failed to serialize value for key {key}: {e}")
            return False
        
        with self._lock:
            expires_at = None
            if ttl:
                expires_at = time.time() + ttl
            elif self.config.ttl_default:
                expires_at = time.time() + self.config.ttl_default
            
            self._store[key] = {
                'value': value,
                'expires_at': expires_at,
                'created_at': time.time()
            }
        
        return True
    
    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False
    
    def exists(self, key: str) -> bool:
        with self._lock:
            if key not in self._store:
                return False
            
            entry = self._store[key]
            
            # Check TTL
            if entry.get('expires_at') and time.time() > entry['expires_at']:
                del self._store[key]
                return False
            
            return True
    
    def clear(self) -> bool:
        with self._lock:
            self._store.clear()
        return True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics"""
        with self._lock:
            total_keys = len(self._store)
            expired_keys = 0
            total_size = 0
            
            current_time = time.time()
            for entry in self._store.values():
                if entry.get('expires_at') and current_time > entry['expires_at']:
                    expired_keys += 1
                try:
                    total_size += len(self._serialize_value(entry['value']))
                except:
                    pass
            
            return {
                'total_keys': total_keys,
                'expired_keys': expired_keys,
                'total_size_bytes': total_size,
                'backend_type': 'memory'
            }
    
    def _start_cleanup_thread(self):
        """Start background thread for TTL cleanup"""
        def cleanup():
            while True:
                try:
                    current_time = time.time()
                    with self._lock:
                        expired_keys = [
                            key for key, entry in self._store.items()
                            if entry.get('expires_at') and current_time > entry['expires_at']
                        ]
                        for key in expired_keys:
                            del self._store[key]
                    
                    if expired_keys:
                        logger.debug(f"Cleaned up {len(expired_keys)} expired keys")
                    
                    time.sleep(60)  # Cleanup every minute
                except Exception as e:
                    logger.error(f"Error in cleanup thread: {e}")
                    time.sleep(60)
        
        self._cleanup_thread = threading.Thread(target=cleanup, daemon=True)
        self._cleanup_thread.start()


class RedisStateBackend(StateBackend):
    """
    Redis-based state backend for production multi-user environments.
    Requires redis-py package.
    """
    
    def __init__(self, config: Optional[StateBackendConfig] = None, 
                 redis_url: str = "redis://localhost:6379/0"):
        super().__init__(config)
        self.redis_url = redis_url
        self._client = None
        self._connect()
    
    def _connect(self):
        """Initialize Redis connection"""
        try:
            import redis
            self._client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            self._client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except ImportError:
            raise ImportError("redis package required for RedisStateBackend")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def get(self, key: str) -> Optional[Any]:
        if not self._validate_key(key):
            return None
        
        try:
            value_str = self._client.get(key)
            if value_str is None:
                return None
            return self._deserialize_value(value_str)
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self._validate_key(key):
            return False
        
        try:
            value_str = self._serialize_value(value)
            if len(value_str) > self.config.max_value_size:
                logger.warning(f"Value too large for key {key}")
                return False
            
            ttl_seconds = ttl or self.config.ttl_default
            if ttl_seconds:
                return self._client.setex(key, ttl_seconds, value_str)
            else:
                return self._client.set(key, value_str)
        except Exception as e:
            logger.error(f"Redis set error for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        try:
            return bool(self._client.delete(key))
        except Exception as e:
            logger.error(f"Redis delete error for key {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        try:
            return bool(self._client.exists(key))
        except Exception as e:
            logger.error(f"Redis exists error for key {key}: {e}")
            return False
    
    def clear(self) -> bool:
        """Clear all keys (use with caution)"""
        try:
            return bool(self._client.flushdb())
        except Exception as e:
            logger.error(f"Redis clear error: {e}")
            return False


class DatabaseStateBackend(StateBackend):
    """
    Database-based state backend using SQLite/PostgreSQL.
    For persistent, queryable state storage.
    """
    
    def __init__(self, config: Optional[StateBackendConfig] = None,
                 db_url: str = "sqlite:///state.db"):
        super().__init__(config)
        self.db_url = db_url
        self._engine = None
        self._connect()
    
    def _connect(self):
        """Initialize database connection"""
        try:
            from sqlalchemy import create_engine, MetaData, Table, Column, String, Text, DateTime, text
            from sqlalchemy.sql import select, insert, update, delete
            import datetime
            
            self._engine = create_engine(self.db_url)
            self._metadata = MetaData()
            
            # Define state table
            self._state_table = Table('state_store', self._metadata,
                Column('key', String(1000), primary_key=True),
                Column('value', Text),
                Column('created_at', DateTime, default=datetime.datetime.utcnow),
                Column('expires_at', DateTime, nullable=True)
            )
            
            # Create table if it doesn't exist
            self._metadata.create_all(self._engine)
            
            logger.info(f"Connected to database at {self.db_url}")
        except ImportError:
            raise ImportError("sqlalchemy package required for DatabaseStateBackend")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def get(self, key: str) -> Optional[Any]:
        if not self._validate_key(key):
            return None
        
        try:
            from sqlalchemy.sql import select, text
            import datetime
            
            with self._engine.connect() as conn:
                # Check if key exists and not expired
                stmt = select(self._state_table.c.value).where(
                    self._state_table.c.key == key
                ).where(
                    (self._state_table.c.expires_at.is_(None)) |
                    (self._state_table.c.expires_at > datetime.datetime.utcnow())
                )
                
                result = conn.execute(stmt).fetchone()
                if result:
                    return self._deserialize_value(result[0])
                return None
        except Exception as e:
            logger.error(f"Database get error for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self._validate_key(key):
            return False
        
        try:
            from sqlalchemy.sql import insert
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            import datetime
            
            value_str = self._serialize_value(value)
            if len(value_str) > self.config.max_value_size:
                logger.warning(f"Value too large for key {key}")
                return False
            
            expires_at = None
            if ttl:
                expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=ttl)
            elif self.config.ttl_default:
                expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.config.ttl_default)
            
            with self._engine.connect() as conn:
                # Use upsert for both SQLite and PostgreSQL
                if 'sqlite' in self.db_url:
                    stmt = sqlite_insert(self._state_table).values(
                        key=key, value=value_str, expires_at=expires_at
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['key'],
                        set_=dict(value=stmt.excluded.value, expires_at=stmt.excluded.expires_at)
                    )
                else:
                    stmt = pg_insert(self._state_table).values(
                        key=key, value=value_str, expires_at=expires_at
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['key'],
                        set_=dict(value=stmt.excluded.value, expires_at=stmt.excluded.expires_at)
                    )
                
                conn.execute(stmt)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Database set error for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        try:
            from sqlalchemy.sql import delete
            
            with self._engine.connect() as conn:
                stmt = delete(self._state_table).where(self._state_table.c.key == key)
                result = conn.execute(stmt)
                conn.commit()
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Database delete error for key {key}: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        try:
            from sqlalchemy.sql import select
            import datetime
            
            with self._engine.connect() as conn:
                stmt = select([self._state_table.c.key]).where(
                    self._state_table.c.key == key
                ).where(
                    (self._state_table.c.expires_at.is_(None)) |
                    (self._state_table.c.expires_at > datetime.datetime.utcnow())
                )
                
                result = conn.execute(stmt).fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Database exists error for key {key}: {e}")
            return False
    
    def clear(self) -> bool:
        """Clear all state data (use with caution)"""
        try:
            from sqlalchemy.sql import delete
            
            with self._engine.connect() as conn:
                stmt = delete(self._state_table)
                conn.execute(stmt)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Database clear error: {e}")
            return False