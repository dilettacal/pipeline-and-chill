"""
Redis client for ChillFlow with connection management and utilities.

This module provides a Redis client with connection pooling, session management,
and common Redis operations for the ChillFlow system.
"""

import json
from typing import Any, Dict, Optional

import redis
from core.settings import settings
from core.utils.logging import get_logger
from redis.connection import ConnectionPool

logger = get_logger("redis-client")


class RedisClient:
    """Redis client with connection management and utilities."""

    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize Redis client.

        Args:
            redis_url: Redis connection URL. Uses settings if not provided.
        """
        self.redis_url = redis_url or settings.REDIS_URL
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        self._setup_connection()

    def _setup_connection(self) -> None:
        """Setup Redis connection pool."""
        try:
            self.pool = ConnectionPool.from_url(
                self.redis_url,
                max_connections=50,
                retry_on_timeout=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )

            self.client = redis.Redis(connection_pool=self.pool)

            # Test connection
            self.client.ping()

            logger.info("Redis client initialized", url=self.redis_url)

        except Exception as e:
            logger.error("Failed to initialize Redis client", error=str(e))
            raise

    def get(self, key: str) -> Optional[str]:
        """
        Get value by key.

        Args:
            key: Redis key

        Returns:
            Value as string, or None if key doesn't exist
        """
        try:
            return self.client.get(key)
        except Exception as e:
            logger.error("Redis GET error", key=key, error=str(e))
            raise

    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """
        Set key-value pair with optional expiration.

        Args:
            key: Redis key
            value: Value to store
            ex: Expiration time in seconds

        Returns:
            True if successful
        """
        try:
            return self.client.set(key, value, ex=ex)
        except Exception as e:
            logger.error("Redis SET error", key=key, error=str(e))
            raise

    def delete(self, key: str) -> bool:
        """
        Delete key.

        Args:
            key: Redis key

        Returns:
            True if key was deleted
        """
        try:
            return bool(self.client.delete(key))
        except Exception as e:
            logger.error("Redis DELETE error", key=key, error=str(e))
            raise

    def exists(self, key: str) -> bool:
        """
        Check if key exists.

        Args:
            key: Redis key

        Returns:
            True if key exists
        """
        try:
            return bool(self.client.exists(key))
        except Exception as e:
            logger.error("Redis EXISTS error", key=key, error=str(e))
            raise

    def expire(self, key: str, time: int) -> bool:
        """
        Set expiration time for key.

        Args:
            key: Redis key
            time: Expiration time in seconds

        Returns:
            True if expiration was set
        """
        try:
            return self.client.expire(key, time)
        except Exception as e:
            logger.error("Redis EXPIRE error", key=key, time=time, error=str(e))
            raise

    def hget(self, name: str, key: str) -> Optional[str]:
        """
        Get hash field value.

        Args:
            name: Hash name
            key: Hash field key

        Returns:
            Field value, or None if field doesn't exist
        """
        try:
            return self.client.hget(name, key)
        except Exception as e:
            logger.error("Redis HGET error", name=name, key=key, error=str(e))
            raise

    def hset(self, name: str, key: str, value: str) -> int:
        """
        Set hash field value.

        Args:
            name: Hash name
            key: Hash field key
            value: Field value

        Returns:
            Number of fields added
        """
        try:
            return self.client.hset(name, key, value)
        except Exception as e:
            logger.error("Redis HSET error", name=name, key=key, error=str(e))
            raise

    def hgetall(self, name: str) -> Dict[str, str]:
        """
        Get all hash fields and values.

        Args:
            name: Hash name

        Returns:
            Dictionary of field-value pairs
        """
        try:
            return self.client.hgetall(name)
        except Exception as e:
            logger.error("Redis HGETALL error", name=name, error=str(e))
            raise

    def lpush(self, name: str, *values: str) -> int:
        """
        Push values to list head.

        Args:
            name: List name
            *values: Values to push

        Returns:
            List length after push
        """
        try:
            return self.client.lpush(name, *values)
        except Exception as e:
            logger.error("Redis LPUSH error", name=name, error=str(e))
            raise

    def rpop(self, name: str) -> Optional[str]:
        """
        Pop value from list tail.

        Args:
            name: List name

        Returns:
            Popped value, or None if list is empty
        """
        try:
            return self.client.rpop(name)
        except Exception as e:
            logger.error("Redis RPOP error", name=name, error=str(e))
            raise

    def json_set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """
        Set JSON value.

        Args:
            key: Redis key
            value: JSON-serializable value
            ex: Expiration time in seconds

        Returns:
            True if successful
        """
        try:
            json_value = json.dumps(value)
            return self.set(key, json_value, ex=ex)
        except Exception as e:
            logger.error("Redis JSON SET error", key=key, error=str(e))
            raise

    def json_get(self, key: str) -> Optional[Any]:
        """
        Get JSON value.

        Args:
            key: Redis key

        Returns:
            Deserialized JSON value, or None if key doesn't exist
        """
        try:
            value = self.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error("Redis JSON GET error", key=key, error=str(e))
            raise

    def health_check(self) -> bool:
        """
        Check Redis connection health.

        Returns:
            True if Redis is accessible, False otherwise
        """
        if not self.client:
            return False

        try:
            self.client.ping()
            logger.debug("Redis health check passed")
            return True
        except Exception as e:
            logger.error("Redis health check failed", error=str(e))
            return False

    def get_info(self) -> Dict[str, Any]:
        """
        Get Redis server information.

        Returns:
            Dictionary with Redis server info
        """
        if not self.client:
            return {"status": "not_initialized"}

        try:
            info = self.client.info()
            return {
                "status": "connected",
                "version": info.get("redis_version"),
                "memory_used": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "uptime": info.get("uptime_in_seconds"),
            }
        except Exception as e:
            logger.error("Failed to get Redis info", error=str(e))
            return {"status": "error", "error": str(e)}

    def close(self) -> None:
        """Close Redis connections."""
        if self.client:
            self.client.close()
            logger.info("Redis connections closed")


# Global Redis client instance (lazy-loaded)
redis_client: Optional[RedisClient] = None


def get_redis_client() -> RedisClient:
    """
    Get the global Redis client instance.

    Returns:
        Redis client instance
    """
    global redis_client
    if redis_client is None:
        redis_client = RedisClient()
    return redis_client
