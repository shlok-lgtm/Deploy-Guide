"""
Shared Token Bucket Rate Limiter
=================================
All workers and collectors draw from the same per-provider token bucket.
Replaces per-collector sleep() calls with centralized rate management.

Features:
- Per-provider token buckets with configurable rates
- Auto-backoff on 429 responses
- Thread-safe for concurrent workers
- Integrates with api_usage_tracker for observability

Usage:
    from app.shared_rate_limiter import rate_limiter

    # Acquire permission before making an API call:
    await rate_limiter.acquire("coingecko")  # async
    rate_limiter.acquire_sync("etherscan")   # sync

    # Report a 429 to trigger backoff:
    rate_limiter.report_429("coingecko")

    # Get current state for dashboard:
    state = rate_limiter.get_state()
"""

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class TokenBucket:
    """Token bucket with burst capacity and auto-refill."""
    rate_per_second: float        # sustained rate
    max_tokens: float             # burst capacity
    tokens: float = field(init=False)
    last_refill: float = field(init=False)
    backoff_until: float = field(default=0.0)
    consecutive_429s: int = field(default=0)
    total_acquired: int = field(default=0)
    total_waited_ms: float = field(default=0.0)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self.tokens = self.max_tokens
        self.last_refill = time.monotonic()

    def _refill(self):
        """Add tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate_per_second)
        self.last_refill = now

    def try_acquire(self, count: int = 1) -> tuple[bool, float]:
        """
        Try to acquire tokens. Returns (acquired, wait_seconds).
        If not acquired, wait_seconds is how long to wait before retrying.
        """
        with self.lock:
            now = time.monotonic()

            # Check backoff
            if now < self.backoff_until:
                return False, self.backoff_until - now

            self._refill()

            if self.tokens >= count:
                self.tokens -= count
                self.total_acquired += count
                return True, 0.0

            # Calculate wait time until enough tokens are available
            deficit = count - self.tokens
            wait = deficit / self.rate_per_second
            return False, wait

    def report_429(self):
        """Report a 429 response — exponential backoff."""
        with self.lock:
            self.consecutive_429s += 1
            backoff = min(2 ** self.consecutive_429s, 60)  # max 60s
            self.backoff_until = time.monotonic() + backoff
            logger.warning(
                f"Rate limit 429 — backoff {backoff}s "
                f"(consecutive: {self.consecutive_429s})"
            )

    def report_success(self):
        """Report a successful call — reset 429 counter."""
        with self.lock:
            if self.consecutive_429s > 0:
                self.consecutive_429s = 0

    def get_state(self) -> dict:
        """Return current bucket state for dashboard."""
        with self.lock:
            self._refill()
            now = time.monotonic()
            return {
                "rate_per_second": self.rate_per_second,
                "max_tokens": self.max_tokens,
                "available_tokens": round(self.tokens, 2),
                "utilization_pct": round(
                    (1 - self.tokens / self.max_tokens) * 100, 1
                ) if self.max_tokens > 0 else 0,
                "total_acquired": self.total_acquired,
                "total_waited_ms": round(self.total_waited_ms, 1),
                "in_backoff": now < self.backoff_until,
                "backoff_remaining_s": max(0, round(self.backoff_until - now, 1)),
                "consecutive_429s": self.consecutive_429s,
            }


class SharedRateLimiter:
    """
    Centralized rate limiter for all external API providers.
    Thread-safe. Works with both sync and async callers.
    """

    # Provider configs: (rate_per_second, burst_capacity)
    # Burst = rate * window to allow short spikes
    PROVIDER_CONFIGS = {
        "coingecko": (7.5, 30),      # 500/min ≈ 8.3/s, use 7.5 for higher throughput
        "etherscan": (8.0, 20),       # 10/s limit (Standard plan), use 8.0 (80% safety margin)
        "blockscout": (4.0, 12),      # 5/s limit, 100K credits/day per chain. 4.0/s = 80% margin
        "defillama": (5.0, 20),       # generous, no hard limit
        "snapshot": (2.0, 10),        # no hard limit but be respectful
        "tally": (1.0, 5),            # conservative
        "helius": (8.0, 20),          # 10 RPS limit
        "immunefi": (1.0, 5),         # public API, be conservative
        "wormhole": (2.0, 10),
        "axelar": (2.0, 10),
        "rated": (2.0, 10),
        "firecrawl": (2.0, 5),
        "parallel": (2.0, 5),
        "exchange_health": (10.0, 30),  # health checks, generous
        "alchemy": (3.0, 10),  # ~260K calls/day max, well within 1M CU budget
    }

    def __init__(self):
        self._buckets: dict[str, TokenBucket] = {}
        self._init_lock = threading.Lock()

    def _get_bucket(self, provider: str) -> TokenBucket:
        """Get or create the token bucket for a provider."""
        if provider not in self._buckets:
            with self._init_lock:
                if provider not in self._buckets:
                    rate, burst = self.PROVIDER_CONFIGS.get(provider, (2.0, 10))
                    self._buckets[provider] = TokenBucket(
                        rate_per_second=rate,
                        max_tokens=burst,
                    )
        return self._buckets[provider]

    def acquire_sync(self, provider: str, count: int = 1, timeout: float = 30.0) -> bool:
        """
        Synchronous acquire. Blocks until tokens are available.
        Returns True if acquired, False if timed out.
        """
        bucket = self._get_bucket(provider)
        deadline = time.monotonic() + timeout
        total_wait = 0.0

        while time.monotonic() < deadline:
            acquired, wait = bucket.try_acquire(count)
            if acquired:
                if total_wait > 0:
                    bucket.total_waited_ms += total_wait * 1000
                # Track the call
                try:
                    from app.api_usage_tracker import track_api_call
                    track_api_call(provider, "_rate_limited", caller="rate_limiter")
                except Exception:
                    pass
                return True

            sleep_time = min(wait, deadline - time.monotonic(), 0.5)
            if sleep_time <= 0:
                break
            time.sleep(sleep_time)
            total_wait += sleep_time

        logger.warning(f"Rate limiter timeout for {provider} after {timeout}s")
        return False

    async def acquire(self, provider: str, count: int = 1, timeout: float = 30.0) -> bool:
        """
        Async acquire. Yields control while waiting.
        Returns True if acquired, False if timed out.
        """
        bucket = self._get_bucket(provider)
        deadline = time.monotonic() + timeout
        total_wait = 0.0

        while time.monotonic() < deadline:
            acquired, wait = bucket.try_acquire(count)
            if acquired:
                if total_wait > 0:
                    bucket.total_waited_ms += total_wait * 1000
                return True

            sleep_time = min(wait, deadline - time.monotonic(), 0.5)
            if sleep_time <= 0:
                break
            await asyncio.sleep(sleep_time)
            total_wait += sleep_time

        logger.warning(f"Rate limiter timeout for {provider} after {timeout}s")
        return False

    def report_429(self, provider: str):
        """Report a 429 response from this provider."""
        bucket = self._get_bucket(provider)
        bucket.report_429()

    def report_success(self, provider: str):
        """Report a successful response from this provider."""
        bucket = self._get_bucket(provider)
        bucket.report_success()

    def get_state(self) -> dict:
        """Return state of all provider buckets for dashboard."""
        return {
            provider: bucket.get_state()
            for provider, bucket in sorted(self._buckets.items())
        }

    def get_provider_state(self, provider: str) -> dict:
        """Return state of a single provider bucket."""
        bucket = self._get_bucket(provider)
        return bucket.get_state()


# Module-level singleton
rate_limiter = SharedRateLimiter()
