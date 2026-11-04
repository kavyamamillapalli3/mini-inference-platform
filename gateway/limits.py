"""
Per-tenant admission control.

TokenBucket: limits *tokens* per minute, not requests, because a 100K-token
prompt costs far more GPU than "hi". We charge an estimate up front
(prompt tokens + max_tokens) and refund the unused part when the request ends.

Concurrency limit: caps how many requests a tenant can have in flight, so one
tenant can't fill every batch slot on the replicas.
"""

import time


class TokenBucket:
    def __init__(self, tokens_per_min: float):
        self.capacity = float(tokens_per_min)        # allow up to one minute of burst
        self.rate = tokens_per_min / 60.0            # refill per second
        self.tokens = self.capacity
        self.updated = time.monotonic()

    def _refill(self):
        now = time.monotonic()
        self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
        self.updated = now

    def try_consume(self, n: float):
        """Returns (ok, retry_after_seconds)."""
        self._refill()
        if n > self.capacity:
            return False, None                        # can never fit: a 400, not a 429
        if self.tokens >= n:
            self.tokens -= n
            return True, 0.0
        return False, (n - self.tokens) / self.rate

    def refund(self, n: float):
        self._refill()
        self.tokens = min(self.capacity, self.tokens + max(0.0, n))


class Tenant:
    def __init__(self, name, tier, tokens_per_min, max_concurrency,
                 max_prompt_tokens, max_output_tokens):
        self.name = name
        self.tier = tier
        self.bucket = TokenBucket(tokens_per_min)
        self.max_concurrency = max_concurrency
        self.max_prompt_tokens = max_prompt_tokens
        self.max_output_tokens = max_output_tokens
        self.in_flight = 0
        self.stats = {"allowed": 0, "rejected_rate": 0, "rejected_concurrency": 0,
                      "rejected_too_large": 0, "upstream_overloaded": 0,
                      "tokens_charged": 0, "tokens_refunded": 0}
