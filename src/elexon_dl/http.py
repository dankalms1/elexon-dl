import asyncio, random, time, json, hashlib
from typing import Any, Dict, Optional, Tuple
from collections import deque
from pathlib import Path
import httpx
from .config import Settings

RETRIABLE = {429, 500, 502, 503, 504}

def _now() -> float:
    return time.time()

def _cache_key(url: str, params: Dict[str, Any]) -> str:
    # stable key from url + sorted params
    payload = json.dumps([url, sorted(params.items())], separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()  # 40 hex

class RateLimiter:
    def __init__(self, rate_per_sec: float, capacity: Optional[float] = None):
        self.rate = rate_per_sec
        self.capacity = capacity or max(1.0, rate_per_sec)
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

class HTTPMetrics:
    def __init__(self, max_samples: int = 5000):
        self.count = 0
        self.latencies = deque(maxlen=max_samples)
        self._lock = asyncio.Lock()

    async def record(self, elapsed: float):
        async with self._lock:
            self.count += 1
            self.latencies.append(elapsed)

    def snapshot(self):
        n = self.count
        if self.latencies:
            avg = sum(self.latencies)/len(self.latencies)
            mx = max(self.latencies)
        else:
            avg = 0.0
            mx = 0.0
        return {"count": n, "avg_latency": avg, "max_latency": mx}

class AsyncHTTP:
    def __init__(self, settings: Settings):
        self.s = settings
        self._client: Optional[httpx.AsyncClient] = None
        self._limiter = RateLimiter(self.s.rate_per_sec)
        self.metrics = HTTPMetrics()
        # NEW: cache filesystem prep
        self._cache_dir: Optional[Path] = None
        if self.s.cache_enabled:
            d = Path(self.s.cache_dir or (Path.home() / ".cache" / "elexon-dl" / "http"))
            d.mkdir(parents=True, exist_ok=True)
            self._cache_dir = d
        # convenience
        self._ttl = None if not self.s.cache_ttl_s or self.s.cache_ttl_s <= 0 else int(self.s.cache_ttl_s)

    async def __aenter__(self):
        headers = {"User-Agent": self.s.user_agent}
        # do NOT set no-cache; you want caches to be usable
        self._client = httpx.AsyncClient(
            timeout=self.s.timeout_s,
            headers=headers,
            limits=httpx.Limits(
                max_keepalive_connections=self.s.max_concurrency,
                max_connections=self.s.max_concurrency,
            ),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client:
            await self._client.aclose()

    async def _sleep_backoff(self, attempt: int):
        base = self.s.backoff_base
        cap = self.s.backoff_cap
        sleep = min(cap, base * (2 ** attempt))
        sleep *= 0.5 + random.random()
        await asyncio.sleep(sleep)

    # NEW: try read cached response from disk
    def _cache_read(self, url: str, params: Dict[str, Any]) -> Optional[httpx.Response]:
        if not self._cache_dir:
            return None
        key = _cache_key(url, params)
        path = self._cache_dir / f"{key}.bin"
        meta = self._cache_dir / f"{key}.meta.json"
        if not path.exists():
            return None
        if self._ttl is not None:
            try:
                m = json.loads(meta.read_text("utf-8"))
                if (_now() - float(m.get("ts", 0))) > self._ttl:
                    return None  # expired
            except Exception:
                return None
        try:
            data = path.read_bytes()
            req = httpx.Request("GET", url, params=params, headers={"User-Agent": self.s.user_agent})
            # Build a Response object as if it came from the network
            resp = httpx.Response(200, request=req, content=data)
            # Mark it so you can introspect later if you like
            resp.extensions["from_cache"] = True
            return resp
        except Exception:
            return None

    # NEW: write response to disk cache
    def _cache_write(self, url: str, params: Dict[str, Any], resp: httpx.Response) -> None:
        if not self._cache_dir:
            return
        if resp.status_code != 200:
            return
        key = _cache_key(url, params)
        path = self._cache_dir / f"{key}.bin"
        meta = self._cache_dir / f"{key}.meta.json"
        try:
            path.write_bytes(resp.content)
            meta.write_text(json.dumps({"ts": _now(), "url": url, "params": params}, ensure_ascii=False))
        except Exception:
            # best-effort; silence disk races
            pass

    async def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        assert self._client is not None, "Use within 'async with AsyncHTTP(settings)'"
        params = params or {}

        # Try cache first
        if self._cache_dir:
            cached = self._cache_read(url, params)
            if cached is not None:
                # record near-zero latency for metrics
                await self.metrics.record(0.0)
                return cached

        # Rate limit + network fetch with retries
        await self._limiter.acquire()
        for attempt in range(self.s.max_retries + 1):
            t0 = time.perf_counter()
            resp = await self._client.get(url, params=params)
            elapsed = time.perf_counter() - t0
            if resp.status_code in RETRIABLE and attempt < self.s.max_retries:
                await self._sleep_backoff(attempt)
                continue
            await self.metrics.record(elapsed)
            # Write to cache on success
            if resp.status_code == 200:
                self._cache_write(url, params, resp)
            return resp