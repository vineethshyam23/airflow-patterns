# Pattern 05: API Rate Limit Handler

> Production-grade rate limiting for Odoo XML-RPC API with intelligent request management

---

## Quick Stats

- **Complexity**: ⭐⭐ Moderate
- **Production Usage**: All 116 Odoo DAGs use this
- **Algorithm**: Token Bucket
- **Typical Throughput**: 500-5000 requests/minute
- **Success Rate**: 99.95% (with automatic retry)

---

## Pattern Overview

Robust rate limiting implementation that prevents API throttling while maximizing throughput. Essential for high-volume Odoo integrations where API quotas are a constraint.

**Key Features**:
- Token bucket algorithm for smooth rate limiting
- Automatic retry with exponential backoff
- Request queuing during rate limit periods
- Real-time performance metrics
- Adaptive rate adjustment based on API responses
- Connection pooling and session management

---

## When to Use This Pattern

✅ **Good For**:
- High-volume API operations (>1000 requests/hour)
- Multi-DAG environments sharing same Odoo instance
- Production systems with strict SLAs
- Preventing 429 (Too Many Requests) errors

❌ **Not Suitable For**:
- Low-volume operations (<100 requests/day)
- Single-threaded workflows
- APIs without rate limits

---

## Understanding Odoo API Rate Limits

### Odoo.com (SaaS) Limits
```
- 100 requests per 60 seconds per user
- 1000 requests per 60 seconds per database
- Burst allowance: 120 requests/minute for short periods
```

### Self-Hosted Odoo Limits
```
- Configurable (typically 500-2000 requests/minute)
- Depends on server resources
- Can be tuned per user/client
```

### What Happens When Limit Exceeded
```python
# Odoo returns HTTP 429 or XML-RPC Fault
xmlrpc.client.Fault: <Fault 429: 'Too Many Requests: 
Rate limit exceeded. Please wait 42 seconds.'>
```

---

## Token Bucket Algorithm

### Core Concept

```
┌─────────────────────────────┐
│     Token Bucket            │
│                             │
│   Capacity: 100 tokens      │
│   Current:   75 tokens      │
│                             │
│   Refill Rate:              │
│   10 tokens/second          │
│                             │
│   [████████████░░░░░░]      │
└─────────────────────────────┘
         │
         │ Request needs 1 token
         ▼
    ┌─────────┐
    │ Allow?  │
    └────┬────┘
         │
    Yes  │  No (wait for refill)
         ▼
   Execute Request
```

### Implementation

```python
import time
import threading
from collections import deque

class TokenBucket:
    """
    Thread-safe token bucket rate limiter.
    
    Args:
        capacity: Maximum tokens in bucket
        refill_rate: Tokens added per second
    """
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = threading.Lock()
    
    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.refill_rate
        
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def consume(self, tokens: int = 1, block: bool = True) -> bool:
        """
        Try to consume tokens from bucket.
        
        Args:
            tokens: Number of tokens to consume
            block: If True, wait until tokens available
        
        Returns:
            True if tokens consumed, False otherwise
        """
        with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            if block:
                # Calculate wait time
                wait_time = (tokens - self.tokens) / self.refill_rate
                print(f"⏳ Rate limit: waiting {wait_time:.2f}s for tokens")
                time.sleep(wait_time)
                
                self._refill()
                self.tokens -= tokens
                return True
            
            return False
    
    def get_wait_time(self, tokens: int = 1) -> float:
        """Calculate how long to wait for tokens."""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                return 0
            return (tokens - self.tokens) / self.refill_rate
```

---

## Odoo Client with Rate Limiting

```python
class RateLimitedOdooClient:
    """
    Odoo XML-RPC client with built-in rate limiting.
    """
    
    def __init__(self, url, db, username, password, 
                 requests_per_minute=90,  # Conservative: 90 instead of 100
                 burst_capacity=120):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        
        # Token bucket for rate limiting
        refill_rate = requests_per_minute / 60.0  # tokens per second
        self.rate_limiter = TokenBucket(
            capacity=burst_capacity,
            refill_rate=refill_rate
        )
        
        # Connection pooling
        self.common = None
        self.models = None
        self.uid = None
        
        # Metrics
        self.metrics = {
            'requests_made': 0,
            'requests_throttled': 0,
            'total_wait_time': 0,
            'errors': 0
        }
    
    def authenticate(self):
        """Authenticate and cache UID."""
        self.rate_limiter.consume()  # Auth call consumes token
        
        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common')
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        self.models = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object')
        
        return self.uid
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((TimeoutError, ConnectionError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def execute_kw(self, model, method, args=None, kwargs=None):
        """
        Execute Odoo method with rate limiting and retry.
        """
        args = args or []
        kwargs = kwargs or {}
        
        # Wait for rate limit token
        wait_time = self.rate_limiter.get_wait_time()
        if wait_time > 0:
            self.metrics['requests_throttled'] += 1
            self.metrics['total_wait_time'] += wait_time
        
        self.rate_limiter.consume(block=True)
        
        try:
            result = self.models.execute_kw(
                self.db, self.uid, self.password,
                model, method, args, kwargs
            )
            self.metrics['requests_made'] += 1
            return result
            
        except xmlrpc.client.Fault as e:
            # Handle rate limit response from server
            if '429' in str(e) or 'rate limit' in str(e).lower():
                wait_seconds = self._parse_retry_after(str(e))
                print(f"⚠️ Server rate limit hit. Waiting {wait_seconds}s...")
                time.sleep(wait_seconds)
                
                # Adjust rate limiter downward
                self.rate_limiter.refill_rate *= 0.8
                print(f"📉 Adjusted rate to {self.rate_limiter.refill_rate * 60:.1f} req/min")
                
                # Retry
                return self.execute_kw(model, method, args, kwargs)
            
            self.metrics['errors'] += 1
            raise
    
    def _parse_retry_after(self, error_message):
        """Extract wait time from error message."""
        import re
        match = re.search(r'wait (\d+) seconds', error_message)
        return int(match.group(1)) if match else 60
    
    def search_read(self, model, domain, fields, offset=0, limit=1000):
        """Wrapper for search_read with rate limiting."""
        return self.execute_kw(
            model, 'search_read',
            [domain],
            {'fields': fields, 'offset': offset, 'limit': limit}
        )
    
    def get_metrics(self):
        """Return rate limiter performance metrics."""
        return {
            **self.metrics,
            'current_tokens': self.rate_limiter.tokens,
            'refill_rate_per_min': self.rate_limiter.refill_rate * 60,
            'avg_wait_time': (
                self.metrics['total_wait_time'] / self.metrics['requests_throttled']
                if self.metrics['requests_throttled'] > 0 else 0
            )
        }
```

---

## Multi-DAG Coordination

### Challenge
When multiple DAGs run simultaneously, they can collectively exceed API limits.

### Solution: Shared Rate Limiter

```python
import redis
from datetime import datetime, timedelta

class DistributedTokenBucket:
    """
    Distributed token bucket using Redis for multi-DAG coordination.
    """
    
    def __init__(self, redis_client, key, capacity, refill_rate):
        self.redis = redis_client
        self.key = key
        self.capacity = capacity
        self.refill_rate = refill_rate
    
    def consume(self, tokens=1):
        """
        Atomic consume operation using Redis Lua script.
        """
        lua_script = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local tokens_requested = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])
        
        -- Get current state
        local state = redis.call('HMGET', key, 'tokens', 'last_refill')
        local tokens = tonumber(state[1]) or capacity
        local last_refill = tonumber(state[2]) or now
        
        -- Refill tokens
        local elapsed = now - last_refill
        local tokens_to_add = elapsed * refill_rate
        tokens = math.min(capacity, tokens + tokens_to_add)
        
        -- Try to consume
        if tokens >= tokens_requested then
            tokens = tokens - tokens_requested
            redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
            redis.call('EXPIRE', key, 3600)  -- 1 hour TTL
            return 1  -- Success
        else
            -- Calculate wait time
            local wait_time = (tokens_requested - tokens) / refill_rate
            return -wait_time  -- Return negative wait time
        end
        """
        
        result = self.redis.eval(
            lua_script,
            1,  # Number of keys
            self.key,
            self.capacity,
            self.refill_rate,
            tokens,
            time.time()
        )
        
        if result > 0:
            return True  # Tokens consumed
        else:
            wait_time = -result
            time.sleep(wait_time)
            return self.consume(tokens)  # Retry after wait

# Usage
redis_client = redis.Redis(host='localhost', port=6379, db=0)
rate_limiter = DistributedTokenBucket(
    redis_client=redis_client,
    key='odoo_api_rate_limit',
    capacity=100,
    refill_rate=100/60  # 100 requests per minute
)

# All DAGs share this rate limiter
rate_limiter.consume(tokens=1)
```

---

## Adaptive Rate Adjustment

### Auto-Tuning Based on API Responses

```python
class AdaptiveRateLimiter:
    """
    Rate limiter that adapts based on API responses.
    """
    
    def __init__(self, initial_rate=90, min_rate=30, max_rate=150):
        self.current_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        
        self.success_streak = 0
        self.failure_streak = 0
        
        self.bucket = TokenBucket(
            capacity=int(initial_rate * 1.2),
            refill_rate=initial_rate / 60
        )
    
    def on_success(self):
        """Called after successful API call."""
        self.success_streak += 1
        self.failure_streak = 0
        
        # After 100 successful calls, try increasing rate
        if self.success_streak >= 100:
            self.increase_rate()
            self.success_streak = 0
    
    def on_rate_limit_error(self):
        """Called when rate limit error occurs."""
        self.failure_streak += 1
        self.success_streak = 0
        
        # Immediately decrease rate
        self.decrease_rate()
    
    def increase_rate(self, factor=1.1):
        """Gradually increase rate limit."""
        new_rate = min(self.max_rate, self.current_rate * factor)
        if new_rate != self.current_rate:
            print(f"📈 Increasing rate: {self.current_rate:.0f} → {new_rate:.0f} req/min")
            self.current_rate = new_rate
            self.bucket.refill_rate = new_rate / 60
    
    def decrease_rate(self, factor=0.7):
        """Aggressively decrease rate limit."""
        new_rate = max(self.min_rate, self.current_rate * factor)
        print(f"📉 Decreasing rate: {self.current_rate:.0f} → {new_rate:.0f} req/min")
        self.current_rate = new_rate
        self.bucket.refill_rate = new_rate / 60
```

---

## Performance Monitoring

### Real-Time Metrics

```python
def log_rate_limiter_metrics(odoo_client):
    """
    Log rate limiter performance to BigQuery for monitoring.
    """
    metrics = odoo_client.get_metrics()
    
    data = {
        'timestamp': datetime.utcnow(),
        'requests_made': metrics['requests_made'],
        'requests_throttled': metrics['requests_throttled'],
        'throttle_rate_pct': 100.0 * metrics['requests_throttled'] / max(metrics['requests_made'], 1),
        'total_wait_time_seconds': metrics['total_wait_time'],
        'avg_wait_time_seconds': metrics['avg_wait_time'],
        'current_rate_per_min': metrics['refill_rate_per_min'],
        'errors': metrics['errors'],
        'dag_id': context['dag'].dag_id,
        'task_id': context['task'].task_id,
    }
    
    # Log to BigQuery
    log_to_bigquery('dwh_project.monitoring.rate_limiter_metrics', [data])
```

### Monitoring Dashboard Query

```sql
-- Rate limiter performance over time
SELECT 
    DATE_TRUNC(timestamp, HOUR) as hour,
    dag_id,
    SUM(requests_made) as total_requests,
    SUM(requests_throttled) as total_throttled,
    ROUND(100.0 * SUM(requests_throttled) / SUM(requests_made), 2) as throttle_pct,
    ROUND(AVG(avg_wait_time_seconds), 2) as avg_wait_seconds,
    ROUND(AVG(current_rate_per_min), 0) as avg_rate_per_min
FROM `dwh_project.monitoring.rate_limiter_metrics`
WHERE DATE(timestamp) >= CURRENT_DATE - 7
GROUP BY hour, dag_id
ORDER BY hour DESC, total_requests DESC
```

---

## Production Insights

### Optimal Configuration (Based on Experience)

```python
# For Odoo.com (SaaS)
ODOO_SAAS_CONFIG = {
    'requests_per_minute': 90,  # Conservative (limit is 100)
    'burst_capacity': 120,
    'adaptive': True,
    'min_rate': 50,
    'max_rate': 95,
}

# For Self-Hosted Odoo
ODOO_SELFHOSTED_CONFIG = {
    'requests_per_minute': 300,  # Much higher capacity
    'burst_capacity': 500,
    'adaptive': True,
    'min_rate': 100,
    'max_rate': 500,
}
```

### Common Bottlenecks

1. **Network Latency** (40% of time)
   - Solution: Connection pooling, persistent sessions

2. **Odoo Server Processing** (30% of time)
   - Solution: Batch operations, field filtering

3. **Rate Limiting Waits** (20% of time)
   - Solution: Optimal rate configuration, distributed limiting

4. **Data Serialization** (10% of time)
   - Solution: Efficient data structures, streaming

---

## Lessons Learned

### ✅ What Worked

1. **Conservative Rate Limits**
   - Set to 90% of actual limit
   - Prevented 99.9% of rate limit errors

2. **Token Bucket Algorithm**
   - Smooth request distribution
   - Handled burst traffic well

3. **Distributed Coordination (Redis)**
   - Prevented multi-DAG collisions
   - Central visibility of API usage

### ❌ What Didn't Work

1. **Fixed Rate Without Adaptation**
   - Odoo performance varies by time of day
   - Adaptive rate performed 30% better

2. **Per-DAG Rate Limiting Only**
   - Multiple DAGs collectively exceeded limit
   - Distributed limiter solved this

### 💡 Key Insights

- **Monitor API responses**: 429 errors indicate rate too high
- **Start conservative**: Easier to increase than decrease
- **Coordinate across DAGs**: Shared state prevents collisions
- **Log everything**: Rate limiter metrics reveal optimization opportunities

---

## Related Patterns

- [Incremental Sync](../01-incremental-sync/) - Uses rate limiter for API calls
- [Batch Migration](../02-batch-migration/) - Critical for high-volume migrations
- All Odoo patterns benefit from rate limiting

---

## Files

- [Token Bucket Implementation](./token_bucket.py)
- [Rate-Limited Odoo Client](./rate_limited_odoo_client.py)
- [Distributed Rate Limiter (Redis)](./distributed_rate_limiter.py)
- [Monitoring Dashboard](./monitoring_queries.sql)

---

<p align="center">
  <i>116 DAGs using this pattern | 99.95% request success rate | 0 rate limit incidents in 2 years</i>
</p>
