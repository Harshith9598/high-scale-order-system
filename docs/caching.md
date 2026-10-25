# Redis Caching Strategy

## Why Redis?

- **Speed:** In-memory, microsecond latency vs millisecond DB
- **Read-heavy workload:** Product catalog browsed far more than updated
- **High-frequency access:** Inventory checked on every order creation
- **Shared cache:** Works across multiple app instances
- **Rich data structures:** Could use for rate limiting, leaderboards, etc.

Alternatives:
- **Caffeine (in-memory):** Faster per instance but not shared, each instance has own cache → inconsistent, higher DB load
- **No cache:** Simpler but DB becomes bottleneck under load

## What Is Cached?

| Cache Name | Key | Value | TTL | Why |
|------------|-----|-------|-----|-----|
| `products` | category + search + page | Page<ProductDto> | 5 min | Product listing is read-heavy, filtering is expensive |
| `product` | productId | ProductDto | 10 min | Single product view frequent |
| `inventory` | productId | InventoryDto | 2 min | Inventory checked on every order, but changes frequently |
| `orders` | (future) | OrderDto | 5 min | User's order history |

**Not cached:**
- Orders creation (write-heavy, must be consistent)
- Payments (sensitive, must be fresh)
- Notifications (user-specific, real-time)

## Configuration

```java
@Bean
public CacheManager cacheManager(RedisConnectionFactory factory) {
    RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
        .entryTtl(Duration.ofMinutes(10))
        .serializeKeysWith(StringRedisSerializer)
        .serializeValuesWith(GenericJackson2JsonRedisSerializer)
        .disableCachingNullValues();

    return RedisCacheManager.builder(factory)
        .cacheDefaults(config)
        .withCacheConfiguration("products", config.entryTtl(Duration.ofMinutes(5)))
        .withCacheConfiguration("product", config.entryTtl(Duration.ofMinutes(10)))
        .withCacheConfiguration("inventory", config.entryTtl(Duration.ofMinutes(2)))
        .build();
}
```

## Invalidation Strategy

- **Write-through invalidation:** On product update/delete, evict cache
```java
@Caching(evict = {
    @CacheEvict(value="products", allEntries=true),
    @CacheEvict(value="product", key="#id")
})
public ProductDto updateProduct(...) { ... }
```
- **Why allEntries for products?** Product list cache key includes filters, hard to know which pages contain updated product → evict all.
- **TTL as fallback:** Even if invalidation missed, TTL ensures eventual consistency.

**Trade-off:** `allEntries=true` is blunt, could cause cache stampede. Alternative: use cache key patterns or versioning, but more complex.

## What Happens If Redis Is Down?

**Design goal:** System should continue working, just slower.

**Implementation:**

1. **Custom CacheErrorHandler:**
```java
public void handleCacheGetError(...) {
    log.warn("Cache GET error, falling back to DB");
    // Don't throw, let method execute and fetch from DB
}
```

2. **Spring Cache behavior:** If cache error handler doesn't throw, Spring proceeds to call actual method (DB).

3. **Result:**
   - GET: Cache miss → DB query → response (slower)
   - PUT: Cache write fails → logged, but DB already updated → eventual consistency when Redis recovers
   - EVICT: Fails → stale cache may remain until TTL expires

**Monitoring:**
- Logs warn on cache errors
- Metric: cache hits/misses
- Actuator: Redis health indicator shows DOWN, but app health still UP (if configured)

**Future:**
- Circuit breaker: if Redis down repeatedly, stop trying for some time to reduce log spam
- Read-replica Redis for HA

## Performance Impact

- **Cache hit:** ~1-2ms (Redis) vs ~20-50ms (PostgreSQL with join)
- **Cache miss:** Slight overhead (check Redis then DB) but still acceptable
- **Write:** Extra Redis evict call, negligible

**To measure:**
- Enable metrics: `cache.hits.total`, `cache.misses.total`
- Log cache hit/miss ratio
- Use performance test script to compare with/without Redis

## Rate Limiting (Future)

Redis could also be used for:
- API rate limiting per user/IP using token bucket
- Prevent brute force login attempts
- Example: `INCR user:123:requests` with expiry

Not implemented in v1 to keep scope focused.

## Testing

- Unit test: mock cache manager
- Integration test: Testcontainers Redis, verify cache hit on second call
- Manual: stop Redis container, verify API still works

## Configuration via Env

```
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
```

Supports password for production.

## Serialization

- **Key:** StringRedisSerializer (human-readable)
- **Value:** GenericJackson2JsonRedisSerializer (JSON, handles Java 8 time)
- **Why JSON not JDK serialization?** Human-readable in redis-cli, language-agnostic, handles schema evolution better

## Memory Management

- TTL ensures automatic eviction
- Redis `maxmemory-policy allkeys-lru` recommended in production
- Monitor Redis memory usage via `INFO memory`
