# Performance Testing

## Goals

Measure system performance under realistic load to identify bottlenecks and ensure scalability.

## Metrics to Measure

- **Requests per second (RPS)** - throughput
- **Average latency** - mean response time
- **p95 latency** - 95% of requests faster than this
- **p99 latency** - 99% of requests faster than this (tail latency)
- **Kafka events/sec** - producer/consumer throughput
- **Consumer throughput** - orders processed per second
- **DB query performance** - slow query log
- **Redis cache performance** - hit rate, latency
- **Error rate** - % failed requests

## Test Scenarios

### 1. Product Browsing (Read-Heavy)

- 90% GET /api/products
- 10% GET /api/products/{id}
- Simulates browsing catalog
- Expected to benefit from Redis cache

### 2. Order Creation (Write-Heavy)

- 100% POST /api/orders
- Each order 1-3 items
- Measures order creation + outbox + Kafka publishing

### 3. Mixed Workload

- 70% product browsing
- 20% order creation
- 10% order status queries
- Most realistic

### 4. Concurrent Orders (Stress)

- Many users ordering same low-stock product
- Tests concurrency control and overselling prevention

## Tools

### Option 1: JMeter (GUI, mature)

- Thread groups for concurrent users
- HTTP Request samplers
- Aggregate report for metrics

### Option 2: k6 (Modern, JS scripting)

```javascript
import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  stages: [
    { duration: '30s', target: 50 },  // ramp up to 50 users
    { duration: '1m', target: 50 },   // stay at 50
    { duration: '30s', target: 100 }, // ramp to 100
    { duration: '1m', target: 100 },
    { duration: '30s', target: 0 },   // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // p95 < 500ms
    http_req_failed: ['rate<0.01'],   // error rate < 1%
  },
};

export default function () {
  let res = http.get('http://localhost:8080/api/products');
  check(res, { 'status 200': (r) => r.status === 200 });
  sleep(1);
}
```

Run: `k6 run script.js`

### Option 3: Custom Java Performance Test

See `performance-tests/` directory.

## Setup

1. Start full stack:
```bash
docker compose up -d
```

2. Wait for healthy:
```bash
curl http://localhost:8080/actuator/health
```

3. Create test user and get token:
```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"perfuser","email":"perf@test.com","password":"perf123"}'
```

4. Run performance test:
```bash
cd performance-tests
./run-perf-test.sh
```

## Performance Test Implementation

We provide a simple Java-based performance tester that:

- Uses `RestTemplate` or `WebClient` for HTTP calls
- Measures latency with `System.nanoTime()`
- Calculates RPS, avg, p95, p99
- Reports results to console and file

See `performance-tests/PerformanceTest.java`.

## Expected Bottlenecks

- **DB:** Order creation involves multiple writes (orders, order_items, outbox). Index and batch insert help.
- **Kafka:** Producer acks=all adds latency but ensures durability. Could use acks=1 for lower latency if acceptable.
- **Redis:** Should be fast, but network adds ~1-2ms. Local cache (Caffeine) could be L1.
- **Inventory atomic update:** High contention on popular products. Could shard or use queue.

## Optimization Ideas

- **DB:** Read replica for product browsing, write to primary
- **Cache:** Cache product list with short TTL, inventory with even shorter
- **Kafka:** Increase partitions for more parallelism, batch publishing
- **Connection pool:** Tune HikariCP size based on load
- **JVM:** Tune GC, heap size
- **Async:** Make order creation async (return 202 Accepted, poll status)

## Benchmark Results Template

**Do NOT invent numbers. Fill after running tests.**

```
Environment:
- CPU: ...
- RAM: ...
- Docker: ...
- DB: PostgreSQL 15, 1 instance
- Redis: 7-alpine, 1 instance
- Kafka: KRaft, 1 broker, 3 partitions
- App: 1 instance, 512MB heap

Test: Product Browsing (50 concurrent users, 2 min)
- RPS: 
- Avg latency: 
- p95 latency: 
- p99 latency: 
- Error rate: 
- Cache hit rate: 

Test: Order Creation (20 concurrent users, 2 min)
- RPS: 
- Avg latency: 
- p95: 
- p99: 
- Orders created: 
- Orders completed: 
- Kafka events/sec: 

Test: Mixed Workload (50 users, 5 min)
- RPS: 
- Avg latency: 
- p95: 
- p99: 
- Error rate: 

Notes:
- ...
```

## How to Document Results

1. Run tests 3 times, take median
2. Note environment details (CPU, RAM, Docker resources)
3. Include app logs for errors
4. Include DB slow query log if any
5. Include Kafka consumer lag
6. Be honest about limitations

## Continuous Performance Testing

- Add performance test to CI (nightly, not per commit)
- Track metrics over time
- Alert if p95 latency increases >20%
- Use for capacity planning

## Future: Load Testing in CI

- Use GitHub Actions with k6
- Fail build if thresholds breached
- Store results as artifacts
