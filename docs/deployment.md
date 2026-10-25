# Deployment Guide

## Overview

This application is designed for containerized deployment with Docker, but can also be deployed to cloud platforms.

## Local Deployment (Docker Compose)

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 4GB RAM available
- Ports 8080, 5432, 6379, 9092 free

### Quick Start

```bash
# Clone repository
git clone <repo-url>
cd order-platform

# Copy env file
cp .env.example .env
# Edit .env if needed

# Start all services
docker compose up -d

# Check logs
docker compose logs -f app

# Check health
curl http://localhost:8080/actuator/health

# Should return {"status":"UP"}
```

### Services

- **app:** Spring Boot on http://localhost:8080
- **postgres:** PostgreSQL on localhost:5432
- **redis:** Redis on localhost:6379
- **kafka:** Kafka on localhost:9092

### Useful Commands

```bash
# Stop
docker compose down

# Stop and remove volumes (clean DB)
docker compose down -v

# Rebuild app after code change
docker compose up -d --build app

# View logs
docker compose logs -f app
docker compose logs -f postgres
docker compose logs -f kafka

# Exec into containers
docker exec -it order-postgres psql -U orderuser -d orderdb
docker exec -it order-redis redis-cli
docker exec -it order-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### API Access

- Swagger UI: http://localhost:8080/swagger-ui.html
- API Docs: http://localhost:8080/api-docs
- Actuator: http://localhost:8080/actuator

## Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| DATABASE_URL | JDBC URL | jdbc:postgresql://localhost:5432/orderdb | Yes |
| DATABASE_USERNAME | DB user | orderuser | Yes |
| DATABASE_PASSWORD | DB password | orderpass | Yes |
| REDIS_HOST | Redis host | localhost | Yes |
| REDIS_PORT | Redis port | 6379 | No |
| REDIS_PASSWORD | Redis password | (empty) | No |
| KAFKA_BOOTSTRAP_SERVERS | Kafka brokers | localhost:9092 | Yes |
| JWT_SECRET | JWT signing key (Base64) | dev secret | Yes (prod) |
| JWT_EXPIRATION_MS | Token expiry | 86400000 (24h) | No |
| SERVER_PORT | App port | 8080 | No |

**Never commit .env with real secrets.**

## Production Deployment Considerations

### 1. Database

**Option A: Managed PostgreSQL**
- AWS RDS, GCP Cloud SQL, Azure Database
- Automated backups, HA, scaling
- Set DATABASE_URL to managed instance

**Option B: Self-hosted**
- Use StatefulSet in Kubernetes
- Persistent volumes
- Regular backups

**Migrations:**
- Flyway runs automatically on startup
- Ensure app has DDL permissions
- For zero-downtime, use backward-compatible migrations

### 2. Redis

**Option A: Managed Redis**
- AWS ElastiCache, GCP Memorystore, Redis Cloud
- Set REDIS_HOST, REDIS_PASSWORD

**Option B: Self-hosted**
- Redis Sentinel for HA
- Or Redis Cluster for sharding

### 3. Kafka

**Challenge:** Kafka is hard to host cheaply.

**Options:**
- **Confluent Cloud:** Managed, pay per throughput, free trial
- **AWS MSK:** Managed Kafka on AWS
- **Self-hosted on K8s:** Use Strimzi operator, needs 3+ brokers for HA, Zookeeper or KRaft
- **Local only for portfolio:** Document that full stack requires Docker Compose locally, and production would use managed Kafka. This is acceptable for portfolio.

**For this project:**
- Provide reproducible local Docker deployment
- Document production Kafka options
- Don't pretend entire stack is hosted on free tier

### 4. Application

**Docker:**
- Build image: `docker build -t order-platform:1.0.0 .`
- Push to registry: `docker push <registry>/order-platform:1.0.0`
- Run with env vars

**Kubernetes:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-platform
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: app
        image: order-platform:1.0.0
        ports:
        - containerPort: 8080
        env:
        - name: DATABASE_URL
          valueFrom: { secretKeyRef: ... }
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka:9092"
        livenessProbe:
          httpGet: { path: /actuator/health, port: 8080 }
        readinessProbe:
          httpGet: { path: /actuator/health, port: 8080 }
```

**Cloud Run / ECS / App Engine:**
- Containerized, set env vars
- Ensure health check endpoint
- Scale based on CPU/memory or RPS

### 5. Security

- **JWT_SECRET:** Generate strong Base64 secret: `openssl rand -base64 64`
- **DB password:** Strong, rotate regularly
- **Network:** App should not expose DB/Redis/Kafka publicly, only app port
- **HTTPS:** Terminate TLS at load balancer
- **Secrets management:** Use AWS Secrets Manager, GCP Secret Manager, or K8s Secrets

### 6. Monitoring

- **Logs:** Centralized logging (ELK, Cloud Logging)
- **Metrics:** Prometheus + Grafana, or Cloud Monitoring
- **Tracing:** OpenTelemetry + Jaeger
- **Alerting:** Alert on health DOWN, high error rate, high latency

### 7. CI/CD

**Example GitHub Actions:**

```yaml
name: Build and Deploy

on:
  push:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-java@v3
      with: { java-version: '21', distribution: 'temurin' }
    - run: mvn clean package -DskipTests
    - run: docker build -t order-platform:${{ github.sha }} .
    - run: docker push ...

  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-java@v3
      with: { java-version: '21' }
    - run: mvn test
```

## Health Checks

- **Liveness:** `/actuator/health/liveness` - is app running?
- **Readiness:** `/actuator/health/readiness` - is app ready to serve?
- **Overall:** `/actuator/health` - includes DB, Redis, Kafka

Kubernetes uses these to restart unhealthy pods and stop routing to not-ready pods.

## Scaling

- **Horizontal:** Increase app replicas, Kafka partitions, consumer concurrency
- **Vertical:** Increase heap, DB connection pool
- **DB:** Read replicas for reads, sharding for writes (future)
- **Cache:** Redis Cluster
- **Kafka:** More brokers, more partitions

## Backup & Recovery

- **PostgreSQL:** Daily snapshots, WAL archiving for PITR
- **Kafka:** Replication factor 3, min.insync.replicas=2
- **Redis:** AOF + RDB, but cache can be rebuilt
- **Outbox:** If app crashes, PENDING events remain, will be published on restart

## Cost Optimization

- Use spot instances for non-critical consumers
- Right-size DB and Redis
- Use managed services to reduce ops cost
- Monitor and scale down off-peak

## Deployment Checklist

- [ ] Strong JWT secret generated
- [ ] DB password strong and stored in secret manager
- [ ] Topics pre-created in prod Kafka
- [ ] Flyway migrations tested
- [ ] Health checks configured
- [ ] Logs centralized
- [ ] Metrics and alerts configured
- [ ] HTTPS enabled
- [ ] Backup configured
- [ ] Performance tested
- [ ] Security scanned (dependencies, container)
- [ ] Documentation updated
