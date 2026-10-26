# High-Scale Event & Order Processing System

This is a backend project built using Java and Spring Boot to handle order processing using Apache Kafka. It demonstrates how orders, inventory, payments, and notifications can work together using event-driven architecture.

## Tech Stack

- Java 21
- Spring Boot
- PostgreSQL
- Apache Kafka
- Redis
- Spring Security and JWT
- Docker and Docker Compose
- GitHub Actions

## Features

- User registration and login
- Role-based authentication
- Product and inventory management
- Order creation and tracking
- Event-driven processing using Kafka
- Simulated payment processing
- Automatic retries for failed events
- Duplicate event handling
- Redis caching
- Inventory management to prevent overselling
- Notifications
- API documentation using Swagger
- Unit and integration tests

## Architecture

```text
Client
  ↓
Spring Boot REST API
  ↓
PostgreSQL
  ↓
Kafka
  ├── Inventory Processing
  ├── Payment Processing
  ├── Order Status Updates
  └── Notifications

Redis – Caching
Docker – Running Services
```

## How to Run

Make sure Docker and Docker Compose are installed.

```bash
cp .env.example .env
docker compose up -d
```

Check the application health:

```bash
curl http://localhost:8080/actuator/health
```

Set the required environment variables as described in `.env.example` before running the application.

## API Documentation

After starting the application, open:

`http://localhost:8080/swagger-ui.html`

## Testing

The project includes unit tests, integration tests, and concurrency tests to check order processing, inventory management, and event handling.

## Project Structure

- `controller/` – REST API endpoints
- `service/` – Business logic
- `repository/` – Database operations
- `entity/` – Database entities
- `kafka/` – Event producers and consumers
- `security/` – Authentication and authorization
- `outbox/` – Reliable event publishing
- `src/test/` – Automated tests
- `docs/` – Project documentation

## Future Improvements

- Add a frontend dashboard
- Implement distributed tracing
- Improve monitoring and performance testing
- Add automated deployment

## Author

**Harshith Reddy**

GitHub: [Harshith9598](https://github.com/Harshith9598)

## Note

This is a portfolio project for learning backend development and distributed systems. Payment processing is simulated, and no real money is involved.
