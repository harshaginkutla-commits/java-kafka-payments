# Java Kafka Payments

Payment transaction processing system built with **Spring Boot + Apache Kafka**.
Implements the architecture from the "Kafka Explained in One Picture" diagram.

## Architecture

```
Mobile/Web App
      │
      ▼ POST /api/transactions
Spring Boot API (Producer)
      │
      ▼ userId as partition key
Kafka Topic: "transactions"  (3 partitions, 3 brokers)
      │
      ├──► Fraud Detection Consumer    (fraud-detection-group)
      ├──► Payment Processing Consumer (payment-processing-group)
      ├──► Notification Consumer       (notification-group)
      └──► Analytics Consumer          (analytics-group)
```

## Run Locally

```bash
# 1. Start Kafka (3 brokers) + app
docker compose up

# 2. Open dashboard
open http://localhost:8080

# 3. Open Kafka UI (inspect topics, partitions, consumer groups)
open http://localhost:8090

# 4. Generate demo transactions
curl -X POST http://localhost:8080/api/transactions/demo?count=10

# 5. Check analytics
curl http://localhost:8080/api/analytics
```

## Key Kafka Concepts Demonstrated

| Concept | Where |
|---|---|
| **Topic** | `transactions` topic in `KafkaTopicConfig` |
| **Partitions** | 3 partitions, messages routed by `userId` |
| **Producer** | `TransactionProducer.java` |
| **Consumer Groups** | 4 independent groups, each reads ALL messages |
| **Offset** | Logged in producer: `offset=X` |
| **Replication** | 3 brokers, replication factor 3 |
| **Idempotent Producer** | `enable.idempotence=true` in `application.yml` |
| **Dead Letter Topic** | `transactions.DLT` for failed messages |
