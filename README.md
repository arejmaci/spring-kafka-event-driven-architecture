# Spring Boot & Apache Kafka Event-Driven Architecture

[![Java](https://img.shields.io/badge/Java-21-orange?style=flat-square&logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen?style=flat-square&logo=springboot)](https://spring.io/projects/spring-boot)
[![Spring Cloud Stream](https://img.shields.io/badge/Spring%20Cloud%20Stream-Binder-blue?style=flat-square&logo=spring)](https://spring.io/projects/spring-cloud-stream)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black?style=flat-square&logo=apachekafka)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)

A production-ready reference architecture for **Event-Driven Architecture (EDA)** built with **Java 21**, **Spring Boot 3.x**, **Spring Cloud Stream**, and **Apache Kafka (KRaft mode)**. This repository showcases how to design decoupled, fault-tolerant, and high-performance distributed microservices.

---

## 🏗️ Architecture & Key Concepts

* **Event-Driven Architecture (EDA)**: Asynchronous communication between decoupled microservices leveraging Apache Kafka as the central event broker.
* **Spring Cloud Stream Abstraction**: Functional programming model (`Function`, `Consumer`, `Supplier`) to isolate core business logic from underlying messaging infrastructure.
* **Request-Reply Pattern**: A robust implementation of synchronous messaging over asynchronous event streams using Correlation IDs and dedicated reply topics (documented in [RFC-001](./docs/RFC-001-request-reply-pattern.md)).
* **Apache Kafka in KRaft Mode**: Operates without ZooKeeper to streamline deployment topology, reduce resource overhead, and speed up container startup times.

---

## 📂 Repository Structure

```text
spring-kafka-event-driven-architecture/
├── docker-compose.yml              # Local infrastructure orchestration (Kafka KRaft + Kafka UI)
├── docs/                           # Technical specifications and RFCs
│   └── RFC-001-request-reply-pattern.md
├── services/                       # Microservice modules
│   ├── producer-service/           # Event producer and request initiator
│   └── consumer-service/           # Event processor and domain logic handler
└── pom.xml                         # Root Maven multi-module POM
```

---

## 🚀 Getting Started

### Prerequisites
* **Java 21 (JDK 21)**
* **Maven 3.9+**
* **Docker & Docker Compose**

### 1. Clone the Repository
```bash
git clone https://github.com/arejmaci/spring-kafka-event-driven-architecture.git
cd spring-kafka-event-driven-architecture
```

### 2. Start Infrastructure (Kafka KRaft & Kafka UI)
Spin up the Kafka broker in KRaft mode alongside the web UI:
```bash
docker compose up -d
```
> 📊 **Kafka UI** will be available at [http://localhost:8080](http://localhost:8080) for real-time inspection of topics, partitions, and consumer groups.

### 3. Build & Run the Microservices
Build the entire multi-module project:
```bash
mvn clean install
```

Run the microservices independently:
* **Producer Service**:
  ```bash
  cd services/producer-service
  mvn spring-boot:run
  ```
* **Consumer Service**:
  ```bash
  cd services/consumer-service
  mvn spring-boot:run
  ```

---

## 📄 Technical Documentation

* **[RFC-001: Request-Reply Pattern Implementation](./docs/RFC-001-request-reply-pattern.md)**: Detailed specification and sequence diagrams covering the request-reply pattern implementation using Spring Cloud Stream and Kafka.

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to open a pull request or report an issue.

---

## 📝 License

This project is licensed under the MIT License. See the `LICENSE` file for details.
