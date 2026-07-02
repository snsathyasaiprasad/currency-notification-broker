# 💱 Currency Notification Broker

**Real-time currency exchange rate tracking and threshold-based alert system**, built as a distributed microservices project using Spring Boot, Apache Kafka, Docker, Kubernetes, and Jenkins CI/CD.

![Java](https://img.shields.io/badge/Java-17-orange?style=flat-square&logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen?style=flat-square&logo=springboot)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-black?style=flat-square&logo=apachekafka)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat-square&logo=docker)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Deployed-326CE5?style=flat-square&logo=kubernetes)
![Jenkins](https://img.shields.io/badge/Jenkins-CI%2FCD-D24939?style=flat-square&logo=jenkins)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql)

---

## 📖 Overview

This project implements a real-time currency exchange monitoring system split into two independent microservices that communicate exclusively through **Apache Kafka** — a genuine message broker architecture, not a direct service-to-service call chain.

- **`currency-service`** polls a live exchange-rate API on a schedule, persists every rate to PostgreSQL, and publishes each update onto a Kafka topic.
- **`notification-service`** consumes that Kafka stream, checks incoming rates against user-defined thresholds, and pushes real-time alerts over WebSocket (plus logs) when a rate crosses a configured boundary.

Both services are fully containerized, deployed to Kubernetes, and built/deployed automatically through a Jenkins CI/CD pipeline on every push.

---

## 🏗️ Architecture

```
                     ┌────────────────────┐
                     │  Frankfurter API    │   (live exchange rates)
                     └──────────┬──────────┘
                                │ polls every 15s
                                ▼
                  ┌──────────────────────────┐
                  │     currency-service       │
                  │  Spring Boot · REST · JPA  │
                  │  Postgres: currency_db     │
                  └──────────────┬─────────────┘
                                 │ publishes
                                 ▼
                  ┌──────────────────────────┐
                  │        Apache Kafka        │
                  │      topic: currency-rates │
                  └──────────────┬─────────────┘
                                 │ consumes
                                 ▼
                  ┌──────────────────────────┐
                  │   notification-service     │
                  │ Spring Boot · Kafka Consumer│
                  │ Postgres: notification_db  │
                  │ WebSocket alerts (STOMP)   │
                  └──────────────┬─────────────┘
                                 │
                                 ▼
                     ┌────────────────────┐
                     │   Client / Browser   │  ← subscribes to
                     │  (WebSocket / REST)  │    /topic/alerts/{userId}
                     └────────────────────┘
```

---

## ✨ Features

| Feature | Description |
|---|---|
| 🔄 Real-time rate polling | Fetches USD-based rates for EUR, GBP, INR, JPY, AUD every 15 seconds |
| 📨 Event-driven architecture | Services communicate only via Kafka — no direct REST calls between them |
| 🔔 Threshold alerts | Users register `ABOVE` / `BELOW` thresholds per currency pair; alerts fire automatically |
| 🌐 Live WebSocket push | Alerts stream to subscribed clients in real time via STOMP over WebSocket |
| 🗄️ Persistent history | Every fetched rate and every threshold is stored in PostgreSQL (separate databases per service) |
| 🐳 Fully containerized | Multi-stage Docker builds for lean production images |
| ☸️ Kubernetes-native | Deployments, Services, and namespace isolation via declarative YAML manifests |
| 🚀 CI/CD automated | Jenkins pipeline builds, tests, containerizes, pushes, and deploys on every commit |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17+ |
| Framework | Spring Boot 4.x (Web, Data JPA, Kafka, WebSocket, Actuator) |
| Messaging | Apache Kafka + Zookeeper |
| Database | PostgreSQL 16 |
| Containerization | Docker (multi-stage builds) |
| Orchestration | Kubernetes (minikube) |
| CI/CD | Jenkins (declarative pipeline) |
| Version Control | Git + GitHub |
| External API | [Frankfurter API](https://frankfurter.dev) (free, no key required) |

---

## 📂 Project Structure

```
currency-notification-broker/
├── currency-service/          # Fetches & publishes exchange rates
│   ├── src/main/java/com/broker/currency/
│   │   ├── client/            # External API DTOs
│   │   ├── config/            # Kafka producer config
│   │   ├── controller/        # REST endpoints
│   │   ├── dto/                # Kafka event payloads
│   │   ├── model/              # JPA entities
│   │   ├── repository/         # Spring Data repositories
│   │   └── service/            # Scheduled fetch + publish logic
│   └── Dockerfile
├── notification-service/       # Consumes rates & fires alerts
│   ├── src/main/java/com/broker/notification/
│   │   ├── config/              # WebSocket config
│   │   ├── controller/          # Threshold REST API
│   │   ├── model/                # JPA entities
│   │   ├── repository/           # Spring Data repositories
│   │   └── service/              # Kafka listener + alert logic
│   └── Dockerfile
├── k8s/                          # Kubernetes manifests
│   ├── 00-namespace.yaml
│   ├── 01-postgres.yaml
│   ├── 02-kafka.yaml
│   ├── 03-currency-service.yaml
│   └── 04-notification-service.yaml
├── scripts/
│   └── init-multi-db.sh          # Creates both Postgres databases
├── docker-compose.yml             # Local multi-container dev environment
├── Jenkinsfile                     # CI/CD pipeline definition
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Java 17+
- Maven
- Docker & Docker Compose
- kubectl + minikube (for Kubernetes deployment)
- Git

### Run locally with Docker Compose
```bash
git clone https://github.com/snsathyasaiprasad/currency-notification-broker.git
cd currency-notification-broker
docker compose up --build
```

### Try it out
```bash
# Check live rate history
curl http://localhost:8081/api/rates/USD/EUR/history

# Register a price alert
curl -X POST http://localhost:8082/api/thresholds \
  -H "Content-Type: application/json" \
  -d '{"userId":"sathya","baseCurrency":"USD","targetCurrency":"EUR","thresholdValue":0.80,"direction":"ABOVE"}'
```

### Deploy to Kubernetes
```bash
minikube start --driver=docker --cpus=4 --memory=6g
kubectl apply -f k8s/
minikube service currency-service -n currency-broker --url
minikube service notification-service -n currency-broker --url
```

---

## 🔌 API Reference

### `currency-service` — port `8081`
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/rates/{base}/{target}/history` | Last 20 recorded rates for a currency pair |

### `notification-service` — port `8082`
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/thresholds` | Register (or update) a threshold alert |
| `GET` | `/api/thresholds` | List all registered thresholds |
| `WS` | `/ws` → `/topic/alerts/{userId}` | Live alert stream (STOMP over WebSocket) |

---

## 🔄 CI/CD Pipeline

Every push to `main` triggers the Jenkins pipeline:

```
Checkout → Build & Test (both services) → Docker Build & Push → Deploy to Kubernetes
```

Images are tagged with the Jenkins build number and pushed to Docker Hub, then rolled out to the running Kubernetes deployment automatically.

---

## 🗺️ Roadmap

- [ ] Real email / SMS alerts (Spring Mail, Twilio)
- [ ] Frontend dashboard subscribing to live WebSocket alerts
- [ ] Ingress + custom domain instead of NodePort
- [ ] Prometheus + Grafana metrics dashboards
- [ ] Kubernetes Secrets for credentials instead of plaintext env vars
- [ ] Unit & integration test coverage

---

## 👤 Author

**S N Sathya Sai Prasad**
Information Science and Engineering, DSATM (VTU)

---

## 📄 License

This project is available for academic and educational use.
