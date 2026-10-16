# TraceMesh 🕸️
**Real-Time Microservice Observability & Anomaly Detection Pipeline**

Modern microservice architectures fail in complex ways. A single user click can trigger a chain reaction across dozens of backend services, making root-cause analysis difficult during an incident. 

**TraceMesh** is a real-time data engineering pipeline built to ingest continuous telemetry logs, calculate streaming latency metrics (like P99), and automatically identify backend services breaching their Service Level Agreements (SLAs) before they cause total system outages.

## 🎯 The SRE Problem It Solves
Instead of waiting for customer complaints, TraceMesh uses **sliding window aggregations** over high-throughput event streams to detect temporary micro-spikes in latency. It isolates the exact degrading service and stores the historical context for post-incident reviews (PIRs).

## 🏗️ Architecture & Data Flow

TraceMesh is orchestrated via Docker Compose, running a distributed 9-node local cluster.

```text
[Synthetic Microservices] (Spring Boot) 
       │ (JSON Trace Logs @ 1k+ events/sec)
       ▼
[Message Broker]          (Apache Kafka - KRaft Mode)
       │ (Buffered high-throughput stream)
       ▼
[Stream Processor]        (Apache Spark Structured Streaming)
       │ (10s sliding windows, P99 calculation, Event-time watermarking)
       ▼
[Data Warehouse]          (Hadoop HDFS / Apache Hive)
       │ (Partitioned Parquet storage)
       ▼
[Analytics Engine]        (HiveServer2 SQL Queries)
```

### Core Tech Stack
* **Ingestion:** Java, Spring Boot, Apache Kafka
* **Stream Processing:** Apache Spark (Java API)
* **Storage & Analytics:** HDFS, Apache Hive, PostgreSQL (Metastore)
* **Infrastructure:** Docker, Docker Compose

## ⚡ Key Features (Phase 1 - Complete)
* **Fault-Tolerant Streaming:** Utilizes Spark Checkpointing to HDFS, ensuring exactly-once processing semantics even if the worker nodes crash.
* **Parallel Processing:** Kafka topics are explicitly partitioned to allow Spark to read and process the telemetry stream across multiple concurrent tasks.
* **Late Data Handling:** Implements event-time watermarking to accommodate network jitter and out-of-order logs without leaking memory.
* **Simulated Degradation:** Includes a REST endpoint to artificially inject network latency into specific microservices to test the pipeline's anomaly detection capabilities.

## 🚀 Roadmap & Future Work
TraceMesh is currently a local PoC. The next phase of development focuses on productionizing the deployment and integrating standard SRE tooling:

- [ ] **OpenTelemetry Integration:** Replace the synthetic log generator with a real OpenTelemetry collector ingesting distributed traces from actual containerized apps.
- [ ] **Infrastructure as Code (IaC):** Migrate from Docker Compose to **Kubernetes (Helm)** for container orchestration, and use **Terraform** to provision the cloud infrastructure.
- [ ] **Visualization & Alerting:** Mount **Grafana** on top of the analytical datastore to visualize the P99 latency graphs in real-time, integrating Webhook alerts to Slack/PagerDuty when SLA thresholds are breached.
- [ ] **Dead Letter Queues (DLQ):** Implement robust error handling in Spark to route malformed JSON logs to a DLQ topic rather than dropping them.

## 🛠️ Quick Start (Local Development)

Ensure Docker Desktop is running with at least 8GB of RAM allocated.

**1. Spin up the cluster:**
```bash
docker-compose up -d
```
*(Wait ~30 seconds for Kafka and HDFS to achieve quorum).*

**2. Trigger a simulated incident:**
Force the `payment-service` to breach its SLA (spiking latency from ~50ms to >2000ms).
```bash
curl -X POST "http://localhost:8081/api/anomaly/toggle?enable=true"
```

**3. Open the dashboard:** [http://localhost:3000](http://localhost:3000)

The React dashboard (`frontend/`) shows live P99 per service, the SLA breach log, a live span tail, Hive warehouse analytics, and the health of every pipeline stage. The anomaly can be injected from the UI into any service.

**4. Identify the bottleneck via Hive:**
```bash
# Execute the SLA violation query against the data warehouse
docker exec -it hive-server hive -f /workspace/queries/01_detect_sla_violations.sql
```
## 📊 Dashboard & API

```text
[React dashboard :3000] ──nginx /api──▶ [trace-producer :8081]
                                          ├─ live: tails Kafka (own consumer group), 10s/5s windows in memory
                                          ├─ history: Hive JDBC → tracemesh_db.service_latency_metrics
                                          └─ health: probes Kafka, Spark master, HDFS NameNode, HiveServer2
```

| Endpoint | Description |
|---|---|
| `GET  /api/metrics/live` | Current window per service (P99, avg, spans, 5xx, breach flag) plus cluster KPIs |
| `GET  /api/metrics/history?points=120` | P99 per service for each 5s tick |
| `GET  /api/stream` | Server-sent events: a `snapshot` every tick and on every anomaly toggle |
| `GET  /api/incidents` | SLA breaches (open on first window over SLA, resolve on first window back under) |
| `GET  /api/spans/recent?limit=50&service=` | Latest raw spans from Kafka |
| `POST /api/anomaly/toggle?enable=true&service=payment-service` | Inject latency (`service` optional, defaults to payment-service) |
| `GET  /api/anomaly/status` | Anomaly state |
| `GET  /api/warehouse/queries` | The three warehouse queries |
| `GET  /api/warehouse/queries/{id}?service=` | Run `sla-violations`, `service-trend` or `historical-summary` on Hive |
| `GET  /api/pipeline/health` | Status of each pipeline stage |

The SLA threshold (default 1000 ms) and window sizes are in `trace-producer/src/main/resources/application.yml`.

**Frontend dev mode** (hot reload against the backend on :8081):
```bash
cd frontend && npm install && npm run dev
```
Then open http://localhost:5173.
