# Real-Time Monitoring System

A real-time pipeline that tracks button clicks on a web page and turns them into live Grafana dashboards — built with **React, Spring Boot, Apache Kafka, Prometheus, and Grafana**.

Click a button on the frontend and, within seconds, watch the click appear on a Grafana graph.

## Architecture

```
 React frontend (5173)
        │  HTTP POST on button click
        ▼
 Producer service - Spring Boot (8080)
        │  publishes event
        ▼
 Apache Kafka (9092)
   topics: "funny" (Enroll Now) and "serious" (Buy Now)
        │  consumer pulls messages
        ▼
 Consumer service - Spring Boot (8081)
        │  +1 to a Prometheus counter per message,
        │  exposes counters at :8082/metrics
        ▼
 Prometheus (9090)  ── scrapes :8082/metrics every 5s
        ▼
 Grafana (3000)     ── queries Prometheus, draws live dashboards
```

Key idea: everything **left** of Kafka *pushes* data; everything **right** of Kafka *pulls* it. Kafka decouples the two sides — if the consumer is down, messages wait safely in Kafka and are processed when it returns (try it: stop the consumer, click 5 times, start it again, and watch the counter jump by 5).

| Component | Tech | Port | What it does |
|---|---|---|---|
| Frontend | React + Vite | 5173 | Two buttons that fire click events |
| Producer | Spring Boot | 8080 | REST endpoint → publishes to Kafka |
| Kafka | Apache Kafka (KRaft mode) | 9092 | Message broker; stores events durably |
| Consumer | Spring Boot | 8081 (+8082 for metrics) | Consumes events, counts them |
| Prometheus | Prometheus | 9090 | Scrapes and stores the counters over time |
| Grafana | Grafana | 3000 | Dashboards on top of Prometheus |

## Prerequisites

- **Java 21** (`java -version`)
- **Node.js 18+** (`node -v`)
- **Apache Kafka 4.x** — [download](https://kafka.apache.org/downloads) (no Zookeeper needed; Kafka 4.x runs in KRaft mode)
- **Prometheus** — [download](https://prometheus.io/download/) or `brew install prometheus` / `apt install prometheus`
- **Grafana** — [download](https://grafana.com/grafana/download) or `brew install grafana` / apt via [grafana.com docs](https://grafana.com/docs/grafana/latest/setup-grafana/installation/debian/)

Everything below assumes all services run on **one machine** (`localhost`). To split them across machines (e.g. Kafka on a cloud server), see [Running across machines](#running-across-machines).

## Running the system (step by step, in order)

### 1. Start Kafka

```bash
cd <your-kafka-folder>

# First time only: initialize the storage directory
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties

# Start the broker (daemon = keeps running in the background)
bin/kafka-server-start.sh -daemon config/server.properties

# Verify it's alive (empty list is fine on first run)
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

The topics `funny` and `serious` are created automatically on first use.

### 2. Start the consumer service

```bash
cd Consumer_kafka_service
./gradlew run
```

Wait for `Started App in ... seconds`. Then verify the metrics page exists:
open <http://localhost:8082/metrics> — you should find `kafka_funny_events_total 0.0`.

### 3. Start the producer service (new terminal)

```bash
cd Producer_kafka_service
./gradlew run
```

Verify without the frontend:

```bash
curl -X POST http://localhost:8080/producer/event/funny
```

The producer terminal should print `Message sent to Kafka, offset: 0` and the
consumer terminal `Received funny event: Button Clicked: Enroll Now`.
**If you see this, the whole Kafka pipeline works.**

### 4. Start the frontend (new terminal)

```bash
cd frontend
npm install     # first time only
npm run dev
```

Open <http://localhost:5173> and click **Enroll Now** / **Buy Now** — each click should appear in the consumer terminal.

### 5. Start Prometheus (new terminal)

```bash
prometheus --config.file=monitoring/prometheus.yml
```

Open <http://localhost:9090/targets> — the `kafka-consumer` target must show **UP**.
Then try a query on the Graph tab: `kafka_funny_events_total`.

### 6. Start Grafana and build a dashboard

```bash
brew services start grafana        # macOS
# or: sudo systemctl start grafana-server   (Linux)
```

1. Open <http://localhost:3000> — first login is `admin` / `admin`
2. **Connections → Data sources → Add data source → Prometheus**, URL `http://localhost:9090` → **Save & test**
3. **Dashboards → New → Add visualization**, then enter a query:
   - `kafka_funny_events_total` — total Enroll Now clicks
   - `rate(kafka_funny_events_total[1m])` — clicks per second (the fun one)
4. Set auto-refresh (top right) to **5s**, click buttons, and watch the graph move

## How it works — the details worth understanding

**Producer** ([MultiProducerController.java](Producer_kafka_service/app/src/main/java/org/example/controller/MultiProducerController.java)):
receives the HTTP POST from the frontend and calls `kafkaProducer.send()`. The send is asynchronous — a callback logs the broker's confirmation (the *offset*, i.e. the message's position in the topic).

**Consumer** ([ConsumerService.java](Consumer_kafka_service/app/src/main/java/org/example/service/ConsumerService.java)):
`@KafkaListener` methods are poll loops run by Spring in background threads. Each message increments a Prometheus `Counter`. The consumer belongs to the group `example-group` — Kafka remembers the group's read position, so no message is lost or double-counted across restarts.

**Metrics** ([PrometheusConfig.java](Consumer_kafka_service/app/src/main/java/org/example/config/PrometheusConfig.java)):
starts a tiny standalone HTTP server on port 8082 that prints all counters as plain text. `DefaultExports.initialize()` also exposes JVM health metrics (heap, threads, GC) — useful for monitoring the consumer itself.

**Prometheus** ([monitoring/prometheus.yml](monitoring/prometheus.yml)):
pull-based — it reads `:8082/metrics` every 5 seconds and stores each value with a timestamp. That history is what turns a single number into a graph.

**Grafana**: queries Prometheus with PromQL (e.g. `rate(...[1m])` = per-second rate averaged over the last minute) and renders it.

## Running across machines

This project also runs split across machines — for example Kafka + consumer + Prometheus + Grafana on a Linux server, with the frontend + producer on your laptop (that is how I deployed it). Three rules cover it:

1. **Addresses are relative to where the app runs.** An app on the same machine as Kafka uses `localhost:9092`; an app on another machine uses the server's address. The producer's `BOOTSTRAP_SERVERS` and the consumer's `BOOTSTRAP_SERVERS_CONFIG` are the two places to change.
2. **Kafka must advertise a reachable address.** In Kafka's `config/server.properties`, set
   `advertised.listeners=PLAINTEXT://<server-address>:9092` — clients connect to the bootstrap address first, but the broker then tells them where to send actual traffic; if it advertises `localhost`, remote clients hang.
3. **Open only the ports you need** in the server's firewall (9092 for Kafka clients, 3000/9090 for browsing Grafana/Prometheus), ideally restricted to your own IP. Port 8082 should stay private — only Prometheus needs it, from inside the server.

To run the consumer as a proper server process (instead of `gradlew run`):

```bash
./gradlew bootJar   # builds a single self-contained JAR
nohup java -Xmx256m -jar app/build/libs/app.jar > ~/consumer.log 2>&1 &
```

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Producer logs `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka isn't running, or the bootstrap address doesn't point at it |
| `Connection refused` from a remote machine | Firewall, or the service isn't running (refused = machine reachable, port closed) |
| Producer connects but sends time out | `advertised.listeners` is wrong (see rule 2 above) |
| One-time `UNKNOWN_TOPIC_OR_PARTITION` warning on the very first message | Normal — the topic is auto-created on first use; the message is retried and delivered |
| Prometheus target DOWN with `connection refused` on 8082 | Consumer isn't running, or Prometheus can't reach it |
| Consumer counters reset to 0 after restart | Expected — counters live in memory; Prometheus keeps the history, and `rate()` handles resets automatically |
| Clicks appear in Grafana ~10s late | Normal — scrape interval (5s) + dashboard refresh (5s) |

## Project structure

```
├── frontend/                      # React app (two buttons)
├── Producer_kafka_service/        # Spring Boot: HTTP -> Kafka
├── Consumer_kafka_service/        # Spring Boot: Kafka -> Prometheus counters
├── monitoring/prometheus.yml      # Prometheus scrape config
└── README.md
```
