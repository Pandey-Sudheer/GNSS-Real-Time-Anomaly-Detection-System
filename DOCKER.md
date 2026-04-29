# Docker Run Guide

## Start the stack

```bash
docker compose up --build
```

This starts:

- `kafka`
- `consumer`
- `producer`
- `dashboard`

Dashboard URL:

- `http://localhost:8501`

## Stop the stack

```bash
docker compose down
```

To also remove Kafka's persisted topic data:

```bash
docker compose down
```

Then delete:

- `docker-data/kafka`

## Where the data is stored

Kafka topic data is persisted on your machine at:

- `docker-data/kafka`

Spark/Ivy downloaded jars are cached at:

- `docker-data/ivy`

Spark checkpoints are persisted at:

- `docker-data/checkpoints`
- `docker-data/results/checkpoints`

Any result files written by the application go to:

- `docker-data/results`

## Data flow

- Producer writes JSON records to Kafka topic `gnss_topic`
- Consumer reads `gnss_topic` and writes processed JSON to Kafka topic `processed_gnss`
- Dashboard reads `processed_gnss`

With the current code, the main persisted stream data is in Kafka, not CSV files.
