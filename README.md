# 🛰️ GNSS Real-Time Anomaly Detection System

A real-time system for processing GNSS NMEA data streams and detecting anomalies using Apache Kafka and Apache Spark Streaming.

To Run Manually: 
for Start:
cd c:\Users\Admin\Desktop\gnss-project-main
docker compose up --build

for Stop:
docker compose down

URL:  http://localhost:8501

## 📖 System Overview

The **GNSS Real-Time Anomaly Detection System** is an enterprise-grade, distributed data pipeline designed to ingest, process, and analyze Global Navigation Satellite System (GNSS) data in real time. It monitors live coordinates and signal metadata to instantly detect anomalies such as signal degradation, physical blockages, and unexpected coordinate jumps.

This project is built to handle high-throughput, continuous data streams from hardware GNSS receivers, providing instant alerting and actionable mitigation strategies.

---

## 🏗️ System Architecture & Full Pipeline

The system is organized into a highly decoupled, scalable architecture utilizing **Apache Kafka** for message brokering and **Apache Spark** for stream processing.

```text
===================================================================================================
                                PHASE 1: DATA ACQUISITION
===================================================================================================
[ 🛰️ GNSS Satellites ] (GPS, GLONASS, Galileo) 
          │ 
          │ (Radio Frequency L1/L2 Signals)
          ▼
[ 📡 Hardware Receivers / 🌐 NTRIP Internet Casters ]
          │ 
          │ Streams Raw Serial Data (1Hz - 10Hz)
          │ Protocols: NMEA 0183 / RTCM v3
          ▼
===================================================================================================
                              PHASE 2: INGESTION & PARSING
===================================================================================================
[ 🐍 Python Kafka Producer ] (producer/kafka_producer.py)
          │ 
          ├─► Extracts $GPGGA: Parses Latitude, Longitude, Altitude, Fix Quality
          ├─► Extracts $GPGSV: Parses Satellite IDs, SNR (Signal-to-Noise Ratio), Azimuth
          │
          │ Converts raw sentences into unified, time-stamped JSON Payloads
          ▼
===================================================================================================
                             PHASE 3: HIGH-THROUGHPUT QUEUEING
===================================================================================================
[ 📨 Apache Kafka Broker ] (Topic: `gnss_topic`)
          │ 
          │ Buffers incoming JSON data to ensure no packets are lost during traffic spikes.
          │ (Distributed, fault-tolerant, horizontally scalable)
          ▼
===================================================================================================
                            PHASE 4: REAL-TIME ANOMALY DETECTION
===================================================================================================
[ ⚡ Apache Spark Streaming Engine ] (consumer/spark_consumer.py)
          │ 
          │ Ingests data in micro-batches (e.g., every 2 seconds).
          │ Applies mathematical algorithms to detect issues:
          │ 
          ├─► 1. SIGNAL_DROP Detection: Checks if SNR < 20.0 dB
          ├─► 2. SATELLITE_LOSS Detection: Checks if visible satellites < 5
          ├─► 3. POSITION_DRIFT Detection: Calculates Euclidean distance against base station
          │
          │ Appends boolean flags (e.g., "anomaly_detected": True) to the JSON payload
          ▼
===================================================================================================
                                PHASE 5: EVENT DISPATCHING
===================================================================================================
[ 📨 Apache Kafka Broker ] (Topic: `processed_gnss`)
          │
          │ Receives the analyzed, flagged data back from Spark.
          │ 
          ├────────────────────────────────────────────────────────────────────────┐
          ▼                                                                        ▼
============================================       ================================================
     PHASE 6A: STORAGE & PERSISTENCE                    PHASE 6B: VISUALIZATION & MITIGATION
============================================       ================================================
[ 💾 Persistent Storage Engine ]                   [ 🖥️ Streamlit Interactive Dashboard ]
          │                                                  │
          │ Intercepts any payload where                     ├─► Live Geospatial Map (Folium)
          │ `anomaly_detected == True`.                      ├─► Time-Series Metrics (Plotly)
          │                                                  │
          ▼                                                  ▼
[ 📄 `results/anomaly_history.csv` ]               [ 🧠 Expert System (anomaly_analyzer.py) ]
Stores anomalies permanently to hard disk          Reads flagged data and outputs human-readable
for future Machine Learning / Data Science.        "Root Cause Analysis" & "Prevention Strategies".
===================================================================================================
```

---

## 🔬 Detailed Step-by-Step Breakdown

### Step 1: Data Ingestion & Signal Parsing
**Module:** `producer/kafka_producer.py`, `utils/nmea_parser.py`, `utils/rtcm_decoder.py`

The pipeline begins by ingesting live GNSS data from multiple supported sources:
- **NTRIP (Networked Transport of RTCM via Internet Protocol)**: Connects directly to real-world GNSS base stations (e.g., AUSCORS) to stream high-precision RTK data.
- **TCP/IP**: Reads raw streams from local hardware receivers via network sockets.
- **Test Generation**: Generates synthetic, mathematically simulated GNSS paths with randomized severe anomalies for system validation.

**Parsing Algorithm:**
The system reads the byte stream and identifies sentence boundaries. For **NMEA 0183** data, it extracts:
- `$GPGGA` sentences: Extracts Latitude, Longitude, Altitude, and Fix Quality.
- `$GPGSV` sentences: Extracts Satellite IDs, Elevation, Azimuth, and most importantly, **Signal-to-Noise Ratio (SNR)**.

The extracted multi-sentence data is synchronized by timestamp, aggregated into a single JSON payload representing the current epoch, and published to Apache Kafka.

### Step 2: Message Brokering (Apache Kafka)
**Component:** `gnss_topic`

To ensure that the system can handle thousands of GNSS messages per second without dropping packets, we use Apache Kafka as a buffer. Kafka decouples the producer (data ingestion) from the consumer (data processing), allowing the system to scale horizontally and providing fault tolerance if the processing engine temporarily goes offline.

### Step 3: Anomaly Detection Engine (Apache Spark Streaming)
**Module:** `consumer/spark_consumer.py`

Apache Spark continuously consumes the JSON stream from Kafka using micro-batch processing. It loads the data into a distributed DataFrame and applies a suite of deterministic algorithms to flag anomalies in real-time.

**Algorithms Used:**
1. **Signal Drop Detection (Thresholding Algorithm)**:
   - *Logic*: The system evaluates the SNR of individual satellites against a configured baseline (`snr_threshold`, default 20.0 dB). 
   - *Phenomenon Detected*: Atmospheric interference, heavy cloud cover, or multipath reflections (signals bouncing off buildings).

2. **Satellite Loss Detection**:
   - *Logic*: Evaluates the total count of actively tracked satellites in the current epoch. If `num_sats < min_satellites`, an anomaly is flagged.
   - *Phenomenon Detected*: Physical sky-view blockage, such as entering a tunnel, an "urban canyon", or experiencing severe localized RF jamming.

3. **Position Drift Detection (Euclidean Distance Algorithm)**:
   - *Logic*: To detect impossible jumps in location, the algorithm calculates the distance between the incoming coordinate and a known reference base station (or the previous epoch).
   - *Math*: It utilizes a scaled Euclidean distance approximation for fast processing. It converts degrees to meters using the constant `111,320 meters/degree` for latitude, and scales longitude by $\cos(\text{latitude})$.
   - $\text{Distance} = \sqrt{(\Delta \text{lat} \times 111320)^2 + (\Delta \text{lon} \times 111320 \times \cos(\text{lat}))^2}$
   - If the calculated distance exceeds the `drift_threshold`, a `POSITION_DRIFT` is flagged.
   - *Phenomenon Detected*: Loss of RTK correction stream, severe spoofing attacks, or catastrophic multipath failure.

### Step 4: Expert Mitigation System
**Module:** `utils/anomaly_analyzer.py`

Detecting an anomaly is only the first step. The system includes a Rule-Based Expert System that maps the detected anomaly flags to real-world physics. It generates a **Root Cause Analysis** and provides immediate, actionable **Mitigation Strategies** (e.g., advising the operator to inspect antenna cables, switch to multi-constellation GNSS, or integrate an Inertial Measurement Unit for dead reckoning).

### Step 5: Visualization & Persistence
**Module:** `dashboard/app.py`

The processed data stream (now containing boolean anomaly flags) is consumed by the frontend dashboard.
- **Geospatial Mapping**: Uses `Folium` and `Leaflet.js` to draw the receiver's historical path and drop color-coded markers for anomalies.
- **Time-Series Analytics**: Uses `Plotly` to render live line charts tracking SNR degradation and satellite availability over time.
- **Persistent Storage**: To enable future Machine Learning training, the dashboard intercepts any data points flagged as anomalous and permanently appends them to a local database file (`results/anomaly_history.csv`).

---

## 📂 Project Structure

```text
gnss-project/
├── config/
│   └── settings.yaml          # System configuration & threshold parameters
├── consumer/
│   └── spark_consumer.py      # Apache Spark streaming anomaly detection engine
├── dashboard/
│   └── app.py                 # Streamlit interactive UI & analytics
├── producer/
│   └── kafka_producer.py      # Kafka producer (NTRIP/TCP/Test ingestion & parsing)
├── utils/
│   ├── anomaly_analyzer.py    # Expert system mapping anomalies to physical mitigations
│   ├── nmea_parser.py         # Custom NMEA 0183 protocol decoder
│   └── rtcm_decoder.py        # RTCM v3 format decoder for RTK corrections
├── results/                   
│   └── anomaly_history.csv    # Permanent persistent storage for flagged events
├── docker-compose.yml         # Container orchestration configuration
└── readme.md                  # Project documentation
```

---

## 🚀 Execution Guide

The entire distributed system is containerized for seamless, one-click execution using Docker.

1. Ensure **Docker Desktop** is installed and running.
2. Open your terminal and navigate to the project root:
   ```powershell
   cd c:\Users\Admin\Desktop\gnss-project-main
   ```
3. Boot the complete pipeline (Kafka, Zookeeper, Spark, Dashboard):
   ```powershell
   docker compose up --build
   ```
4. Access the Live Operations Dashboard:
   Open your browser and navigate to **[http://localhost:8501](http://localhost:8501)**

To safely spin down the distributed system and save all state, press `Ctrl+C` in the terminal or run:
```powershell
docker compose down
```
