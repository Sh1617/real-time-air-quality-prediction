# 🌍 Air Quality Streaming & ML Pipeline

## 📌 Overview

This project builds an **end-to-end data pipeline** for analyzing air quality data using **Kafka, PySpark, and batch processing**. It simulates real-time data streaming, processes it using Spark, and performs analytical computations such as **city-wise AQI ranking and window-based aggregations**.

---

## 🚀 Features

* 📡 Real-time data streaming using Kafka
* ⚡ Stream processing with PySpark
* 🗂️ Batch processing for historical data
* 📊 Sliding window aggregation
* 🏙️ City-wise air quality ranking
* 🔮 *(Future Scope)* Machine Learning-based AQI prediction

---

## 🏗️ Project Architecture

```
Kafka Producer → Kafka Topic → Spark Streaming → Processing → Output
                           ↘ Batch Processing ↗
```

---

## 📁 Project Structure

```
air-quality-streaming-ml/
│── airquality_producer.py   # Kafka producer for streaming data
│── airquality_consumer.py   # Spark streaming consumer
│── airquality_batch.py      # Batch processing script
│── README.md
```

---

## 🛠️ Tech Stack

* Python
* Apache Kafka
* PySpark (Structured Streaming)
* Google Cloud Storage (GCS) *(if used)*

---

## ▶️ How to Run

### 1. Start Kafka

Make sure Kafka server is running locally.

### 2. Run Producer

```
python airquality_producer.py
```

### 3. Run Consumer (Streaming)

```
python airquality_consumer.py
```

### 4. Run Batch Processing

```
python airquality_batch.py
```

---

## 📊 Use Cases

* Real-time environmental monitoring
* Smart city analytics
* Pollution trend analysis
* Data engineering + ML pipeline foundation

---

## 🔮 Future Enhancements

* Add Machine Learning models for AQI prediction
* Deploy pipeline on cloud (GCP/AWS)
* Build dashboard for visualization
* Integrate real-time alerts system

---
