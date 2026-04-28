# 🚗 Fleet Driver Behaviour Analytics Pipeline

## 📌 Project Overview
This project analyzes fleet vehicle GPS data to evaluate **driver behaviour, safety, and risk levels** using modern data engineering tools.

It demonstrates an end-to-end pipeline using:

- Azure Databricks (PySpark)
- Delta Lake (Medallion Architecture)
- Power BI (Dashboard)
- Notion (Project Planning)
- draw.io (Architecture Design)

---

## 🧱 Architecture

The solution follows a **Medallion Architecture (Bronze → Silver → Gold)**.

📊 Architecture Diagram:
```
docs/architecture.png
```

🧩 Editable Diagram:
```
docs/architecture.drawio
```

---

## 🗂 Project Planning (Notion)

Project planning and execution tracking was managed using **Notion**, including:

- Epics & Tasks breakdown
- Daily progress tracking
- Data pipeline milestones
- Dashboard development stages

📌 Notion Structure:
- Epic 1: Data Ingestion (Bronze)
- Epic 2: Data Cleaning (Silver)
- Epic 3: Aggregation (Gold)
- Epic 4: Power BI Dashboard
- Epic 5: Documentation & GitHub

> *(Optional: Add your Notion link here if public)*

---

## 🔄 Data Pipeline

### 🟤 Bronze Layer
- Raw GPS data ingestion
- Metadata columns added:
  - `_ingest_ts`
  - `_source_file`

---

### ⚪ Silver Layer
- Data cleaning & transformation
- Remove duplicates
- Feature engineering:
  - Overspeed events
  - Harsh braking events
  - Harsh acceleration events
  - Idle time

---

### 🟡 Gold Layer
- Driver-level aggregation
- Metrics:
  - Average safety score
  - Risk classification:
    - Low
    - Medium
    - High

---

## 📊 Power BI Dashboard

The dashboard provides:

- 🚦 Driver Risk Distribution
- 📈 Average Driver Safety Score
- 🚗 Driver Performance Summary
- ⚠️ Top Overspeeding Drivers
- 🛑 Harsh Braking Analysis

Dashboard file:
```
dashboard/fleet_dashboard.pbix
```

---

## 📂 Project Structure

```
fleet-driver-analytics-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_data_cleaning.py
│   ├── 03_gold_aggregation.py
│
├── data/
│   └── sample_vehicle_data.csv
│
├── docs/
│   ├── architecture.png
│   ├── architecture.drawio
│   └── data_description.md
│
├── dashboard/
│   └── fleet_dashboard.pbix
│
└── README.md
```

---

## 📊 Dataset

Sample dataset included:
```
data/sample_vehicle_data.csv
```

Fields:
- VehicleId
- VehicleNo
- Latitude
- Longitude
- Location
- Datetime
- Speed
- Ignition
- Direction
- GPSstatus

> ⚠️ Full dataset (~500K+ records) not included due to privacy & size.

---

## 🚀 How to Run

1. Upload data to Azure Data Lake / Databricks Volume  
2. Run notebooks:
   - Bronze → Silver → Gold  
3. Validate Gold tables  
4. Connect Power BI to Databricks  
5. Open dashboard  

---

## 🧠 Key Insights

- Identifies risky driving behaviour
- Calculates driver safety score
- Enables fleet monitoring & optimization
- Provides business-ready analytics dashboard

---

## 🛠 Tech Stack

- Azure Databricks  
- PySpark  
- Delta Lake  
- Power BI  
- Notion  
- draw.io  

---

## 👤 Author

**Sai Krishna Reddy**  
Aspiring Data Engineer | Cloud & Analytics Enthusiast  

📌 LinkedIn: https://www.linkedin.com/in/sai-krishna-reddy-k-14008b27a/  
📧 Email: saikrishnareddy478@example.com  

---

## ⭐ Support

If you found this project useful, give it a ⭐ on GitHub!
