# Transit Punctuality Tracker

A production-grade data engineering pipeline analyzing real-time public transport punctuality and schedule adherence for Chișinău, Moldova's transit system. Built using Apache Spark, Delta Lake, and the medallion architecture pattern to process and analyze vehicle tracking data at scale.

##  Project Overview

This project implements an end-to-end data pipeline that:
- **Collects** real-time vehicle positions from the Tranzy.ai API every 25 seconds during operating hours - DONE
- **Processes** GTFS-compliant transit data (routes, trips, stops, schedules) using Apache Spark - DONE
- **Calculates** schedule adherence by detecting vehicle arrivals at stops using geospatial distance calculations - DONE
- **Aggregates** performance metrics including average speeds, punctuality scores, and inferred schedules - DONE
- **Serves** analytical insights via SQL Server for downstream reporting and visualization - Under Development

### Key Business Questions Answered
- **Main Objective** - Can we infer a data-based schedule based on historical arrival patterns?
- What is the average speed of vehicles by route and time of day?
- How many actual arrivals occurred vs. expected arrivals at each stop?
- Which routes and time periods show the best/worst punctuality?

### Medallion Architecture (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                          BRONZE LAYER                            │
│                    (Raw Data Ingestion)                          │
├─────────────────────────────────────────────────────────────────┤
│  • Delta Lake partitioned by ingestion date                      │
│  • JSON format with metadata timestamps                          │
│  • Append-only for vehicles (time-series)                        │
│  • Overwrite for reference data (routes, stops, trips)           │
│                                                                   │
│  Datasets: vehicles, stops, routes, trips, stop_times           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER                            │
│                    (Cleaned & Enriched)                          │
├─────────────────────────────────────────────────────────────────┤
│  fact_vehicle_speed                                              │
│  • Grain: One row per vehicle per poll                           │
│  • Metrics: speed, position, timestamp                           │
│  • Dimensions: route_id, hour, date_key                          │
│                                                                   │
│  fact_stop_arrival                                               │
│  • Grain: One row per vehicle arrival at a stop                  │
│  • Haversine distance calculation (vehicles within 20m of stop)  │
│  • Deduplication using LAG() window function (25-min window)     │
│  • Dimensions: route_id, stop_id, stop_sequence, timestamp       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER                             │
│                     (Business Aggregates)                        │
├─────────────────────────────────────────────────────────────────┤
│  • Stored in SQL Server for BI tools                             │
│                                                                   │
│  agg_speed_hourly                                                │
│  • Average speed by route, hour, date                            │
│  • Weekend flag for time-of-week analysis                        │
│                                                                   │
│  agg_arrivals_adherence                                          │
│  • Expected vs actual arrivals per stop per day                  │
│  • Delta calculation for punctuality scoring                     │
│                                                                   │
│  inferred_schedule                                               │
│  • Statistical schedule inference from historical arrivals       │
│  • Average arrival time by route, stop, day of week, trip seq    │
└─────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Layer | Technology                | Purpose |
|-------|---------------------------|---------|
| **Data Processing** | Apache Spark 3.5.7        | Distributed data processing engine |
| **Storage** | Delta Lake 3.3.0          | ACID transactions, time travel, schema evolution |
| **Database** | Microsoft SQL Server 2022 | Gold layer analytics storage |
| **API Client** | Tranzy.ai API             | Real-time GTFS data source |
| **Orchestration** | Python Schedule           | Job scheduling and automation |
| **API Layer** | FastAPI                   | RESTful API endpoints (in development) |
| **Containerization** | Docker Compose            | SQL Server deployment |

##  Data Model

### Fact Tables (Silver Layer)

#### fact_vehicle_speed
```sql
vehicle_id     STRING
route_id       STRING
trip_id        STRING
latitude       DOUBLE
longitude      DOUBLE
speed          DOUBLE
timestamp      TIMESTAMP
hour           INT
date_key       DATE
```

#### fact_stop_arrival
```sql
vehicle_id          STRING
route_id            STRING
trip_id             STRING
stop_id             STRING
stop_name           STRING
stop_sequence       INT
arrival_timestamp   TIMESTAMP
hour                INT
date_key            DATE
```

### Aggregate Tables (Gold Layer)

#### agg_speed_hourly
```sql
route_id        STRING
hour            INT
date_key        DATE
day_of_week     INT
month           INT
avg_speed       DECIMAL(4,1)
is_weekend      INT
sample_count    INT
```

#### agg_arrivals_adherence
```sql
route_id                   STRING
stop_id                    STRING
date_key                   DATE
expected_daily_arrivals    INT
actual_arrivals            INT
delta                      INT     -- (actual - expected)
```

#### inferred_schedule
```sql
route_id              STRING
stop_id               STRING
day_of_week           INT
trip_sequence         INT
avg_arrival_minutes   DOUBLE   -- Minutes since midnight
sample_count          INT
```

## 🔧 Key Technical Implementations

### 1. Geospatial Stop Arrival Detection

Implements the Haversine formula to calculate great-circle distance between vehicle positions and stops:

```python
distance_m = 6371000 * 2 * ATAN2(
    SQRT(
        POWER(SIN(RADIANS(stop_lat - vehicle_lat) / 2), 2) +
        COS(RADIANS(vehicle_lat)) * COS(RADIANS(stop_lat)) *
        POWER(SIN(RADIANS(stop_lon - vehicle_lon) / 2), 2)
    ),
    SQRT(1 - (...))
)
```

Vehicles are considered "arrived" when within 20 meters of a stop.

### 2. Intelligent Deduplication

Uses LAG() window function to prevent duplicate arrival records:

```sql
LAG(arrival_timestamp) OVER (
    PARTITION BY vehicle_id, stop_id 
    ORDER BY arrival_timestamp
) as prev_arrival

-- Filter: Keep only if 25+ minutes since last arrival
WHERE prev_arrival IS NULL 
   OR arrival_timestamp > prev_arrival + INTERVAL 25 MINUTES
```

This handles cases where vehicles idle near stops or make multiple passes through a stop area.

### 3. Automated Data Collection

- **Vehicle polling**: Every 25 seconds during operating hours (6 AM - 1 AM)
- **Route planning refresh**: Every 4 weeks
- **Bronze → Silver transformation**: Daily at 2:00 AM
- **Silver → Gold aggregation**: Weekly

### 4. Incremental Processing

The pipeline supports both full rebuild and incremental processing:
- Full rebuild: Processes entire Bronze layer history
- Incremental: Filters Bronze data by `_ingest_date = yesterday`

This enables efficient backfills and daily updates without reprocessing all historical data.

## 📁 Project Structure

```
transit-punctuality-tracker/
├── spark_script.py           # Core ETL pipeline (Bronze/Silver/Gold)
├── sources.py                # Tranzy API client wrapper
├── schedule_.py              # Automated scheduling logic
├── scrape_route_schedule.py  # PDF schedule scraper for validation
├── endpoints.py              # FastAPI REST endpoints (WIP)
├── migration.py              # Delta Lake migration utilities
├── docker-compose.yaml       # SQL Server container setup
├── requirements.txt          # Python dependencies
└── .gitignore
```

## 🚀 Setup & Installation

### Prerequisites

- Python 3.12
- Java 17
- Apache Spark 3.5.7
- Docker
- 10GB+ free disk space for data storage

### 1. Spark Installation
```bash
wget https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz
tar -xzf spark-3.5.7-bin-hadoop3.tgz -C ~/spark
```

Add to `~/.bashrc`:
```bash
export SPARK_HOME=~/spark/spark-3.5.7-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

### 2. Required JARs
Download to `$SPARK_HOME/jars/`:
```bash
cd $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.0/delta-storage-3.3.0.jar
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar
```

### 3. Python Environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. MSSQL (Docker)
```bash
docker-compose up -d
```

### 5. Environment Variables
Create `.env`:
```
TRANZY_KEY=your_api_key
DB_URL=jdbc:sqlserver://localhost:1433;databaseName=gold;encrypt=false
DB_USER=sa
DB_PASSWORD=YourStrongPassword
```

### 6. Initialize Database Schema
```sql
CREATE DATABASE gold;
GO
USE gold;
GO
CREATE SCHEMA gold;
GO
```

### 7. Run Initial Data Load
```python
from schedule_ import run_bronze_to_silver, run_silver_to_gold

run_bronze_to_silver(rebuild=True)
run_silver_to_gold()
```

### 8. Start Automated Scheduler
```bash
python schedule_.py
```

## 📈 Usage Examples

### Query Average Speed by Route

```sql
SELECT 
    route_id,
    hour,
    AVG(avg_speed) as avg_speed_kmh,
    SUM(sample_count) as total_samples
FROM gold.agg_speed_hourly
WHERE date_key >= DATEADD(day, -7, GETDATE())
GROUP BY route_id, hour
ORDER BY route_id, hour;
```

### Analyze Schedule Adherence

```sql
SELECT 
    route_id,
    stop_id,
    AVG(delta) as avg_punctuality_delta,
    AVG(CAST(actual AS FLOAT) / expected) as adherence_rate
FROM gold.agg_arrivals_adherence
WHERE date_key >= DATEADD(day, -30, GETDATE())
GROUP BY route_id, stop_id
HAVING AVG(CAST(actual AS FLOAT) / expected) < 0.8  -- Less than 80% adherence
ORDER BY adherence_rate;
```

### Get Inferred Schedule

```sql
SELECT 
    route_id,
    stop_id,
    day_of_week,
    trip_sequence,
    FLOOR(avg_arrival_minutes / 60) as hour,
    avg_arrival_minutes % 60 as minute,
    sample_count
FROM gold.inferred_schedule
WHERE route_id = '5' AND day_of_week = 2  -- Route 5, Monday
ORDER BY trip_sequence;
```

##  Data Pipeline Flow

1. **Ingestion (Bronze)**
   - Poll Tranzy API every 25 seconds for vehicle positions
   - Fetch reference data (routes, stops, trips) weekly
   - Store raw JSON in Delta Lake with ingestion timestamps
   - Partition by `_ingest_date` for efficient querying

2. **Transformation (Silver)**
   - Parse JSON to structured DataFrames
   - Join vehicles with trips, stops, and stop_times
   - Calculate distances using Haversine formula
   - Detect arrivals (distance < 20m)
   - Deduplicate using window functions
   - Create fact tables with proper grain

3. **Aggregation (Gold)**
   - Calculate hourly speed averages by route
   - Compare expected vs actual arrivals
   - Infer schedules from historical patterns
   - Write to SQL Server for BI tools

## 🎓 Learning Outcomes & Technical Highlights

This project demonstrates:

-  **Medallion Architecture** implementation following industry best practices
-  **Apache Spark** for large-scale data processing with optimized transformations
-  **Delta Lake** for ACID transactions and time-travel capabilities
-  **Complex SQL** including window functions (LAG, ROW_NUMBER), CTEs, and aggregations
-  **Geospatial Analysis** using Haversine distance calculations
-  **Data Modeling** with proper fact/dimension design and grain definition
-  **Performance Optimization** through partitioning and incremental processing
-  **Automated Orchestration** with scheduled data collection and transformation
-  **API Integration** with error handling and retry logic
-  **Deduplication Logic** handling edge cases in real-time data streams

## 🐛 Known Issues & Future Enhancements

### Known Issues
- [ ] Arrival counts inflated - investigating whether Haversine formula or deduplication window needs tuning
- [ ] Expected arrivals calculation returning 1 - GTFS trip/stop_times join logic needs review

### In Progress
- [ ] Complete FastAPI endpoints for programmatic access
- [ ] Add data quality checks and alerting
- [ ] Implement streaming with Spark Structured Streaming
- [ ] Add comprehensive unit and integration tests

### Future Enhancements
- [ ] Real-time dashboard with Apache Superset or Grafana
- [ ] Predictive modeling for arrival time forecasting
- [ ] Historical trend analysis and anomaly detection
- [ ] Integration with external weather data
- [ ] Route optimization recommendations
- [ ] Mobile app integration

## Data Source

This project uses data from [Tranzy.ai](https://tranzy.ai/), which provides GTFS-compliant real-time transit data for Chișinău, Moldova, operated by RTEC & PUA.


## 👤 Author
**Cristi** - Data Enthusiast 
- Location: Chișinău, Moldova
- GitHub: [@Kronbach](https://github.com/Kronbach)


