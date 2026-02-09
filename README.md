# Chișinău Public Transit Analytics Pipeline

Real-time vehicle tracking and analytics for Chișinău's public transportation system. Built with Apache Spark, Delta Lake, and FastAPI.

## Overview

This pipeline ingests live GPS data from RTEC (Regia Transport Electric Chișinău), processes it through a medallion architecture, and serves analytics via REST API. It tracks ~500 vehicles across 50+ routes, detecting stop arrivals and inferring schedules from historical patterns.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  RTEC API   │────▶│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  (GPS data) │     │  (raw JSON) │     │ (facts/dims)│     │(aggregates) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                      Delta Lake          Delta Lake            MSSQL
                                                                   │
                                                                   ▼
                                                              FastAPI
```

### Bronze Layer
Raw JSON ingested every 25 seconds via threaded buffer:
- `vehicles` - GPS positions, speed, route assignments
- `stops` - Stop locations and names
- `routes` - Route definitions
- `trips` - Trip patterns per route
- `stop_times` - Stop sequences per trip

### Silver Layer
Cleaned and transformed data:

| Table | Description |
|-------|-------------|
| `fact_vehicle_speed` | Vehicle positions with validated speed readings |
| `fact_stop_arrival` | Detected arrivals using Haversine distance (<30m from stop) |
| `dim_stops` | Stop reference data |
| `dim_trips` | Trip reference data |
| `dim_stop_times` | Stop sequence reference data |

### Gold Layer
Business-ready aggregations served via MSSQL:

| Table | Description |
|-------|-------------|
| `agg_arrivals_daily` | Daily arrival counts, vehicle coverage, headway per stop |
| `agg_speed_hourly` | Hourly speed statistics per route |
| `inferred_schedule` | Average arrival times derived from historical data |

## Key Implementations

### Arrival Detection (Haversine)
```sql
-- Detect when vehicle is within 30m of a stop
6371 * 2 * ASIN(SQRT(
    POWER(SIN(RADIANS(stop_lat - vehicle_lat) / 2), 2) +
    COS(RADIANS(vehicle_lat)) * COS(RADIANS(stop_lat)) *
    POWER(SIN(RADIANS(stop_lon - vehicle_lon) / 2), 2)
)) AS distance_km
```

### Deduplication (LAG)
```sql
-- Remove consecutive duplicate positions
LAG(latitude) OVER (PARTITION BY id ORDER BY timestamp) != latitude
OR LAG(longitude) OVER (PARTITION BY id ORDER BY timestamp) != longitude
```

### Threaded Buffer
Decouples API polling (25s) from Delta writes (~90s):
```python
# Fetch thread: polls API → buffer (fast)
# Write thread: buffer → Delta Lake (slow, batched)
# Reduces commits from ~2700/day to ~144/day
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /infer_schedule/{route_id}/{is_weekend}` | Inferred timetable by stop |
| `GET /arrivals/{route_id}?date_from=&date_to=` | Daily arrival statistics |
| `GET /speed/{route_id}?date_from=&date_to=` | Hourly speed statistics |

API docs: `http://localhost:8000/docs`

## Setup

### Prerequisites
- Python 3.12+
- Java 17
- Docker (for MSSQL)

### Installation
```bash
# Clone and setup
git clone https://github.com/yourusername/chisinau-transit-analytics.git
cd chisinau-transit-analytics
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Download Spark JARs
cd $SPARK_HOME/jars
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0.jar
wget https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.0/delta-storage-3.3.0.jar
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar

# Start MSSQL
docker-compose up -d

# Run pipeline
python schedule_.py
```

### Environment Variables
```bash
export SPARK_HOME=~/spark/spark-3.5.7-bin-hadoop3
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export DB_URL="jdbc:sqlserver://localhost:1434;databaseName=transit;encrypt=false"
export DB_USER="sa"
export DB_PASSWORD="YourPassword"
```

## Scheduler

| Time | Job |
|------|-----|
| 06:00-01:00 | Vehicle polling (every 25s, buffered) |
| 02:02 | Bronze → Silver transform (daily) |
| 02:30 | Silver → Gold transform (weekly) |
| Monthly | GTFS reference data refresh |

## Data Quality

Sample metrics (2026-01-28):
- 518 unique vehicles tracked
- 1,114 stops monitored
- 39,503 arrivals detected
- ~76 arrivals per vehicle per day

## Known Limitations

### GTFS Data Constraint
The source GTFS data contains route patterns only (2 trips per route - one per direction), not complete schedules with frequencies. This means:

- **No expected arrivals**: Cannot calculate schedule adherence against planned service
- **Inferred patterns**: Service frequency is derived from actual vehicle positions
- This is common with real-world transit data where GTFS is used for routing, not operations

### Inferred Schedule Variance
Schedule inference averages trip sequences across days. Variance increases when daily service levels differ (holidays, disruptions, reduced service). Rows with high `stddev_minutes` should be treated as less reliable.

## Resolved Challenges

### Duplicate Arrival Detection
**Problem**: Same arrival detected multiple times when vehicle lingered at stop.
**Solution**: LAG window function filters consecutive positions at same location, keeping only the first detection per stop visit.

### Delta Log Performance Degradation
**Problem**: Write times increased from ~2s to ~90s after 2000+ commits due to transaction log overhead.
**Solution**: Threaded buffer batches 24 polls into single write, reducing daily commits by 95%.

### Phantom Speed Readings
**Problem**: Vehicles reported movement while stationary (GPS drift).
**Solution**: Filter speeds < 2 km/h when consecutive positions are identical.

### Schedule Inference Variance
**Problem**: Trip sequence averaging produced 400+ minute stddev when polling start times varied between days.
**Solution**: Documented as limitation; future improvement could use time-bucket alignment or percentile-based sequencing.

## Roadmap

- [ ] Outlier day filtering for schedule inference (exclude holidays, disruptions)
- [ ] Real-time WebSocket endpoint for live vehicle positions
- [ ] Historical trend analysis (month-over-month comparisons)
- [ ] Integration with official GTFS-realtime feed if available

## Tech Stack

- **Processing**: Apache Spark 3.5.7
- **Storage**: Delta Lake 3.3.0
- **Database**: MSSQL 2022
- **API**: FastAPI
- **Scheduling**: Python schedule library

## Project Structure

```
├── spark_script.py      # Bronze/Silver/Gold transforms
├── schedule_.py         # Job scheduler and polling
├── api.py               # FastAPI endpoints
├── sources.py           # RTEC API client
├── helpers/
│   └── _utils.py        # Shared utilities (Haversine, view prep)
├── docker-compose.yaml  # MSSQL container
└── requirements.txt
```

## License

MIT
