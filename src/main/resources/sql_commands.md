How about a **real-time traffic monitoring system**? It's straightforward and easy to grasp for beginners. The system can track and analyze traffic data from different sensors or cameras placed at various intersections.

### Traffic Monitoring System

#### Overview

1. **Traffic Data**: Collect data including sensor ID, intersection ID, vehicle count, speed, and timestamp.
2. **Traffic Aggregation**: Compute average vehicle count and speed at each intersection.
3. **Anomaly Detection**: Identify intersections where traffic flow is unusually high.

### 1. **Drop Existing Tables**

```sql
DROP TABLE IF EXISTS traffic_data;
DROP TABLE IF EXISTS average_traffic_data;
DROP TABLE IF EXISTS high_traffic_alerts;
```

### 2. **Create Tables**

#### a. **Create `traffic_data` Table**

Ingest traffic data from Kafka:

```sql
CREATE TABLE traffic_data (
    sensor_id STRING,
    intersection_id STRING,
    vehicle_count INT,
    speed DOUBLE,
    event_timestamp TIMESTAMP(3),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'traffic-data',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);
```

#### b. **Create `average_traffic_data` Table**

Table to store average traffic data for each intersection:

```sql
CREATE TABLE average_traffic_data (
    intersection_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_vehicle_count DOUBLE,
    avg_speed DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'average-traffic-data',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);
```

#### c. **Create `high_traffic_alerts` Table**

Table to store alerts for high traffic conditions:

```sql
CREATE TABLE high_traffic_alerts (
    intersection_id STRING,
    alert_type STRING,
    alert_timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'high-traffic-alerts',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);
```

### 3. **Insert Data and Transform**

#### a. **Compute Average Traffic Data**

Aggregate average vehicle count and speed for each intersection over a 30-minute window:

```sql
INSERT INTO average_traffic_data
SELECT
    intersection_id,
    TUMBLE_START(event_timestamp, INTERVAL '30' MINUTE) AS window_start,
    TUMBLE_END(event_timestamp, INTERVAL '30' MINUTE) AS window_end,
    AVG(vehicle_count) AS avg_vehicle_count,
    AVG(speed) AS avg_speed
FROM traffic_data
GROUP BY intersection_id, TUMBLE(event_timestamp, INTERVAL '30' MINUTE);
```

#### b. **Generate High Traffic Alerts**

Identify intersections where the average vehicle count exceeds a certain threshold:

```sql
INSERT INTO high_traffic_alerts
SELECT
    intersection_id,
    'High Traffic' AS alert_type,
    CURRENT_TIMESTAMP AS alert_timestamp
FROM average_traffic_data
WHERE avg_vehicle_count > 50;  -- Threshold for high vehicle count
```


### 4. Calculate Moving Average of Vehicle Counts
Objective: Compute the moving average of vehicle counts over a 3-hour window.

```sql
-- Create a table to store moving averages
CREATE TABLE moving_averages (
    intersection_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_vehicle_count DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'moving-averages',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);
```

```sql
-- Insert moving averages into the moving_averages table
INSERT INTO moving_averages
SELECT
    intersection_id,
    HOP_START(event_timestamp, INTERVAL '1' HOUR, INTERVAL '3' HOUR) AS window_start,
    HOP_END(event_timestamp, INTERVAL '1' HOUR, INTERVAL '3' HOUR) AS window_end,
    AVG(vehicle_count) AS avg_vehicle_count
FROM traffic_data
GROUP BY
    intersection_id,
    HOP(event_timestamp, INTERVAL '1' HOUR, INTERVAL '3' HOUR);

```

### 5. **Cleanup**

To remove the tables:

```sql
DROP TABLE IF EXISTS traffic_data;
DROP TABLE IF EXISTS average_traffic_data;
DROP TABLE IF EXISTS high_traffic_alerts;
DROP TABLE IF EXISTS moving_averages;
```

### Sample_Data
```sql
INSERT INTO traffic_data VALUES
('sensor1', 'intersection1', 22, 34.0, TO_TIMESTAMP('2024-08-09 08:00:00')),
('sensor2', 'intersection1', 24, 32.0, TO_TIMESTAMP('2024-08-09 08:15:00')),
('sensor3', 'intersection2', 58, 27.0, TO_TIMESTAMP('2024-08-09 08:30:00')),
('sensor4', 'intersection2', 62, 28.5, TO_TIMESTAMP('2024-08-09 08:45:00')),
('sensor5', 'intersection3', 15, 41.0, TO_TIMESTAMP('2024-08-09 09:00:00')),
('sensor6', 'intersection3', 18, 40.5, TO_TIMESTAMP('2024-08-09 09:15:00')),
('sensor7', 'intersection4', 30, 35.0, TO_TIMESTAMP('2024-08-09 09:30:00')),
('sensor8', 'intersection4', 33, 34.0, TO_TIMESTAMP('2024-08-09 09:45:00')),
('sensor9', 'intersection5', 8, 50.0, TO_TIMESTAMP('2024-08-09 10:00:00')),
('sensor10', 'intersection5', 11, 48.0, TO_TIMESTAMP('2024-08-09 10:15:00')),
('sensor11', 'intersection6', 20, 38.0, TO_TIMESTAMP('2024-08-09 10:30:00')),
('sensor12', 'intersection6', 22, 37.5, TO_TIMESTAMP('2024-08-09 10:45:00')),
('sensor13', 'intersection7', 40, 31.0, TO_TIMESTAMP('2024-08-09 11:00:00')),
('sensor14', 'intersection7', 43, 30.5, TO_TIMESTAMP('2024-08-09 11:15:00')),
('sensor15', 'intersection8', 56, 29.0, TO_TIMESTAMP('2024-08-09 11:30:00')),
('sensor16', 'intersection8', 58, 28.5, TO_TIMESTAMP('2024-08-09 11:45:00')),
('sensor17', 'intersection9', 14, 46.0, TO_TIMESTAMP('2024-08-09 12:00:00')),
('sensor18', 'intersection9', 16, 45.5, TO_TIMESTAMP('2024-08-09 12:15:00')),
('sensor19', 'intersection10', 50, 23.0, TO_TIMESTAMP('2024-08-09 12:30:00')),
('sensor20', 'intersection10', 52, 22.5, TO_TIMESTAMP('2024-08-09 12:45:00')),
('sensor21', 'intersection1', 23, 33.0, TO_TIMESTAMP('2024-08-09 13:00:00')),
('sensor22', 'intersection1', 25, 32.0, TO_TIMESTAMP('2024-08-09 13:15:00')),
('sensor23', 'intersection2', 61, 26.0, TO_TIMESTAMP('2024-08-09 13:30:00')),
('sensor24', 'intersection2', 64, 27.0, TO_TIMESTAMP('2024-08-09 13:45:00')),
('sensor25', 'intersection3', 17, 40.0, TO_TIMESTAMP('2024-08-09 14:00:00')),
('sensor26', 'intersection3', 20, 39.5, TO_TIMESTAMP('2024-08-09 14:15:00')),
('sensor27', 'intersection4', 31, 34.0, TO_TIMESTAMP('2024-08-09 14:30:00')),
('sensor28', 'intersection4', 34, 33.0, TO_TIMESTAMP('2024-08-09 14:45:00')),
('sensor29', 'intersection5', 9, 49.0, TO_TIMESTAMP('2024-08-09 15:00:00')),
('sensor30', 'intersection5', 12, 47.0, TO_TIMESTAMP('2024-08-09 15:15:00')),
('sensor31', 'intersection6', 21, 37.0, TO_TIMESTAMP('2024-08-09 15:30:00')),
('sensor32', 'intersection6', 23, 36.5, TO_TIMESTAMP('2024-08-09 15:45:00')),
('sensor33', 'intersection7', 42, 30.0, TO_TIMESTAMP('2024-08-09 16:00:00')),
('sensor34', 'intersection7', 45, 29.5, TO_TIMESTAMP('2024-08-09 16:15:00')),
('sensor35', 'intersection8', 57, 28.0, TO_TIMESTAMP('2024-08-09 16:30:00')),
('sensor36', 'intersection8', 60, 27.5, TO_TIMESTAMP('2024-08-09 16:45:00')),
('sensor37', 'intersection9', 15, 45.0, TO_TIMESTAMP('2024-08-09 17:00:00')),
('sensor38', 'intersection9', 18, 44.5, TO_TIMESTAMP('2024-08-09 17:15:00')),
('sensor39', 'intersection10', 53, 22.0, TO_TIMESTAMP('2024-08-09 17:30:00')),
('sensor40', 'intersection10', 55, 21.5, TO_TIMESTAMP('2024-08-09 17:45:00')),
('sensor41', 'intersection1', 26, 32.0, TO_TIMESTAMP('2024-08-09 18:00:00')),
('sensor42', 'intersection1', 28, 31.0, TO_TIMESTAMP('2024-08-09 18:15:00')),
('sensor43', 'intersection2', 66, 26.0, TO_TIMESTAMP('2024-08-09 18:30:00')),
('sensor44', 'intersection2', 69, 27.0, TO_TIMESTAMP('2024-08-09 18:45:00')),
('sensor45', 'intersection3', 22, 39.0, TO_TIMESTAMP('2024-08-09 19:00:00')),
('sensor46', 'intersection3', 24, 38.5, TO_TIMESTAMP('2024-08-09 19:15:00')),
('sensor47', 'intersection4', 32, 33.0, TO_TIMESTAMP('2024-08-09 19:30:00')),
('sensor48', 'intersection4', 35, 32.0, TO_TIMESTAMP('2024-08-09 19:45:00')),
('sensor49', 'intersection5', 10, 48.0, TO_TIMESTAMP('2024-08-09 20:00:00')),
('sensor50', 'intersection5', 13, 47.5, TO_TIMESTAMP('2024-08-09 20:15:00')),
('sensor51', 'intersection6', 25, 36.0, TO_TIMESTAMP('2024-08-09 20:30:00')),
('sensor52', 'intersection6', 27, 35.5, TO_TIMESTAMP('2024-08-09 20:45:00')),
('sensor53', 'intersection7', 46, 29.0, TO_TIMESTAMP('2024-08-09 21:00:00')),
('sensor54', 'intersection7', 48, 28.5, TO_TIMESTAMP('2024-08-09 21:15:00')),
('sensor55', 'intersection8', 62, 27.0, TO_TIMESTAMP('2024-08-09 21:30:00')),
('sensor56', 'intersection8', 64, 26.5, TO_TIMESTAMP('2024-08-09 21:45:00')),
('sensor57', 'intersection9', 20, 44.0, TO_TIMESTAMP('2024-08-09 22:00:00')),
('sensor58', 'intersection9', 23, 43.5, TO_TIMESTAMP('2024-08-09 22:15:00')),
('sensor59', 'intersection10', 57, 21.5, TO_TIMESTAMP('2024-08-09 22:30:00')),
('sensor60', 'intersection10', 59, 21.0, TO_TIMESTAMP('2024-08-09 22:45:00')),
('sensor61', 'intersection1', 30, 31.5, TO_TIMESTAMP('2024-08-09 23:00:00')),
('sensor62', 'intersection1', 33, 30.0, TO_TIMESTAMP('2024-08-09 23:15:00')),
('sensor63', 'intersection2', 70, 25.5, TO_TIMESTAMP('2024-08-10 00:00:00')),
('sensor64', 'intersection2', 72, 26.0, TO_TIMESTAMP('2024-08-10 00:15:00')),
('sensor65', 'intersection3', 28, 38.0, TO_TIMESTAMP('2024-08-10 00:30:00')),
('sensor66', 'intersection3', 30, 37.5, TO_TIMESTAMP('2024-08-10 00:45:00')),
('sensor67', 'intersection4', 36, 32.0, TO_TIMESTAMP('2024-08-10 01:00:00')),
('sensor68', 'intersection4', 38, 31.0, TO_TIMESTAMP('2024-08-10 01:15:00')),
('sensor69', 'intersection5', 12, 47.0, TO_TIMESTAMP('2024-08-10 01:30:00')),
('sensor70', 'intersection5', 15, 46.5, TO_TIMESTAMP('2024-08-10 01:45:00')),
('sensor71', 'intersection6', 29, 35.0, TO_TIMESTAMP('2024-08-10 02:00:00')),
('sensor72', 'intersection6', 32, 34.5, TO_TIMESTAMP('2024-08-10 02:15:00')),
('sensor73', 'intersection7', 50, 28.0, TO_TIMESTAMP('2024-08-10 02:30:00')),
('sensor74', 'intersection7', 52, 27.5, TO_TIMESTAMP('2024-08-10 02:45:00')),
('sensor75', 'intersection8', 65, 26.0, TO_TIMESTAMP('2024-08-10 03:00:00')),
('sensor76', 'intersection8', 68, 25.5, TO_TIMESTAMP('2024-08-10 03:15:00')),
('sensor77', 'intersection9', 25, 43.0, TO_TIMESTAMP('2024-08-10 03:30:00')),
('sensor78', 'intersection9', 28, 42.5, TO_TIMESTAMP('2024-08-10 03:45:00')),
('sensor79', 'intersection10', 60, 20.5, TO_TIMESTAMP('2024-08-10 04:00:00')),
('sensor80', 'intersection10', 62, 20.0, TO_TIMESTAMP('2024-08-10 04:15:00')),
('sensor81', 'intersection1', 35, 29.5, TO_TIMESTAMP('2024-08-10 04:30:00')),
('sensor82', 'intersection1', 37, 29.0, TO_TIMESTAMP('2024-08-10 04:45:00')),
('sensor83', 'intersection2', 74, 24.5, TO_TIMESTAMP('2024-08-10 05:00:00')),
('sensor84', 'intersection2', 77, 25.0, TO_TIMESTAMP('2024-08-10 05:15:00')),
('sensor85', 'intersection3', 32, 36.0, TO_TIMESTAMP('2024-08-10 05:30:00')),
('sensor86', 'intersection3', 34, 35.5, TO_TIMESTAMP('2024-08-10 05:45:00')),
('sensor87', 'intersection4', 40, 31.0, TO_TIMESTAMP('2024-08-10 06:00:00')),
('sensor88', 'intersection4', 42, 30.5, TO_TIMESTAMP('2024-08-10 06:15:00')),
('sensor89', 'intersection5', 14, 46.0, TO_TIMESTAMP('2024-08-10 06:30:00')),
('sensor90', 'intersection5', 16, 45.5, TO_TIMESTAMP('2024-08-10 06:45:00')),
('sensor91', 'intersection6', 35, 34.0, TO_TIMESTAMP('2024-08-10 07:00:00')),
('sensor92', 'intersection6', 37, 33.5, TO_TIMESTAMP('2024-08-10 07:15:00')),
('sensor93', 'intersection7', 55, 27.5, TO_TIMESTAMP('2024-08-10 07:30:00')),
('sensor94', 'intersection7', 57, 27.0, TO_TIMESTAMP('2024-08-10 07:45:00')),
('sensor95', 'intersection8', 70, 25.0, TO_TIMESTAMP('2024-08-10 08:00:00')),
('sensor96', 'intersection8', 72, 24.5, TO_TIMESTAMP('2024-08-10 08:15:00')),
('sensor97', 'intersection9', 30, 42.0, TO_TIMESTAMP('2024-08-10 08:30:00')),
('sensor98', 'intersection9', 32, 41.5, TO_TIMESTAMP('2024-08-10 08:45:00')),
('sensor99', 'intersection10', 65, 19.5, TO_TIMESTAMP('2024-08-10 09:00:00')),
('sensor100', 'intersection10', 68, 19.0, TO_TIMESTAMP('2024-08-10 09:15:00'));

```


### Summary

- **Setup**: Create tables for ingesting traffic data and generating alerts.
- **Transform**: Compute average vehicle count and speed, and detect high traffic conditions.
- **Sample Data**: Insert sample data to test the setup.


