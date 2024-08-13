# Formula1TransformationsJob

This project demonstrates an Apache Flink job that performs various transformations on Formula 1 lap times data. The job reads data from a CSV file, processes it with several custom transformations, and prints the results.

## Overview

The `Formula1TransformationsJob` class illustrates how to:
- Set up a Flink execution environment for local execution.
- Read and parse data from a CSV file.
- Apply several custom transformations, including mapping, filtering, enriching, and reducing.
- Print the results to the console.

## Features

- **CSV Parsing**: Reads and parses lap times data from a CSV file.
- **Custom Transformations**: Converts lap times, filters data, enriches with custom logic, counts lap occurrences, and identifies drivers with all "Fast Laps."
- **Local Execution**: Runs the job locally with a parallelism of 1.

## Requirements

- Apache Flink (compatible with the version used in this project)
- Java 11 or higher

## Setup

**Clone the Repository**

```bash
git clone <repository-url>
cd <repository-directory>
```

## Build

```bash
mvn clean package
```

## Configure Input Data

Place the `f1_lap_times.csv` file in the `src/main/resources` directory. The CSV file should have the following format:

```
driver_id,driver_name,team_name,lap_time
44,Lewis Hamilton,Mercedes,88.5
33,Max Verstappen,Red Bull,89.1
16,Charles Leclerc,Ferrari,87.3
11,Sergio Perez,Red Bull,90.0
...
```

## Running the Job

Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.basics.Formula1TransformationsJob target/flink-tutorial-1.1-<version>.jar
```

## Code Overview

**Formula1TransformationsJob**: Main class to run the Flink job.

- **Data Parsing**: Reads the CSV file and parses each line into a `Tuple4` containing driver ID, driver name, team name, and lap time.
- **Transformations**:
    - **Convert Lap Times**: Converts lap times from seconds to milliseconds.
    - **Filter Slow Laps**: Filters out laps with times above 89 seconds.
    - **Enrich Data**: Adds a category for lap speed ("Fast Lap" or "Standard Lap").
    - **Count Laps**: Counts the number of laps per driver.
    - **Sum Laps**: Sums the lap counts for each driver.
    - **Identify Fast Drivers**: Identifies drivers who had all "Fast Laps."

## Data Transformations Explained

### Convert Lap Times

**Operation: `mapped.map(value -> new Tuple4<>(value.f0, value.f1, value.f2, value.f3 * 1000))`**

**Purpose:** Converts lap times from seconds to milliseconds for more granular analysis.

### Filter Slow Laps

**Operation: `filtered.filter(value -> value.f3 < 89000)`**

**Purpose:** Filters out lap times that exceed 89 seconds (89,000 milliseconds), focusing on faster laps.

### Enrich Data

**Operation: `enriched.map(value -> new Tuple4<>(value.f0, value.f1, value.f2, value.f3 < 88000 ? "Fast Lap" : "Standard Lap"))`**

**Purpose:** Adds a new field to categorize laps as "Fast Lap" or "Standard Lap" based on the lap time.

### Count Laps

**Operation: `lapCounts.flatMap(value -> out.collect(new Tuple2<>(value.f1, 1)))`**

**Purpose:** Outputs a count of 1 for each lap, associated with the driver's name, to facilitate counting.

### Sum Lap Counts

**Operation: `totalLaps.keyBy(value -> value.f0).reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))`**

**Purpose:** Aggregates the total number of laps per driver.

### Identify Fast Drivers

**Operation: `fastDrivers.keyBy(value -> value.f1).reduce((value1, value2) -> "Fast Lap".equals(value1.f3) && "Fast Lap".equals(value2.f3) ? value1 : new Tuple4<>(value1.f0, value1.f1, value1.f2, "Not All Fast Laps"))`**

**Purpose:** Identifies drivers who had all laps classified as "Fast Lap."

## Dependencies

- Apache Flink
- CSV File Processing

---
