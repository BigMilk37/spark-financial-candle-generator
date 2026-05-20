# Spark Financial Candle Generator

A PySpark-based data processing script that ingests raw tick-level financial market transactions and aggregates them into time-based Japanese Candlesticks (OHLC: Open, High, Low, Close). 

This project demonstrates core Data Engineering principles, including custom schema definition, time-series data manipulation, and optimized data partitioning in Apache Spark.

## Features
* **Explicit Schema Enforcement:** Utilizes Spark's `StructType` and `StructField` to strictly define financial data types, ensuring data integrity during ingestion.
* **Custom Time-Based Windowing:** Converts transaction timestamps (`MOMENT`) into specific millisecond intervals to bucket trades into fixed candle widths (default: 30,000ms / 30 seconds).
* **OHLC Aggregation:** Dynamically extracts the **Open, High, Low, and Close** prices, along with the exact timestamp for each generated candle.
* **Optimized Storage:** Partitions the final aggregated dataset by stock symbol (`SYMBOL`) and appends it to a local CSV repository for optimized querying.

## How It Works

1. **Configuration Ingestion:** The script optionally accepts a path to an XML configuration file via command-line arguments to override default parameters (e.g., candle width, date/time filtering bounds).
2. **Data Cleansing & Transformation:** * It reads a raw financial dataset (`fin_sample.csv`).
   * Strips and splits the string-based transaction timestamps into independent `DATE` and `TIME` metrics.
   * Filters out transactions outside of specified historical boundaries.
3. **Mathematical Candlestick Bucketing:** It maps each transaction time into a continuous millisecond timeline, dividing by the desired candle width to assign a distinct `candle` index.
4. **Aggregation:** Groups the data by stock symbol and candle index, using Spark aggregation functions (`first`, `max`, `min`, `last`) to construct the final market metrics.

## Tech Stack
* **Language:** Python 3
* **Framework:** Apache Spark (PySpark SQL)
* **Libraries:** `xml.etree.ElementTree`, `math`

## Setup and Usage

### Prerequisites
* Python 3.x
* Apache Spark installed locally or running in a notebook environment (e.g., Google Colab / Jupyter).
* A source file named `fin_sample.csv` in the root directory matching the expected schema.

### Sample Data Ingestion Format
Since the raw transaction log (`fin_sample.csv`) is omitted due to size constraints, the pipeline expects an ingested CSV schema structured as follows:

| #SYMBOL | SYSTEM | MOMENT            | ID_DEAL | PRICE_DEAL | VOLUME | OPEN_POS | DIRECTION |
|---------|--------|-------------------|---------|------------|--------|----------|-----------|
| AAPL    | EQTY   | 20110103100015123 | 9876543 | 150.25     | 100    | 50000    | B         |
| MSFT    | EQTY   | 20110103100045456 | 9876544 | 240.50     | 50     | 12000    | S         |

*Note: The `MOMENT` string format represents `YYYYMMDDHHMMSSmmm` (Year, Month, Day, Hour, Minute, Second, Millisecond).*


### Execution
To run the script using default configurations:
```bash
python main.py

/content/partitionBy/
  ├── SYMBOL=AAPL/
  │     └── part-*.csv
  ├── SYMBOL=MSFT/
  │     └── part-*.csv
