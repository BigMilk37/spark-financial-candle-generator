Spark Financial Candle Generator

A PySpark-based data processing pipeline that ingests raw tick-level financial market transactions and aggregates them into time-based Japanese Candlesticks (OHLC: Open, High, Low, Close).

This project demonstrates core Data Engineering principles, including custom schema definition, time-series data manipulation, and optimized data partitioning in Apache Spark.

Features

Explicit Schema Enforcement: Utilizes Spark's StructType and StructField to strictly define financial data types, ensuring data integrity during ingestion.

Custom Time-Based Windowing: Converts transaction timestamps (MOMENT) into specific millisecond intervals to bucket trades into fixed candle widths (default: 30,000ms / 30 seconds).

OHLC Aggregation: Dynamically extracts the Open, High, Low, and Close prices, along with the exact timestamp for each generated candle.

Optimized Storage: Partitions the final aggregated dataset by stock symbol (SYMBOL) and appends it to a local CSV repository for optimized querying.

How It Works

Configuration Ingestion: The script optionally accepts a path to an XML configuration file via command-line arguments to override default parameters (e.g., candle width, date/time filtering bounds).

Data Cleansing & Transformation:

Reads a raw financial dataset (fin_sample.csv).

Strips and splits the string-based transaction timestamps into independent DATE and TIME metrics.

Filters out transactions outside of specified historical boundaries.

Mathematical Candlestick Bucketing: Maps each transaction time into a continuous millisecond timeline, dividing by the desired candle width to assign a distinct candle index.

Aggregation: Groups the data by stock symbol and candle index, using Spark aggregation functions (first, max, min, last) to construct the final market metrics.

Tech Stack

Language: Python 3

Framework: Apache Spark (PySpark SQL)

Libraries: xml.etree.ElementTree, math

Setup and Usage

Prerequisites

Python 3.x

Apache Spark installed locally or running in a notebook environment (e.g., Google Colab / Jupyter).

A source file named fin_sample.csv in the root directory matching the expected schema.

Execution

To run the script using default configurations:

python main.py


To run the script with a custom XML configuration file:

python main.py path/to/config.xml


Output Architecture

The pipeline outputs structured data partitioned dynamically by ticker symbol to maximize read efficiency:

/content/partitionBy/
  ├── SYMBOL=AAPL/
  │     └── part-*.csv
  ├── SYMBOL=MSFT/
  │     └── part-*.csv
