# ETL Data Pipeline using Python + Spark

## Project Overview
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline using PySpark to process retail transaction data.

## Dataset
online_retail_II.csv

## ETL Steps

### Extract
Data is extracted from a CSV dataset using PySpark.

### Transform
- Removed null values using dropna()
- Filtered invalid records (negative Quantity and Price)
- Converted InvoiceDate to timestamp format
- Created a new column: total_price = Quantity * Price

### Load
The transformed data is stored in Parquet format for efficient storage and querying.

## Architecture
![Architecture](docs/screenshots/architecture.png)

## Sample Data

### Raw Data
![Raw Data](docs/screenshots/raw_data.png)

### Transformed Data
![Transformed Data](docs/screenshots/transformed_data.png)

## Technologies Used
- Python
- PySpark
- Linux
- Parquet

## Output
Cleaned and transformed data is stored in the output folder as Parquet files.

## How to Run

```bash
spark-submit spark_script/etl_pipeline.py
