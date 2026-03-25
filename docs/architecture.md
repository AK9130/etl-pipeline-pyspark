# ETL Pipeline Architecture

## Architecture Diagram
![Architecture](./screenshots/architecture.png)

## Flow Description

Dataset (CSV)
      │
      ▼
PySpark ETL Script
      │
      ▼
Data Cleaning (dropna)
      │
      ▼
Transformation (total_price column)
      │
      ▼
Parquet Output
