# ETL Data Pipeline Using PySpark
## 1. Project Introduction
This project demonstrates an ETL (Extract, Transform, Load) pipeline using PySpark to process online retail transaction data.
The pipeline reads retail transaction data from a CSV file, cleans and transforms the data, and stores the processed data in Parquet format on Hadoop HDFS.

## 2. Problem Statement
Raw retail transaction data can contain missing values and invalid records. Before analysis or further processing, the data needs to be cleaned and transformed into a structured format.
The objective of this project is to build a PySpark ETL pipeline that:

- Extracts data from a CSV file
- Removes null values
- Filters invalid Quantity and Price values
- Converts the InvoiceDate column to timestamp format
- Calculates the total price for each transaction
- Stores the transformed data in Parquet format on HDFS

## 3. Dataset
The dataset used in this project is an online retail transaction dataset.

**Dataset file:**

```text
data_sets/online_retail_transactions/online_retail.csv
```

The dataset contains fields such as:

- Invoice
- StockCode
- Description
- Quantity
- InvoiceDate
- Price
- Customer ID
- Country

## 4. Technologies Used
- Python
- PySpark
- Apache Spark
- Hadoop HDFS
- Linux
- Parquet

## 5. Project Architecture / Flow

```text
Online Retail CSV
       |
       v
    PySpark
       |
       v
   Data Loading
       |
       v
 Remove Null Values
       |
       v
Filter Invalid Quantity / Price
       |
       v
Convert InvoiceDate to Timestamp
       |
       v
Create total_price Column
       |
       v
Save as Parquet
       |
       v
      HDFS
```

## 6. Implementation / Processing
The project is implemented using PySpark.

### Processing Steps
1. Read the online retail CSV file using PySpark.
2. Display the initial records.
3. Count the rows before cleaning.
4. Remove rows containing null values using `dropna()`.
5. Remove records where Quantity or Price is less than or equal to zero.
6. Count the rows after cleaning.
7. Convert `InvoiceDate` into timestamp format.
8. Create a `total_price` column using:

```text
total_price = Quantity × Price
```

9. Display the transformed data.
10. Write the transformed data in Parquet format to HDFS.

The main PySpark script is:

```text
spark_script/etl_pipeline.py
```

## 7. My Role
I worked on the complete ETL pipeline, including:

- Reading the retail CSV dataset using PySpark
- Cleaning the data by removing null values
- Filtering invalid Quantity and Price records
- Converting the InvoiceDate column to timestamp format
- Creating the `total_price` column
- Storing the transformed data in Parquet format on HDFS
- Running the PySpark ETL application using `spark-submit`

## 8. ETL Operations Performed
### Extract
The retail transaction data is extracted from the CSV file using PySpark.

### Transform
The following transformations are performed:

- Remove null values using `dropna()`
- Filter records with valid Quantity and Price values
- Convert `InvoiceDate` from string to timestamp
- Calculate `total_price` from Quantity and Price

### Load
The cleaned and transformed dataset is written in Parquet format and stored on Hadoop HDFS at:

```text
/user/aaqib/output_projects/3_etl_pipeline/retail_data
```

## 9. Output / Results
The processed data is stored as Parquet files on HDFS.

**HDFS output path:**

```text
/user/aaqib/output_projects/3_etl_pipeline/retail_data
```

Project screenshots are available in:

```text
docs/screenshots/
```

- `raw_data.png`
- `transformed_data.png`

## 10. Challenges & Learning
### Challenges
- Handling missing values in retail transaction data
- Filtering invalid Quantity and Price values
- Converting date values stored as strings into timestamp format
- Transforming raw CSV data into a structured Parquet format
- Writing processed data to Hadoop HDFS

### Learning
Through this project, I gained practical experience in:

- Building an ETL pipeline using PySpark
- Reading CSV data using Spark
- Data cleaning using DataFrame operations
- Filtering invalid records
- Converting string dates to timestamps
- Creating calculated columns using PySpark
- Writing data in Parquet format
- Storing processed data on Hadoop HDFS
- Running PySpark applications using `spark-submit`

## 11. Project Structure

```text
ETL_Pipeline_PySpark/
│
├── data_sets/
│   └── online_retail_transactions/
│       └── online_retail.csv
│
├── docs/
│   └── screenshots/
│       ├── raw_data.png
│       └── transformed_data.png
│
├── output/
│   └── retail_data/
│       └── Parquet Output
│
├── spark_script/
│   └── etl_pipeline.py
│
└── README.md
```

## 12. Conclusion
This project demonstrates how PySpark can be used to build an ETL pipeline for retail transaction data.
The project provides practical experience in data extraction, data cleaning, filtering, timestamp conversion, calculated columns, Parquet-based data storage, and Hadoop HDFS.

## 13. How to Run
From the project root directory, run:

```bash
spark-submit spark_script/etl_pipeline.py
```
