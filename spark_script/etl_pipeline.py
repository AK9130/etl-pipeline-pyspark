from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

input_path = "file:///home/aaqib/PROJECTS/3_ETL_Pipeline_PySpark/data_sets/online_retail_transactions/online_retail.csv"
output_path = "file:///home/aaqib/PROJECTS/3_ETL_Pipeline_PySpark/output/retail_data"

df = spark.read.csv(input_path, header=True, inferSchema=True)
print("Data Loaded")

df.show(5)

# BEFORE cleaning
print("Rows before cleaning:", df.count())

# null values remove
df_clean = df.dropna()

# Remove negative quantity / price
df_clean = df_clean.filter((col("Quantity") > 0) & (col("Price") > 0))

# AFTER cleaning
print("Rows after cleaning:", df_clean.count())

# Convert date column
df_clean1 = df_clean.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm:ss"))

# total price column
df_transform = df_clean1.withColumn("total_price", col("Quantity") * col("Price"))

df_transform.show(5)

df_transform.write.mode("overwrite").parquet(output_path)

spark.stop()
