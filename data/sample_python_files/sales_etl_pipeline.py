# ETL Pipeline - Sales Data Processing
# This notebook demonstrates PySpark ETL operations that need Unity Catalog migration.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SalesETL').enableHiveSupport().getOrCreate()

# Read from tables - needs UC migration
sales_df = spark.read.table('raw_sales_data')
customers_df = spark.read.table('customer_master')
products_df = spark.read.table('product_catalog')

# Transform and join data
result_df = sales_df.join(customers_df, 'customer_id') \
    .join(products_df, 'product_id') \
    .withColumn('revenue_category', 
        when(col('sale_amount') > 1000, 'High')
        .when(col('sale_amount') > 500, 'Medium')
        .otherwise('Low'))

# Write to target tables - needs UC migration
result_df.write.mode('overwrite').saveAsTable('processed_sales_fact')

# SQL operations
spark.sql('CREATE TABLE IF NOT EXISTS sales_summary AS SELECT * FROM processed_sales_fact')
