# Unity Catalog Converted Python File
# This file has been automatically converted for Unity Catalog compatibility using the PyUCX-AI Multi-Agent Framework.
#
# Changes Made:
# - Updated table references to use three-part naming (catalog.schema.table)
# - Modified Spark configurations for Unity Catalog
# - Updated SQL queries for UC compatibility
# - Added Unity Catalog-specific configurations
#
# Next Steps:
# 1. Review the converted code for accuracy
# 2. Test the file in your Unity Catalog environment
# 3. Update any hardcoded catalog/schema names as needed
# 4. Verify data access permissions
#
# ETL Pipeline - Sales Data Processing
# This notebook demonstrates PySpark ETL operations that need Unity Catalog migration.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SalesETL').config("spark.sql.catalog.spark_catalog", "unity").getOrCreate()

# Read from tables - needs UC migration
sales_df = spark.read.table('main.default.raw_sales_data')
customers_df = spark.read.table('main.default.customer_master')
products_df = spark.read.table('main.default.product_catalog')

# Transform and join data
result_df = sales_df.join(customers_df, 'customer_id') \
    .join(products_df, 'product_id') \
    .withColumn('revenue_category', 
        when(col('sale_amount') > 1000, 'High')
        .when(col('sale_amount') > 500, 'Medium')
        .otherwise('Low'))

# Write to target tables - needs UC migration
result_df.write.mode('overwrite').saveAsTable('main.default.processed_sales_fact')

# SQL operations
spark.sql('CREATE TABLE IF NOT EXISTS sales_summary AS SELECT * FROM main.default.processed_sales_fact')
