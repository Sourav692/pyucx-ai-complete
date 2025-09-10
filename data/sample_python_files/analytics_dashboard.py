# Data Analytics Dashboard
# Advanced analytics using Databricks and PySpark with legacy table references.

import databricks.sql as sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.config('spark.sql.warehouse.dir', '/user/hive/warehouse').getOrCreate()

# Load analytics data from hive_metastore
analytics_df = spark.sql('SELECT * FROM hive_metastore.analytics.user_behavior')
events_df = spark.read.table('event_log')
metrics_df = spark.read.table('performance_metrics')

# Complex analytics queries
user_stats = spark.sql('''
    SELECT customer_id, 
           COUNT(*) as total_events, 
           SUM(event_value) as total_value
    FROM event_log e
    JOIN user_profiles u ON e.user_id = u.customer_id
    WHERE event_date >= '2024-01-01'
    GROUP BY customer_id
''')

# Save results using databricks utilities
user_stats.write.saveAsTable('analytics_summary')

# Use dbutils for file operations
dbutils.fs.cp('/mnt/analytics/', '/mnt/archive/')
