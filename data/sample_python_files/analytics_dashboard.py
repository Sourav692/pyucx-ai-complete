# Data Analytics Dashboard
# Advanced analytics using Databricks and PySpark with legacy table references.

from pyspark.sql.functions import *

# Load analytics data from hive_metastore
analytics_df = spark.sql('SELECT * FROM analytics.user_behavior')
events_df = spark.read.table('analytics.event_log')
metrics_df = spark.read.table('analytics.performance_metrics')

# Complex analytics queries
user_stats = spark.sql('''
    SELECT customer_id, 
           COUNT(*) as total_events, 
           SUM(event_value) as total_value
    FROM analytics.event_log e
    JOIN analytics.user_profiles u ON e.user_id = u.customer_id
    WHERE event_date >= '2024-01-01'
    GROUP BY customer_id
''')

# Save results using databricks utilities
user_stats.write.saveAsTable('analytics.analytics_summary')

# Use dbutils for file operations
dbutils.fs.cp('/mnt/analytics/', '/mnt/archive/')
