# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1918d65e-31bf-40a4-b7af-8b4062f430b1",
# META       "default_lakehouse_name": "SampleLakehouse",
# META       "default_lakehouse_workspace_id": "ea456edd-6df5-48c5-98df-d001f8ec5b61",
# META       "known_lakehouses": [
# META         {
# META           "id": "1918d65e-31bf-40a4-b7af-8b4062f430b1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read orders to get date range
orders_df = spark.sql('SELECT MIN(order_date) as min_d, MAX(order_date) as max_d FROM SampleLakehouse.orders')
row = orders_df.collect()[0]
print(f'Min: {row.min_d}, Max: {row.max_d}')

# Generate dates using Spark sequence
dates_df = spark.sql("""
    SELECT 
        date_key,
        YEAR(date_key) as year,
        MONTH(date_key) as month_number,
        DATE_FORMAT(date_key, 'MMMM') as month_name,
        CONCAT('Q', QUARTER(date_key)) as quarter,
        DATE_FORMAT(date_key, 'EEEE') as day_of_week
    FROM (
        SELECT explode(sequence(
            (SELECT MIN(CAST(order_date AS DATE)) FROM SampleLakehouse.orders),
            (SELECT MAX(CAST(order_date AS DATE)) FROM SampleLakehouse.orders),
            interval 1 day
        )) as date_key
    )
""")

dates_df.write.mode('overwrite').format('delta').option('overwriteSchema', 'true').saveAsTable('SampleLakehouse.dates')
print(f'Created dates table with {dates_df.count()} rows')
dates_df.show(5)

