# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
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

from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import date, datetime
import random

# Sample Customers
customers = []
for i in range(1, 101):
    customers.append(Row(
        customer_id=i,
        first_name='Customer_%d' % i,
        last_name='LastName_%d' % i,
        email='customer%d@example.com' % i,
        city=['Milan','Rome','Naples','Turin','Florence','Bologna','Venice','Palermo','Genoa','Bari'][i % 10],
        country='Italy',
        signup_date=date(2024, (i % 12) + 1, (i % 28) + 1)
    ))
df_customers = spark.createDataFrame(customers)
df_customers.write.mode('overwrite').format('delta').saveAsTable('customers')
print('Customers: %d rows' % df_customers.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sample Products
products = []
categories = ['Electronics','Clothing','Food','Books','Home','Sports','Beauty','Toys','Garden','Auto']
for i in range(1, 51):
    products.append(Row(
        product_id=i,
        product_name='Product_%d' % i,
        category=categories[i % 10],
        price=round(random.uniform(5.0, 500.0), 2),
        cost=round(random.uniform(2.0, 250.0), 2),
        in_stock=random.choice([True, True, True, False])
    ))
df_products = spark.createDataFrame(products)
df_products.write.mode('overwrite').format('delta').saveAsTable('products')
print('Products: %d rows' % df_products.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sample Orders
orders = []
for i in range(1, 1001):
    orders.append(Row(
        order_id=i,
        customer_id=random.randint(1, 100),
        product_id=random.randint(1, 50),
        quantity=random.randint(1, 10),
        order_date=date(2025, random.randint(1, 12), random.randint(1, 28)),
        status=random.choice(['Completed','Shipped','Processing','Cancelled'])
    ))
df_orders = spark.createDataFrame(orders)
df_orders.write.mode('overwrite').format('delta').saveAsTable('orders')
print('Orders: %d rows' % df_orders.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify all tables
for t in ['customers','products','orders']:
    count = spark.table(t).count()
    print('%s: %d rows' % (t, count))
print('Sample data loaded successfully!')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
