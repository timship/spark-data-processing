#!/usr/bin/env python
# coding: utf-8

# In[2]:


#Spark connection
import os
import socket
from pyspark.sql import SparkSession

APACHE_MASTER_IP = socket.gethostbyname("apache-spark-master-0.apache-spark-headless.apache-spark.svc.cluster.local")
APACHE_MASTER_URL = f"spark://{APACHE_MASTER_IP}:7077"
POD_IP = os.environ["MY_POD_IP"]
SPARK_APP_NAME = f"spark-{os.environ['HOSTNAME']}"
JARS = "/nfs/env/lib/python3.8/site-packages/pyspark/jars/clickhouse-native-jdbc-shaded-2.6.5.jar"
MEM = "512m"
CORES = 1

spark = SparkSession.        builder.        appName(SPARK_APP_NAME).        master(APACHE_MASTER_URL).        config("spark.executor.memory", MEM).        config("spark.jars", JARS).        config("spark.executor.cores", CORES).        config("spark.driver.host", POD_IP).        getOrCreate()


# In[3]:


from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DoubleType,
)

schema = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_name", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField(
            "items",
            StructType(
                [
                    StructField("product_name", StringType(), False),
                    StructField("quantity", IntegerType(), False),
                    StructField("price", DoubleType(), False),
                ]
            ),
            False,
        ),
    ]
)


# In[4]:


from datetime import datetime

data = [
    (
        1,
        "Иван Петров",
        datetime.strptime("2023-09-15", "%Y-%m-%d").date(),
        ("Ноутбук", 1, 999.99),
    ),
    (
        2,
        "Анна Сидорова",
        datetime.strptime("2023-09-16", "%Y-%m-%d").date(),
        ("Смартфон", 2, 599.50),
    ),
    (
        3,
        "Елена Иванова",
        datetime.strptime("2023-09-17", "%Y-%m-%d").date(),
        ("Наушники", 3, 79.99),
    ),
    (
        4,
        "Анна Козлова",
        datetime.strptime("2023-09-18", "%Y-%m-%d").date(),
        ("Смартфон", 1, 699.99),
    ),
    (
        5,
        "Петр Смирнов",
        datetime.strptime("2023-09-19", "%Y-%m-%d").date(),
        ("Планшет", 1, 449.99),
    ),
]


# In[5]:


df = spark.createDataFrame(data, schema)
df.show()

from pyspark.sql.functions import desc
df_sorted_by_date = df.sort(desc("order_date"))
df_sorted_by_date.show()

from pyspark.sql.functions import col

df_sorted_by_price = df.sort(col("items")['price'])
df_sorted_by_price.show()

df_sorted_by_name_quantity = (df.sort(col("customer_name"))).sort(desc(col("items")['quantity']))
df_sorted_by_name_quantity.show()

df_sorted_by_total_cost = df.sort(desc(col("items")["price"] * col("items")["quantity"]))
df_sorted_by_total_cost.show()

df_sorted_by_product_date = (df.sort(col("items")["product_name"])).sort(desc(col("order_date")))
df_sorted_by_product_date.show()


spark.stop()


# In[ ]:




