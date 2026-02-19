#!/usr/bin/env python
# coding: utf-8

# In[8]:


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


# In[9]:


data = [
    (1, "Иван Петров", "2023-09-15", "Ноутбук", "Электроника", 1, 999.99, "Курьер"),
    (2, "Анна Сидорова", "2023-09-16", "Смартфон", "Электроника", 2, 599.50, "Самовывоз"),
    (3, "Елена Иванова", "2023-09-17", "Наушники", "Аксессуары", 3, 79.99, "Почта"),
    (4, "Анна Козлова", "2023-09-18", "Смартфон", "Электроника", 1, 699.99, "Курьер"),
    (5, "Петр Смирнов", "2023-09-19", "Планшет", "Электроника", 1, 449.99, "Самовывоз"),
    (6, "Иван Петров", "2023-09-20", "Чехол", "Аксессуары", 2, 19.99, "Почта"),
    (7, "Анна Сидорова", "2023-09-20", "Зарядное устройство", "Аксессуары", 1, 29.99, "Самовывоз"),
    (8, "Елена Иванова", "2023-09-21", "Смартфон", "Электроника", 1, 799.99, "Курьер")
]


# In[10]:


schema = ["order_id", "customer_name", "order_date", "product_name", "category", "quantity", "price", "delivery_method"]
df = spark.createDataFrame(data, schema)
df.show()


# In[11]:


#1
"""Подсчитайте общее количество заказов для каждого клиента (total_orders), 
сохраните в df_orders_per_customer и напечатайте содержимое.
"""
from pyspark.sql.functions import sum, col
df_orders_per_customer = df.groupBy('customer_name').agg(sum(col("quantity")).alias("total_orders"))
df_orders_per_customer.show()


# In[12]:


#2
"""Найдите суммарную стоимость заказов по каждой категории товаров (total_value), 
сохраните в df_total_by_category и напечатайте содержимое.
"""
from pyspark.sql.functions import count

df_total_by_category = df.groupBy('category').agg(sum("price").alias('total_value'))
df_total_by_category.show()


# In[13]:


#3
"""Определите средний чек заказа для каждого дня (average_check), 
сохраните в df_avg_check_per_day и напечатайте содержимое.
"""
from pyspark.sql.functions import avg

df_avg_check_per_day = df.groupBy("order_date").agg(avg("price").alias('average_check'))
df_avg_check_per_day.show()


# In[19]:


#4
""" Выявите топ-3 самых продаваемых товара (по количеству), 
назовите колонку - total_quantity, отсортируйте по убыванию
и сохраните в df_top_products и напечатайте содержимое.
"""
df_top_products = df.     select('product_name', 'quantity').     groupBy('product_name').     sum('quantity').     orderBy('sum(quantity)', ascending=False).     toDF('total_quantity', 'quantity')
df_top_products.show(3)


# In[35]:


#5
"""
Рассчитайте процентное соотношение заказов по способам доставки (percentage), 
округлите до двух знаков и сохраните в df_delivery_percentage 
и напечатайте содержимое. 
Ожидаемые колонки — delivery_method, order_count, percentage. 
Результат отсортируйте по delivery_method
"""
from pyspark.sql.functions import count, percent_rank, round
from pyspark.sql.window import Window

df_delivery_percentage = df.     groupBy('delivery_method').     agg(count('quantity').alias('order_count'))
window = Window.partitionBy()
df_delivery_percentage = df_delivery_percentage.withColumn('percentage', round(col('order_count') / sum('order_count').over(window)*100, 2))
df_delivery_percentage.sort('delivery_method', ascending=True).show()


# In[18]:


spark.stop()


# In[ ]:




