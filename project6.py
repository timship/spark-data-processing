#!/usr/bin/env python
# coding: utf-8

# In[88]:


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


# In[89]:


customers_data = [
    (1, "Иван Петров", "ivan@example.com"),
    (2, "Анна Сидорова", "anna@example.com"),
    (3, "Елена Иванова", "elena@example.com"),
    (4, "Петр Смирнов", "petr@example.com"),
    (5, "Мария Кузнецова", "maria@example.com")
]
customers_schema = ["customer_id", "customer_name", "email"]
customers_df = spark.createDataFrame(customers_data, customers_schema)
customers_df.show()


# In[90]:


orders_data = [
    (101, 1, "2023-09-15", 999.99),
    (102, 2, "2023-09-16", 1199.00),
    (103, 3, "2023-09-17", 239.97),
    (104, 2, "2023-09-18", 699.99),
    (105, 4, "2023-09-19", 449.99)
]
orders_schema = ["order_id", "customer_id", "order_date", "total_amount"]
orders_df = spark.createDataFrame(orders_data, orders_schema)
orders_df.show()


# In[91]:


products_data = [
    (201, "Ноутбук", 999.99, 10),
    (202, "Смартфон", 599.50, 20),
    (203, "Наушники", 79.99, 50),
    (204, "Планшет", 449.99, 15)
]
products_schema = ["product_id", "product_name", "price", "stock_quantity"]
products_df = spark.createDataFrame(products_data, products_schema)
products_df.show()


# In[92]:


order_items_data = [
    (101, 201, 1),
    (102, 202, 2),
    (103, 203, 3),
    (104, 202, 1),
    (105, 204, 1)
]
order_items_schema = ["order_id", "product_id", "quantity"]
order_items_df = spark.createDataFrame(order_items_data, order_items_schema)
order_items_df.show()


# In[93]:


#1
"""Объедините таблицы customers_df и orders_df. 
Отсортируйте результат по дате заказа в убывающем порядке, 
сохраните в df_customer_orders и напечатайте содержимое.

Ожидаемые колонки: customer_id, customer_name, email, order_id, order_date, total_amount
"""
from pyspark.sql.functions import desc
df_customer_orders = customers_df.     join(orders_df, 'customer_id').     sort(desc('order_date'))
df_customer_orders.show()


# In[94]:


#2
"""
Объедините таблицы orders_df, order_items_df и products_df, 
чтобы получить подробную информацию о заказах, включая названия продуктов и их количество. 
Отфильтруйте заказы с общей суммой больше 500, сохраните в df_order_details и напечатайте содержимое.

Ожидаемые колонки: product_id, order_id, customer_id, order_date, total_amount, quantity, product_name, price, stock_quantity
"""
df_order_details = orders_df.     join(order_items_df, 'order_id').     join(products_df, 'product_id').     select('product_id', 'order_id', 'customer_id', 'order_date', 'total_amount', 'quantity', 'product_name', 'price', 'stock_quantity'
). \
    filter('total_amount > 500')
df_order_details.show()


# In[95]:


#3
"""
Выполните Left Join customers_df и orders_df, 
чтобы получить список всех клиентов и их заказов, 
включая клиентов без заказов, сохраните в df_all_customers и напечатайте содержимое.

Ожидаемые колонки: customer_id, customer_name, email, order_id, order_date, total_amount
"""
df_all_customers = customers_df.     join(orders_df, 'customer_id', 'left')
df_all_customers.show()


# In[96]:


#4
"""
Объедините все четыре таблицы (customers_df, orders_df, order_items_df, products_df) 
и вычислите общую сумму покупок (price * quantity) для каждого клиента, 
назовите колонку - total_purchases. Отсортируйте результат по общей сумме в убывающем порядке, 
сохраните в df_customer_total и напечатайте содержимое.

В результат не включайте записи с null значениями.

Ожидаемые колонки: customer_id, customer_name, total_purchases
"""
from pyspark.sql.functions import col
df_customer_total = customers_df.     join(orders_df, 'customer_id').     join(order_items_df, 'order_id').     join(products_df, 'product_id').     select('customer_id', 'customer_name', (col('price')*col('quantity')).alias('total_purchases')).     sort(desc('total_purchases'))
df_customer_total.show()
#df_customer_total.printSchema()


# In[110]:


#5
"""
Объедините таблицы products_df и order_items_df, найдите топ-3 самых продаваемых продукта 
(по общему количеству в заказах), назовите колонку - total_quantity 
и сохраните в df_top_products и напечатайте содержимое. 
Результат отсортируйте по убыванию total_quantity и product_id

Ожидаемые колонки: product_id, product_name, total_quantity
"""


df_top_products = products_df.     join(order_items_df, 'product_id').     groupBy('product_id').agg(sum('quantity').alias('total_quantity')).     select('product_id','product_id','total_quantity').     sort(desc('total_quantity'), desc('product_id'))
df_top_products.show(3)
  


# In[111]:


spark.stop()


# In[ ]:




