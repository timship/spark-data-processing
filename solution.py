# Spark connection with S3 options
import os
import socket
from pyspark.sql import SparkSession

# Замените следующие значения на свои credentials
aws_access_key = "j612yzOjcIwYyPhVD14P"
aws_secret_key = "bVTEwJ7sJ0TboubM5wJcn2ieYrV91uXk3N9dJ457"
s3_bucket = "startde-datasets"
s3_endpoint_url = "https://s3.lab.karpov.courses"

APACHE_MASTER_IP = socket.gethostbyname("apache-spark-master-0.apache-spark-headless.apache-spark.svc.cluster.local")
APACHE_MASTER_URL = f"spark://{APACHE_MASTER_IP}:7077"
POD_IP = os.environ["MY_POD_IP"]
SPARK_APP_NAME = f"spark-{os.environ['HOSTNAME']}"
JARS = """/nfs/env/lib/python3.8/site-packages/pyspark/jars/clickhouse-native-jdbc-shaded-2.6.5.jar, 
/nfs/env/lib/python3.8/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar,
/nfs/env/lib/python3.8/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.433.jar
"""

MEM = "512m"
CORES = 1

spark = SparkSession. \
    builder. \
    appName(SPARK_APP_NAME). \
    master(APACHE_MASTER_URL). \
    config("spark.executor.memory", MEM). \
    config("spark.jars", JARS). \
    config("spark.executor.cores", CORES). \
    config("spark.driver.host", POD_IP). \
    config("spark.hadoop.fs.s3a.access.key", aws_access_key). \
    config("spark.hadoop.fs.s3a.secret.key", aws_secret_key). \
    config("fs.s3a.endpoint", s3_endpoint_url). \
    config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"). \
    config("spark.hadoop.fs.s3a.committer.name", "directory"). \
    config("spark.hadoop.fs.s3a.path.style.access", True). \
    config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"). \
    config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"). \
    getOrCreate()


#В переменную student_directory необходимо подставить свой username в karpov.courses
student_directory = 'marija-shkurat-wrn7887'

schema = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""
df_csv = spark.read.csv('s3a://startde-datasets/shared/orders.csv', schema=schema)
df_csv.printSchema()
df_csv.count()

spark.stop()