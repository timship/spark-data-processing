from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, datediff, current_timestamp,
    when, countDistinct, lag, first, coalesce, lit, lower, trim, round
)
from pyspark.sql.window import Window

# Создаем SparkSession
# Создаем стандартно SparkSession как в ноутбуках с практикой

# Читаем данные
users_df = spark.read.csv("s3a://startde-datasets/final_dz/users.csv",
                          header=True, inferSchema=True)
transactions_df = spark.read.csv(
    "s3a://startde-datasets/final_dz/transactions.csv", header=True,
    inferSchema=True)
activity_df = spark.read.csv(
    "s3a://startde-datasets/final_dz/user_activity.csv", header=True,
    inferSchema=True)

# Базовая очистка таблиц
users_clean = users_df.select(
    "user_id",
    "registration_date",
    "last_login_date"
)

transactions_clean = transactions_df.select(
    "user_id",
    "transaction_date",
    coalesce(col("amount"), lit(0)).alias('amount'),
    "status"). \
    filter(col('status') == 'completed')



activity_clean = activity_df.select(
    "user_id",
    "timestamp",
    lower(trim(col("device_type"))).alias('device_type'),
    coalesce(col("session_duration"), lit(0)).alias('session_duration')). \
    dropDuplicates(['user_id', 'timestamp', 'device_type'])

# Расчёт метрик сессий
session_metrics = activity_clean. \
    groupBy("user_id").agg(
    count("*").alias("total_sessions"),
    round(avg("session_duration"), 2).alias("avg_session_duration"),
    countDistinct("device_type").alias("unique_devices")
)

# Расчёт метрик транзакций


w = Window.partitionBy('user_id').orderBy("transaction_date")

transaction_metrics = transactions_clean. \
    withColumn('days_between_purchases', datediff(col("transaction_date"), lag("transaction_date", 1).over(w))). \
    groupBy("user_id").agg(
    count("*").alias("total_transactions"),
    round(avg("amount"), 2).alias("avg_transaction_amount"),
    round(avg('days_between_purchases'), 2).alias("avg_days_between_purchases"))

# Определение активных пользователей
active_users = activity_clean.groupBy("user_id").agg(
    lit(1).alias("is_active")
)

# Сборка финальной витрины
final_df = users_clean.join(
    session_metrics, "user_id", "left"
).join(
    transaction_metrics, "user_id", "left"
).join(
    active_users, "user_id", "left"
). \
    fillna(0)

# Пример результата
final_df.show(5)


# Проверки качества данных
def basic_quality_checks(df):
    print("Checking data quality...")
    null_sessions = df.filter(col("total_sessions").isNull()).count()
    null_transactions = df.filter(col("total_transactions").isNull()).count()

    print(f"Found {null_sessions} users without session data")
    print(f"Found {null_transactions} users without transaction data")


basic_quality_checks(final_df)

