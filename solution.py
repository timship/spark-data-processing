from pyspark.sql.functions import col, lower, trim, regexp_replace, when, lit, length, concat, substring


# Чтение данных из S3
users_df = spark.read.csv(
    "s3a://startde-datasets/final_dz/users.csv",
    header=True,
    inferSchema=True
)

cleaned_df = users_df.select(
    "user_id",
    "email",
    "phone_number",
    "registration_date",
    "last_login_date"
).dropDuplicates(["user_id"])

# TODO: Добавить очистку email
cleaned_df = cleaned_df.withColumn(
     "clean_email", lower(trim('email')))


# TODO: Добавить проверку корректности email
# Пример регулярного выражения для проверки email:
# email_regex = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
cleaned_df = cleaned_df.withColumn(
     "is_valid_email", col('clean_email').rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))

# TODO: Добавить очистку телефона
# 1. Убрать все не-цифровые символы
# 2. Привести к формату +7XXXXXXXXXX:
#    - если начинается с 8: заменить на +7
#    - если начинается с 7: добавить +
#    - если 10 цифр и начинается с 9: добавить +7
cleaned_df = cleaned_df.withColumn(
     "clean_phone", concat(lit("+7"), substring(regexp_replace(col('phone_number'), r"\D", ""), -10, 10)))

# TODO: Добавить проверку корректности телефона
# - должен начинаться с +7
# - длина должна быть 12 символов (включая +7)
cleaned_df = cleaned_df.withColumn(
     "is_valid_phone", (substring(col("clean_phone"), 1, 2) == "+7") & (length(col('clean_phone')) == 12))

# TODO: Добавить итоговый флаг валидности контактных данных
cleaned_df = cleaned_df.withColumn(
     "is_valid_contact", (col('is_valid_email')) & (col('is_valid_phone')))

# Проверки корректности данных
wrong_email_format = cleaned_df.filter(
    col("is_valid_email") &
    (~col("clean_email").rlike("^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$"))
).count()

wrong_phone_format = cleaned_df.filter(
    col("is_valid_phone") &
    (~col("clean_phone").startswith("+7") | (length(col("clean_phone")) != 12))
).count()

invalid_contact_flag = cleaned_df.filter(
    col("is_valid_contact") &
    (~col("is_valid_email") | ~col("is_valid_phone"))
).count()

# Проверяем результаты
assert wrong_email_format == 0, "Найдены некорректно очищенные email"
assert wrong_phone_format == 0, "Найдены некорректно очищенные телефоны"
assert invalid_contact_flag == 0, "Найдены ошибки в is_valid_contact"
