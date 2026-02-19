from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, when, coalesce, split, explode, array

# Создаем SparkSession
spark = SparkSession.builder.appName("ColumnOperations").getOrCreate()

# Создаем DataFrame
data = [
    (1, "Иван Петров", "Москва,Санкт-Петербург", 30, 50000, None),
    (2, "Анна Сидорова", "Казань", 25, 60000, "Менеджер"),
    (3, "Елена Иванова", "Новосибирск,Екатеринбург,Омск", 35, 70000, "Инженер"),
    (4, "Петр Смирнов", None, 40, 80000, "Директор"),
    (5, "Мария Кузнецова", "Владивосток", 28, 55000, None)
]
schema = ["id", "full_name", "cities", "age", "salary", "position"]
df = spark.createDataFrame(data, schema)

print("Исходный DataFrame:")
df.show()

#1
"""
Используя функции concat и lit, создайте новую колонку 'greeting', 
которая будет содержать приветствие вида 
"Привет, {full_name}! Ваш ID: {id}", сохраните в df_greeting и напечатайте содержимое.
"""
from pyspark.sql.functions import col, concat, lit

df_greeting = df.select(
    '*', concat(lit('Привет, '), col('full_name'), lit('! Ваш ID: '), col('id')).alias('greeting'))
df_greeting.show(truncate=False)


#2
"""
С помощью функции withColumn добавьте колонку 'salary_grade', 
которая будет содержать строку 
'Высокая' для зарплат больше 60000, 
'Средняя' для зарплат от 50000 до 60000 включительно, и 
'Низкая' для зарплат ниже 50000, 
сохраните в df_salary_grade и напечатайте содержимое.
"""
from pyspark.sql.functions import when

df_salary_grade = df. \
    withColumn('salary_grade', \
        when(col('salary') > 60000, 'Высокая'). \
        when(col('salary').between(50000, 60000), 'Средняя'). \
        when(col('salary') < 50000, 'Низкая'))
df_salary_grade.show()


#3
"""
Используя функцию coalesce, создайте колонку 'employee_info', 
которая будет содержать значение из колонки 'position', 
если оно доступно, или строку 'Сотрудник' в противном случае, 
сохраните в df_employee_info и напечатайте содержимое.
"""

from pyspark.sql.functions import coalesce

df_employee_info = df. \
    withColumn('employee_info', coalesce('position', lit('Сотрудник')))

df_employee_info.show()



#4
"""
Примените функции split и explode к колонке 'cities', 
чтобы создать новый DataFrame, 
где каждая строка будет содержать одного сотрудника и один город 
(дублируйте строки для сотрудников с несколькими городами), 
сохраните в df_exploded_cities и напечатайте содержимое.
"""
from pyspark.sql.functions import split, explode, when

df_exploded_cities = df \
    .withColumn("city", explode(split("cities", ",")))

df_exploded_cities.show()


#5
"""
Создайте новую колонку 'age_group', 
которая будет содержать возрастную группу сотрудника:  '18-30', '31-50', '51+'. 
Используйте функцию when для этого преобразования, 
сохраните в df_age_group и напечатайте содержимое.
"""

df_age_group = df.select('*') \
    .withColumn('age_group', \
        when(col('age').between(18, 30),'18-30'). \
        when(col('age').between(31, 50), '31-50'). \
        when(col('age') >= 51, '51+'))
df_age_group.show()
