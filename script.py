# Импортируем необходимые библиотеки
from pyspark.sql import SparkSession
from faker import Faker
from operator import add

# Инициализация Spark-сессии с указанием порта для Spark UI
spark = SparkSession.builder \
    .appName("DistributedDatabaseExample") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Генерация данных
fake = Faker()
num_records = 1_000_000  # 1 миллион записей
data = [(fake.company(),) for _ in range(num_records)]

# Создание DataFrame с названиями компаний
companies_df = spark.createDataFrame(data, ["company_name"])

# Распределение данных на 4 параллельные части
companies_df = companies_df.repartition(4)

# Фильтрация данных
# Пример: отбираем компании, в названии которых есть слово "Group"
filtered_df = companies_df.filter(companies_df.company_name.contains("Group"))

# Приводим названия компаний к верхнему регистру с помощью map
mapped_df = filtered_df.rdd.map(lambda row: (row.company_name.upper(),)).toDF(["company_name"])

# Шаг 1: Применение функции map для группировки по первому символу названия компании
grouped_rdd = mapped_df.rdd.map(lambda row: (row.company_name[0], 1))

# Шаг 2: Применение reduceByKey для суммирования количества компаний в каждой группе
result_rdd = grouped_rdd.reduceByKey(add)

# Показать результаты группировки
print("Распределение количества компаний по первым буквам названий:")
result = result_rdd.collect()
for letter, count in result:
    print(f"{letter}: {count}")

# Настройка туннеля для Spark UI
import subprocess
import time

# Запуск туннеля и вывод адреса Spark UI
print("Настройка туннеля для Spark UI...")
tunnel_process = subprocess.Popen(["lt", "--port", "4040", "-s", "mytunnelpassword"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
time.sleep(2)  # Подождать пару секунд, чтобы туннель установился

# Чтение и вывод URL для Spark UI
tunnel_output = tunnel_process.stdout.readline().decode("utf-8")
print("Spark UI доступен по следующему URL:")
print(tunnel_output)
