from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("bronze").getOrCreate()

clientes = spark.read.option("header", True).csv("data/clientes.csv")
ventas   = spark.read.option("header", True).csv("data/ventas_mes.csv")
fact     = spark.read.option("header", True).csv("data/facturas_meta.csv")

ventas.write.mode("overwrite").partitionBy("fecha").parquet("bronze/ventas")

clientes.write.mode("overwrite").parquet("bronze/clientes")
fact.write.mode("overwrite").parquet("bronze/facturas_meta")

print("Bronze listo. ")
spark.stop()


