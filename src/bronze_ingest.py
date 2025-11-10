from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze").getOrCreate()

clientes = spark.read.option("header", True).csv("data/clientes.csv")
ventas   = spark.read.option("header", True).csv("data/ventas_mes.csv")
fact     = spark.read.option("header", True).csv("data/facturas_meta.csv")
logs_web = spark.read.option("multiLine", "true").json("data/logs_web.json")

ventas.write.mode("overwrite").parquet("bronze/ventas")
clientes.write.mode("overwrite").parquet("bronze/clientes")
fact.write.mode("overwrite").parquet("bronze/facturas_meta")
logs_web.write.mode("overwrite").parquet("bronze/logs_web")

print("Bronze listo. ")
spark.stop()


