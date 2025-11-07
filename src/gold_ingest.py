from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("gold").getOrCreate()

# Read from silver layer
ventas = spark.read.parquet("silver/ventas")

# Show top 5 sales by amount
print("Top 5 ventas por importe:")
ventas.orderBy(col("importe").desc()).show(5)

# Save to gold layer
ventas.write.mode("overwrite").parquet("gold/ventas")

print("Gold listo.")
spark.stop()