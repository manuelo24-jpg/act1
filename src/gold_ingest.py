from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, date_format

spark = SparkSession.builder.appName("gold").getOrCreate()

# Read from silver layer
ventas = spark.read.parquet("silver/ventas")

# Show top 5 sales by amount
print("Top 5 ventas por importe:")
ventas.orderBy(col("importe").desc()).show(5)

# Ventas agrupadas por día
print("\nVentas por día:")
ventas_diarias = (ventas
    .groupBy("fecha")
    .agg(
        count("id_venta").alias("num_ventas"),
        sum("importe").alias("importe_total"),
        sum("unidades").alias("unidades_total")
    )
    .orderBy("fecha")
)

ventas_diarias.show(truncate=False)

# Save to gold layer
ventas.write.mode("overwrite").parquet("gold/ventas")
ventas_diarias.write.mode("overwrite").parquet("gold/ventas_diarias")

print("Gold listo.")
spark.stop()