from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round,coalesce, lit

spark = SparkSession.builder.appName("gold").getOrCreate()

# Read from silver layer
ventas = spark.read.parquet("silver/ventas")

# Show top 5 sales by amount
print("Top 5 ventas por importe:")
top5 = (ventas.groupBy("id_producto")
        .agg(
            sum("importe").alias("importe_total"),
            sum("unidades").alias("unidades_total")
        )
        .orderBy(col("unidades_total").desc())
        .show(5))


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

def verify_amounts():
    # Read both dataframes
    fact = spark.read.parquet("silver/facturas_meta")
    ventas = spark.read.parquet("silver/ventas")
    
    # Calculate totals by client and date
    fact_totals = (fact
        .groupBy("id_cliente", "fecha")
        .agg(round(sum("importe_total"), 2).alias("total_facturas"))
    )
    
    ventas_totals = (ventas
        .groupBy("id_cliente", "fecha")
        .agg(round(sum("importe"), 2).alias("total_ventas"))
    )
    
    # Compare both totals and handle nulls
    comparison = (fact_totals
        .join(ventas_totals, ["id_cliente", "fecha"], "full_outer")
        .withColumn("total_facturas", coalesce(col("total_facturas"), lit(0)))
        .withColumn("total_ventas", coalesce(col("total_ventas"), lit(0)))
        .withColumn("diferencia", col("total_facturas") - col("total_ventas"))
        .orderBy("fecha", "id_cliente")
    )
    
    print("\nComparación de importes facturas vs ventas:")
    comparison.show(truncate=False)
    
    # Check if there are differences
    differences = comparison.filter(col("diferencia") != 0)
    if differences.count() > 0:
        print("⚠️ Se encontraron diferencias en los importes!")
    else:
        print("✅ Los importes coinciden correctamente")

# ...existing code...

# Add after ventas_diarias.show(truncate=False):
verify_amounts()

print("\nVentas diarias detalladas:")
ventas_diarias.show(truncate=False)

# Save to gold layer
ventas.write.mode("overwrite").parquet("gold/ventas")
ventas_diarias.write.mode("overwrite").parquet("gold/ventas_diarias")

print("Gold listo.")
spark.stop()