from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder.appName("silver").getOrCreate()

clientes = spark.read.parquet("bronze/clientes")
ventas   = spark.read.parquet("bronze/ventas")
fact     = spark.read.parquet("bronze/facturas_meta")

ventas = (ventas
.withColumn("id_venta", col("id_venta").cast(IntegerType())) 
.withColumn("id_cliente", col("id_cliente").cast(IntegerType())) 
.withColumn("unidades", col("unidades").cast(IntegerType())) 
.withColumn("importe", col("importe").cast(DecimalType()))
.filter((col("importe") > 0))
.withColumn("id_producto", col("id_producto"))
.withColumn("fecha", to_date(col("fecha")))
.dropDuplicates(["id_venta"])
)

fact = (fact.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
.withColumn("importe_total", col("importe_total").cast(DecimalType()))
.filter((col("importe_total") > 0))
.withColumn("fecha", to_date(col("fecha")))
.dropDuplicates(["id_factura"])
)

clientes = (clientes.withColumn("id_cliente", col("id_cliente").cast(IntegerType())) 
.withColumn("fecha_alta", to_date(col("fecha_alta")))
.dropDuplicates(["id_cliente"]))

ventas.write.mode("overwrite").parquet("silver/ventas")
fact.write.mode("overwrite").parquet("silver/facturas_meta")
clientes.write.mode("overwrite").parquet("silver/clientes")

print("Silver listo. ")
spark.stop()


