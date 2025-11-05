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
.withColumn("fecha", to_date(col("fecha")))
.dropDuplicates(["id_venta"])
)

ventas.write.mode("overwrite").parquet("silver/ventas")

print("Silver listo. ")
spark.stop()


