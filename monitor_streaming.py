"""
Consumidor con Spark Structured Streaming: monitorea en tiempo real los
reportes de accidentes del topic 'accidentes_bga'.
  - Panel por comuna en ventanas de 2 minutos que avanzan cada 30 segundos.
  - Indicadores globales de cada microlote (accidentes, % con víctimas, motos).
  - Alertas de accidentes con muertos o con peatones.

Autora: María Alejandra Herrera
Uso: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 monitor_streaming.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ESQUEMA = StructType(
    [StructField("reporte", IntegerType()), StructField("hora", StringType()),
     StructField("comuna", StringType()), StructField("barrio", StringType()),
     StructField("gravedad", StringType()), StructField("peatones", IntegerType())]
    + [StructField(v, IntegerType()) for v in ["automovil", "camioneta", "bus", "camion", "moto", "bicicleta"]])

spark = (SparkSession.builder.appName("Tarea3_Monitor_Accidentes")
         .config("spark.sql.shuffle.partitions", "3")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

reportes = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "accidentes_bga")
            .option("startingOffsets", "latest")
            .load()
            .select(F.col("key").cast("string").alias("llave"),
                    F.from_json(F.col("value").cast("string"), ESQUEMA).alias("r"))
            .select("r.*")
            .withColumn("momento", F.to_timestamp("hora"))
            .withColumn("victimas", F.col("gravedad").isin("Con Heridos", "Con Muertos").cast("int")))

# 1. Panel por comuna en ventanas deslizantes
panel = (reportes.withWatermark("momento", "1 minute")
         .groupBy(F.window("momento", "2 minutes", "30 seconds"), "comuna")
         .agg(F.count("*").alias("accidentes"), F.sum("victimas").alias("con_victimas"),
              F.sum((F.col("gravedad") == "Con Muertos").cast("int")).alias("con_muertos"),
              F.sum("moto").alias("motos")))


def mostrar_panel(lote, id_lote):
    if lote.isEmpty():
        return
    ventana = lote.agg(F.max("window")).first()[0]
    actual = lote.filter(F.col("window.start") == F.lit(ventana.start))
    totales = actual.agg(F.sum("accidentes").alias("a"), F.sum("con_victimas").alias("v"),
                         F.sum("motos").alias("m")).first()
    print(f"\n### Lote {id_lote} | ventana {ventana.start:%H:%M:%S}-{ventana.end:%H:%M:%S} | "
          f"accidentes={totales.a} | con víctimas={totales.v / totales.a:.0%} | motos={totales.m}")
    (actual.orderBy(F.desc("accidentes"))
           .select("comuna", "accidentes", "con_victimas", "con_muertos", "motos")
           .show(6, truncate=False))


consulta_panel = (panel.writeStream.outputMode("update").foreachBatch(mostrar_panel)
                  .trigger(processingTime="20 seconds").start())

# 2. Alertas inmediatas
alertas = (reportes.filter((F.col("gravedad") == "Con Muertos") | (F.col("peatones") > 0))
           .select(F.date_format("momento", "HH:mm:ss").alias("hora"), "comuna", "barrio",
                   "gravedad", "peatones", "moto")
           .withColumn("alerta", F.when(F.col("gravedad") == "Con Muertos", "FATAL").otherwise("PEATÓN")))

consulta_alertas = (alertas.writeStream.outputMode("append").format("console")
                    .option("truncate", "false").queryName("alertas")
                    .trigger(processingTime="20 seconds").start())

spark.streams.awaitAnyTermination()
