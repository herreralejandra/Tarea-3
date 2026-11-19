"""
Procesamiento en batch con Apache Spark
Problema: ¿dónde, cuándo y con qué vehículos ocurren los accidentes de tránsito
más graves en Bucaramanga?
Fuente: Accidentes de tránsito ocurridos en el municipio de Bucaramanga
(datos.gov.co, 7cci-nqqb), cargado en HDFS.

Autora: María Alejandra Herrera
Uso: spark-submit accidentes_batch.py [ruta_entrada]
"""
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ENTRADA = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3/accidentes.csv"
SALIDA = os.path.expanduser("~/tarea3_maria/salida")
VEHICULOS = ["AUTOMOVIL", "CAMPERO", "CAMIONETA", "MICRO", "BUSETA", "BUS",
             "CAMION", "VOLQUETA", "MOTO", "BICICLETA", "OTRO"]


def seccion(numero, titulo):
    print(f"\n{'-' * 72}\n {numero}. {titulo}\n{'-' * 72}")


def cargar(spark):
    """Lee el CSV y deja nombres de columna sin tildes ni espacios."""
    df = (spark.read.option("header", True).option("inferSchema", True).csv(ENTRADA)
          .withColumnRenamed("AÑO", "ANIO")
          .withColumnRenamed("DÍA", "DIA")
          .withColumnRenamed("Propietario de Vehículo", "PROPIETARIO")
          .withColumnRenamed("DIURNIO/NOCTURNO", "JORNADA"))
    return df


def limpiar(df):
    """Normaliza textos, crea variables derivadas y marca datos faltantes."""
    hora = F.hour(F.to_timestamp("HORA"))
    return (df
            .withColumn("GRAVEDAD", F.initcap(F.lower(F.trim("GRAVEDAD"))))
            .withColumn("COMUNA", F.when(F.col("COMUNA") == "NO DISPONIBLE", None).otherwise(F.col("COMUNA")))
            .withColumn("BARRIO", F.when(F.col("BARRIO") == "NO DISPONIBLE", None).otherwise(F.col("BARRIO")))
            .withColumn("HORA_DIA", hora)
            .withColumn("FRANJA", F.when(hora < 6, "1. Madrugada (0-5)")
                                   .when(hora < 12, "2. Mañana (6-11)")
                                   .when(hora < 18, "3. Tarde (12-17)")
                                   .otherwise("4. Noche (18-23)"))
            .withColumn("TOTAL_VEHICULOS", sum(F.col(v) for v in VEHICULOS))
            .withColumn("CON_MOTO", (F.col("MOTO") > 0).cast("int"))
            .withColumn("CON_PEATON", (F.col("PEATON") > 0).cast("int"))
            .withColumn("HERIDOS", (F.col("GRAVEDAD") == "Con Heridos").cast("int"))
            .withColumn("MUERTOS", (F.col("GRAVEDAD") == "Con Muertos").cast("int"))
            .withColumn("VICTIMAS", F.col("HERIDOS") + F.col("MUERTOS"))
            .dropDuplicates(["ORDEN"]))


def main():
    spark = (SparkSession.builder.appName("Tarea3_Batch_Accidentes_BGA")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    seccion(1, "CARGA DESDE HDFS")
    crudo = cargar(spark)
    print(f"Archivo: {ENTRADA}")
    print(f"Filas: {crudo.count():,} | Columnas: {len(crudo.columns)} | "
          f"Particiones: {crudo.rdd.getNumPartitions()}")
    crudo.printSchema()

    seccion(2, "LIMPIEZA Y TRANSFORMACIÓN")
    print("Escrituras distintas de la gravedad antes de limpiar:")
    crudo.groupBy("GRAVEDAD").count().show()
    df = limpiar(crudo).cache()
    print("Valores faltantes después de marcar 'NO DISPONIBLE':")
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
               for c in ["COMUNA", "BARRIO", "HORA_DIA", "GRAVEDAD"]]).show()
    df.select("FECHA", "HORA_DIA", "FRANJA", "COMUNA", "GRAVEDAD",
              "TOTAL_VEHICULOS", "CON_MOTO", "CON_PEATON").show(5, truncate=False)

    seccion(3, "EVOLUCIÓN ANUAL (función de ventana lag)")
    w = Window.orderBy("ANIO")
    (df.groupBy("ANIO").agg(F.count("*").alias("accidentes"), F.sum("VICTIMAS").alias("con_victimas"),
                            F.sum("MUERTOS").alias("con_muertos"))
       .withColumn("variacion_pct", F.round((F.col("accidentes") / F.lag("accidentes").over(w) - 1) * 100, 1))
       .orderBy("ANIO").show(15))

    seccion(4, "¿LA MOTO Y EL PEATÓN AUMENTAN LA GRAVEDAD?")
    for col, nombre in [("CON_MOTO", "Moto involucrada"), ("CON_PEATON", "Peatón involucrado")]:
        print(nombre)
        (df.groupBy(col).agg(F.count("*").alias("accidentes"),
                             F.round(F.avg("VICTIMAS") * 100, 2).alias("pct_con_victimas"),
                             F.round(F.avg("MUERTOS") * 100, 2).alias("pct_con_muertos"))
           .orderBy(col).show())

    seccion(5, "FRANJA HORARIA Y DÍA DE LA SEMANA")
    df.groupBy("DIA").pivot("FRANJA").count().orderBy("DIA").show(truncate=False)
    print("Horas con más accidentes:")
    df.groupBy("HORA_DIA").count().orderBy(F.desc("count")).show(5)

    seccion(6, "ÍNDICE DE RIESGO POR COMUNA (Spark SQL)")
    df.createOrReplaceTempView("accidentes")
    riesgo = spark.sql("""
        SELECT COMUNA,
               COUNT(*)                                        AS accidentes,
               SUM(HERIDOS)                                    AS con_heridos,
               SUM(MUERTOS)                                    AS con_muertos,
               ROUND((SUM(HERIDOS) + 5 * SUM(MUERTOS)) / COUNT(*), 3) AS indice_riesgo
        FROM accidentes
        WHERE COMUNA IS NOT NULL
        GROUP BY COMUNA
        HAVING COUNT(*) >= 300
        ORDER BY indice_riesgo DESC
    """)
    riesgo.show(10, truncate=False)

    seccion(7, "VEHÍCULOS INVOLUCRADOS (RDD: flatMap + reduceByKey)")
    vehiculos = (df.select(*VEHICULOS, "PEATON").rdd
                 .flatMap(lambda fila: [(k, v) for k, v in fila.asDict().items() if v])
                 .reduceByKey(lambda a, b: a + b)
                 .sortBy(lambda par: -par[1]))
    for tipo, total in vehiculos.take(8):
        print(f"   {tipo:<10} {total:>8,}")

    seccion(8, "VISUALIZACIÓN Y ALMACENAMIENTO")
    os.makedirs(SALIDA, exist_ok=True)
    riesgo.coalesce(1).write.mode("overwrite").option("header", True).csv("file://" + SALIDA + "/riesgo_comunas")
    df.write.mode("overwrite").partitionBy("ANIO").parquet("file://" + SALIDA + "/accidentes_parquet")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    anual = df.groupBy("ANIO").count().orderBy("ANIO").toPandas()
    horas = df.groupBy("HORA_DIA").count().orderBy("HORA_DIA").toPandas()
    moto = df.groupBy("CON_MOTO").agg(F.avg("MUERTOS").alias("m")).orderBy("CON_MOTO").toPandas()
    top = riesgo.limit(8).toPandas()

    fig, ax = plt.subplots(2, 2, figsize=(15, 9))
    ax[0, 0].plot(anual["ANIO"], anual["count"], marker="o", color="#1d3557")
    ax[0, 0].set_title("Accidentes por año")
    ax[0, 1].bar(horas["HORA_DIA"], horas["count"], color="#457b9d")
    ax[0, 1].set_title("Accidentes por hora del día")
    ax[1, 0].bar(["Sin moto", "Con moto"], moto["m"] * 100, color=["#a8dadc", "#e63946"])
    ax[1, 0].set_title("Accidentes con muertos (%)")
    ax[1, 1].barh(top["COMUNA"][::-1], top["indice_riesgo"][::-1], color="#f4a261")
    ax[1, 1].set_title("Índice de riesgo por comuna")
    fig.suptitle("Accidentes de tránsito en Bucaramanga 2012-2023 - Apache Spark")
    fig.tight_layout()
    fig.savefig(SALIDA + "/tablero_accidentes.png", dpi=110)
    print(f"Resultados guardados en {SALIDA}: riesgo_comunas (CSV), accidentes_parquet, tablero_accidentes.png")
    spark.stop()


if __name__ == "__main__":
    main()
