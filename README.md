# Accidentes de tránsito en Bucaramanga con Apache Spark y Kafka

Tarea 3 del curso Big Data (UNAD) · María Alejandra Herrera

Análisis de los 39.193 accidentes de tránsito registrados en Bucaramanga entre 2012 y 2023
([datos.gov.co, 7cci-nqqb](https://www.datos.gov.co/d/7cci-nqqb)):

- **Procesamiento en batch** con PySpark desde HDFS: limpieza, variables derivadas, evolución anual
  con funciones de ventana, efecto de la moto y del peatón en la gravedad, tabla por día y franja
  horaria, índice de riesgo por comuna con Spark SQL y conteo de vehículos con RDD.
- **Procesamiento en tiempo real** con Kafka y Spark Structured Streaming: un productor simula los
  reportes de los agentes de tránsito y un monitor calcula indicadores por comuna en ventanas de
  tiempo y emite alertas de accidentes fatales o con peatones.

| Archivo | Descripción |
|---|---|
| `accidentes_batch.py` | Procesamiento en batch (Spark DataFrames, Spark SQL y RDD) |
| `reportes_producer.py` | Productor de Kafka para el topic `accidentes_bga` |
| `monitor_streaming.py` | Consumidor con Spark Structured Streaming |

## Requisitos
Ubuntu 22.04, Java 11, Hadoop 3.3.6, Spark 3.5, Kafka 3.9.2 en `/opt/Kafka` y Python 3.10 con
`pip install pyspark kafka-python matplotlib pandas`.

## Procesamiento en batch
```bash
wget -O accidentes.csv https://www.datos.gov.co/api/views/7cci-nqqb/rows.csv
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put accidentes.csv /Tarea3/
spark-submit --driver-memory 2g accidentes_batch.py
```
Los resultados quedan en `~/tarea3_maria/salida/`: `riesgo_comunas` (CSV),
`accidentes_parquet` (Parquet particionado por año) y `tablero_accidentes.png`.

## Procesamiento en tiempo real
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1 --topic accidentes_bga
python3 reportes_producer.py --por-segundo 4          # terminal 3
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 monitor_streaming.py   # terminal 4
```
Interfaz web de Spark: `http://<ip-de-la-maquina>:4040` (pestaña Structured Streaming).
