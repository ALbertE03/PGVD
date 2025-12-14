#!/usr/bin/env python3
"""
Spark Streaming Consumer - Lee de Kafka, calcula métricas y escribe en HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, count, avg, 
    min as spark_min, max as spark_max, window
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import os
import time

# Configuración
KAFKA_BROKER = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
DASHBOARD_URL = os.getenv('DASHBOARD_URL', 'http://dashboard:5000')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE_URL', 'hdfs://namenode:9000')

# Definir esquema para los SNP messages 
snp_data_schema = StructType([
    StructField("chromosome", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("genotype", StringType(), True)
])

snp_schema = StructType([
    StructField("family_id", StringType(), True),
    StructField("member_type", StringType(), True),
    StructField("person_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_created", StringType(), True),
    StructField("total_snps", IntegerType(), True),
    StructField("snp_data", snp_data_schema, True),
    StructField("timestamp", StringType(), True)
])

def create_spark_session():
    """Crea y configura la sesión de Spark"""
    print(f"🚀 Conectando al cluster Spark...")
    
    # Obtener número de cores del cluster dinámicamente
    # Spark autodetectará esto, pero podemos dar un hint inicial
    spark = SparkSession.builder \
        .appName("GenomicDataConsumer") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.scheduler.revive.interval", "1s") \
        .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "120s") \
        .config("spark.scheduler.minRegisteredResourcesRatio", "0.3") \
        .config("spark.rpc.retry.wait", "5s") \
        .config("spark.rpc.numRetries", "10") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "30s") \
        .config("spark.network.timeoutInterval", "240s") \
        .config("spark.rpc.lookupTimeout", "240s") \
        .config("spark.core.connection.ack.wait.timeout", "600s") \
        .config("spark.storage.blockManagerTimeoutIntervalMs", "600000") \
        .config("spark.blacklist.enabled", "false") \
        .config("spark.task.maxFailures", "20") \
        .config("spark.stage.maxConsecutiveAttempts", "20") \
        .config("spark.streaming.kafka.consumer.poll.ms", "512") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRetries", "5") \
        .config("spark.streaming.receiver.writeAheadLog.enable", "true") \
        .config("spark.executor.allowSparkContext", "true") \
        .config("spark.rpc.io.clientThreads", "8") \
        .config("spark.rpc.io.serverThreads", "8") \
        .config("spark.rpc.connect.threads", "128") \
        .config("spark.rpc.message.maxSize", "256") \
        .getOrCreate()
    
    # Ajustar particiones dinámicamente basado en cores reales
    sc = spark.sparkContext
    total_cores = int(sc._conf.get("spark.executor.instances", "3")) * int(sc._conf.get("spark.executor.cores", "2"))
    
    # Configurar particiones igual al número de cores para máxima paralelización
    spark.conf.set("spark.sql.shuffle.partitions", str(total_cores))
    spark.conf.set("spark.default.parallelism", str(total_cores))
    
    print(f"✅ Spark configurado con {total_cores} cores totales")
    print(f"   - Shuffle partitions: {total_cores}")
    print(f"   - Default parallelism: {total_cores}")
    print(f"   - Configuración de tolerancia a fallos: ACTIVADA")
    print(f"   - Timeouts extendidos para reconexiones: ACTIVADO")
    
    # Configurar nivel de logging
    spark.sparkContext.setLogLevel("ERROR")
    
    # Suprimir warnings específicos de Kafka
    import logging
    logging.getLogger("org.apache.kafka.clients.admin.KafkaAdminClient").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql.execution.streaming").setLevel(logging.ERROR)
    
    print("✅ Sesión de Spark creada exitosamente")
    return spark

def read_kafka_stream(spark, topic, schema):
    """Lee un stream de Kafka con el esquema especificado"""
    print(f"📡 Leyendo del topic de Kafka: {topic}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.session.timeout.ms", "60000") \
        .option("kafka.request.timeout.ms", "70000") \
        .option("kafka.connections.max.idle.ms", "300000") \
        .option("kafka.max.poll.interval.ms", "600000") \
        .option("kafka.heartbeat.interval.ms", "10000") \
        .option("kafka.metadata.max.age.ms", "30000") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()
    
    # Parsear JSON
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_at", current_timestamp())
    
    return parsed_df

def calculate_and_send_metrics(batch_df, batch_id, member_type):
    """Calcula métricas del batch y las envía al dashboard - Solo para gráfica de velocidad de procesamiento"""
    if batch_df.isEmpty():
        return
    
    try:
        # Solo contamos registros para la gráfica de velocidad
        total_records = batch_df.count()
        
        metrics = {
            'member_type': member_type,
            'batch_id': batch_id,
            'timestamp': str(batch_df.select(current_timestamp()).first()[0]),
            'total_records': total_records
        }
        
        # Enviar métricas al dashboard
        response = requests.post(
            f"{DASHBOARD_URL}/api/metrics",
            json=metrics,
            timeout=5
        )
        
        if response.status_code == 200:
            print(f"📊 [{member_type}] Batch {batch_id}: {total_records} registros procesados")
        else:
            print(f"⚠️  Error enviando métricas [{member_type}]: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error calculando/enviando métricas [{member_type}]: {e}")
        import traceback
        traceback.print_exc()

def write_to_hdfs(df, path_name):
    """Escribe el stream a HDFS en formato Parquet"""
    hdfs_path = f"{HDFS_NAMENODE}/genomic_data/{path_name}"
    
    # Checkpoint en HDFS para recuperación ante fallos del driver
    query = df.writeStream \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", f"{HDFS_NAMENODE}/checkpoints/hdfs_{path_name}") \
        .outputMode("append") \
        .trigger(processingTime="1 seconds") \
        .start()
    
    print(f"💾 HDFS: Guardando en {hdfs_path}")
    return query


def main():
    """Función principal que inicia el streaming"""
    print("=" * 80)
    print("🧬 INICIANDO CONSUMIDOR DE DATOS GENÓMICOS CON SPARK STREAMING")
    print("=" * 80)
    
    # Esperar un momento para que los servicios estén listos
    print("\n⏳ Esperando inicialización de servicios...")
    time.sleep(10)
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    try:
        # Leer streams de Kafka (todos usan el mismo esquema SNP)
        fathers_df = read_kafka_stream(spark, "fathers", snp_schema)
        mothers_df = read_kafka_stream(spark, "mothers", snp_schema)
        children_df = read_kafka_stream(spark, "children", snp_schema)
        
        print("\n✅ Streams de Kafka configurados correctamente")
        print(f"   - Topic: fathers")
        print(f"   - Topic: mothers")
        print(f"   - Topic: children")
        
        # Configurar procesamiento y envío de métricas
        print("\n📊 Configurando cálculo de métricas...")
        
        def process_fathers(batch_df, batch_id):
            calculate_and_send_metrics(batch_df, batch_id, "fathers")
        
        def process_mothers(batch_df, batch_id):
            calculate_and_send_metrics(batch_df, batch_id, "mothers")
        
        def process_children(batch_df, batch_id):
            calculate_and_send_metrics(batch_df, batch_id, "children")
        
        # Checkpoints en HDFS para recuperación ante fallos
        query_fathers_metrics = fathers_df.writeStream \
            .foreachBatch(process_fathers) \
            .option("checkpointLocation", f"{HDFS_NAMENODE}/checkpoints/fathers_metrics") \
            .trigger(processingTime="1 second") \
            .start()
        
        query_mothers_metrics = mothers_df.writeStream \
            .foreachBatch(process_mothers) \
            .option("checkpointLocation", f"{HDFS_NAMENODE}/checkpoints/mothers_metrics") \
            .trigger(processingTime="1 second") \
            .start()
        
        query_children_metrics = children_df.writeStream \
            .foreachBatch(process_children) \
            .option("checkpointLocation", f"{HDFS_NAMENODE}/checkpoints/children_metrics") \
            .trigger(processingTime="1 second") \
            .start()
        
        # Escribir a HDFS
        print("\n💾 Configurando escritura a HDFS...")
        hdfs_fathers = write_to_hdfs(fathers_df, "fathers")
        hdfs_mothers = write_to_hdfs(mothers_df, "mothers")
        hdfs_children = write_to_hdfs(children_df, "children")
        
        print("\n" + "=" * 80)
        print("✅ CONSUMIDOR INICIADO - Procesando datos en tiempo real...")
        print("=" * 80)
        print(f"\n🔗 Kafka Broker: {KAFKA_BROKER}")
        print(f"🔗 Dashboard: {DASHBOARD_URL}")
        print(f"🔗 HDFS: {HDFS_NAMENODE}")
        
        # Esperar a que terminen los queries (streaming continuo)
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Deteniendo el consumidor...")
        spark.stop()
        print("✅ Consumidor detenido correctamente")
    except Exception as e:
        print(f"\n❌ Error en el consumidor: {str(e)}")
        import traceback
        traceback.print_exc()
        spark.stop()
        raise

if __name__ == "__main__":
    main()
