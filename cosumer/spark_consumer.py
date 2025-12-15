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
from datetime import datetime

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
    
    spark = SparkSession.builder \
        .master("spark://spark-master-1:7077") \
        .appName("GenomicDataConsumer") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.host", "spark-driver") \
        .config("spark.driver.port", "7078") \
        .config("spark.driver.blockManager.port", "7079") \
        .config("spark.rpc.netty.dispatcher.numThreads", "16") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
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
        .load()
    
    # Parsear JSON
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_at", current_timestamp())
    
    return parsed_df

def calculate_and_send_metrics(batch_df, batch_id, member_type):
    """Calcula métricas del batch y las envía al dashboard mejorado"""
    if batch_df.isEmpty():
        return
    
    try:
        # Contar registros
        total_records = batch_df.count()
        
        # Convertir a lista de registros para análisis
        records = batch_df.collect()
        records_dict = [record.asDict() for record in records]
        
        # Convertir objetos datetime a strings ISO en los registros
        for record in records_dict:
            for key, value in record.items():
                if isinstance(value, datetime):
                    record[key] = value.isoformat()
        
        # Preparar datos para enviar
        metrics = {
            'member_type': member_type,
            'batch_id': batch_id,
            'timestamp': datetime.now().isoformat(),
            'total_records': total_records,
            'records': records_dict
        }
        
        # Enviar métricas al dashboard mejorado
        response = requests.post(
            f"{DASHBOARD_URL}/api/metrics/update",
            json=metrics,
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"📊 [{member_type}] Batch {batch_id}: {total_records} registros → Dashboard")
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
    """Función principal que inicia el streaming con reintentos"""
    import time
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        spark = None
        try:
            print("=" * 80)
            print("🧬 INICIANDO CONSUMIDOR DE DATOS GENÓMICOS CON SPARK STREAMING")
            if retry_count > 0:
                print(f"   (Intento {retry_count + 1}/{max_retries})")
            print("=" * 80)
            
            # Crear sesión de Spark
            spark = create_spark_session()
            
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
                .trigger(processingTime="5 seconds") \
                .start()
            
            query_mothers_metrics = mothers_df.writeStream \
                .foreachBatch(process_mothers) \
                .option("checkpointLocation", f"{HDFS_NAMENODE}/checkpoints/mothers_metrics") \
                .trigger(processingTime="5 seconds") \
                .start()
            
            query_children_metrics = children_df.writeStream \
                .foreachBatch(process_children) \
                .option("checkpointLocation", f"{HDFS_NAMENODE}/checkpoints/children_metrics") \
                .trigger(processingTime="5 seconds") \
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
            
            # Si llegamos aquí, el streaming terminó exitosamente
            break
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Deteniendo el consumidor...")
            if spark:
                spark.stop()
            print("✅ Consumidor detenido correctamente")
            break
            
        except Exception as e:
            print(f"\n❌ Error en el consumidor (intento {retry_count + 1}/{max_retries}): {str(e)}")
            import traceback
            traceback.print_exc()
            
            # Limpiar recursos
            if spark:
                try:
                    spark.stop()
                except:
                    pass
            
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(10 * retry_count, 30)  # Max 30 segundos
                print(f"⏳ Reintentando en {wait_time} segundos...")
                time.sleep(wait_time)
            else:
                print(f"❌ Se alcanzó el máximo de reintentos ({max_retries})")
                raise

if __name__ == "__main__":
    main()
