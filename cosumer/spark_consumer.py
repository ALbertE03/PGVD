#!/usr/bin/env python3
"""
Consumidor Spark Streaming que procesa datos genómicos de Kafka.
Replica la misma lógica de interactive_console.py pero usando Spark para procesamiento distribuido.
Calcula estadísticas por familia y miembro en tiempo real.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, count, countDistinct, approx_count_distinct, collect_set, size, array_distinct,
    sum as spark_sum, max as spark_max, min as spark_min, avg, stddev, 
    when, lit, current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ArrayType
)
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================

KAFKA_BROKER = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE_URL', 'hdfs://namenode:9000')
CHECKPOINT_DIR = f"{HDFS_NAMENODE}/spark-checkpoints/genomic-consumer"

# Topics de entrada (datos genómicos)
INPUT_TOPICS = "fathers,mothers,children"

# Topics de salida (estadísticas procesadas)
OUTPUT_TOPIC_FAMILY = "family-stats"
OUTPUT_TOPIC_MEMBER = "member-stats"

# ==================== ESQUEMA DE DATOS ====================

# Schema para los mensajes de Kafka (mismo que interactive_console.py)
message_schema = StructType([
    StructField("family_id", StringType(), True),
    StructField("member_type", StringType(), True),  # father, mother, child
    StructField("person_id", StringType(), True),
    StructField("total_snps", IntegerType(), True),
    StructField("snp_data", StructType([
        StructField("chromosome", StringType(), True),
        StructField("position", LongType(), True),
        StructField("genotype", StringType(), True)
    ]), True),
    StructField("message_type", StringType(), True)  # FAMILY_COMPLETE o null
])

# ==================== FUNCIONES ====================

def create_spark_session():
    """Crea y configura la sesión de Spark"""
    logger.info("🚀 Creando sesión de Spark...")
    
    spark = SparkSession.builder \
        .appName("GenomicDataConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.kafka.maxRatePerPartition", "500") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Sesión de Spark creada exitosamente")
    
    return spark

def read_kafka_stream(spark):
    """Lee el stream de datos desde Kafka"""
    logger.info(f"📡 Conectando a Kafka: {KAFKA_BROKER}")
    logger.info(f"📋 Topics: {INPUT_TOPICS}")
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPICS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("✅ Conectado a Kafka stream")
    return kafka_df

def process_genomic_data(kafka_df):
    """
    Procesa los datos genómicos exactamente como lo hace interactive_console.py
    pero usando Spark para procesamiento distribuido
    """
    logger.info("⚙️  Configurando pipeline de procesamiento...")
    
    # 1. Parsear JSON de Kafka
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), message_schema).alias("data")) \
        .select("data.*")
    
    # 2. Separar datos genómicos de tokens de finalización
    genomic_data = parsed_df.filter(col("message_type").isNull())
    completion_tokens = parsed_df.filter(col("message_type") == "FAMILY_COMPLETE")
    
    # 3. ESTADÍSTICAS POR MIEMBRO (equivalente a families_data['members'])
    # Optimizado: usamos approx_count_distinct (requerido para streaming)
    member_stats = genomic_data.groupBy("family_id", "person_id", "member_type") \
        .agg(
            count("*").alias("snp_count"),
            approx_count_distinct("snp_data.genotype").alias("unique_genotypes"),
            count(when(col("snp_data.genotype").isNotNull(), 1)).alias("genotypes_count"),
            approx_count_distinct("snp_data.position").alias("unique_positions"),
            spark_max("total_snps").alias("total_snps"),
            avg("snp_data.position").alias("avg_position")
        )
    
    # 4. ESTADÍSTICAS POR FAMILIA (equivalente a families_data[family_id])
    # Optimizado: usar approx_count_distinct (requerido para streaming)
    family_stats = genomic_data.groupBy("family_id") \
        .agg(
            count("*").alias("snp_records"),
            spark_max("total_snps").alias("total_snps"),
            approx_count_distinct("snp_data.chromosome").alias("unique_chromosomes"),
            approx_count_distinct("person_id").alias("total_members"),
            # Contar PERSONAS distintas por tipo (no SNPs)
            approx_count_distinct(when(col("member_type") == "father", col("person_id"))).alias("fathers"),
            approx_count_distinct(when(col("member_type") == "mother", col("person_id"))).alias("mothers"),
            approx_count_distinct(when(col("member_type") == "child", col("person_id"))).alias("children")
        )
    
    # 5. Marcar familias completadas (usando tokens de finalización)
    # Esto es un stream separado que podríamos usar para tracking
    
    logger.info("✅ Pipeline de procesamiento configurado")
    
    return genomic_data, member_stats, family_stats, completion_tokens

def print_statistics_to_console(df, query_name, output_mode="complete"):
    """Imprime estadísticas a la consola (similar a la visualización de interactive_console.py)"""
    query = df.writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .queryName(query_name) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query

def write_to_hdfs_parquet(df, path: str, query_name: str) -> None:
    """
    Escribe resultados a HDFS en formato Parquet usando foreachBatch
    
    Args:
        df: DataFrame de Spark streaming a escribir
        path: Ruta HDFS de destino
        query_name: Nombre único para la query y checkpoint
    
    Note:
        Usa foreachBatch para escribir cada micro-batch como archivo Parquet.
        Esto permite trabajar con DataFrames batch donde append mode sí funciona.
    """
    def write_batch_to_hdfs(batch_df, batch_id):
        """Función que procesa cada micro-batch"""
        if batch_df.count() > 0:
            # Escribir el batch con timestamp para evitar sobrescribir
            output_path = f"{path}/batch_{batch_id}"
            batch_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            logger.info(f"📦 Batch {batch_id} escrito a HDFS: {output_path} ({batch_df.count()} registros)")
    
    # Usar foreachBatch para procesar cada micro-batch
    query = df.writeStream \
        .foreachBatch(write_batch_to_hdfs) \
        .outputMode("update") \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, query_name)) \
        .queryName(query_name) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    return query


def write_stats_to_kafka(df, topic, query_name, output_mode="update"):
    """Escribe estadísticas procesadas de vuelta a Kafka para que la consola las consuma"""
    from pyspark.sql.functions import to_json, struct
    
    # Convertir el dataframe a formato JSON (solo las columnas que queremos)
    json_df = df.select(
        to_json(struct("*")).alias("value")
    )
    
    query = json_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{query_name}") \
        .outputMode(output_mode) \
        .queryName(query_name) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query

# ==================== MAIN ====================

def main():
    """Función principal del consumidor Spark"""
    
    print("\n" + "="*80)
    print("🧬 GENOMIC DATA CONSUMER - SPARK STREAMING")
    print("="*80)
    print()
    
    try:
        # 1. Crear sesión de Spark
        spark = create_spark_session()
        
        # 2. Leer stream de Kafka
        kafka_df = read_kafka_stream(spark)
        
        # 3. Procesar datos genómicos (misma lógica que interactive_console.py)
        raw_data, member_stats, family_stats, completion_tokens = process_genomic_data(kafka_df)
        
        # 4. Configurar outputs
        logger.info("📊 Configurando visualización de estadísticas...")
        
        # Preparar datos para la consola (seleccionar solo las columnas necesarias)
        family_stats_clean = family_stats.select(
            col("family_id"),
            col("snp_records"),
            col("total_snps"),
            col("unique_chromosomes"),
            col("total_members"),
            col("fathers").cast("int"),
            col("mothers").cast("int"),
            col("children").cast("int")
        )
        
        member_stats_clean = member_stats.select(
            col("family_id"),
            col("person_id"),
            col("member_type"),
            col("snp_count").cast("int"),
            col("unique_genotypes"),
            col("genotypes_count").cast("int"),
            col("unique_positions"),
            col("total_snps"),
            col("avg_position")
        )
        
        # Escribir estadísticas a Kafka para que la consola las lea
        query_family_kafka = write_stats_to_kafka(
            family_stats_clean,
            OUTPUT_TOPIC_FAMILY,
            "family_stats_kafka",
            output_mode="update"
        )
        
        query_member_kafka = write_stats_to_kafka(
            member_stats_clean,
            OUTPUT_TOPIC_MEMBER,
            "member_stats_kafka",
            output_mode="update"
        )
        
        # Procesar y reenviar tokens de finalización de familias
        completion_tokens_clean = completion_tokens.select(
            col("family_id"),
            col("message_type")
        )
        
        query_completion_kafka = write_stats_to_kafka(
            completion_tokens_clean,
            "family-completion",
            "family_completion_kafka",
            output_mode="append"
        )
        
        # ✅ Escribir a HDFS usando foreachBatch (soporta agregaciones)
        logger.info("💾 Configurando escritura a HDFS...")
        query_hdfs_member = write_to_hdfs_parquet(
            member_stats, 
            f"{HDFS_NAMENODE}/genomic-data/member_stats",
            "member_stats_hdfs"
        )
        query_hdfs_family = write_to_hdfs_parquet(
            family_stats,
            f"{HDFS_NAMENODE}/genomic-data/family_stats", 
            "family_stats_hdfs"
        )
        
        print()
        logger.info("✅ Spark Streaming iniciado correctamente")
        print()
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print("📊 PROCESAMIENTO EN TIEMPO REAL:")
        print("   • Consumiendo de Kafka topics: fathers, mothers, children")
        print("   • Calculando estadísticas por familia y miembro")
        print(f"   • Publicando resultados a Kafka: {OUTPUT_TOPIC_FAMILY}, {OUTPUT_TOPIC_MEMBER}")
        print(f"   • Guardando en HDFS: {HDFS_NAMENODE}/genomic-data/")
        print("   • Actualizando cada 30 segundos")
        print()
        print("🔍 ESTADÍSTICAS CALCULADAS:")
        print("   • SNPs procesados por miembro")
        print("   • Genotipos únicos encontrados")
        print("   • Posiciones genómicas únicas")
        print("   • Cromosomas detectados")
        print("   • Conteo de padres, madres e hijos")
        print()
        print("⚡ OPTIMIZACIONES:")
        print("   • Uso de approx_count_distinct (requerido para streaming)")
        print("   • Escritura a HDFS usando foreachBatch (soporta agregaciones)")
        print("   • Procesamiento cada 30 segundos")
        print()
        print("💾 PERSISTENCIA:")
        print("   • Kafka: Estadísticas en tiempo real")
        print("   • HDFS: Archivos Parquet para análisis histórico")
        print()
        print("💡 La consola interactiva lee las estadísticas desde Kafka")
        print("💡 Ejecuta: cd cosumer && ./console.sh")
        print("💡 Presiona Ctrl+C para detener")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print()
        
        # Esperar a que terminen las queries
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        logger.info("\n\n🛑 Deteniendo Spark Streaming...")
        spark.stop()
        logger.info("✅ Spark detenido correctamente")
        
    except Exception as e:
        logger.error(f"❌ Error en el consumidor Spark: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
