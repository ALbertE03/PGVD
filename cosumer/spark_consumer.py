#!/usr/bin/env python3
"""
Spark Streaming Consumer - Lee continuamente de Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import os

# Configuración
KAFKA_BROKER = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:genomic2025@mongodb:27017/genomic_db?authSource=admin')
SPARK_MASTER = os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE_URL', 'hdfs://namenode:9000')

# Definir esquema para los SNP messages (todos los topics tienen la misma estructura)
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
    print(f"🚀 Conectando al cluster Spark en: {SPARK_MASTER}")
    
    spark = SparkSession.builder \
        .appName("GenomicDataConsumer") \
        .master(SPARK_MASTER) \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.mongodb.output.uri", MONGODB_URI) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
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

def write_to_mongodb(df, collection_name):
    """Escribe el stream a MongoDB usando foreach para compatibilidad"""
    from pymongo import MongoClient
    
    def write_to_mongo(batch_df, batch_id):
        """Escribe cada batch a MongoDB"""
        try:
            client = MongoClient(MONGODB_URI)
            db = client.genomic_db
            collection = db[collection_name]
            
            # Convertir el DataFrame a lista de diccionarios
            records = batch_df.toPandas().to_dict('records')
            if records:
                collection.insert_many(records)
                print(f"📝 MongoDB: Insertados {len(records)} registros en {collection_name}")
        except Exception as e:
            print(f"❌ Error escribiendo a MongoDB: {e}")
        finally:
            client.close()
    
    query = df.writeStream \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", f"/tmp/checkpoint/{collection_name}") \
        .start()
    
    return query

def write_to_hdfs(df, path_name):
    """Escribe el stream a HDFS en formato Parquet"""
    hdfs_path = f"{HDFS_NAMENODE}/genomic_data/{path_name}"
    
    query = df.writeStream \
        .format("parquet") \
        .option("path", hdfs_path) \
        .option("checkpointLocation", f"/tmp/checkpoint/hdfs_{path_name}") \
        .outputMode("append") \
        .start()
    
    print(f"💾 HDFS: Guardando en {hdfs_path}")
    return query

def write_to_console(df, topic_name):
    """Escribe el stream a consola para debugging"""
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    return query

def main():
    """Función principal que inicia el streaming"""
    print("=" * 80)
    print("🧬 INICIANDO CONSUMIDOR DE DATOS GENÓMICOS CON SPARK STREAMING")
    print("=" * 80)
    
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
        
        # Escribir a MongoDB
        print("\n📝 Configurando escritura a MongoDB...")
        query_fathers = write_to_mongodb(fathers_df, "fathers")
        query_mothers = write_to_mongodb(mothers_df, "mothers")
        query_children = write_to_mongodb(children_df, "children")
        
        # Escribir a HDFS
        print("\n💾 Configurando escritura a HDFS...")
        hdfs_fathers = write_to_hdfs(fathers_df, "fathers")
        hdfs_mothers = write_to_hdfs(mothers_df, "mothers")
        hdfs_children = write_to_hdfs(children_df, "children")
        
        # Mostrar en consola
        print("\n📺 Configurando salida a consola...")
        console_fathers = write_to_console(fathers_df, "fathers")
        console_mothers = write_to_console(mothers_df, "mothers")
        console_children = write_to_console(children_df, "children")
        
        print("\n" + "=" * 80)
        print("✅ CONSUMIDOR INICIADO - Procesando datos en tiempo real...")
        print("=" * 80)
        print(f"\n🔗 Kafka Broker: {KAFKA_BROKER}")
        print(f"🔗 MongoDB: {MONGODB_URI.split('@')[1].split('/')[0]}")
        print(f"🔗 HDFS: {HDFS_NAMENODE}")
        print(f"🔗 Spark Master: {SPARK_MASTER}")
        print("\n💡 Presiona Ctrl+C para detener el consumidor\n")
        
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
