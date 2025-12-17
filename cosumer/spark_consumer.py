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
    

    spark = SparkSession.builder \
        .appName("GenomicDataConsumer") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.scheduler.revive.interval", "1s") \
        .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "120s") \
        .config("spark.scheduler.minRegisteredResourcesRatio", "0.3") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "30s") \
        .config("spark.rpc.lookupTimeout", "240s") \
        .config("spark.core.connection.ack.wait.timeout", "600s") \
        .config("spark.storage.blockManagerTimeoutIntervalMs", "600000") \
        .config("spark.excludeOnFailure.enabled", "false") \
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

def normalize_chromosome(chrom):
    if chrom is None:
        return None
    chrom = str(chrom).upper().replace("CHR", "")
    return chrom

def extract_genetic_data(snp_data_row, genotype_from_batch):
    """Extrae datos genéticos en formato esperado por el dashboard
    snp_data_row puede ser un Spark Row o un diccionario"""
    if not snp_data_row:
        return None
    
    try:
        # Convertir Spark Row a diccionario si es necesario
        if hasattr(snp_data_row, 'asDict'):
            # Es un Spark Row
            snp_dict = snp_data_row.asDict()
        elif isinstance(snp_data_row, dict):
            # Ya es un diccionario
            snp_dict = snp_data_row
        else:
            # Intentar acceso directo como atributos
            snp_dict = {
                'chromosome': getattr(snp_data_row, 'chromosome', '0'),
                'position': getattr(snp_data_row, 'position', 0),
                'genotype': getattr(snp_data_row, 'genotype', '0/0')
            }
        
        # DEBUG: Loguear datos reales recibidos
        print(f"🔍 DEBUG - SNP Data recibido: chromosome={snp_dict.get('chromosome')}, "
              f"position={snp_dict.get('position')}, "
              f"genotype_en_snp={snp_dict.get('genotype')}, "
              f"genotype_parametro={genotype_from_batch}")
        
        # Detectar tipo de variante (simplificado)
        def get_variant_type(snp_data):
            return "SNP"
        
        # Mapeo simplificado de genes por cromosoma y posición
        def get_gene_name(chromosome, position):
            # Mapeo más flexible de genes por cromosoma
            GENE_MAP = {
                # Gen -> (cromosoma_start, cromosoma_end)
                "BRCA1": (17, 43000000, 43500000),
                "BRCA2": (13, 32800000, 33400000),
                "TP53": (17, 7000000, 7600000),
                "EGFR": (7, 55000000, 56500000),
                "KRAS": (12, 25200000, 25400000),
                "BRAF": (7, 140700000, 140900000),
                "FMR1": ("X",  153000000, 153500000),
                "AR":("X", 66700000, 67000000),
                "DMD": ("X", 48700000, 48800000),
                "SRY":   ("Y", 2650000, 2800000),
                "TSPY":  ("Y", 9500000, 10500000),
                "DAZ":   ("Y", 23500000, 24500000),
                "RBMY":  ("Y", 22000000, 22500000),
                "USP9Y": ("Y", 12500000, 13000000),
                "ZFY":   ("Y", 2850000, 3000000),
                # Mitocondrial (MT)
                "MT-ND1":  ("MT", 3307, 4262),
                "MT-ND2":  ("MT", 4470, 5511),
                "MT-CO1":  ("MT", 5904, 7445),
                "MT-CO2":  ("MT", 7586, 8269),
                "MT-ATP8": ("MT", 8366, 8572),
                "MT-ATP6": ("MT", 8527, 9207),
                "MT-CO3":  ("MT", 9207, 9990),
                "MT-ND3":  ("MT", 10059, 10404),
                "MT-ND4":  ("MT", 10760, 12137),
                "MT-ND5":  ("MT", 12337, 14148),
                "MT-CYB":  ("MT", 14747, 15887),
                "MT-ND6":  ("MT", 14149, 14673),
                "MT-RNR1": ("MT", 648, 1601),
                "MT-RNR2": ("MT", 1671, 3229),

            }
            
            try:
                chrom = normalize_chromosome(chromosome)
                pos = int(position) if position is not None else None

                if chrom is None or pos is None:
                    return "Unknown"

                for gene, (c, start, end) in GENE_MAP.items():
                    if chrom == c and start <= pos <= end:
                        return gene

                # fallback útil para debug
                return f"Chr{chrom}"
            except Exception as e:
                print(f"  ⚠️ Error en mapeo de genes: {e}")
                pass
            
            return "Unknown"
        
        # Extraer valores de forma segura
        chromosome = snp_dict.get("chromosome", "0")
        position = snp_dict.get("position", 0)
        
        genetic_data = {
            "variant_type": get_variant_type(snp_dict),
            "gene": get_gene_name(chromosome, position),
            "genotype": genotype_from_batch if genotype_from_batch else "0/0",
            "chromosome": str(chromosome),
            "position": int(position) if position else 0,
            "quality": 99
        }
        
        return genetic_data
    except Exception as e:
        print(f"⚠️ Error extrayendo datos genéticos: {e}")
        return None

def calculate_and_send_metrics(batch_df, batch_id, member_type):
    """Calcula métricas del batch y las envía al dashboard - INCLUYENDO DATOS GENÉTICOS"""
    if batch_df.isEmpty():
        return
    
    try:
        # Medir tiempo de procesamiento
        start_time = time.time()
        
        # Detectar tokens de finalización de familia
        completion_tokens = batch_df.filter(col("snp_data").isNull()).collect()
        for token_row in completion_tokens:
            token_data = token_row.asDict()
            if 'family_id' in token_data:
                # Enviar token de finalización al dashboard
                completion_payload = {
                    'message_type': 'FAMILY_COMPLETE',
                    'family_id': token_data.get('family_id'),
                    'timestamp': str(current_timestamp())
                }
                requests.post(
                    f"{DASHBOARD_URL}/api/metrics",
                    json=completion_payload,
                    timeout=5
                )
        
        # Contar todos los registros para la gráfica de procesamiento
        total_records = batch_df.count()
        
        # Contar registros válidos (con datos SNP) para análisis genético
        valid_records = batch_df.filter(col("snp_data").isNotNull()).count()
        
        # ========== EXTRAER DATOS GENÉTICOS CON FAMILY_ID ==========
        genetic_data = None
        if valid_records > 0:
            # Obtener primera fila con datos SNP
            sample_row = batch_df.filter(col("snp_data").isNotNull()).first()
            
            if sample_row:
                sample_dict = sample_row.asDict()
                snp_data = sample_dict.get("snp_data")  # Esto es un Spark Row
                
                # Si snp_data es un Spark Row, convertirlo a dict
                if snp_data and hasattr(snp_data, 'asDict'):
                    snp_data_dict = snp_data.asDict()
                elif isinstance(snp_data, dict):
                    snp_data_dict = snp_data
                else:
                    snp_data_dict = {}
                
                # El genotype ESTÁ DENTRO de snp_data, no fuera
                genotype_from_snp = snp_data_dict.get("genotype", "0/0")
                genotype_from_batch = sample_dict.get("genotype", "0/0")
                
                # Usar el genotype de snp_data si está disponible, si no el del nivel superior
                genotype = genotype_from_snp if genotype_from_snp != "0/0" else genotype_from_batch
                
                # Extraer datos genéticos pasando snp_data como dict
                genetic_data = extract_genetic_data(snp_data_dict, genotype)
                
                # AGREGAR family_id y member_type para filtrado en dashboard
                if genetic_data:
                    genetic_data['family_id'] = sample_dict.get('family_id')
                    genetic_data['member_type'] = member_type
        
        # Calcular tiempo de procesamiento en milisegundos
        processing_time = (time.time() - start_time) * 1000
        
        # Construir payload con genetic_data
        metrics = {
            'member_type': member_type,
            'batch_id': batch_id,
            'timestamp': str(batch_df.select(current_timestamp()).first()[0]),
            'total_records': total_records,
            'processing_time': processing_time
        }
        
        # ========== NUEVO: AGREGAR DATOS GENÉTICOS AL PAYLOAD ==========
        if genetic_data:
            metrics['genetic_data'] = genetic_data
            print(f"🧬 [{member_type}] Datos genéticos: Gene={genetic_data.get('gene')}, Genotype={genetic_data.get('genotype')}")
        
        # Enviar métricas al dashboard
        response = requests.post(
            f"{DASHBOARD_URL}/api/metrics",
            json=metrics,
            timeout=5
        )
        
        if response.status_code == 200:
            print(f"📊 [{member_type}] Batch {batch_id}: {total_records} registros ({valid_records} válidos) en {processing_time:.2f}ms")
            if genetic_data:
                print(f"   ✅ Datos genéticos enviados al dashboard")
        else:
            print(f"⚠️  Error enviando métricas [{member_type}]: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error calculando/enviando métricas [{member_type}]: {e}")
        import traceback
        traceback.print_exc()

def calculate_chromosome_distribution(df, member_type):
    """Analiza la distribución de variantes por cromosoma"""
    try:
        chromosome_stats = df.filter(col("snp_data").isNotNull()) \
            .select(
                col("snp_data.chromosome").alias("chromosome"),
                col("snp_data.genotype").alias("genotype")
            ) \
            .groupBy("chromosome", "genotype") \
            .agg(count("*").alias("count")) \
            .collect()
        
        if chromosome_stats:
            # Agrupar por cromosoma
            chrom_distribution = {}
            for row in chromosome_stats:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                chrom = str(row_dict.get('chromosome', 'Unknown')) if row_dict.get('chromosome') else "Unknown"
                genotype = row_dict.get('genotype', '0/0') if row_dict.get('genotype') else "0/0"
                cnt = int(row_dict.get('count', 0))
                
                if chrom not in chrom_distribution:
                    chrom_distribution[chrom] = {'total': 0, 'genotypes': {}}
                
                chrom_distribution[chrom]['total'] += cnt
                chrom_distribution[chrom]['genotypes'][genotype] = cnt
            
            # Enviar al dashboard
            chromo_metrics = {
                'message_type': 'CHROMOSOME_DISTRIBUTION',
                'member_type': member_type,
                'chromosome_distribution': chrom_distribution,
                'timestamp': str(current_timestamp())
            }
            
            try:
                response = requests.post(
                    f"{DASHBOARD_URL}/api/metrics",
                    json=chromo_metrics,
                    timeout=5
                )
                if response.status_code == 200:
                    print(f"🧬 [{member_type}] Distribución de cromosomas enviada")
            except Exception as e:
                print(f"⚠️  Error enviando distribución de cromosomas: {e}")
    
    except Exception as e:
        print(f"❌ Error calculando distribución de cromosomas: {e}")


def classify_genotype(genotype):
    if not genotype or len(genotype) != 2:
        return "invalid"

    a, b = genotype[0], genotype[1]

    if a == b:
        if a.isupper():
            return "dominant"
        else:
            return "recessive"
    else:
        return "heterozygous"

def calculate_individual_heterozygosity(df, member_type):
    """
    Calcula heterocigosis por individuo (person_id)
    """
    try:
        rows = (
            df.filter(col("snp_data").isNotNull())
              .select(
                  col("person_id"),
                  col("snp_data.genotype").alias("genotype")
              )
              .collect()
        )

        if not rows:
            return

        per_person = {}

        for row in rows:
            pid = row.person_id
            gt = row.genotype
            if not pid or not gt or len(gt) != 2:
                continue

            if pid not in per_person:
                per_person[pid] = {"hetero": 0, "homo": 0}

            if gt[0] == gt[1]:
                per_person[pid]["homo"] += 1
            else:
                per_person[pid]["hetero"] += 1

        for pid, counts in per_person.items():
            total = counts["hetero"] + counts["homo"]
            if total == 0:
                continue

            payload = {
                "message_type": "GENETIC_INDIVIDUAL",
                "member_type": member_type,
                "person_id": pid,
                "heterozygous_pct": round(100 * counts["hetero"] / total, 2),
                "homozygous_pct": round(100 * counts["homo"] / total, 2),
                "total_snps": total,
                "timestamp": str(current_timestamp())
            }

            requests.post(
                f"{DASHBOARD_URL}/api/metrics",
                json=payload,
                timeout=5
            )

            print(
                f"🧍 [{member_type}] {pid} → "
                f"HET={payload['heterozygous_pct']}% | "
                f"HOM={payload['homozygous_pct']}%"
            )

    except Exception as e:
        print(f"❌ Error heterocigosis individual: {e}")

def calculate_population_heterozygosity(df, member_type):
    """
    Métrica poblacional: hetero vs homo por grupo
    """
    try:
        genotypes = (
            df.filter(col("snp_data").isNotNull())
              .select(col("snp_data.genotype").alias("genotype"))
              .collect()
        )

        if not genotypes:
            return

        hetero = 0
        homo = 0

        for row in genotypes:
            gt = row.genotype
            if not gt or len(gt) != 2:
                continue

            if gt[0] == gt[1]:
                homo += 1
            else:
                hetero += 1

        total = hetero + homo
        if total == 0:
            return

        p = hetero/total
        q = homo / total
        He = round(2*p*q, 4)

        timestamp = str(current_timestamp())


        payload = {
            "message_type": "GENETIC_POPULATION",
            "member_type": member_type,
            "heterozygous_pct": round(100 * hetero / total, 2),
            "homozygous_pct": round(100 * homo / total, 2),
            "diversity": He,
            "total_snps": total,
            "timestamp": timestamp
        }

        requests.post(
            f"{DASHBOARD_URL}/api/metrics",
            json=payload,
            timeout=5
        )

        print(
            f"🧬 [{member_type}] "
            f"HET={payload['heterozygous_pct']}% | "
            f"HOM={payload['homozygous_pct']}% "
        )

    except Exception as e:
        print(f"❌ Error heterocigosis poblacional: {e}")


def calculate_position_hotspots(df, member_type):
    """Identifica hotspots (posiciones con alta frecuencia de variantes)"""
    try:
        # Top 10 posiciones más frecuentes
        hotspots = df.filter(col("snp_data").isNotNull()) \
            .select(
                col("snp_data.chromosome").alias("chromosome"),
                col("snp_data.position").alias("position")
            ) \
            .groupBy("chromosome", "position") \
            .agg(count("*").alias("frequency")) \
            .filter(col("frequency") >= 1) \
            .orderBy(col("frequency").desc()) \
            .limit(10) \
            .collect()
        
        if hotspots:
            hotspot_list = []
            for row in hotspots:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                hotspot_list.append({
                    'chromosome': str(row_dict.get('chromosome', 'Unknown')),
                    'position': int(row_dict.get('position', 0)),
                    'frequency': int(row_dict.get('frequency', 0))
                })
            
            hotspot_metrics = {
                'message_type': 'POSITION_HOTSPOTS',
                'member_type': member_type,
                'hotspots': hotspot_list,
                'timestamp': str(current_timestamp())
            }
            
            try:
                response = requests.post(
                    f"{DASHBOARD_URL}/api/metrics",
                    json=hotspot_metrics,
                    timeout=5
                )
                if response.status_code == 200:
                    print(f"🔥 [{member_type}] Hotspots identificados: {len(hotspot_list)}")
            except Exception as e:
                print(f"⚠️  Error enviando hotspots: {e}")
    
    except Exception as e:
        print(f"❌ Error calculando hotspots: {e}")

def calculate_genotype_trends(df, member_type):
    """Calcula tendencias de genotipos en tiempo real"""
    try:
        genotype_distribution = df.filter(col("snp_data").isNotNull()) \
            .select(col("snp_data.genotype").alias("genotype")) \
            .groupBy("genotype") \
            .agg(count("*").alias("count")) \
            .collect()
        
        if genotype_distribution:
            genotype_stats = {}
            total = 0
            for row in genotype_distribution:
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
                genotype = row_dict.get('genotype', '0/0') if row_dict.get('genotype') else "0/0"
                cnt = int(row_dict.get('count', 0))
                genotype_stats[genotype] = cnt
                total += cnt
            
            # Calcular porcentajes
            genotype_percentages = {
                gt: round((count / total) * 100, 2) if total > 0 else 0
                for gt, count in genotype_stats.items()
            }
            
            genotype_trends = {
                'message_type': 'GENOTYPE_TRENDS',
                'member_type': member_type,
                'genotype_distribution': genotype_stats,
                'genotype_percentages': genotype_percentages,
                'total_genotypes': total,
                'timestamp': str(current_timestamp())
            }
            
            try:
                response = requests.post(
                    f"{DASHBOARD_URL}/api/metrics",
                    json=genotype_trends,
                    timeout=5
                )
                if response.status_code == 200:
                    print(f"📈 [{member_type}] Tendencias de genotipos: {genotype_percentages}")
            except Exception as e:
                print(f"⚠️  Error enviando tendencias de genotipos: {e}")
    
    except Exception as e:
        print(f"❌ Error calculando tendencias de genotipos: {e}")


def calculate_mutation_rate(df, member_type):
    """Calcula la tasa de mutación (porcentaje de variantes vs genoma de referencia)"""
    try:
        # Contar variantes (genotype != "0/0")
        total_snps = df.filter(col("snp_data").isNotNull()).count()
        variant_snps = df.filter(
            (col("snp_data").isNotNull()) & 
            (col("snp_data.genotype") != "0/0")
        ).count()
        
        # Calcular porcentaje de mutación
        mutation_rate = round((variant_snps / total_snps * 100), 2) if total_snps > 0 else 0
        
        mutation_data = {
            'message_type': 'MUTATION_RATE',
            'member_type': member_type,
            'mutation_rate': mutation_rate,
            'total_snps': total_snps,
            'variant_snps': variant_snps,
            'timestamp': str(current_timestamp())
        }
        
        try:
            response = requests.post(
                f"{DASHBOARD_URL}/api/metrics",
                json=mutation_data,
                timeout=5
            )
            if response.status_code == 200:
                print(f"🧬 [{member_type}] Tasa de mutación: {mutation_rate}% ({variant_snps}/{total_snps})")
        except Exception as e:
            print(f"⚠️  Error enviando tasa de mutación: {e}")
    
    except Exception as e:
        print(f"❌ Error calculando tasa de mutación: {e}")

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


def process_batch_with_genetics(batch_df, batch_id, member_type):
    """Función para procesar batches con análisis genéticos en paralelo
    """
    from concurrent.futures import ThreadPoolExecutor
    
    # Métricas por batch (esto se ejecuta primero siempre)
    calculate_and_send_metrics(batch_df, batch_id, member_type)
    
    # Ejecutar análisis genéticos en paralelo usando threads
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Cache del DataFrame para evitar re-cómputos
        cached_df = batch_df.cache()
        
        # Lista de funciones de análisis a ejecutar
        analysis_functions = [
            calculate_chromosome_distribution,
            calculate_position_hotspots,
            calculate_genotype_trends,
            calculate_population_heterozygosity,
            calculate_individual_heterozygosity,
            calculate_mutation_rate
        ]
        
        # Enviar todas las tareas de análisis en paralelo
        futures = [executor.submit(func, cached_df, member_type) for func in analysis_functions]
        
        # Esperar a que terminen todas (esto permite que se ejecuten en paralelo)
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"⚠️ Error en análisis paralelo [{member_type}]: {e}")
        
        # Liberar cache
        cached_df.unpersist()


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
        
        # Usar función genérica para eliminar código repetido
        def process_fathers(batch_df, batch_id):
            process_batch_with_genetics(batch_df, batch_id, "fathers")

        def process_mothers(batch_df, batch_id):
            process_batch_with_genetics(batch_df, batch_id, "mothers")

        def process_children(batch_df, batch_id):
            process_batch_with_genetics(batch_df, batch_id, "children")
        
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
