import time
import json
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from conf import CONFIG
from family_generator import FamilyGenerator

print("="*80)
print("🧬 KAFKA PRODUCER - GENERADOR DE FAMILIAS GENÓMICAS")
print("="*80)
print(f"Partición: {CONFIG.PARTITION_NUMBER}")
print(f"Hilos: {CONFIG.NUM_THREADS}")
print("="*80)

print(f"\n⏳ Esperando conexión con Kafka broker...")
for attempt in range(30): 
    try:
        producer = KafkaProducer(
            bootstrap_servers=[CONFIG.KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            batch_size=CONFIG.BATCH_SIZE,
            linger_ms=10,
            compression_type='gzip',
            buffer_memory=CONFIG.BUFFER_MEMORY,
            max_in_flight_requests_per_connection=10,  
            acks=1,  
            retries=CONFIG.RETRIES,
            max_request_size=CONFIG.MAX_REQUEST_SIZE
        )
        print("✅ Conectado a Kafka!")
        break
    except NoBrokersAvailable:
        print(f"  Intento {attempt + 1}: Kafka no disponible, esperando...")
        time.sleep(1)
else:
    print("❌ No se pudo conectar a Kafka después de 30 intentos")
    exit(1)
  
try:
    # Configurar rutas de genomas reales
    genome_paths = {
        'father': 'data/archive-2/Father Genome.csv',
        'mother': 'data/archive-2/Mother Genome.csv',
        'children': [
            'data/archive-2/Child 1 Genome.csv',
            'data/archive-2/Child 2 Genome.csv',
            'data/archive-2/Child 3 Genome.csv'
        ]
    }
    
    # Inicializar generador de familias
    family_gen = FamilyGenerator(genome_paths=genome_paths)
    
    # Topics separados para cada miembro de la familia
    topics = {
        'fathers': 'genomic-fathers',
        'mothers': 'genomic-mothers',
        'children': 'genomic-children'
    }
    
    # Iniciar generación infinita con múltiples hilos
    streaming_threads = family_gen.start_infinite_streaming(
        kafka_producer=producer,
        topics=topics,
        partition_number=CONFIG.PARTITION_NUMBER,
        num_threads=CONFIG.NUM_THREADS
    )
    
    print(f"💫 Generación infinita iniciada!")
    print(f"📊 Monitoreando rendimiento...\n")
    
    # Monitoreo continuo
    last_families = 0
    last_members = 0
    start_time = time.time()
    
    while True:
        time.sleep(5)
        
        stats = family_gen.get_statistics()
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        # Tasas de generación
        families_rate = stats['families_generated'] / elapsed_time if elapsed_time > 0 else 0
        members_rate = stats['total_members_sent'] / elapsed_time if elapsed_time > 0 else 0
        
        recent_families_rate = (stats['families_generated'] - last_families) / 5.0
        recent_members_rate = (stats['total_members_sent'] - last_members) / 5.0
        
        print(f"{'='*80}")
        print(f"📍 PARTICIÓN {CONFIG.PARTITION_NUMBER} | ⏱️  {elapsed_time:.1f}s transcurridos")
        print(f"{'='*80}")
        print(f"👨‍👩‍👧‍👦 Familias generadas: {stats['families_generated']:,}")
        print(f"   └─ Padres:  {stats['fathers_sent']:,}")
        print(f"   └─ Madres:  {stats['mothers_sent']:,}")
        print(f"   └─ Hijos:   {stats['children_sent']:,}")
        print(f"📤 Total miembros enviados: {stats['total_members_sent']:,}")
        print(f"")
        print(f"📈 TASA PROMEDIO:")
        print(f"   └─ Familias: {families_rate:.1f} familias/seg")
        print(f"   └─ Miembros: {members_rate:.1f} personas/seg")
        print(f"")
        print(f"⚡ TASA RECIENTE (últimos 5s):")
        print(f"   └─ Familias: {recent_families_rate:.1f} familias/seg")
        print(f"   └─ Miembros: {recent_members_rate:.1f} personas/seg")
        print(f"")
        print(f"🔧 Hilos activos: {len([t for t in streaming_threads if t.is_alive()])}/{len(streaming_threads)}")
        print(f"{'='*80}\n")
        
        last_families = stats['families_generated']
        last_members = stats['total_members_sent']
        
        producer.flush()
        
except KeyboardInterrupt:
    print(f"\n⚠️  Deteniendo generador de familias (Partición {CONFIG.PARTITION_NUMBER})...")
    family_gen.stop_streaming()
    
    for thread in streaming_threads:
        thread.join(timeout=2)
    
    final_stats = family_gen.get_statistics()
    print(f"\n{'='*80}")
    print(f"📊 ESTADÍSTICAS FINALES")
    print(f"{'='*80}")
    print(f"Familias generadas: {final_stats['families_generated']:,}")
    print(f"Total miembros enviados: {final_stats['total_members_sent']:,}")
    print(f"{'='*80}\n")
    
except Exception as e:
    print(f"❌ Error inesperado en producer: {e}")
    import traceback
    traceback.print_exc()
    family_gen.stop_streaming()
        
finally:
    producer.flush()
    producer.close()
    print("✅ Producer cerrado correctamente")
