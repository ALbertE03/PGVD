import time
import json
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from conf import CONFIG
from const import *
from genomic_generator import GenomicGenerator

print(f"Waiting for Kafka to be ready on partition {CONFIG.PARTITION_NUMBER}...")
for attempt in range(30): 
    try:
        producer = KafkaProducer(
            bootstrap_servers=[CONFIG.KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            batch_size=CONFIG.BATCH_SIZE,
            linger_ms=10,      # Wait 10ms to batch messages
            compression_type='gzip',
            buffer_memory=CONFIG.BUFFER_MEMORY,
            max_in_flight_requests_per_connection=10,  
            acks=1,  
            retries=CONFIG.RETRIES,
            max_request_size=CONFIG.MAX_REQUEST_SIZE

        )
        break
    except NoBrokersAvailable:
        print(f"Attempt {attempt + 1}: Kafka not ready yet, waiting...")
        time.sleep(1)
else:
    print("Failed to connect to Kafka after 30 attempts")
    exit(1)
  
try:
    message_count = 0
    start_time = time.time()
    print(f"🧬 STARTING GENOMIC DATA PRODUCTION ON PARTITION {CONFIG.PARTITION_NUMBER}!")
    print(f"📊 Target: MAXIMUM THROUGHPUT with REAL GENOMIC DATA - press Ctrl+C to stop")
    print(f"🔌 Connected to Kafka broker: {CONFIG.KAFKA_BROKER}")
    print(f"📝 Writing to topic: {CONFIG.KAFKA_TOPIC}")
    print(f"🧵 Using {CONFIG.NUM_THREADS} threads for data generation")
    print("=" * 70)
    
    print("🔬 Initializing Genomic Data Generator...")
    gen = GenomicGenerator(num_threads=CONFIG.NUM_THREADS)
    print(f"✅ Genomic Generator initialized with {len(gen.data):,} base samples")
    
    batch_size = 10000
    current_batch = []
    batch_index = 0
    
    while True:
        if not current_batch or batch_index >= len(current_batch):
            batch_start = time.time()
            
            current_batch = gen.generate_kafka_records(num_records=batch_size)
            batch_index = 0

            print(current_batch[0])

            for record in current_batch:
                record['partition'] = CONFIG.PARTITION_NUMBER
                record['generation_batch'] = int(batch_start)
            
            batch_time = time.time() - batch_start
            print(f"✅ Generated {len(current_batch)} records in {batch_time:.2f}s ({len(current_batch)/batch_time:.0f} records/sec)")
        
        chunk_size = min(1000, len(current_batch) - batch_index)
        
        for i in range(chunk_size):
            record = current_batch[batch_index + i]
            try:
                if CONFIG.PARTITION_NUMBER is not None:
                    producer.send(CONFIG.KAFKA_TOPIC, value=record, partition=CONFIG.PARTITION_NUMBER)
                else:
                    producer.send(CONFIG.KAFKA_TOPIC, value=record)
                message_count += 1
            except Exception as e:
                print(f"❌ Error sending message: {e}")
                continue
        
        batch_index += chunk_size
        
        if message_count % 1000000 == 0:
            current_time = time.time()
            elapsed_time = current_time - start_time
            messages_per_second = message_count / elapsed_time if elapsed_time > 0 else 0
            
            print(f"🚀 PARTITION {CONFIG.PARTITION_NUMBER}: {message_count:,} genomic messages in {elapsed_time:.1f}s")
            print(f"⚡ THROUGHPUT: {messages_per_second:,.0f} messages/second")
            print(f"🔥 RATE: {messages_per_second/1000:.1f}K msg/s")
            print(f"🧬 Batch progress: {batch_index}/{len(current_batch)}")
            
            producer.flush()
    
        time.sleep(0.001)
        
except KeyboardInterrupt:
    print(f"\n🛑 Shutting down producer for partition {CONFIG.PARTITION_NUMBER}...")
except Exception as e:
    print(f"❌ Unexpected error in producer: {e}")
    import traceback
    traceback.print_exc()
        
finally:
    producer.flush()
    producer.close()