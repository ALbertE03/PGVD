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

    
    gen = GenomicGenerator(num_threads=CONFIG.NUM_THREADS)
 
    streaming_threads = gen.generate_threaded_kafka_stream(
        kafka_producer=producer,
        kafka_topic=CONFIG.KAFKA_TOPIC,
        partition_number=CONFIG.PARTITION_NUMBER,
        records_per_thread=CONFIG.BATCH_SIZE_PER_THREAD
    )
    
    print(f"🚀 {len(streaming_threads)} threads now streaming data directly to Kafka!")
    print("📈 Monitoring total throughput...")
    
    
    last_count = 0
    while True:
        time.sleep(5)  
        
        current_count = gen.total_sent
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        total_rate = current_count / elapsed_time if elapsed_time > 0 else 0
        recent_rate = (current_count - last_count) / 5.0 
        
        print(f"🚀 PARTITION {CONFIG.PARTITION_NUMBER}: {current_count:,} messages in {elapsed_time:.1f}s")
        print(f"⚡ TOTAL RATE: {total_rate:,.0f} msg/sec")
        print(f"🔥 RECENT RATE: {recent_rate:,.0f} msg/sec ({recent_rate/1000:.1f}K/sec)")
        print(f"📊 ACTIVE THREADS: {len([t for t in streaming_threads if t.is_alive()])}/{len(streaming_threads)}")
        
        last_count = current_count
        
        producer.flush()
        
except KeyboardInterrupt:
    print(f"\n🛑 Shutting down streaming producer for partition {CONFIG.PARTITION_NUMBER}...")
    gen.stop_streaming()  
    
    # Wait for threads to finish
    for thread in streaming_threads:
        thread.join(timeout=2)
    
    print(f"✅ All threads stopped. Total sent: {gen.total_sent:,}")
    
except Exception as e:
    print(f"❌ Unexpected error in producer: {e}")
    import traceback
    traceback.print_exc()
    gen.stop_streaming() 
        
finally:
    producer.flush()
    producer.close()