import os

class CONFIG:
    KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
    KAFKA_TOPIC = 'genomic-data'
    PARTITION_NUMBER = int(os.getenv('PARTITION_NUMBER', '0')) 
    BATCH_SIZE=65536  # 64KB batches 
    BUFFER_MEMORY= 134217728  # 128MB buffer 
    MAX_REQUEST_SIZE=10485760  # 10MB max request 
    RETRIES=3
    NUM_THREADS = 10
    BATCH_SIZE_PER_THREAD = 3000  