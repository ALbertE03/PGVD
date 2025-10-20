#!/usr/bin/env python3
"""
Script para consumir datos de Kafka y cargarlos en HDFS en formato Parquet
"""

import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import pandas as pd
from collections import defaultdict
import threading

# Configuración
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPICS = ['fathers', 'mothers', 'children']
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE_URL', 'hdfs://namenode:9000')
HDFS_USER = os.getenv('HDFS_USER', 'root')
BATCH_SIZE = 1000  
HDFS_BASE_PATH = '/genomic_data'

class HDFSLoader:
    def __init__(self):
        # Cliente HDFS
        namenode_url = HDFS_NAMENODE.replace('hdfs://', 'http://').replace(':9000', ':9870')
        self.hdfs_client = InsecureClient(namenode_url, user=HDFS_USER)
        
        # Buffers por tipo de miembro
        self.buffers = {
            'fathers': [],
            'mothers': [],
            'children': []
        }
        
        # Estadísticas
        self.stats = {
            'total_processed': 0,
            'batches_written': 0,
            'start_time': datetime.now()
        }
        
        self.lock = threading.Lock()
        
        # Crear directorios en HDFS
        self._initialize_hdfs_structure()
    
    def _initialize_hdfs_structure(self):
        """Crea la estructura de directorios en HDFS"""
        print("📂 Inicializando estructura en HDFS...")
        
        paths = [
            f'{HDFS_BASE_PATH}/fathers',
            f'{HDFS_BASE_PATH}/mothers',
            f'{HDFS_BASE_PATH}/children',
            f'{HDFS_BASE_PATH}/families_metadata'
        ]
        
        for path in paths:
            try:
                self.hdfs_client.makedirs(path)
                print(f"   ✅ Creado: {path}")
            except Exception as e:
                print(f"   ℹ️  {path} ya existe o error: {e}")
        
        print("✅ Estructura HDFS lista\n")
    
    def process_message(self, message, topic):
        """Procesa un mensaje y lo añade al buffer apropiado"""
        data = message
        
        # Ignorar tokens de completación
        if data.get('message_type') == 'FAMILY_COMPLETE':
            self._save_family_metadata(data)
            return
        
        # Extraer información
        family_id = data.get('family_id')
        person_id = data.get('person_id', '')
        snp_data = data.get('snp_data', {})
        
        # Crear registro aplanado para Parquet
        record = {
            'family_id': family_id,
            'person_id': person_id,
            'member_type': data.get('member_type'),
            'chromosome': snp_data.get('chromosome'),
            'position': snp_data.get('position'),
            'genotype': snp_data.get('genotype'),
            'timestamp': data.get('timestamp'),
            'gender': data.get('gender')
        }
        
        # Determinar tipo y agregar al buffer
        with self.lock:
            if 'father' in topic:
                self.buffers['fathers'].append(record)
            elif 'mother' in topic:
                self.buffers['mothers'].append(record)
            elif 'child' in topic:
                self.buffers['children'].append(record)
            
            self.stats['total_processed'] += 1
            
            # Escribir batch si alcanzó el tamaño
            self._check_and_write_batches()
    
    def _check_and_write_batches(self):
        """Verifica y escribe batches si alcanzaron el tamaño objetivo"""
        for member_type, buffer in self.buffers.items():
            if len(buffer) >= BATCH_SIZE:
                self._write_batch_to_hdfs(member_type, buffer)
                buffer.clear()
    
    def _write_batch_to_hdfs(self, member_type, records):
        """Escribe un batch de registros a HDFS en formato Parquet"""
        if not records:
            return
        
        try:
            # Convertir a DataFrame
            df = pd.DataFrame(records)
            
            # Nombre del archivo con timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            batch_id = self.stats['batches_written']
            filename = f'batch_{timestamp}_{batch_id}.parquet'
            
            # Path en HDFS
            hdfs_path = f'{HDFS_BASE_PATH}/{member_type}/{filename}'
            
            # Guardar como Parquet localmente primero
            local_path = f'/tmp/{filename}'
            df.to_parquet(local_path, engine='pyarrow', compression='snappy')
            
            # Subir a HDFS
            with open(local_path, 'rb') as f:
                self.hdfs_client.write(hdfs_path, f, overwrite=True)
            
            # Limpiar archivo temporal
            os.remove(local_path)
            
            self.stats['batches_written'] += 1
            
            print(f"✅ Batch escrito: {hdfs_path} ({len(records):,} registros)")
            
        except Exception as e:
            print(f"❌ Error escribiendo batch: {e}")
            import traceback
            traceback.print_exc()
    
    def _save_family_metadata(self, completion_data):
        """Guarda metadatos de familia completada"""
        try:
            family_id = completion_data['family_id']
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'family_{family_id}_{timestamp}.json'
            hdfs_path = f'{HDFS_BASE_PATH}/families_metadata/{filename}'
            
            # Escribir JSON a HDFS
            json_content = json.dumps(completion_data, indent=2)
            self.hdfs_client.write(hdfs_path, json_content.encode('utf-8'), overwrite=True)
            
            print(f"📝 Metadata guardada: {family_id}")
            
        except Exception as e:
            print(f"❌ Error guardando metadata: {e}")
    
    def flush_all_buffers(self):
        """Fuerza la escritura de todos los buffers restantes"""
        print("\n🔄 Flushing buffers restantes...")
        with self.lock:
            for member_type, buffer in self.buffers.items():
                if buffer:
                    self._write_batch_to_hdfs(member_type, buffer)
                    buffer.clear()
        print("✅ Flush completado\n")
    
    def get_stats(self):
        """Retorna estadísticas de carga"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        return {
            'total_processed': self.stats['total_processed'],
            'batches_written': self.stats['batches_written'],
            'records_per_second': self.stats['total_processed'] / elapsed if elapsed > 0 else 0,
            'elapsed_seconds': elapsed
        }

def main():
    """Función principal del loader"""
    print("="*80)
    print("🚀 HDFS LOADER - INICIANDO")
    print("="*80)
    print(f"📡 Kafka Broker: {KAFKA_BROKER}")
    print(f"📂 HDFS Namenode: {HDFS_NAMENODE}")
    print(f"📦 Batch Size: {BATCH_SIZE:,}")
    print("="*80 + "\n")
    
    # Inicializar loader
    loader = HDFSLoader()
    
    # Crear consumer de Kafka
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # Leer desde el principio
        enable_auto_commit=True,
        group_id=f'hdfs-loader-{int(time.time())}',  # Group ID único basado en timestamp
        max_poll_records=500
    )
    
    print("✅ Consumer conectado")
    print(f"📡 Escuchando topics: {TOPICS}\n")
    
    try:
        message_count = 0
        last_stats_time = time.time()
        
        for message in consumer:
            loader.process_message(message.value, message.topic)
            message_count += 1
            
            # Mostrar estadísticas cada 10 segundos
            if time.time() - last_stats_time > 10:
                stats = loader.get_stats()
                print(f"📊 Stats: {stats['total_processed']:,} procesados | "
                      f"{stats['batches_written']} batches | "
                      f"{stats['records_per_second']:,.0f} rec/s")
                last_stats_time = time.time()
        
    except KeyboardInterrupt:
        print("\n⏹️  Deteniendo consumer...")
    finally:
        # Flush buffers restantes
        loader.flush_all_buffers()
        consumer.close()
        
        # Estadísticas finales
        stats = loader.get_stats()
        print("\n" + "="*80)
        print("📊 ESTADÍSTICAS FINALES")
        print("="*80)
        print(f"Total procesados: {stats['total_processed']:,}")
        print(f"Batches escritos: {stats['batches_written']}")
        print(f"Tiempo total: {stats['elapsed_seconds']/60:.2f} minutos")
        print(f"Throughput: {stats['records_per_second']:,.0f} registros/segundo")
        print("="*80)

if __name__ == '__main__':
    main()
