#!/usr/bin/env python3
"""
Gestor de streaming para la comunicación con Kafka.
Maneja la creación del productor, el envío de mensajes y la gestión de hilos.
"""

import time
import threading
from typing import Dict, List
from kafka import KafkaProducer
import json


class StreamingManager:
    """
    Gestiona el streaming de datos genéticos a Kafka.
    """
    
    def __init__(self, family_generator, kafka_settings: Dict, topics: Dict, 
                 num_threads: int = 1, partition_number: int = 0):
        """
        Inicializa el gestor de streaming.
        
        Args:
            family_generator: Instancia de FamilyGenerator
            kafka_settings: Configuración de Kafka {'bootstrap_servers': str, 'client_id': str}
            topics: Diccionario de topics {'fathers': str, 'mothers': str, 'children': str}
            num_threads: Número de hilos de generación
            partition_number: Número de partición para envío
        """
        self.family_generator = family_generator
        self.kafka_settings = kafka_settings
        self.topics = topics
        self.num_threads = num_threads
        self.partition_number = partition_number
        
        self.kafka_producer = None
        self.threads = []
        self._stop_flag = threading.Event()
        self._lock = threading.Lock()
        
        # Estadísticas
        self.families_generated = 0
        self.total_snps_sent = 0
        
        self._initialize_kafka_producer()
    
    def _initialize_kafka_producer(self):
        """Inicializa la conexión con Kafka."""
        print("Conectando con Kafka...")
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_settings['bootstrap_servers'],
                client_id=self.kafka_settings['client_id'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='gzip'
            )
            print(f"✅ Conectado a Kafka en {self.kafka_settings['bootstrap_servers']}")
        except Exception as e:
            print(f"❌ Error conectando a Kafka: {e}")
            print("⚠️  Continuando sin conexión a Kafka (modo de prueba)")
            self.kafka_producer = None
    
    def _streaming_thread(self, thread_id: int):
        """
        Hilo que genera familias y las envía a Kafka.
        
        Args:
            thread_id: ID del hilo
        """
        print(f"🧵 Hilo {thread_id} iniciado")
        
        families_count = 0
        
        while not self._stop_flag.is_set():
            try:
                # Generar una familia completa
                family_generator = self.family_generator.generate_complete_family()
                
                message_batch = []
                current_family_id = None
                member_counters = {'father': 0, 'mother': 0, 'child': 0}
                
                # Procesar cada SNP y el token de finalización
                for member_type, message in family_generator:
                    if current_family_id is None and 'family_id' in message:
                        current_family_id = message['family_id']
                    
                    # Manejar el token de finalización
                    if member_type == 'completion':
                        # Enviar mensajes pendientes
                        if message_batch and self.kafka_producer:
                            for topic, msg, part in message_batch:
                                self.kafka_producer.send(topic, value=msg, partition=part)
                            message_batch.clear()
                        
                        # Enviar el token de finalización
                        if self.kafka_producer:
                            self.kafka_producer.send(
                                self.topics['children'],
                                value=message,
                                partition=self.partition_number
                            )
                        
                        # Actualizar estadísticas
                        with self._lock:
                            self.families_generated += 1
                        
                        families_count += 1
                        print(f"🏁 Hilo {thread_id}: Familia #{families_count} completada - {current_family_id}")
                        continue
                    
                    # Determinar el topic
                    if member_type == 'father':
                        topic = self.topics['fathers']
                    elif member_type == 'mother':
                        topic = self.topics['mothers']
                    elif member_type == 'child':
                        topic = self.topics['children']
                    else:
                        continue
                    
                    # Agregar al batch
                    message_batch.append((topic, message, self.partition_number))
                    member_counters[member_type] += 1
                    
                    with self._lock:
                        self.total_snps_sent += 1
                    
                    # Enviar batch cuando alcanza el tamaño
                    if len(message_batch) >= 1000 and self.kafka_producer:
                        for topic, msg, part in message_batch:
                            self.kafka_producer.send(topic, value=msg, partition=part)
                        message_batch.clear()
                
                # Enviar mensajes restantes
                if message_batch and self.kafka_producer:
                    for topic, msg, part in message_batch:
                        self.kafka_producer.send(topic, value=msg, partition=part)
                    message_batch.clear()
                
                time.sleep(0.001)
                
            except Exception as e:
                print(f"❌ Error en hilo {thread_id}: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)
    
    def start(self):
        """Inicia el streaming en múltiples hilos."""
        print(f"\n🚀 Iniciando {self.num_threads} hilo(s) de generación...")
        
        self._stop_flag.clear()
        self.families_generated = 0
        self.total_snps_sent = 0
        
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self._streaming_thread,
                args=(i,),
                name=f"StreamingThread-{i}",
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
        
        print(f"✅ {self.num_threads} hilo(s) activo(s)")
        print(f"📤 Enviando a: {list(self.topics.values())}")
        print(f"📍 Partición: {self.partition_number}\n")
    
    def stop(self):
        """Detiene todos los hilos de streaming."""
        print("\n🛑 Deteniendo hilos de generación...")
        self._stop_flag.set()
        
        # Esperar a que los hilos terminen
        for thread in self.threads:
            thread.join(timeout=5)
        
        # Cerrar el productor de Kafka
        if self.kafka_producer:
            print("📡 Cerrando conexión con Kafka...")
            self.kafka_producer.flush()
            self.kafka_producer.close()
        
        print("✅ Todos los hilos detenidos")
    
    def get_statistics(self) -> Dict:
        """Retorna las estadísticas de generación."""
        with self._lock:
            return {
                'families_generated': self.families_generated,
                'total_snps_sent': self.total_snps_sent,
                'avg_snps_per_family': self.total_snps_sent / max(1, self.families_generated)
            }
