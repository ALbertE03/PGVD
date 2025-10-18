#!/usr/bin/env python3
"""
Generador de familias completas con ID único y distribuciones realistas.
Envía datos a 3 topics diferentes: fathers, mothers, children.
"""

import time
import threading
import uuid
from typing import Dict, List
from family.father import Father
from family.mother import Mother
from family.childs import Child
import random

class FamilyGenerator:
    """
    Generador de familias completas con herencia genética.
    Cada familia tiene: 1 padre + 1 madre + 3 hijos.
    """
    
    def __init__(self, genome_paths: Dict[str, str] = None):
        """
        Inicializa el generador de familias.
        
        Args:
            genome_paths: Diccionario con rutas a genomas reales
                         {'father': path, 'mother': path, 'children': [path1, path2, path3]}
        """
        
        if genome_paths:
            father_file = genome_paths.get('father')
            mother_file = genome_paths.get('mother')
            children_files = genome_paths.get('children', [])
            
            self.father_generator = Father(genome_file=father_file)
            self.mother_generator = Mother(genome_file=mother_file)
            self.child_generator = Child(child_genome_files=children_files)
        
        self.families_generated = 0
        self.total_members_sent = 0
        self._lock = threading.Lock()
        self._stop_flag = threading.Event()
        
        print("sGenerador de familias listo")
        print("="*80 + "\n")
    
    def generate_complete_family(self) -> Dict:
        """
        Genera una familia completa con ID único.
        
        Returns:
            Diccionario con padre, madre y 3 hijos
        """
        num = random.randint(1, 3)
        # Generar ID único de familia
        family_id = f"FAM_{uuid.uuid4().hex[:12].upper()}"
        
        # Generar padre
        father_data = self.father_generator.generate(family_id)
        
        # Generar madre
        mother_data = self.mother_generator.generate(family_id)
        
        # Generar  hijos con herencia de los genomas COMPLETOS de los padres
        children_data = []
        for i in range(1,num+1):
            child_data = self.child_generator.generate(
                family_id=family_id,
                child_number=i,
                father_genome=father_data['genome'],  
                mother_genome=mother_data['genome']   
            )
            children_data.append(child_data)
        
        return {
            'family_id': family_id,
            'father': father_data,
            'mother': mother_data,
            'children': children_data,
            'family_size': 2+num,
            'generation_timestamp': time.time()
        }
    
    def _streaming_thread(self, kafka_producer, topics: Dict[str, str], 
                         partition_number: int, thread_id: int):
        """
        Hilo que genera familias infinitamente y las envía a Kafka.
        
        Args:
            kafka_producer: Instancia de KafkaProducer
            topics: Diccionario con nombres de topics {'fathers': name, 'mothers': name, 'children': name}
            partition_number: Número de partición
            thread_id: ID del hilo
        """
        print(f"Hilo {thread_id} iniciado en partición {partition_number}")
        
        families_count = 0
        
        while not self._stop_flag.is_set():
            try:
                # Generar familia completa
                family = self.generate_complete_family()
                
                # Enviar padre al topic de padres
                kafka_producer.send(
                    topics['fathers'],
                    value=family['father'],
                    partition=partition_number
                )
                
                # Enviar madre al topic de madres
                kafka_producer.send(
                    topics['mothers'],
                    value=family['mother'],
                    partition=partition_number
                )
                
                # Enviar cada hijo al topic de hijos
                for child in family['children']:
                    kafka_producer.send(
                        topics['children'],
                        value=child,
                        partition=partition_number
                    )
                
                families_count += 1
                
                # Actualizar contadores globales
                with self._lock:
                    self.families_generated += 1
                    self.total_members_sent += 5  # padre + madre + 3 hijos
                
                # Pequeña pausa para no saturar
                time.sleep(0.001)
                
            except Exception as e:
                print(f"❌ Error en hilo {thread_id}: {e}")
                time.sleep(1)
                continue
    
    def start_infinite_streaming(self, kafka_producer, topics: Dict[str, str],
                                 partition_number: int, num_threads: int = 10) -> List[threading.Thread]:
        """
        Inicia generación infinita de familias con múltiples hilos.
        
        Args:
            kafka_producer: Instancia de KafkaProducer
            topics: Diccionario con nombres de topics
            partition_number: Número de partición
            num_threads: Número de hilos a usar
            
        Returns:
            Lista de hilos activos
        """
        self._stop_flag.clear()
        self.families_generated = 0
        self.total_members_sent = 0
        
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(
                target=self._streaming_thread,
                args=(kafka_producer, topics, partition_number, i),
                name=f"FamilyThread-{i}",
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        print(f"\n{'='*80}")
        print(f"✅ {num_threads} hilos generando familias infinitamente")
        print(f"📤 Enviando a topics: {list(topics.values())}")
        print(f"📍 Partición: {partition_number}")
        print(f"{'='*80}\n")
        
        return threads
    
    def stop_streaming(self):
        """Detiene todos los hilos de generación."""
        print("\n🛑 Deteniendo generación de familias...")
        self._stop_flag.set()
    
    def get_statistics(self) -> Dict:
        """Retorna estadísticas de generación."""
        return {
            'families_generated': self.families_generated,
            'total_members_sent': self.total_members_sent,
            'fathers_sent': self.families_generated,
            'mothers_sent': self.families_generated,
            'children_sent': self.families_generated * 3
        }
