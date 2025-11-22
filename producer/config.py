#!/usr/bin/env python3
"""
Configuración centralizada para el productor de genomas.
Carga valores desde variables de entorno con valores por defecto sensatos.
"""

import os
from typing import List


class Settings:
    """Clase de configuración para el productor de genomas."""
    
    def __init__(self):
        # Configuración de Kafka
        self.KAFKA_BROKER_URL: str = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
        self.KAFKA_CLIENT_ID: str = os.getenv('KAFKA_CLIENT_ID', 'genomic-producer')
        
        # Topics de Kafka
        self.KAFKA_TOPIC_FATHERS: str = os.getenv('KAFKA_TOPIC_FATHERS', 'fathers')
        self.KAFKA_TOPIC_MOTHERS: str = os.getenv('KAFKA_TOPIC_MOTHERS', 'mothers')
        self.KAFKA_TOPIC_CHILDREN: str = os.getenv('KAFKA_TOPIC_CHILDREN', 'children')
        
        # Rutas a los archivos de genoma
        self.DATA_DIR: str = os.path.join(os.path.dirname(__file__), 'data', 'archive-2')
        self.FATHER_GENOME_PATH: str = os.path.join(self.DATA_DIR, 'Father Genome.csv')
        self.MOTHER_GENOME_PATH: str = os.path.join(self.DATA_DIR, 'Mother Genome.csv')
        self.CHILD1_GENOME_PATH: str = os.path.join(self.DATA_DIR, 'Child 1 Genome.csv')
        self.CHILD2_GENOME_PATH: str = os.path.join(self.DATA_DIR, 'Child 2 Genome.csv')
        self.CHILD3_GENOME_PATH: str = os.path.join(self.DATA_DIR, 'Child 3 Genome.csv')
        
        # Configuración de rendimiento
        self.NUM_THREADS: int = int(os.getenv('NUM_THREADS', '1'))
        self.SEND_BATCH_SIZE: int = int(os.getenv('SEND_BATCH_SIZE', '1000'))
        self.LOG_INTERVAL: int = int(os.getenv('LOG_INTERVAL', '100000'))
        
        # Configuración de particiones
        self.PARTITION_NUMBER: int = int(os.getenv('PARTITION_NUMBER', '0'))
        
        # Validar que los archivos existen
        self._validate_genome_files()
    
    def _validate_genome_files(self):
        """Valida que los archivos de genoma existan."""
        genome_files = [
            self.FATHER_GENOME_PATH,
            self.MOTHER_GENOME_PATH,
            self.CHILD1_GENOME_PATH,
            self.CHILD2_GENOME_PATH,
            self.CHILD3_GENOME_PATH
        ]
        
        missing_files = []
        for file_path in genome_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            print("⚠️  Advertencia: Los siguientes archivos de genoma no se encontraron:")
            for file_path in missing_files:
                print(f"   - {file_path}")
    
    def get_all_genome_paths(self) -> dict:
        """Retorna un diccionario con todas las rutas de genomas."""
        return {
            'father': self.FATHER_GENOME_PATH,
            'mother': self.MOTHER_GENOME_PATH,
            'children': [
                self.CHILD1_GENOME_PATH,
                self.CHILD2_GENOME_PATH,
                self.CHILD3_GENOME_PATH
            ]
        }
    
    def display_config(self):
        """Muestra la configuración actual."""
        print("\n" + "="*80)
        print(". CONFIGURACIÓN DEL PRODUCTOR")
        print("="*80)
        print(f"Kafka Broker: {self.KAFKA_BROKER_URL}")
        print(f"Client ID: {self.KAFKA_CLIENT_ID}")
        print(f"Topics: {self.KAFKA_TOPIC_FATHERS}, {self.KAFKA_TOPIC_MOTHERS}, {self.KAFKA_TOPIC_CHILDREN}")
        print(f"Hilos: {self.NUM_THREADS}")
        print(f"Batch Size: {self.SEND_BATCH_SIZE}")
        print(f"Log Interval: {self.LOG_INTERVAL:,}")
        print(f"Partición: {self.PARTITION_NUMBER}")
        print("="*80 + "\n")


settings = Settings()
