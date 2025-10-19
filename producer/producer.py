#!/usr/bin/env python3
"""
Productor de genomas familiares a Kafka.
Punto de entrada principal de la aplicación.
"""

import signal
import sys
import time
from config import settings
from family_generator import FamilyGenerator
from streaming_manager import StreamingManager


# Variable global para el gestor de streaming
streaming_manager = None


def graceful_shutdown(signum, frame):
    """Maneja la parada segura de la aplicación cuando se recibe Ctrl+C."""
    print("\n🛑 Iniciando parada segura... Por favor, espere.")
    if streaming_manager:
        streaming_manager.stop()
    time.sleep(2)
    print("✅ Aplicación detenida de forma segura.")
    sys.exit(0)


def main():
    """
    Función principal que orquesta toda la aplicación:
    1. Carga la configuración
    2. Inicializa el generador de familias
    3. Inicia el streaming a Kafka
    4. Monitoriza el estado
    """
    global streaming_manager
    
    print("="*80)
    print("PRODUCTOR DE GENOMAS FAMILIARES")
    print("="*80)
    
        # Mostrar configuración
    settings.display_config()
    
    # 1. Configurar el generador de familias con los genomas reales
    genome_paths = settings.get_all_genome_paths()
    family_generator = FamilyGenerator(genome_paths=genome_paths)
    
    # 2. Configurar el gestor de streaming con Kafka
    streaming_manager = StreamingManager(
        family_generator=family_generator,
        kafka_settings={
            'bootstrap_servers': settings.KAFKA_BROKER_URL,
            'client_id': settings.KAFKA_CLIENT_ID
        },
        topics={
            'fathers': settings.KAFKA_TOPIC_FATHERS,
            'mothers': settings.KAFKA_TOPIC_MOTHERS,
            'children': settings.KAFKA_TOPIC_CHILDREN
        },
        num_threads=settings.NUM_THREADS,
        partition_number=settings.PARTITION_NUMBER
    )
    
    # 3. Registrar manejadores de señales para parada segura
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    # 4. Iniciar el streaming
    streaming_manager.start()
    
    print("\n✅ El productor está en funcionamiento.")
    print("💡 Presiona Ctrl+C para detenerlo de forma segura.\n")
    
    # 5. Mantener el proceso vivo y mostrar estadísticas periódicamente
    try:
        while True:
            time.sleep(10)
            stats = streaming_manager.get_statistics()
            print(f"📊 Familias: {stats['families_generated']} | "
                  f"SNPs enviados: {stats['total_snps_sent']:,} | "
                  f"Promedio: {stats['avg_snps_per_family']:,.0f} SNPs/familia")
    except (KeyboardInterrupt, SystemExit):
        graceful_shutdown(None, None)


if __name__ == "__main__":
    main()

