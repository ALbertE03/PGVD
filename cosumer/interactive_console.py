#!/usr/bin/env python3
"""
Consola interactiva para visualizar estadísticas de familias en tiempo real.
Consume estadísticas PRE-CALCULADAS por Spark desde topics de Kafka.
El procesamiento pesado lo hace Spark Streaming en spark_consumer.py
"""

import os
import sys
import time
from datetime import datetime
from kafka import KafkaConsumer
import json
from collections import defaultdict
import threading
import statistics

# ==================== CONFIGURACIÓN ====================

KAFKA_BROKER = os.getenv('KAFKA_BROKER_URL', 'kafka:9092')
# Consumir estadísticas pre-calculadas por Spark en lugar de datos crudos
TOPICS = ['family-stats', 'member-stats', 'family-completion']

# ==================== ESTADO GLOBAL ====================

# Estadísticas de familias (calculadas por Spark)
families_data = defaultdict(lambda: {
    'snp_records': 0,
    'total_snps': 0,
    'unique_chromosomes': 0,
    'total_members': 0,
    'fathers': 0,
    'mothers': 0,
    'children': 0,
    'last_update': None,
    'completed': False
})

# Estadísticas de miembros (calculadas por Spark)
members_data = {}  # {(family_id, person_id): {stats}}

stats_lock = threading.Lock()
running = True

# ==================== COLORES ANSI ====================

class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# ==================== FUNCIONES DE VISUALIZACIÓN ====================

def clear_screen():
    """Limpia la pantalla"""
    os.system('clear' if os.name != 'nt' else 'cls')

def print_header():
    """Imprime el header de la consola"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'🧬 GENOMIC ANALYZER - CONSOLA INTERACTIVA 🧬'.center(80)}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.ENDC}\n")
    print(f"{Colors.YELLOW}⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.ENDC}")
    print(f"{Colors.BLUE}📡 Kafka: {KAFKA_BROKER}{Colors.ENDC}")
    print(f"{Colors.GREEN}✅ Visualización en tiempo real{Colors.ENDC}")
    print(f"{Colors.CYAN}⚡ Procesamiento: Spark Streaming (spark_consumer.py){Colors.ENDC}\n")

def print_family_summary():
    """Imprime resumen de todas las familias"""
    with stats_lock:
        if not families_data:
            print(f"{Colors.YELLOW}⏳ Esperando datos de familias...{Colors.ENDC}\n")
            return
        
        total_families = len(families_data)
        completed_families = sum(1 for f in families_data.values() if f.get('completed', False))
        in_progress_families = total_families - completed_families
        total_members = sum(f['total_members'] for f in families_data.values())
        total_snps = sum(f['total_snps'] for f in families_data.values())
        
        print(f"{Colors.BOLD}{Colors.GREEN}📊 RESUMEN GLOBAL{Colors.ENDC}")
        print(f"{'─'*80}")
        print(f"  🏠 Total de familias: {Colors.BOLD}{total_families}{Colors.ENDC}")
        print(f"  ✅ Familias completadas: {Colors.GREEN}{completed_families}{Colors.ENDC}")
        print(f"  ⏳ En proceso: {Colors.YELLOW}{in_progress_families}{Colors.ENDC}")
        print(f"  👥 Total de miembros: {Colors.BOLD}{total_members}{Colors.ENDC}")
        print(f"  🧬 Total de SNPs procesados: {Colors.BOLD}{total_snps:,}{Colors.ENDC}\n")

def print_family_details(limit=10):
    """Imprime detalles de las últimas familias"""
    with stats_lock:
        if not families_data:
            return
        
        print(f"{Colors.BOLD}{Colors.BLUE}🏠 DETALLES DE FAMILIAS (últimas {limit}){Colors.ENDC}")
        print(f"{'─'*80}\n")
        
        # Ordenar por última actualización
        sorted_families = sorted(
            families_data.items(),
            key=lambda x: x[1]['last_update'] or datetime.min,
            reverse=True
        )[:limit]
        
        for family_id, data in sorted_families:
            status_icon = "✅" if data.get('completed', False) else "⏳"
            status_text = "Completada" if data.get('completed', False) else "En proceso"
            
            print(f"{Colors.CYAN}{status_icon} Familia: {Colors.BOLD}{family_id}{Colors.ENDC} [{status_text}]")
            print(f"   👨 Padres: {data['fathers']} | "
                  f"👩 Madres: {data['mothers']} | "
                  f"👶 Hijos: {data['children']}")
            print(f"   👥 Total miembros: {data['total_members']}")
            print(f"   🧬 SNPs procesados: {Colors.GREEN}{data['snp_records']:,}{Colors.ENDC}")
            print(f"   🧬 Cromosomas únicos: {data['unique_chromosomes']}")
            
            if data['last_update']:
                time_ago = (datetime.now() - data['last_update']).total_seconds()
                print(f"   ⏰ Última actualización: hace {time_ago:.0f}s")
            
            print()

def print_member_statistics():
    """Imprime estadísticas detalladas por miembro de familia"""
    with stats_lock:
        if not members_data:
            return
        
        print(f"{Colors.BOLD}{Colors.YELLOW}👥 ESTADÍSTICAS POR MIEMBRO (últimas 3 familias){Colors.ENDC}")
        print(f"{'─'*80}\n")
        
        # Agrupar miembros por familia y ordenar
        family_members = defaultdict(list)
        for (family_id, person_id), member_info in members_data.items():
            family_members[family_id].append((person_id, member_info))
        
        # Ordenar familias por última actualización de sus miembros
        sorted_families = sorted(
            family_members.items(),
            key=lambda x: max((m[1].get('last_update', datetime.min) for m in x[1]), default=datetime.min),
            reverse=True
        )[:3]
        
        for family_id, members in sorted_families:
            # Verificar si la familia está completada
            family_data = families_data.get(family_id, {})
            status_icon = "✅" if family_data.get('completed', False) else "⏳"
            print(f"{Colors.CYAN}{status_icon} {family_id}{Colors.ENDC}")
            
            if not members:
                print(f"   {Colors.YELLOW}Sin datos de miembros aún{Colors.ENDC}\n")
                continue
            
            for person_id, member_info in members:
                member_type = member_info.get('member_type', 'unknown')
                icon = "👨" if member_type == 'father' else "👩" if member_type == 'mother' else "👶"
                
                print(f"   {icon} {member_type.upper()}: {person_id[:30]}")
                print(f"      📊 SNPs procesados: {Colors.GREEN}{member_info.get('snp_count', 0):,}{Colors.ENDC}")
                print(f"      🧬 Genotipos únicos: {Colors.CYAN}{member_info.get('unique_genotypes', 0)}{Colors.ENDC}")
                print(f"      🔢 Total genotipos procesados: {Colors.BLUE}{member_info.get('genotypes_count', 0):,}{Colors.ENDC}")
                print(f"      📍 Posiciones genómicas únicas: {Colors.BLUE}{member_info.get('unique_positions', 0)}{Colors.ENDC}")
                
                # Mostrar estadísticas de posiciones si están disponibles
                avg_pos = member_info.get('avg_position')
                if avg_pos:
                    print(f"      � Media posiciones: {Colors.YELLOW}{avg_pos:,.2f}{Colors.ENDC}")
            
            print()


def print_menu():
    """Imprime el menú de opciones"""
    print(f"{Colors.BOLD}{Colors.CYAN}{'─'*80}{Colors.ENDC}")
    print(f"{Colors.BOLD}📋 OPCIONES:{Colors.ENDC}")
    print(f"  [R] Refrescar pantalla")
    print(f"  [S] Ver resumen global")
    print(f"  [T] Ver top familias")
    print(f"  [Q] Salir")
    print(f"{Colors.CYAN}{'─'*80}{Colors.ENDC}\n")

# ==================== PROCESAMIENTO DE DATOS ====================

def process_family_stats(data):
    """Procesa estadísticas de familia calculadas por Spark"""
    try:
        family_id = data.get('family_id')
        if not family_id:
            return
        
        with stats_lock:
            family = families_data[family_id]
            family['snp_records'] = data.get('snp_records', 0)
            family['total_snps'] = data.get('total_snps', 0)
            family['unique_chromosomes'] = data.get('unique_chromosomes', 0)
            family['total_members'] = data.get('total_members', 0)
            family['fathers'] = data.get('fathers', 0)
            family['mothers'] = data.get('mothers', 0)
            family['children'] = data.get('children', 0)
            family['last_update'] = datetime.now()
            
    except Exception as e:
        pass  # Ignorar errores de parseo

def process_member_stats(data):
    """Procesa estadísticas de miembro calculadas por Spark"""
    try:
        family_id = data.get('family_id')
        person_id = data.get('person_id')
        
        if not family_id or not person_id:
            return
        
        with stats_lock:
            key = (family_id, person_id)
            members_data[key] = {
                'member_type': data.get('member_type', 'unknown'),
                'snp_count': data.get('snp_count', 0),
                'unique_genotypes': data.get('unique_genotypes', 0),
                'genotypes_count': data.get('genotypes_count', 0),
                'unique_positions': data.get('unique_positions', 0),
                'total_snps': data.get('total_snps', 0),
                'avg_position': data.get('avg_position'),
                'last_update': datetime.now()
            }
            
    except Exception as e:
        pass  # Ignorar errores de parseo

def process_family_completion(data):
    """Marca una familia como completada"""
    try:
        family_id = data.get('family_id')
        message_type = data.get('message_type')
        
        if not family_id or message_type != 'FAMILY_COMPLETE':
            return
        
        with stats_lock:
            if family_id in families_data:
                families_data[family_id]['completed'] = True
                families_data[family_id]['last_update'] = datetime.now()
            
    except Exception as e:
        pass  # Ignorar errores de parseo

def process_message(message):
    """Procesa un mensaje de Kafka (estadísticas pre-calculadas por Spark)"""
    try:
        data = json.loads(message.value.decode('utf-8'))
        topic = message.topic
        
        # Procesar según el topic
        if topic == 'family-stats':
            process_family_stats(data)
        elif topic == 'member-stats':
            process_member_stats(data)
        elif topic == 'family-completion':
            process_family_completion(data)
            
    except Exception as e:
        pass  # Ignorar errores de parseo

def kafka_consumer_thread():
    """Thread que consume mensajes de Kafka"""
    global running
    
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='interactive-console-group',
            value_deserializer=lambda x: x
        )
        
        print(f"{Colors.GREEN}✅ Conectado a Kafka{Colors.ENDC}")
        
        for message in consumer:
            if not running:
                break
            process_message(message)
        
        consumer.close()
    
    except Exception as e:
        print(f"{Colors.RED}❌ Error conectando a Kafka: {e}{Colors.ENDC}")

# ==================== CONSOLA INTERACTIVA ====================

def display_dashboard():
    """Muestra el dashboard completo"""
    clear_screen()
    print_header()
    print_family_summary()
    print_family_details(limit=6)
    print_member_statistics()
    print(f"{Colors.YELLOW}💡 Datos calculados por Spark y consumidos desde Kafka{Colors.ENDC}")
    print(f"{Colors.CYAN}⚡ Procesamiento distribuido con Spark Streaming (spark_consumer.py){Colors.ENDC}")
    print(f"{Colors.YELLOW}💡 Presiona Ctrl+C para salir{Colors.ENDC}\n")

def interactive_console():
    """Ejecuta la consola interactiva"""
    global running
    
    # Iniciar consumer en thread separado
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    # Esperar un poco para que se conecte
    time.sleep(2)
    
    # Modo auto-refresh
    print(f"{Colors.GREEN}🚀 Iniciando consola interactiva...{Colors.ENDC}\n")
    print(f"{Colors.YELLOW}💡 La pantalla se actualizará automáticamente cada 5 segundos{Colors.ENDC}")
    print(f"{Colors.YELLOW}💡 Presiona Ctrl+C para salir{Colors.ENDC}\n")
    
    time.sleep(3)
    
    try:
        refresh_interval = 5  # segundos
        last_refresh = time.time()
        
        while running:
            current_time = time.time()
            
            # Auto-refresh cada N segundos
            if current_time - last_refresh >= refresh_interval:
                display_dashboard()
                last_refresh = current_time
            
            time.sleep(1)
    
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}👋 Deteniendo consola...{Colors.ENDC}")
        running = False
        consumer_thread.join(timeout=2)
        print(f"{Colors.GREEN}✅ Consola detenida{Colors.ENDC}\n")

# ==================== MAIN ====================

def main():
    """Función principal"""
    
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'🧬 GENOMIC ANALYZER - CONSOLA INTERACTIVA 🧬'.center(80)}{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.ENDC}\n")
    
    print()
    
    try:
    
            interactive_console()
       
    
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}👋 ¡Hasta luego!{Colors.ENDC}\n")

if __name__ == "__main__":
    main()
