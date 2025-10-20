#!/usr/bin/env python3
"""
Consola simple de análisis genómico - Solo HDFS + Pandas (sin Spark)
"""

import pandas as pd
from hdfs import InsecureClient
import os
from datetime import datetime
import time
import sys
import io

# ==================== CONFIGURACIÓN ====================
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE_URL', 'hdfs://namenode:9000')
HDFS_BASE_PATH = '/genomic_data'

# Variables globales
_hdfs_client = None

# ==================== GESTIÓN DE CONEXIONES ====================

def get_hdfs_client():
    """Obtiene o crea cliente HDFS"""
    global _hdfs_client
    
    if _hdfs_client is None:
        try:
            namenode_url = HDFS_NAMENODE.replace('hdfs://', 'http://').replace(':9000', ':9870')
            _hdfs_client = InsecureClient(namenode_url, user='root', timeout=30)
            _hdfs_client.list('/')
            print(f"✅ Cliente HDFS conectado: {namenode_url}")
        except Exception as e:
            print(f"❌ Error conectando a HDFS: {e}")
            return None
    
    return _hdfs_client

# ==================== FUNCIONES DE DATOS ====================

def load_sample_data(member_type='fathers', num_rows=100):
    """Carga una muestra de datos directamente desde HDFS con pandas"""
    try:
        client = get_hdfs_client()
        if not client:
            return "❌ Cliente HDFS no disponible", pd.DataFrame()
        
        print(f"\n📥 Cargando {num_rows} filas de {member_type}...")
        path = f'{HDFS_BASE_PATH}/{member_type}'
        
        # Listar archivos parquet
        files = client.list(path)
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        if not parquet_files:
            return f"❌ No hay archivos en {member_type}", pd.DataFrame()
        
        # Leer el primer archivo parquet
        file_path = f"{path}/{parquet_files[0]}"
        print(f"   Leyendo: {file_path}")
        
        with client.read(file_path) as reader:
            data = reader.read()
            df = pd.read_parquet(io.BytesIO(data))
        
        # Limitar filas
        df = df.head(num_rows)
        
        message = f"✅ Cargados {len(df)} registros de {member_type} (archivo: {parquet_files[0]})"
        return message, df
        
    except Exception as e:
        error_msg = f"❌ Error cargando {member_type}: {str(e)[:200]}"
        print(error_msg)
        return error_msg, pd.DataFrame()

def get_file_stats():
    """Obtiene estadísticas de archivos en HDFS"""
    try:
        client = get_hdfs_client()
        if not client:
            return pd.DataFrame()
        
        stats = []
        
        for member_type in ['fathers', 'mothers', 'children']:
            try:
                path = f'{HDFS_BASE_PATH}/{member_type}'
                files = client.list(path)
                parquet_files = [f for f in files if f.endswith('.parquet')]
                
                # Estimar registros (1000 por archivo según hdfs_loader)
                estimated_records = len(parquet_files) * 1000
                
                stats.append({
                    'Tipo': member_type.capitalize(),
                    'Archivos': len(parquet_files),
                    'Registros Est.': f"{estimated_records:,}",
                    'Estado': '✅ OK'
                })
            except:
                stats.append({
                    'Tipo': member_type.capitalize(),
                    'Archivos': 0,
                    'Registros Est.': '0',
                    'Estado': '❌ Error'
                })
        
        return pd.DataFrame(stats)
        
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return pd.DataFrame()

def check_hdfs_status():
    """Verifica el estado de HDFS"""
    try:
        client = get_hdfs_client()
        if not client:
            return "❌ HDFS no disponible"
        
        details = []
        details.append(f"✅ HDFS NameNode: {HDFS_NAMENODE}")
        
        # Contar archivos por tipo
        for dtype in ['fathers', 'mothers', 'children']:
            try:
                path = f'{HDFS_BASE_PATH}/{dtype}'
                files = client.list(path)
                parquet_files = [f for f in files if f.endswith('.parquet')]
                details.append(f"  📁 {dtype}: {len(parquet_files)} archivos Parquet")
            except:
                details.append(f"  📁 {dtype}: 0 archivos")
        
        summary = f"""
📊 Estado de HDFS

{chr(10).join(details)}

Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        return summary
        
    except Exception as e:
        return f"❌ Error verificando HDFS: {e}"

# ==================== INTERFAZ DE CONSOLA ====================

def print_menu():
    """Imprime el menú principal"""
    print("\n" + "="*80)
    print("🧬 GENOMIC ANALYSIS CONSOLE - Simple Mode".center(80))
    print("="*80)
    print("\n📋 MENÚ PRINCIPAL:\n")
    print("  [1] 🏗️  Verificar Estado de HDFS")
    print("  [2] 📊 Explorar Datos (cargar muestra)")
    print("  [3] 📈 Estadísticas de Archivos")
    print("  [0] ❌ Salir")
    print("\n" + "="*80)

def console_explore_data():
    """Exploración de datos por consola"""
    print("\n" + "-"*80)
    print("📊 EXPLORACIÓN DE DATOS")
    print("-"*80)
    
    # Seleccionar tipo
    print("\nTipo de miembro:")
    print("  [1] Fathers")
    print("  [2] Mothers")
    print("  [3] Children")
    
    choice = input("\nSelecciona (1-3): ").strip()
    member_types = {'1': 'fathers', '2': 'mothers', '3': 'children'}
    member_type = member_types.get(choice, 'fathers')
    
    # Número de filas
    rows_input = input("Número de filas a cargar (10-1000, default 100): ").strip()
    try:
        num_rows = int(rows_input) if rows_input else 100
        num_rows = max(10, min(1000, num_rows))
    except:
        num_rows = 100
    
    message, df = load_sample_data(member_type, num_rows)
    print(f"\n{message}")
    
    if not df.empty:
        print(f"\n📋 Primeras 10 filas:\n")
        print(df.head(10).to_string())
        print(f"\n📊 Shape: {df.shape[0]} filas × {df.shape[1]} columnas")
        print(f"\n📑 Columnas: {', '.join(df.columns.tolist())}")
    else:
        print("❌ No se pudieron cargar datos")

def console_statistics():
    """Estadísticas por consola"""
    print("\n" + "-"*80)
    print("📈 ESTADÍSTICAS DE ARCHIVOS")
    print("-"*80)
    
    print("\n📊 Calculando estadísticas...")
    stats_df = get_file_stats()
    
    if not stats_df.empty:
        print("\n✅ Estadísticas de archivos en HDFS:\n")
        print(stats_df.to_string(index=False))
    else:
        print("\n❌ No se pudieron obtener estadísticas")

def run_console():
    """Ejecuta la consola interactiva"""
    print("\n" + "="*80)
    print("🧬 GENOMIC DASHBOARD - MODO SIMPLE (HDFS + Pandas)")
    print("="*80)
    print(f"📍 HDFS: {HDFS_NAMENODE}")
    print("="*80)
    
    # Inicializar HDFS
    print("\n🔌 Conectando a HDFS...")
    get_hdfs_client()
    
    # Bucle principal
    while True:
        try:
            print_menu()
            choice = input("\nSelecciona una opción (0-3): ").strip()
            
            if choice == '1':
                print("\n" + "-"*80)
                print("🏗️ ESTADO DE HDFS")
                print("-"*80)
                status = check_hdfs_status()
                print(status)
                input("\nPresiona Enter para continuar...")
                
            elif choice == '2':
                console_explore_data()
                input("\nPresiona Enter para continuar...")
                
            elif choice == '3':
                console_statistics()
                input("\nPresiona Enter para continuar...")
                
            elif choice == '0':
                print("\n👋 ¡Hasta luego!")
                sys.exit(0)
                
            else:
                print("\n⚠️ Opción no válida. Por favor selecciona 0-3.")
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n\n👋 ¡Hasta luego!")
            sys.exit(0)
        except Exception as e:
            print(f"\n❌ Error inesperado: {e}")
            input("\nPresiona Enter para continuar...")

# ==================== MAIN ====================

if __name__ == "__main__":
    run_console()
