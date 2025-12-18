from flask import Flask, render_template, jsonify, request
from datetime import datetime
from threading import Lock
from collections import deque
import requests
import time
import threading
import requests
import json
import os

app = Flask(__name__)

# Almacenamiento en memoria para métricas de procesamiento
metrics_data = {
    'fathers': {'total_records': 0},
    'mothers': {'total_records': 0},
    'children': {'total_records': 0},
    'last_update': datetime.now(),
    'batches_processed': 0,
    'processing_history': deque(maxlen=50),
    'task_completion_times': [],  # Lista ilimitada de tiempos de completado de tareas
    'families_completed': 0,  # Contador de familias completadas
    # Análisis genéticos
    'genetic_data': {
        'fathers': deque(maxlen=100),
        'mothers': deque(maxlen=100),
        'children': deque(maxlen=100)
    },
    'chromosome_distribution': {
        'fathers': {},
        'mothers': {},
        'children': {}
    },
    'heterozygosity_individual': {
        'fathers': [],
        'mothers': [],
        'children': []
    },
    'heterozygosity_population': {
        'fathers': {},
        'mothers': {},
        'children': {}
    },
    'hotspots': {
        'fathers': [],
        'mothers': [],
        'children': []
    },
    'genotype_trends': {
        'fathers': {},
        'mothers': {},
        'children': {}
    },
    # Métricas avanzadas de streaming genético (como en main)
    'genetic_metrics': {
        'time_window': deque(maxlen=300),  # Últimas 300 muestras (5 min a 1 muestra/seg)
        'variant_types': {},  # Tipo de variante -> contador
        'gene_frequency': {},  # Gen -> frecuencia
        'genotype_distribution': {'dominant': 0, 'recessive': 0, 'heterozygous': 0},
        'mutation_rate_window': deque(maxlen=60),  # Tasa de mutación por ventana
        'anomaly_count': 0  # Conteo de anomalías detectadas
    },
    'family_ids': set()  # Conjunto de IDs de familias procesadas
}


# Archivo para persistir el historial de emergency masters
HISTORY_FILE = '/app/dashboard/emergency_masters.json'

def load_emergency_masters():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            print(f"Error loading history: {e}")
            return set()
    return set()

def save_emergency_master(host):
    try:
        masters = load_emergency_masters()
        masters.add(host)
        # Ensure directory exists
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        with open(HISTORY_FILE, 'w') as f:
            json.dump(list(masters), f)
    except Exception as e:
        print(f"Error saving history: {e}")

# Métricas del cluster - obtenidas de APIs reales
cluster_metrics = {
    'spark': {
        'masters': [],
        'workers': [],
        'active_master': None,
        'status': 'UNKNOWN',
        'total_cores': 0,
        'used_cores': 0,
        'total_memory': 0,
        'used_memory': 0,
        'active_apps': [],
        'emergency_masters_seen': load_emergency_masters()  # Cargar al inicio
    },
    'hdfs': {
        'status': 'UNKNOWN',
        'capacity': 0,
        'used': 0,
        'remaining': 0,
        'percentage': 0,
        'live_nodes': 0,
        'dead_nodes': 0,
        'total_blocks': 0,
        'total_files': 0,
        'under_replicated': 0,
        'corrupt_blocks': 0,
        'missing_blocks': 0,
        'safemode': False,
        'datanodes': []
    },
    'history': {
        'cpu': deque(maxlen=60),
        'ram': deque(maxlen=60),
        'disk': deque(maxlen=60)
    }
}

metrics_lock = Lock()


def discover_spark_services():
    """Descubrir dinámicamente todos los masters y workers en la red"""
    import socket
    
    discovered = {'masters': [], 'workers': []}
    
    # Descubrir masters (intentar spark-master-1 hasta spark-master-10)
    for idx in range(1, 11):
        hostname = f"spark-master-{idx}"
        try:
            # Intentar resolver el hostname
            socket.gethostbyname(hostname)
            discovered['masters'].append(hostname)
        except socket.gaierror:
            # No existe más masters
            if idx > 2:  # Si ya encontramos al menos 2, parar
                break
            continue
    
    # Descubrir workers (intentar spark-worker-1 hasta spark-worker-20)
    for idx in range(1, 21):
        hostname = f"spark-worker-{idx}"
        try:
            socket.gethostbyname(hostname)

            # Resolver puerto dinámicamente: probar un rango corto de puertos típicos
            # (evita hardcodear cuando agregas más workers o cambias puertos)
            found_port = None
            candidates = []
            # patrón común: 8080 + idx
            candidates.append(8080 + idx)
            # puertos comunes en este proyecto
            for p in range(8080, 8091):
                if p not in candidates:
                    candidates.append(p)

            for port in candidates:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(0.15)
                        if s.connect_ex((hostname, port)) == 0:
                            found_port = port
                            break
                except Exception:
                    pass

            discovered['workers'].append((hostname, found_port if found_port else 8081))
        except socket.gaierror:
            if idx > 3:  # Al menos intentar 3 workers
                break
            continue
    
    return discovered


def poll_spark_metrics():
    """Obtener métricas reales de Spark Master API con failover automático a workers"""
    while True:
        try:
            masters_info = []
            active_master_data = None
            
            # Descubrir dinámicamente los servicios disponibles
            services = discover_spark_services()
            master_hosts = services['masters'] if services['masters'] else ['spark-master-1', 'spark-master-2']
            worker_hosts = services['workers'] if services['workers'] else [('spark-worker-1', 8081), ('spark-worker-2', 8082),('spark-worker-3', 8084)]
            
            # Mapa host->puerto para lookups (emergency masters, etc.)
            active_workers_map = {host: port for host, port in worker_hosts}

            print(f"[Spark HA] Discovered {len(master_hosts)} masters and {len(worker_hosts)} workers")
            
            # Consultar todos los masters descubiertos
            for master_host in master_hosts:
                try:
                    url = f"http://{master_host}:8080/json/"
                    response = requests.get(url, timeout=3)
                    if response.status_code == 200:
                        data = response.json()
                        status = data.get('status', 'UNKNOWN')
                        
                        master_info = {
                            'name': master_host,
                            'status': status,
                            'url': f"http://{master_host}:8080",
                            'workers_count': data.get('aliveworkers', 0),
                            'cores': data.get('cores', 0),
                            'memory': data.get('memory', 0),
                            'cores_used': data.get('coresused', 0),
                            'memory_used': data.get('memoryused', 0),
                            'type': 'MASTER'
                        }
                        masters_info.append(master_info)
                        
                        if status == 'ALIVE':
                            active_master_data = data
                            
                except Exception as e:
                    masters_info.append({
                        'name': master_host,
                        'status': 'UNREACHABLE',
                        'error': str(e),
                        'type': 'MASTER'
                    })
            
            # Track which workers are currently alive emergency masters
            active_emergency_masters = set()
            
            # FIRST PASS: Detect active emergency masters and track them
            for worker_host, worker_port in worker_hosts:
                try:
                    url = f"http://{worker_host}:{worker_port}/json/"
                    response = requests.get(url, timeout=2)
                    
                    if response.status_code == 200:
                        data = response.json()
                        # Check if this worker is acting as master
                        if 'status' in data and data.get('status') == 'ALIVE':
                            print(f"[Spark HA] Worker {worker_host} detected as ACTIVE Emergency Master!")
                            active_emergency_masters.add(worker_host)
                            cluster_metrics['spark']['emergency_masters_seen'].add(worker_host)
                            save_emergency_master(worker_host)  # Persistir inmediatamente
                            
                            # If no active master yet, use this one
                            if not active_master_data:
                                active_master_data = data
                            
                            masters_info.append({
                                'name': f"{worker_host} (Emergency Master)",
                                'status': 'ALIVE',
                                'url': f"http://{worker_host}:{worker_port}",
                                'workers_count': data.get('aliveworkers', 0),
                                'cores': data.get('cores', 0),
                                'memory': data.get('memory', 0),
                                'cores_used': data.get('coresused', 0),
                                'memory_used': data.get('memoryused', 0),
                                'type': 'WORKER_AS_MASTER'
                            })
                except Exception:
                    pass  # Will handle dead workers in second pass
            
            # SECOND PASS: Show ALL known emergency masters (even if dead)
            for em_host in cluster_metrics['spark']['emergency_masters_seen']:
                # Skip if already added as active
                if em_host in active_emergency_masters:
                    continue
                
                # Try to check status of this known emergency master
                try:
                    em_port = active_workers_map.get(em_host, 8081)
                    url = f"http://{em_host}:{em_port}/json/"
                    response = requests.get(url, timeout=2)
                    
                    if response.status_code == 200:
                        # It's alive but not acting as master anymore
                        print(f"[Spark HA] Former Emergency Master {em_host} is now DEMOTED")
                        masters_info.append({
                            'name': f"{em_host} (Former Emergency Master)",
                            'status': 'DEMOTED',
                            'url': f"http://{em_host}:{em_port}",
                            'type': 'WORKER_AS_MASTER'
                        })
                    else:
                        # Reachable but bad status
                        print(f"[Spark HA] Former Emergency Master {em_host} is UNREACHABLE (HTTP {response.status_code})")
                        masters_info.append({
                            'name': f"{em_host} (Emergency Master)",
                            'status': 'UNREACHABLE',
                            'error': f'HTTP {response.status_code}',
                            'type': 'WORKER_AS_MASTER'
                        })
                except Exception as e:
                    # Dead or unreachable - this is what we want to persist!
                    print(f"[Spark HA] Former Emergency Master {em_host} is now DEAD: {e}")
                    masters_info.append({
                        'name': f"{em_host} (Emergency Master)",
                        'status': 'DEAD',
                        'error': str(e),
                        'type': 'WORKER_AS_MASTER'
                    })
            
            # Procesar workers del master activo
            workers_info = []
            total_cores = 0
            used_cores = 0
            total_memory = 0
            used_memory = 0
            active_apps = []
            
            if active_master_data:
                for worker in active_master_data.get('workers', []):
                    # Obtener valores con validación
                    cores = int(worker.get('cores', 0))
                    cores_used = int(worker.get('coresused', 0))
                    memory = int(worker.get('memory', 0))
                    memory_used = int(worker.get('memoryused', 0))
                    
                    # Calcular porcentajes de forma segura
                    cpu_percent = (cores_used / cores * 100) if cores > 0 else 0
                    memory_percent = (memory_used / memory * 100) if memory > 0 else 0
                    
                    # Calcular cores y memoria libres
                    cores_free = max(0, cores - cores_used)
                    memory_free = max(0, memory - memory_used)
                    
                    worker_info = {
                        'id': worker.get('id', ''),
                        'host': worker.get('host', ''),
                        'port': worker.get('port', 0),
                        'state': worker.get('state', ''),
                        'cores': cores,
                        'cores_used': cores_used,
                        'coresfree': cores_free,
                        'memory': memory,
                        'memory_used': memory_used,
                        'memoryfree': memory_free,
                        'cpu_percent': round(cpu_percent, 1),
                        'memory_percent': round(memory_percent, 1)
                    }
                    workers_info.append(worker_info)
                    
                    total_cores += cores
                    used_cores += cores_used
                    total_memory += memory
                    used_memory += memory_used
                
                # Aplicaciones activas
                for app in active_master_data.get('activeapps', []):
                    active_apps.append({
                        'id': app.get('id', ''),
                        'name': app.get('name', ''),
                        'cores': app.get('cores', 0),
                        'memory': app.get('memoryperslave', 0),
                        'state': app.get('state', '')
                    })
            
            with metrics_lock:
                cluster_metrics['spark']['masters'] = masters_info
                cluster_metrics['spark']['workers'] = workers_info
                active_master = next((m for m in masters_info if m.get('status') == 'ALIVE'), None)
                cluster_metrics['spark']['active_master'] = active_master
                cluster_metrics['spark']['status'] = 'ALIVE' if active_master else 'DOWN'
                cluster_metrics['spark']['total_cores'] = total_cores
                cluster_metrics['spark']['used_cores'] = used_cores
                cluster_metrics['spark']['total_memory'] = total_memory
                cluster_metrics['spark']['used_memory'] = used_memory
                cluster_metrics['spark']['active_apps'] = active_apps
                
                # Log de estado mejorado
                if active_master:
                    master_type = active_master.get('type', 'MASTER')
                    master_name = active_master.get('name', 'Unknown')
                    print(f"[Spark HA] Active Master: {master_name} ({master_type}) - Workers: {len(workers_info)}")
                    print(f"[Spark HA] Cores: {used_cores}/{total_cores}, Memory: {used_memory}/{total_memory} MB, Apps: {len(active_apps)}")
                else:
                    print("[Spark HA] WARNING: No active master found!")
                
                # Agregar al historial con cálculos seguros
                timestamp = datetime.now().isoformat()
                cpu_percent = round((used_cores / total_cores * 100), 2) if total_cores > 0 else 0
                ram_percent = round((used_memory / total_memory * 100), 2) if total_memory > 0 else 0
                
                cluster_metrics['history']['cpu'].append({
                    'timestamp': timestamp,
                    'value': cpu_percent,
                    'used': used_cores,
                    'total': total_cores
                })
                cluster_metrics['history']['ram'].append({
                    'timestamp': timestamp,
                    'value': ram_percent,
                    'used': used_memory,
                    'total': total_memory
                })
                
        except Exception as e:
            print(f"Error polling Spark: {e}")
        
        time.sleep(3)


def poll_hdfs_metrics():
    """Obtener métricas reales de HDFS NameNode"""
    while True:
        try:
            # Consultar múltiples endpoints JMX del NameNode
            hdfs_data = {
                'status': 'Active',
                'capacity': 0,
                'used': 0,
                'remaining': 0,
                'percentage': 0,
                'live_nodes': 0,
                'dead_nodes': 0,
                'total_blocks': 0,
                'total_files': 0,
                'under_replicated': 0,
                'corrupt_blocks': 0,
                'missing_blocks': 0,
                'safemode': False
            }
            
            # 1. FSNamesystem - métricas principales
            url = "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
            response = requests.get(url, timeout=3)
            
            if response.status_code == 200:
                data = response.json()
                beans = data.get('beans', [])
                
                if beans:
                    bean = beans[0]
                    hdfs_data['capacity'] = bean.get('CapacityTotal', 0)
                    hdfs_data['used'] = bean.get('CapacityUsed', 0)
                    hdfs_data['remaining'] = bean.get('CapacityRemaining', 0)
                    hdfs_data['live_nodes'] = bean.get('NumLiveDataNodes', 0)
                    hdfs_data['dead_nodes'] = bean.get('NumDeadDataNodes', 0)
                    hdfs_data['total_blocks'] = bean.get('BlocksTotal', 0)
                    hdfs_data['total_files'] = bean.get('FilesTotal', 0)
                    hdfs_data['under_replicated'] = bean.get('UnderReplicatedBlocks', 0)
                    hdfs_data['corrupt_blocks'] = bean.get('CorruptBlocks', 0)
                    hdfs_data['missing_blocks'] = bean.get('MissingBlocks', 0)
                    
                    if hdfs_data['capacity'] > 0:
                        hdfs_data['percentage'] = (hdfs_data['used'] / hdfs_data['capacity'] * 100)
            
            # 2. FSNamesystemState - estado adicional
            url2 = "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
            response2 = requests.get(url2, timeout=3)
            
            if response2.status_code == 200:
                data2 = response2.json()
                beans2 = data2.get('beans', [])
                if beans2:
                    bean2 = beans2[0]
                    hdfs_data['safemode'] = bean2.get('FSState', '') == 'safeMode'
            
            # 3. NameNodeInfo - información detallada
            url3 = "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
            response3 = requests.get(url3, timeout=3)
            
            if response3.status_code == 200:
                data3 = response3.json()
                beans3 = data3.get('beans', [])
                if beans3:
                    bean3 = beans3[0]
                    # Parsear información de nodos vivos si está disponible
                    live_nodes_json = bean3.get('LiveNodes', '{}')
                    if live_nodes_json and live_nodes_json != '{}':
                        try:
                            import json
                            live_nodes_data = json.loads(live_nodes_json)
                            hdfs_data['datanodes'] = []
                            for node_name, node_info in live_nodes_data.items():
                                hdfs_data['datanodes'].append({
                                    'name': node_name,
                                    'capacity': node_info.get('capacity', 0),
                                    'used': node_info.get('usedSpace', 0),
                                    'remaining': node_info.get('remaining', 0),
                                    'state': node_info.get('adminState', 'In Service'),
                                    'last_contact': node_info.get('lastContact', 0)
                                })
                        except:
                            pass
            
            with metrics_lock:
                cluster_metrics['hdfs'].update(hdfs_data)
                
                # Agregar al historial
                timestamp = datetime.now().isoformat()
                cluster_metrics['history']['disk'].append({
                    'timestamp': timestamp,
                    'value': hdfs_data['percentage'],
                    'used': hdfs_data['used'],
                    'total': hdfs_data['capacity']
                })
                    
        except Exception as e:
            with metrics_lock:
                cluster_metrics['hdfs']['status'] = 'Unreachable'
        
        time.sleep(5)


# Iniciar hilos de polling
spark_thread = threading.Thread(target=poll_spark_metrics, daemon=True)
spark_thread.start()

hdfs_thread = threading.Thread(target=poll_hdfs_metrics, daemon=True)
hdfs_thread.start()


@app.route('/')
def index():
    """Página principal con dashboard"""
    return render_template('index.html')


@app.route('/api/stats')
def api_stats():
    """API para estadísticas de procesamiento"""
    with metrics_lock:
        return jsonify({
            'fathers': metrics_data['fathers']['total_records'],
            'mothers': metrics_data['mothers']['total_records'],
            'children': metrics_data['children']['total_records'],
            'total': sum([
                metrics_data['fathers']['total_records'],
                metrics_data['mothers']['total_records'],
                metrics_data['children']['total_records']
            ]),
            'batches_processed': metrics_data['batches_processed'],
            'families_completed': metrics_data['families_completed'],
            'last_update': metrics_data['last_update'].isoformat()
        })


@app.route('/api/cluster_stats')
def api_cluster_stats():
    """API para estado del cluster (Spark + HDFS) - Métricas reales"""
    with metrics_lock:
        spark = cluster_metrics['spark']
        hdfs = cluster_metrics['hdfs']
        history = cluster_metrics['history']
        
        return jsonify({
            'spark': {
                'status': spark['status'],
                'masters': spark['masters'],
                'workers': spark['workers'],
                'active_master': spark['active_master'],
                'total_cores': spark['total_cores'],
                'used_cores': spark['used_cores'],
                'total_memory': spark['total_memory'],
                'used_memory': spark['used_memory'],
                'active_apps': spark['active_apps']
            },
            'hdfs': {
                'status': hdfs['status'],
                'capacity': hdfs['capacity'],
                'used': hdfs['used'],
                'remaining': hdfs['remaining'],
                'percentage': hdfs['percentage'],
                'live_nodes': hdfs['live_nodes'],
                'dead_nodes': hdfs['dead_nodes'],
                'total_blocks': hdfs.get('total_blocks', 0),
                'total_files': hdfs.get('total_files', 0),
                'under_replicated': hdfs.get('under_replicated', 0),
                'corrupt_blocks': hdfs.get('corrupt_blocks', 0),
                'missing_blocks': hdfs.get('missing_blocks', 0),
                'safemode': hdfs.get('safemode', False),
                'datanodes': hdfs.get('datanodes', [])
            },
            'current': {
                'cpu': {'value': list(history['cpu'])[-1]['value'] if history['cpu'] else 0},
                'ram': {'value': list(history['ram'])[-1]['value'] if history['ram'] else 0},
                'disk': {'value': hdfs['percentage']}
            },
            'cpu': list(history['cpu']),
            'ram': list(history['ram']),
            'disk': list(history['disk'])
        })


@app.route('/api/processing_history')
def api_processing_history():
    """API para historial de procesamiento"""
    with metrics_lock:
        return jsonify({
            'history': list(metrics_data['processing_history'])
        })


@app.route('/api/task_times')
def api_task_times():
    """API para tiempos de completado de tareas"""
    with metrics_lock:
        return jsonify({
            'task_times': metrics_data['task_completion_times']
        })


@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    """Recibir métricas desde Spark"""
    try:
        data = request.get_json()
        member_type = data.get('member_type')
        message_type = data.get('message_type')
        
        # Detectar token de finalización de familia
        if message_type == 'FAMILY_COMPLETE':
            with metrics_lock:
                family_id = data.get('family_id')
                if family_id:
                    metrics_data['family_ids'].add(family_id)
                metrics_data['families_completed'] += 1
                metrics_data['last_update'] = datetime.now()
            print(f"✅ Familia completada: {family_id} - Total: {metrics_data['families_completed']}")
            return jsonify({'status': 'success', 'family_completed': True})
        
        # Procesar datos genéticos
        if message_type == 'CHROMOSOME_DISTRIBUTION':
            with metrics_lock:
                metrics_data['chromosome_distribution'][member_type] = data.get('chromosome_distribution', {})
                metrics_data['last_update'] = datetime.now()
            return jsonify({'status': 'success'})
        
        if message_type == 'GENETIC_INDIVIDUAL':
            with metrics_lock:
                metrics_data['heterozygosity_individual'][member_type].append({
                    'person_id': data.get('person_id'),
                    'heterozygous_pct': data.get('heterozygous_pct'),
                    'homozygous_pct': data.get('homozygous_pct'),
                    'total_snps': data.get('total_snps'),
                    'timestamp': data.get('timestamp')
                })
                # Mantener solo los últimos 100
                if len(metrics_data['heterozygosity_individual'][member_type]) > 100:
                    metrics_data['heterozygosity_individual'][member_type] = metrics_data['heterozygosity_individual'][member_type][-100:]
                metrics_data['last_update'] = datetime.now()
            return jsonify({'status': 'success'})
        
        if message_type == 'GENETIC_POPULATION':
            with metrics_lock:
                metrics_data['heterozygosity_population'][member_type] = {
                    'heterozygous_pct': data.get('heterozygous_pct'),
                    'homozygous_pct': data.get('homozygous_pct'),
                    'diversity': data.get('diversity'),
                    'total_snps': data.get('total_snps'),
                    'timestamp': data.get('timestamp')
                }
                metrics_data['last_update'] = datetime.now()
            return jsonify({'status': 'success'})
        
        if message_type == 'POSITION_HOTSPOTS':
            with metrics_lock:
                metrics_data['hotspots'][member_type] = data.get('hotspots', [])
                metrics_data['last_update'] = datetime.now()
            return jsonify({'status': 'success'})
        
        if message_type == 'GENOTYPE_TRENDS':
            with metrics_lock:
                metrics_data['genotype_trends'][member_type] = {
                    'distribution': data.get('genotype_distribution', {}),
                    'percentages': data.get('genotype_percentages', {}),
                    'total': data.get('total_genotypes', 0),
                    'timestamp': data.get('timestamp')
                }
                
                # Actualizar genotype_distribution global
                for genotype, count in data.get('genotype_distribution', {}).items():
                    # Clasificar genotipos (simplificado)
                    if genotype and len(genotype) >= 3:  # Formato "X/Y"
                        alleles = genotype.split('/')
                        if len(alleles) == 2:
                            a, b = alleles[0], alleles[1]
                            if a == b:
                                if a.isupper():
                                    metrics_data['genetic_metrics']['genotype_distribution']['dominant'] += count
                                else:
                                    metrics_data['genetic_metrics']['genotype_distribution']['recessive'] += count
                            else:
                                metrics_data['genetic_metrics']['genotype_distribution']['heterozygous'] += count
                
                metrics_data['last_update'] = datetime.now()
            return jsonify({'status': 'success'})
        
        # Métricas normales de batch
        with metrics_lock:
            if member_type in metrics_data:
                # Actualizar conteo de registros
                metrics_data[member_type]['total_records'] += data.get('total_records', 0)
                
                # Almacenar datos genéticos si existen
                genetic_data = data.get('genetic_data')
                if genetic_data:
                    # Add family_id to genetic_data for filtering in frontend
                    genetic_data['family_id'] = data.get('family_id')
                    metrics_data['genetic_data'][member_type].append(genetic_data)
                    
                    # Actualizar genetic_metrics (como en main)
                    # Agregar timestamp a la ventana de tiempo
                    metrics_data['genetic_metrics']['time_window'].append(datetime.now().timestamp())
                    
                    # Actualizar contadores
                    variant_type = genetic_data.get('variant_type', 'SNP')
                    metrics_data['genetic_metrics']['variant_types'][variant_type] = \
                        metrics_data['genetic_metrics']['variant_types'].get(variant_type, 0) + 1
                    
                    gene = genetic_data.get('gene')
                    if gene and gene != 'Unknown' and not gene.startswith('Chr'):
                        metrics_data['genetic_metrics']['gene_frequency'][gene] = \
                            metrics_data['genetic_metrics']['gene_frequency'].get(gene, 0) + 1
                    
                    # Calcular y guardar tasa de mutación actual (variantes por segundo en ventana de 60s)
                    current_time = datetime.now().timestamp()
                    recent_variants = sum(1 for ts in metrics_data['genetic_metrics']['time_window'] 
                                         if current_time - ts < 60)
                    mutation_rate = recent_variants / 60.0  # variantes por segundo
                    metrics_data['genetic_metrics']['mutation_rate_window'].append(mutation_rate)
                
                # Actualizar metadata general
                metrics_data['batches_processed'] += 1
                metrics_data['last_update'] = datetime.now()
                
                # Agregar al historial de procesamiento
                metrics_data['processing_history'].append({
                    'timestamp': datetime.now().isoformat(),
                    'member_type': member_type,
                    'records': data.get('total_records', 0)
                })
                
                # Almacenar tiempo de procesamiento si existe
                processing_time = data.get('processing_time')
                if processing_time is not None:
                    metrics_data['task_completion_times'].append({
                        'timestamp': datetime.now().isoformat(),
                        'member_type': member_type,
                        'processing_time': processing_time,
                        'records': data.get('total_records', 0)
                    })
        
        return jsonify({'status': 'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/genetic_analysis')
def genetic_analysis():
    """API para obtener datos de análisis genéticos"""
    with metrics_lock:
        return jsonify({
            'genetic_data': {
                'fathers': list(metrics_data['genetic_data']['fathers']),
                'mothers': list(metrics_data['genetic_data']['mothers']),
                'children': list(metrics_data['genetic_data']['children'])
            },
            'chromosome_distribution': metrics_data['chromosome_distribution'],
            'heterozygosity_individual': metrics_data['heterozygosity_individual'],
            'heterozygosity_population': metrics_data['heterozygosity_population'],
            'hotspots': metrics_data['hotspots'],
            'genotype_trends': metrics_data['genotype_trends'],
            'genetic_metrics': {
                'mutation_rate_history': list(metrics_data['genetic_metrics']['mutation_rate_window']),
                'variant_types': metrics_data['genetic_metrics']['variant_types'],
                'gene_frequency': dict(sorted(
                    metrics_data['genetic_metrics']['gene_frequency'].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:20]),  # Top 20 genes
                'genotype_distribution': metrics_data['genetic_metrics']['genotype_distribution']
            }
        })

@app.route('/api/families')
def get_families():
    """API para obtener lista de familias procesadas"""
    with metrics_lock:
        families_list = sorted(list(metrics_data['family_ids']))
        return jsonify({
            'families': families_list,
            'total': len(families_list)
        })

@app.route('/api/spark_jobs')
def spark_jobs():
    """API para obtener métricas de jobs y executors de Spark Driver con memoria correcta"""
    base_url = os.getenv('SPARK_DRIVER_URL', 'http://spark-driver:4040')
    result = {
        'appName': None,
        'appId': None,
        'jobs': [],
        'executors': [],
        'error': None
    }
    
    try:
        apps_resp = requests.get(f"{base_url}/api/v1/applications", timeout=2)
        if apps_resp.ok:
            apps = apps_resp.json()
            if apps and len(apps) > 0:
                app_id = apps[0].get('id')
                result['appId'] = app_id
                result['appName'] = apps[0].get('name', 'Unknown Application')
                
                # Obtener jobs (últimos 30)
                jobs_resp = requests.get(f"{base_url}/api/v1/applications/{app_id}/jobs", timeout=2)
                if jobs_resp.ok:
                    jobs_data = jobs_resp.json()
                    # Ordenar por jobId descendente y tomar solo los últimos 5
                    sorted_jobs = sorted(jobs_data, key=lambda x: x.get('jobId', 0), reverse=True)[:5]
                    
                    for j in sorted_jobs:
                        submission = j.get('submissionTime')
                        completion = j.get('completionTime')
                        
                        # Calcular duración si ambas fechas existen
                        duration = None
                        if completion and submission:
                            try:
                                # Si son timestamps numéricos (milisegundos)
                                if isinstance(completion, (int, float)) and isinstance(submission, (int, float)):
                                    duration = completion - submission
                                # Si son strings ISO8601, parsear y calcular
                                elif isinstance(completion, str) and isinstance(submission, str):
                                    from datetime import datetime
                                    # Parsear timestamps ISO8601 (formato: 2025-12-14T16:34:30.682GMT)
                                    comp_dt = datetime.strptime(completion.replace('GMT', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
                                    sub_dt = datetime.strptime(submission.replace('GMT', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
                                    duration = int((comp_dt - sub_dt).total_seconds() * 1000)  # En milisegundos
                            except Exception as e:
                                print(f"Error calculando duración: {e}")
                                pass
                        
                        result['jobs'].append({
                            'jobId': j.get('jobId'),
                            'name': j.get('name', 'Unknown Job'),
                            'status': j.get('status', 'UNKNOWN'),
                            'numTasks': j.get('numTasks', 0),
                            'numActiveTasks': j.get('numActiveTasks', 0),
                            'numCompletedTasks': j.get('numCompletedTasks', 0),
                            'numSkippedTasks': j.get('numSkippedTasks', 0),
                            'numFailedTasks': j.get('numFailedTasks', 0),
                            'duration': duration,
                            'submissionTime': submission,
                            'completionTime': completion
                        })
                
                # Obtener executors con métricas de memoria correctas
                exec_resp = requests.get(f"{base_url}/api/v1/applications/{app_id}/executors", timeout=2)
                if exec_resp.ok:
                    exec_data = exec_resp.json()
                    for e in exec_data:
                        if e.get('id') == 'driver':
                            continue
                        
                        # Obtener métricas de memoria (on-heap y off-heap)
                        memory_metrics = e.get('memoryMetrics', {})
                        used_on_heap = memory_metrics.get('usedOnHeapStorageMemory', 0)
                        used_off_heap = memory_metrics.get('usedOffHeapStorageMemory', 0)
                        total_on_heap = e.get('maxMemory', 0)
                        total_off_heap = memory_metrics.get('totalOffHeapStorageMemory', 0)
                        
                        # Calcular tiempo promedio por tarea
                        completed_tasks = e.get('completedTasks', 0)
                        total_duration = e.get('totalDuration', 0)
                        avg_task_duration = (total_duration / completed_tasks) if completed_tasks > 0 else 0
                        
                        result['executors'].append({
                            'id': e.get('id'),
                            'hostPort': e.get('hostPort'),
                            'isActive': e.get('isActive', True),
                            'rddBlocks': e.get('rddBlocks', 0),
                            'usedOnHeapMemory': used_on_heap,
                            'usedOffHeapMemory': used_off_heap,
                            'totalOnHeapMemory': total_on_heap,
                            'totalOffHeapMemory': total_off_heap,
                            'diskUsed': e.get('diskUsed', 0),
                            'totalCores': e.get('totalCores', 0),
                            'maxTasks': e.get('maxTasks', 0),
                            'activeTasks': e.get('activeTasks', 0),
                            'failedTasks': e.get('failedTasks', 0),
                            'completedTasks': completed_tasks,
                            'totalTasks': e.get('totalTasks', 0),
                            'totalDuration': total_duration,
                            'totalGCTime': e.get('totalGCTime', 0),
                            'totalInputBytes': e.get('totalInputBytes', 0),
                            'totalShuffleRead': e.get('totalShuffleRead', 0),
                            'totalShuffleWrite': e.get('totalShuffleWrite', 0),
                            'avgTaskDuration': avg_task_duration
                        })
                    
    except Exception as e:
        result['error'] = str(e)
    
    return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
