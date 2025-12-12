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
    'processing_history': deque(maxlen=50)
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
    # Mapeo de puertos personalizado según docker-compose.yml
    worker_ports = {
        1: 8081,
        2: 8082,
        3: 8084  # worker-3 usa 8084 (8083 está ocupado por master-2)
    }
    
    for idx in range(1, 21):
        hostname = f"spark-worker-{idx}"
        try:
            socket.gethostbyname(hostname)
            # Usar puerto personalizado o calcular dinámicamente
            port = worker_ports.get(idx, 8080 + idx)
            discovered['workers'].append((hostname, port))
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
            worker_hosts = services['workers'] if services['workers'] else [('spark-worker-1', 8081), ('spark-worker-2', 8082)]
            
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
                    worker_num = int(em_host.split('-')[-1])
                    em_port = worker_ports.get(worker_num, 8081)
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


@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    """Recibir métricas desde Spark"""
    try:
        data = request.get_json()
        member_type = data.get('member_type')
        
        with metrics_lock:
            if member_type in metrics_data:
                # Actualizar conteo de registros
                metrics_data[member_type]['total_records'] += data.get('total_records', 0)
                
                # Actualizar metadata general
                metrics_data['batches_processed'] += 1
                metrics_data['last_update'] = datetime.now()
                
                # Agregar al historial
                metrics_data['processing_history'].append({
                    'timestamp': datetime.now().isoformat(),
                    'member_type': member_type,
                    'records': data.get('total_records', 0)
                })
        
        return jsonify({'status': 'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
