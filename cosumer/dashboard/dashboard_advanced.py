from flask import Flask, render_template, jsonify, request
from datetime import datetime
from threading import Lock
from collections import defaultdict, deque
import requests
import time
import threading

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
        'active_apps': []
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


def poll_spark_metrics():
    """Obtener métricas reales de Spark Master API"""
    while True:
        try:
            masters_info = []
            active_master_data = None
            
            # Consultar ambos masters
            for master_host in ['spark-master-1', 'spark-master-2']:
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
                            'memory_used': data.get('memoryused', 0)
                        }
                        masters_info.append(master_info)
                        
                        if status == 'ALIVE':
                            active_master_data = data
                            
                except Exception as e:
                    masters_info.append({
                        'name': master_host,
                        'status': 'UNREACHABLE',
                        'error': str(e)
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
                    worker_info = {
                        'id': worker.get('id', ''),
                        'host': worker.get('host', ''),
                        'port': worker.get('port', 0),
                        'state': worker.get('state', ''),
                        'cores': worker.get('cores', 0),
                        'cores_used': worker.get('coresused', 0),
                        'memory': worker.get('memory', 0),
                        'memory_used': worker.get('memoryused', 0),
                        'cpu_percent': (worker.get('coresused', 0) / worker.get('cores', 1)) * 100 if worker.get('cores', 0) > 0 else 0,
                        'memory_percent': (worker.get('memoryused', 0) / worker.get('memory', 1)) * 100 if worker.get('memory', 0) > 0 else 0
                    }
                    workers_info.append(worker_info)
                    
                    total_cores += worker.get('cores', 0)
                    used_cores += worker.get('coresused', 0)
                    total_memory += worker.get('memory', 0)
                    used_memory += worker.get('memoryused', 0)
                
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
                cluster_metrics['spark']['active_master'] = next((m for m in masters_info if m.get('status') == 'ALIVE'), None)
                cluster_metrics['spark']['status'] = 'ALIVE' if active_master_data else 'DOWN'
                cluster_metrics['spark']['total_cores'] = total_cores
                cluster_metrics['spark']['used_cores'] = used_cores
                cluster_metrics['spark']['total_memory'] = total_memory
                cluster_metrics['spark']['used_memory'] = used_memory
                cluster_metrics['spark']['active_apps'] = active_apps
                
                # Agregar al historial
                timestamp = datetime.now().isoformat()
                cpu_percent = (used_cores / total_cores * 100) if total_cores > 0 else 0
                ram_percent = (used_memory / total_memory * 100) if total_memory > 0 else 0
                
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
