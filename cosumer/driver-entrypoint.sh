#!/bin/bash
set -e

ZOOKEEPER_URL=${ZOOKEEPER_URL:-"zookeeper:2181"}
ZK_ELECTION_PATH="/spark/leader-election"

echo "🚀 Starting Spark Driver with Dynamic Master Discovery"

# Crear script Python para leer líder de ZooKeeper
cat > /tmp/zk_get_leader.py << 'PYTHON_SCRIPT'
#!/usr/bin/env python3
import sys
from kazoo.client import KazooClient
from kazoo.recipe.election import Election

def main():
    zk_url = sys.argv[1]
    election_path = sys.argv[2]
    
    zk = KazooClient(hosts=zk_url, timeout=5)
    try:
        zk.start(timeout=5)
        election = Election(zk, election_path, "observer")
        contenders = election.contenders()
        
        if contenders:
            # El ID del worker es el hostname
            leader = contenders[0]
            if isinstance(leader, bytes):
                leader = leader.decode('utf-8')
            print(leader)
    except Exception:
        pass
    finally:
        zk.stop()
        zk.close()

if __name__ == "__main__":
    main()
PYTHON_SCRIPT

# Función para descubrir masters disponibles
discover_masters() {
    local masters=()
    
    # 1. Intentar descubrir masters tradicionales (spark-master-X)
    # Usamos un límite configurable (default 15) para escanear nombres DNS
    local scan_limit=${SPARK_MASTER_SCAN_LIMIT:-15}
    for ((i=1; i<=scan_limit; i++)); do
        local master_host="spark-master-$i"
        if getent hosts "$master_host" >/dev/null 2>&1; then
            # Verificar si el master está realmente activo
            if curl -s -f "http://$master_host:8080" >/dev/null 2>&1; then
                masters+=("$master_host:7077")
                echo "✓ Discovered active master: $master_host" >&2
            fi
        else
            if [ ${#masters[@]} -ge 2 ]; then
                break
            fi
        fi
    done
    
    # 2. Si no hay masters tradicionales, consultar ZooKeeper
    if [ ${#masters[@]} -eq 0 ]; then
        echo "⚠️  No traditional masters found, checking ZooKeeper..." >&2
        local leader=$(python3 /tmp/zk_get_leader.py "$ZOOKEEPER_URL" "$ZK_ELECTION_PATH")
        
        if [ -n "$leader" ]; then
            masters+=("$leader:7077")
            echo "✓ Found emergency master via ZooKeeper: $leader" >&2
        fi
    fi
    
    # 3. Fallback: buscar workers actuando como masters (si ZK falla)
    if [ ${#masters[@]} -eq 0 ]; then
        echo "⚠️  Checking workers directly..." >&2
        local worker_scan_limit=${SPARK_WORKER_SCAN_LIMIT:-15}
        for ((i=1; i<=worker_scan_limit; i++)); do
            local worker_host="spark-worker-$i"
            local worker_port=$((8080 + i))
            
            if getent hosts "$worker_host" >/dev/null 2>&1; then
                # Si el worker se promovió a master, su UI dirá "Spark Master"
                if curl -s -f "http://$worker_host:$worker_port" 2>/dev/null | grep -q "Spark Master"; then
                    masters+=("$worker_host:7077")
                    echo "✓ Found worker acting as master: $worker_host" >&2
                    break
                fi
            fi
        done
    fi
    
    # 4. Default
    if [ ${#masters[@]} -eq 0 ]; then
        echo "⚠️  No masters detected, using defaults" >&2
        masters=("spark-master-1:7077" "spark-master-2:7077")
    fi
    
    echo "${masters[@]}"
}

# Loop principal para reiniciar el driver si cambia el master
while true; do
    echo "⏳ Waiting for masters to be ready..."
    sleep 5

    masters=($(discover_masters))
    master_url=$(IFS=, ; echo "${masters[*]}")
    
    # Calcular cores dinámicamente basado en workers disponibles
    total_cores=0
    worker_count=0
    cores_per_worker=2  # Default, puede ser configurado con env var
    
    echo "🔍 Detecting available workers..."
    for ((i=1; i<=15; i++)); do
        worker_host="spark-worker-$i"
        if getent hosts "$worker_host" >/dev/null 2>&1; then
            worker_count=$((worker_count + 1))
            echo "  ✓ Found worker: $worker_host"
        fi
    done
    
    if [ $worker_count -eq 0 ]; then
        echo "  ⚠️  No workers detected, using default (3 workers)"
        worker_count=3
    fi
    
    total_cores=$((worker_count * cores_per_worker))
    echo "  📊 Total workers: $worker_count"
    echo "  📊 Total cores available: $total_cores"

    echo ""
    echo "🎯 Configuration:"
    echo "   Master URL: spark://$master_url"
    echo "   Driver Host: spark-driver"
    echo "   Kafka Broker: ${KAFKA_BROKER_URL}"
    echo "   Executors: $worker_count"
    echo "   Total Cores: $total_cores"
    echo ""

    # Iniciar Spark Submit en background
    echo "🚀 Starting Spark application..."
    /opt/spark/bin/spark-submit \
        --master "spark://$master_url" \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 2 \
        --total-executor-cores $total_cores \
        --conf spark.driver.host=spark-driver \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.driver.port=7078 \
        --conf spark.executor.instances=$worker_count \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        /app/spark_consumer.py &
    
    SPARK_PID=$!
    
    # Loop de monitoreo
    while kill -0 $SPARK_PID 2>/dev/null; do
        sleep 10
        
        # Verificar si hay un nuevo líder en ZooKeeper
        current_leader=$(python3 /tmp/zk_get_leader.py "$ZOOKEEPER_URL" "$ZK_ELECTION_PATH")
        
        if [ -n "$current_leader" ]; then
             # Si el líder actual NO está en la URL de configuración
             if [[ "$master_url" != *"$current_leader"* ]]; then
                 
                 should_restart=true
                 
                 # Si estamos usando masters tradicionales, verificar si siguen vivos antes de cambiar
                 if [[ "$master_url" == *"spark-master"* ]]; then
                     masters_alive=0
                     scan_limit=${SPARK_MASTER_SCAN_LIMIT:-15}
                     for ((i=1; i<=scan_limit; i++)); do
                        if curl -s -f "http://spark-master-$i:8080" >/dev/null 2>&1; then
                            masters_alive=$((masters_alive + 1))
                        fi
                     done
                     
                     if [ "$masters_alive" -gt 0 ]; then
                         # echo "⚠️  Ignoring ZK leader $current_leader because traditional masters are still active."
                         should_restart=false
                     fi
                 fi
                 
                 if [ "$should_restart" = true ]; then
                     echo "🔄 Detected new master ($current_leader) and current masters seem down. Restarting driver..."
                     kill $SPARK_PID
                     wait $SPARK_PID 2>/dev/null || true
                     break
                 fi
             fi
        fi
        
        # Si estamos usando masters tradicionales pero no responden, y hay un líder en ZK, reiniciar
        # (Esto ya está cubierto arriba: si master_url tiene master-1, y ZK dice worker-1, reiniciamos)
    done
    
    echo "⚠️ Spark application exited or was killed. Restarting in 5 seconds..."
    sleep 5
done
