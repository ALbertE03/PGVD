#!/bin/bash
set -e

# Configuración
ZOOKEEPER_URL=${ZOOKEEPER_URL:-"zookeeper:2181"}
WORKER_ID=$(hostname)
WORKER_PORT=${SPARK_WORKER_WEBUI_PORT:-8081}
MASTER_PORT=7077
ZK_ELECTION_PATH="/spark/leader-election"
STATUS_FILE="/tmp/election_status"

echo "🚀 Starting Intelligent Spark Worker with ZooKeeper-Based Leader Election"
echo "   Worker ID: $WORKER_ID"
echo "   ZooKeeper: $ZOOKEEPER_URL"
echo "   Worker Port: $WORKER_PORT"
echo "   Election Path: $ZK_ELECTION_PATH"

# Variable para rastrear si estamos en modo master
RUNNING_AS_MASTER=false
IS_LEADER=false
MASTER_PID=""
WORKER_PID=""
CURRENT_MASTER_URL=""

# Instalar zkCli si no está disponible 
if ! command -v python3 &> /dev/null; then
    echo "⚠️  Python3 not found, installing..."
    apt-get update && apt-get install -y python3 python3-pip
fi

# Verificar/instalar kazoo 
python3 -c "import kazoo" 2>/dev/null || pip3 install kazoo


cat > /tmp/zk_monitor.py << 'PYTHON_SCRIPT'
import sys
import time
import threading
import os
from kazoo.client import KazooClient
from kazoo.recipe.election import Election

def main():
    worker_id = sys.argv[1]
    zk_url = sys.argv[2]
    election_path = sys.argv[3]
    status_file = sys.argv[4]
    
    # Reintentar conexión con backoff exponencial
    max_retries = 10
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Attempting to connect to ZooKeeper (attempt {attempt + 1}/{max_retries})...", file=sys.stderr)
            zk = KazooClient(hosts=zk_url, timeout=30, connection_retry={'max_tries': 5, 'delay': 2, 'backoff': 2})
            zk.start(timeout=30)
            print(f"✅ Connected to ZooKeeper successfully!", file=sys.stderr)
            break
        except Exception as e:
            print(f"⚠️  Connection attempt {attempt + 1} failed: {e}", file=sys.stderr)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)  # Backoff exponencial, max 30s
            else:
                print(f"❌ Failed to connect to ZooKeeper after {max_retries} attempts", file=sys.stderr)
                return
    
    election = Election(zk, election_path, worker_id)
    
    def update_status():
        while True:
            try:
                contenders = election.contenders()
                if contenders:
                    leader = contenders[0]
                    if isinstance(leader, bytes):
                        leader = leader.decode('utf-8')
                    
                    is_leader = (leader == worker_id)
                    status = "IS_LEADER" if is_leader else "FOLLOWER"
                    
                    # Write atomically
                    with open(status_file + ".tmp", "w") as f:
                        f.write(f"LEADER:{leader}\n")
                        f.write(f"STATUS:{status}\n")
                    os.rename(status_file + ".tmp", status_file)
                else:
                    with open(status_file + ".tmp", "w") as f:
                        f.write("LEADER:NONE\n")
                        f.write("STATUS:NO_LEADER\n")
                    os.rename(status_file + ".tmp", status_file)
            except Exception as e:
                pass
            time.sleep(2)

    t = threading.Thread(target=update_status)
    t.daemon = True
    t.start()

    # Block and contend
    def leader_func():
        print(f"🎯 {worker_id} became the leader!", file=sys.stderr)
        while True:
            time.sleep(1)
            
    election.run(leader_func)

if __name__ == "__main__":
    main()
PYTHON_SCRIPT

# Iniciar monitor en background
echo "📡 Starting ZooKeeper Monitor..."
python3 /tmp/zk_monitor.py "$WORKER_ID" "$ZOOKEEPER_URL" "$ZK_ELECTION_PATH" "$STATUS_FILE" &
MONITOR_PID=$!

# Función para descubrir dinámicamente todos los masters disponibles
discover_masters() {
    local masters=()
    
    # Intentar descubrir masters del 1 al 10
    for i in {1..10}; do
        local master_host="spark-master-$i"
        if getent hosts "$master_host" >/dev/null 2>&1; then
            masters+=("$master_host:8080")
        else
            if [ ${#masters[@]} -ge 2 ]; then
                break
            fi
        fi
    done
    
    if [ ${#masters[@]} -eq 0 ]; then
        masters=("spark-master-1:8080" "spark-master-2:8080")
    fi
    
    echo "${masters[@]}"
}

# Función para verificar si hay masters tradicionales disponibles
check_traditional_masters_available() {
    local masters_alive=0
    local masters=($(discover_masters))
    
    for master in "${masters[@]}"; do
        if curl -s -f "http://$master" >/dev/null 2>&1; then
            masters_alive=$((masters_alive + 1))
        fi
    done
    
    echo $masters_alive
}

# Función para verificar estado de elección
check_election_status() {
    if [ -f "$STATUS_FILE" ]; then
        local status=$(grep "^STATUS:" "$STATUS_FILE" | cut -d: -f2)
        if [ "$status" == "IS_LEADER" ]; then
            return 0
        fi
    fi
    return 1
}

# Función para promover este worker a master
promote_to_master() {
    if [ "$RUNNING_AS_MASTER" = true ]; then
        # Verificar si el worker local sigue vivo, si no, reiniciarlo
        if [ -n "$WORKER_PID" ] && ! kill -0 $WORKER_PID 2>/dev/null; then
             echo "🔧 Restarting local worker on Emergency Master..."
             /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
                "spark://$WORKER_ID:$MASTER_PORT" \
                --webui-port 0 &
             WORKER_PID=$!
        fi
        return 0
    fi
    
    echo "🛡️  Promoting $WORKER_ID to Emergency Master"
    
    if [ -n "$WORKER_PID" ] && kill -0 $WORKER_PID 2>/dev/null; then
        kill $WORKER_PID
        wait $WORKER_PID 2>/dev/null || true
    fi
    
    SPARK_NO_DAEMONIZE=true \
    SPARK_MASTER_HOST=$WORKER_ID \
    SPARK_MASTER_PORT=$MASTER_PORT \
    SPARK_MASTER_WEBUI_PORT=$WORKER_PORT \
    SPARK_LOCAL_IP=0.0.0.0 \
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 \
        --port $MASTER_PORT \
        --webui-port $WORKER_PORT &
    
    MASTER_PID=$!
    RUNNING_AS_MASTER=true
    echo "✅ Emergency Master started (PID: $MASTER_PID)"
    
    # Iniciar un Worker local para que el nodo también procese tareas (Self-Worker)
    echo "🔧 Starting local worker on Emergency Master (Self-Worker)..."
    sleep 5
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        "spark://$WORKER_ID:$MASTER_PORT" \
        --webui-port 0 &
    WORKER_PID=$!
}

# Función para iniciar como worker
start_as_worker() {
    local master_url=""
    local master_endpoints=()
    
    local masters=($(discover_masters))
    for master in "${masters[@]}"; do
        local master_host=$(echo "$master" | cut -d: -f1)
        if curl -s -f "http://$master" >/dev/null 2>&1; then
            master_endpoints+=("$master_host:7077")
        fi
    done
    
    if [ ${#master_endpoints[@]} -eq 0 ]; then
        if [ -f "$STATUS_FILE" ]; then
            local leader=$(grep "^LEADER:" "$STATUS_FILE" | cut -d: -f2)
            if [ -n "$leader" ] && [ "$leader" != "NONE" ] && [ "$leader" != "$WORKER_ID" ]; then
                master_endpoints+=("$leader:7077")
                # echo "✅ Found emergency master via ZooKeeper: $leader"
            fi
        fi
    fi
    
    if [ ${#master_endpoints[@]} -eq 0 ]; then
        # echo "❌ No master available to connect to"
        return 1
    fi
    
    local masters_list=$(IFS=, ; echo "${master_endpoints[*]}")
    master_url="spark://$masters_list"
    
    # Si ya estamos conectados al master correcto, no hacer nada
    if [ "$master_url" == "$CURRENT_MASTER_URL" ] && [ -n "$WORKER_PID" ] && kill -0 $WORKER_PID 2>/dev/null; then
        return 0
    fi
    
    # Si el master cambió, reiniciar worker
    if [ -n "$WORKER_PID" ] && kill -0 $WORKER_PID 2>/dev/null; then
        echo "🔄 Switching master from '$CURRENT_MASTER_URL' to '$master_url'"
        kill $WORKER_PID
        wait $WORKER_PID 2>/dev/null || true
    fi
    
    echo "🔗 Connecting to: $master_url"
    
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        "$master_url" \
        --webui-port $WORKER_PORT &
    
    WORKER_PID=$!
    CURRENT_MASTER_URL="$master_url"
}

cleanup() {
    echo "🛑 Shutting down..."
    [ -n "$MASTER_PID" ] && kill $MASTER_PID 2>/dev/null
    [ -n "$WORKER_PID" ] && kill $WORKER_PID 2>/dev/null
    [ -n "$MONITOR_PID" ] && kill $MONITOR_PID 2>/dev/null
    exit 0
}

trap cleanup SIGTERM SIGINT

echo "🚀 Starting initial worker process..."
start_as_worker || echo "⚠️ Could not start worker initially (no master found), entering loop..."

echo "🎯 Entering monitoring loop..."

while true; do
    sleep 5
    
    masters_count=$(check_traditional_masters_available)
    if [ "$masters_count" -gt 0 ]; then
        if [ "$IS_LEADER" = true ]; then
            echo "🔄 Traditional masters recovered, demoting..."
            IS_LEADER=false
            RUNNING_AS_MASTER=false
            [ -n "$MASTER_PID" ] && kill $MASTER_PID 2>/dev/null
            start_as_worker || true
        else
            # Ensure we are connected to the correct master (traditional)
            start_as_worker || true
        fi
        continue
    fi
    
    if check_election_status; then
        if [ "$IS_LEADER" = false ]; then
            echo "🎯 Elected as leader!"
            IS_LEADER=true
            [ -n "$WORKER_PID" ] && kill $WORKER_PID 2>/dev/null
            promote_to_master
        fi
    else
        if [ "$IS_LEADER" = true ]; then
            echo "❌ Lost leadership..."
            IS_LEADER=false
            RUNNING_AS_MASTER=false
            [ -n "$MASTER_PID" ] && kill $MASTER_PID 2>/dev/null
            start_as_worker || true
        else
            # Ensure we are connected to the correct master (emergency)
            start_as_worker || true
        fi
    fi
done
