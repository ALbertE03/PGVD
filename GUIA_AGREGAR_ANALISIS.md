# Guía: Agregar Nuevos Análisis Genéticos

## 📋 Tabla de Contenidos
1. [Arquitectura del Sistema](#arquitectura-del-sistema)
2. [Agregar Análisis desde Datos en Streaming](#agregar-análisis-desde-datos-en-streaming)
3. [Calcular Análisis desde HDFS (Datos Históricos)](#calcular-análisis-desde-hdfs)
4. [Integración Dashboard](#integración-dashboard)

---

## 🏗️ Arquitectura del Sistema

### Flujo de Datos
```
Producer → Kafka → Spark Consumer → Dashboard Flask
                          ↓
                        HDFS (persistencia)
```

### Componentes Clave
- **Producer** (`producer/`): Genera datos genéticos sintéticos y los envía a Kafka
- **Spark Consumer** (`cosumer/spark_consumer.py`): Procesa streams de Kafka y ejecuta análisis
- **Dashboard** (`cosumer/dashboard/`): Visualiza métricas en tiempo real
- **HDFS**: Almacena todos los datos procesados para análisis histórico

---

## ✨ Agregar Análisis desde Datos en Streaming

### Paso 1: Crear Función de Análisis en `spark_consumer.py`

```python
def calculate_mi_nuevo_analisis(df, member_type):
    """
    Calcula un nuevo análisis genético
    
    Args:
        df: DataFrame con columnas [rsid, chromosome, position, genotype, person_id, family_id]
        member_type: 'fathers', 'mothers', o 'children'
    
    Returns:
        dict con resultados del análisis
    """
    try:
        # Ejemplo: Calcular distribución de variantes por tipo
        result = df.groupBy('genotype').count().collect()
        
        data = {
            member_type: {row['genotype']: row['count'] for row in result}
        }
        
        # Enviar al dashboard
        send_to_dashboard({
            'message_type': 'MI_NUEVO_ANALISIS',
            'member_type': member_type,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
        
        print(f"✅ Mi Nuevo Análisis calculado para {member_type}")
        return data
        
    except Exception as e:
        print(f"❌ Error en mi_nuevo_analisis: {str(e)}")
        return {}
```

### Paso 2: Integrar en el Pipeline de Procesamiento

Agregar la función al ThreadPoolExecutor en `process_batch_with_genetics()`:

```python
def process_batch_with_genetics(batch_df, batch_id, member_type):
    """Procesa batch genérico con análisis genéticos paralelos"""
    calculate_and_send_metrics(batch_df, batch_id, member_type)
    
    cached_df = batch_df.cache()
    
    with ThreadPoolExecutor(max_workers=7) as executor:  # Aumentar workers si necesario
        analysis_functions = [
            calculate_chromosome_distribution,
            calculate_position_hotspots,
            calculate_genotype_trends,
            calculate_population_heterozygosity,
            calculate_individual_heterozygosity,
            calculate_mi_nuevo_analisis  # ← AGREGAR AQUÍ
        ]
        
        futures = [executor.submit(func, cached_df, member_type) 
                   for func in analysis_functions]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error en análisis: {str(e)}")
    
    cached_df.unpersist()
```

### Paso 3: Agregar Estructura de Datos en `dashboard_advanced.py`

```python
metrics_data = {
    # ... estructuras existentes ...
    
    'mi_nuevo_analisis': {
        'fathers': {},
        'mothers': {},
        'children': {}
    }
}
```

### Paso 4: Crear Handler en `dashboard_advanced.py`

```python
@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    # ... código existente ...
    
    if message_type == 'MI_NUEVO_ANALISIS':
        with metrics_lock:
            member_type = data.get('member_type')
            analisis_data = data.get('data', {})
            metrics_data['mi_nuevo_analisis'][member_type] = analisis_data
            metrics_data['last_update'] = datetime.now()
        return jsonify({'status': 'success'})
```

### Paso 5: Exponer en API

```python
@app.route('/api/genetic_analysis')
def genetic_analysis():
    with metrics_lock:
        return jsonify({
            # ... datos existentes ...
            'mi_nuevo_analisis': metrics_data['mi_nuevo_analisis']
        })
```

### Paso 6: Visualizar en Frontend (`dashboard.js`)

```javascript
// Crear gráfica
const miNuevoChart = new Chart(ctx, {
    type: 'bar',
    data: {
        labels: [],
        datasets: [{
            label: 'Mi Nuevo Análisis',
            data: [],
            backgroundColor: 'rgba(139, 92, 246, 0.8)'
        }]
    },
    options: commonChartOptions
});

// Actualizar con datos
function updateMiNuevoAnalisis(data) {
    const analisis = data.mi_nuevo_analisis || {};
    const labels = Object.keys(analisis.fathers || {});
    const datasets = [
        {
            label: 'Fathers',
            data: labels.map(k => analisis.fathers[k] || 0),
            backgroundColor: '#3b82f6'
        },
        {
            label: 'Mothers', 
            data: labels.map(k => analisis.mothers[k] || 0),
            backgroundColor: '#ef4444'
        },
        {
            label: 'Children',
            data: labels.map(k => analisis.children[k] || 0),
            backgroundColor: '#10b981'
        }
    ];
    
    updateBarChart(miNuevoChart, labels, datasets);
}

// Llamar en updateDashboard()
async function updateDashboard() {
    const response = await fetch('/api/genetic_analysis');
    const data = await response.json();
    updateMiNuevoAnalisis(data);
    // ... otras actualizaciones ...
}
```

---

## 📊 Calcular Análisis desde HDFS (Datos Históricos)

### Opción 1: Script de Análisis Batch Independiente

Crear `cosumer/batch_analysis.py`:

```python
from pyspark.sql import SparkSession
from datetime import datetime
import requests

def analyze_historical_data():
    """
    Analiza TODOS los datos guardados en HDFS desde el inicio
    """
    spark = SparkSession.builder \
        .appName("Historical Genetic Analysis") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    # Leer TODOS los datos desde HDFS
    hdfs_paths = [
        "hdfs://namenode:9000/data/fathers",
        "hdfs://namenode:9000/data/mothers", 
        "hdfs://namenode:9000/data/children"
    ]
    
    for path, member_type in zip(hdfs_paths, ['fathers', 'mothers', 'children']):
        print(f"\n{'='*50}")
        print(f"Analizando datos históricos de {member_type.upper()}")
        print(f"{'='*50}\n")
        
        try:
            # Leer todos los parquet guardados
            df = spark.read.parquet(path)
            total_records = df.count()
            print(f"📦 Total registros en HDFS: {total_records:,}")
            
            # EJEMPLO: Análisis de variantes raras
            rare_variants = df.groupBy('rsid') \
                .count() \
                .filter('count = 1') \
                .count()
            
            rare_pct = (rare_variants / total_records * 100) if total_records > 0 else 0
            
            print(f"🔬 Variantes raras (únicas): {rare_variants:,} ({rare_pct:.2f}%)")
            
            # EJEMPLO: Distribución cromosómica completa
            chrom_dist = df.groupBy('chromosome').count().collect()
            print(f"\n📊 Distribución Cromosómica Histórica:")
            for row in sorted(chrom_dist, key=lambda x: x['count'], reverse=True)[:5]:
                print(f"   Chr {row['chromosome']}: {row['count']:,} variantes")
            
            # EJEMPLO: Análisis de familia más estudiada
            family_stats = df.groupBy('family_id').count().collect()
            if family_stats:
                top_family = max(family_stats, key=lambda x: x['count'])
                print(f"\n👨‍👩‍👧‍👦 Familia más estudiada: {top_family['family_id']} "
                      f"({top_family['count']:,} variantes)")
            
            # Enviar resultados al dashboard
            send_historical_results({
                'member_type': member_type,
                'total_records': total_records,
                'rare_variants': rare_variants,
                'rare_percentage': rare_pct,
                'chromosome_distribution': {
                    row['chromosome']: row['count'] for row in chrom_dist
                },
                'analysis_timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            print(f"❌ Error procesando {member_type}: {str(e)}")
    
    spark.stop()
    print("\n✅ Análisis histórico completado")

def send_historical_results(data):
    """Envía resultados al dashboard Flask"""
    try:
        response = requests.post(
            'http://dashboard:8080/api/historical_analysis',
            json={
                'message_type': 'HISTORICAL_ANALYSIS',
                'data': data
            },
            timeout=5
        )
        if response.ok:
            print(f"   → Resultados enviados al dashboard")
    except Exception as e:
        print(f"   ⚠️  No se pudo enviar al dashboard: {str(e)}")

if __name__ == '__main__':
    analyze_historical_data()
```

### Ejecutar el Análisis Histórico

```bash
# Desde el contenedor de Spark
docker exec -it spark-master bash

# Instalar requests si no está
pip install requests

# Ejecutar análisis
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /app/batch_analysis.py
```

### Opción 2: Integrar en Spark Consumer con Flag

Modificar `spark_consumer.py` para modo batch inicial:

```python
import os

BATCH_MODE = os.getenv('INITIAL_BATCH', 'false').lower() == 'true'

def run_initial_batch_analysis(spark):
    """
    Ejecuta análisis completo de datos históricos al inicio
    Solo se ejecuta una vez cuando INITIAL_BATCH=true
    """
    if not BATCH_MODE:
        return
    
    print("\n" + "="*70)
    print("🔄 MODO BATCH INICIAL: Analizando datos históricos desde HDFS")
    print("="*70 + "\n")
    
    hdfs_paths = {
        'fathers': 'hdfs://namenode:9000/data/fathers',
        'mothers': 'hdfs://namenode:9000/data/mothers',
        'children': 'hdfs://namenode:9000/data/children'
    }
    
    for member_type, path in hdfs_paths.items():
        try:
            df = spark.read.parquet(path)
            
            # Ejecutar TODOS los análisis sobre datos históricos
            calculate_chromosome_distribution(df, member_type)
            calculate_position_hotspots(df, member_type)
            calculate_genotype_trends(df, member_type)
            calculate_population_heterozygosity(df, member_type)
            calculate_individual_heterozygosity(df, member_type)
            
            print(f"✅ Análisis histórico completado para {member_type}")
            
        except Exception as e:
            print(f"⚠️  No hay datos históricos para {member_type}: {str(e)}")
    
    print("\n✅ Análisis batch inicial completado, iniciando streaming...\n")

# En main():
if __name__ == "__main__":
    spark = create_spark_session()
    
    # Ejecutar análisis histórico al inicio
    run_initial_batch_analysis(spark)
    
    # Luego iniciar streaming normal
    process_kafka_stream(spark)
```

### Activar Modo Batch en docker-compose.yml

```yaml
services:
  spark-driver:
    environment:
      - INITIAL_BATCH=true  # ← Agregar esta línea
```

---

## 🎨 Integración Dashboard

### Agregar Endpoint para Datos Históricos

```python
# dashboard_advanced.py

metrics_data['historical_analysis'] = {}

@app.route('/api/historical_analysis', methods=['POST'])
def receive_historical_analysis():
    """Recibe resultados de análisis histórico"""
    data = request.json
    member_type = data.get('data', {}).get('member_type')
    
    with metrics_lock:
        metrics_data['historical_analysis'][member_type] = data.get('data', {})
        metrics_data['last_update'] = datetime.now()
    
    print(f"📊 Datos históricos recibidos para {member_type}")
    return jsonify({'status': 'success'})

@app.route('/api/historical_analysis', methods=['GET'])
def get_historical_analysis():
    """Devuelve análisis histórico calculado"""
    with metrics_lock:
        return jsonify(metrics_data['historical_analysis'])
```

### Visualizar en Frontend

```javascript
// dashboard.js

async function loadHistoricalData() {
    try {
        const response = await fetch('/api/historical_analysis');
        const data = await response.json();
        
        if (Object.keys(data).length === 0) {
            console.log('No hay datos históricos disponibles');
            return;
        }
        
        // Mostrar estadísticas históricas
        displayHistoricalStats(data);
        
    } catch (error) {
        console.error('Error cargando datos históricos:', error);
    }
}

function displayHistoricalStats(data) {
    const container = document.getElementById('historical-stats');
    
    let html = '<div class="historical-summary">';
    html += '<h3>📊 Análisis Histórico Completo (HDFS)</h3>';
    
    for (const [memberType, stats] of Object.entries(data)) {
        html += `
            <div class="stat-card">
                <h4>${memberType.toUpperCase()}</h4>
                <p>Total Registros: ${stats.total_records?.toLocaleString()}</p>
                <p>Variantes Raras: ${stats.rare_variants?.toLocaleString()} (${stats.rare_percentage?.toFixed(2)}%)</p>
                <p>Análisis: ${new Date(stats.analysis_timestamp).toLocaleString()}</p>
            </div>
        `;
    }
    
    html += '</div>';
    container.innerHTML = html;
}

// Cargar al inicio
document.addEventListener('DOMContentLoaded', () => {
    loadHistoricalData();
    setInterval(updateDashboard, 2000);
});
```

---

## 🚀 Checklist Completo

### Para Nuevo Análisis en Streaming:
- [ ] Crear función `calculate_mi_analisis()` en `spark_consumer.py`
- [ ] Agregar función a `process_batch_with_genetics()`
- [ ] Agregar estructura de datos en `metrics_data` (dashboard)
- [ ] Crear handler POST en `/api/metrics`
- [ ] Exponer datos en `/api/genetic_analysis`
- [ ] Crear gráfica en `dashboard.js`
- [ ] Agregar actualización en `updateDashboard()`

### Para Análisis Histórico (HDFS):
- [ ] Crear `batch_analysis.py` o flag `INITIAL_BATCH`
- [ ] Leer datos con `spark.read.parquet(hdfs_path)`
- [ ] Ejecutar análisis sobre DataFrame completo
- [ ] Crear endpoint `/api/historical_analysis` (POST + GET)
- [ ] Agregar visualización en dashboard
- [ ] Ejecutar con `spark-submit` o al inicio del consumer

---

## 💡 Ejemplos de Análisis Útiles

### 1. Detección de Consanguinidad
```python
def calculate_consanguinity(df, member_type):
    """Detecta homocigosidad elevada que puede indicar consanguinidad"""
    family_stats = df.groupBy('family_id') \
        .agg(
            F.count('*').alias('total_snps'),
            F.sum(F.when(F.col('genotype').rlike(r'^([ACGT])\1$'), 1).otherwise(0)).alias('homozygous')
        ) \
        .withColumn('homozygosity_rate', F.col('homozygous') / F.col('total_snps'))
    
    # Familias con >75% homocigosidad (posible consanguinidad)
    high_consanguinity = family_stats.filter('homozygosity_rate > 0.75').collect()
    
    return {
        'high_risk_families': [row['family_id'] for row in high_consanguinity],
        'homozygosity_rates': {row['family_id']: row['homozygosity_rate'] 
                               for row in family_stats.collect()}
    }
```

### 2. Identificación de Variantes Patogénicas
```python
def identify_pathogenic_variants(df, member_type):
    """Busca patrones asociados a enfermedades conocidas"""
    # Ejemplo: Variantes en BRCA1 (cromosoma 17)
    brca1_variants = df.filter(
        (F.col('chromosome') == '17') & 
        (F.col('position').between(43044295, 43125483))
    ).select('family_id', 'person_id', 'rsid', 'genotype').collect()
    
    return {
        'brca1_carriers': len(brca1_variants),
        'affected_families': list(set(row['family_id'] for row in brca1_variants))
    }
```

### 3. Análisis de Heredabilidad
```python
def calculate_heritability(df_children, df_parents):
    """Calcula qué variantes son heredadas vs de novo"""
    # Join children con padres por family_id
    inherited = df_children.alias('c').join(
        df_parents.alias('p'),
        (F.col('c.family_id') == F.col('p.family_id')) &
        (F.col('c.rsid') == F.col('p.rsid')) &
        (F.col('c.genotype') == F.col('p.genotype'))
    ).count()
    
    total_child_variants = df_children.count()
    de_novo = total_child_variants - inherited
    
    return {
        'inherited_variants': inherited,
        'de_novo_variants': de_novo,
        'heritability_rate': inherited / total_child_variants if total_child_variants > 0 else 0
    }
```

---

## 📝 Notas Importantes

1. **Performance**: Cachear DataFrames grandes con `.cache()` antes de múltiples operaciones
2. **HDFS Paths**: Verificar que las rutas sean correctas: `hdfs://namenode:9000/data/`
3. **Thread Safety**: Usar `metrics_lock` al modificar `metrics_data` en dashboard
4. **Memoria**: Aumentar `spark.executor.memory` si procesas datasets muy grandes
5. **Testing**: Probar análisis con subset pequeño antes de aplicar a todos los datos

---

## 🔧 Troubleshooting

### Error: "Path does not exist: hdfs://..."
```bash
# Verificar contenido de HDFS
docker exec -it namenode bash
hdfs dfs -ls /data/
```

### Error: "OutOfMemoryError"
```python
# Procesar en chunks
df.repartition(100).cache()  # Distribuir en más particiones
```

### Dashboard no recibe datos
```bash
# Verificar conectividad
docker exec -it spark-driver curl http://dashboard:8080/health
```

---

## ✅ Conclusión

Con esta guía puedes:
1. ✅ Agregar nuevos análisis genéticos al pipeline de streaming
2. ✅ Calcular análisis sobre TODOS los datos históricos en HDFS
3. ✅ Integrar resultados en el dashboard en tiempo real
4. ✅ Mantener código limpio y performante

**¡Listo para agregar análisis personalizados!** 🧬🚀
