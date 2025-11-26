from flask import Flask, render_template_string, jsonify, request
from datetime import datetime
from threading import Lock
from collections import defaultdict

app = Flask(__name__)

# Almacenamiento en memoria para métricas avanzadas
metrics_data = {
    'fathers': {
        'total_records': 0,
        'unique_families': 0,
        'unique_persons': 0,
        'gender_distribution': {},
        'top_chromosomes': {},
        'genotype_distribution': {},
        'snp_stats': {'avg': 0, 'max': 0, 'min': 0}
    },
    'mothers': {
        'total_records': 0,
        'unique_families': 0,
        'unique_persons': 0,
        'gender_distribution': {},
        'top_chromosomes': {},
        'genotype_distribution': {},
        'snp_stats': {'avg': 0, 'max': 0, 'min': 0}
    },
    'children': {
        'total_records': 0,
        'unique_families': 0,
        'unique_persons': 0,
        'gender_distribution': {},
        'top_chromosomes': {},
        'genotype_distribution': {},
        'snp_stats': {'avg': 0, 'max': 0, 'min': 0}
    },
    'last_update': datetime.now(),
    'batches_processed': 0,
    'processing_rate': []
}
metrics_lock = Lock()

def get_stats():
    """Obtener estadísticas desde memoria"""
    with metrics_lock:
        return {
            'fathers': metrics_data['fathers'],
            'mothers': metrics_data['mothers'],
            'children': metrics_data['children'],
            'last_update': metrics_data['last_update'],
            'batches_processed': metrics_data['batches_processed'],
            'processing_rate': metrics_data['processing_rate'][-10:] if metrics_data['processing_rate'] else []
        }

# Template HTML
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Genomic Data Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { 
            color: white; 
            text-align: center; 
            margin-bottom: 30px; 
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .stats-grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 20px; 
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s ease;
        }
        .stat-card:hover { transform: translateY(-5px); }
        .stat-label {
            font-size: 0.9em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        .stat-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #333;
        }
        .fathers { border-top: 4px solid #3498db; }
        .mothers { border-top: 4px solid #e74c3c; }
        .children { border-top: 4px solid #2ecc71; }
        .total { border-top: 4px solid #f39c12; }
        .chart-container {
            background: white;
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 20px;
        }
        .bar-chart { margin-top: 20px; }
        .bar {
            display: flex;
            align-items: center;
            margin: 15px 0;
        }
        .bar-label {
            width: 100px;
            font-weight: bold;
            color: #333;
        }
        .bar-fill {
            height: 30px;
            border-radius: 5px;
            display: flex;
            align-items: center;
            padding: 0 10px;
            color: white;
            font-weight: bold;
            transition: width 1s ease;
        }
        .bar-fill.fathers { background: #3498db; }
        .bar-fill.mothers { background: #e74c3c; }
        .bar-fill.children { background: #2ecc71; }
        .footer {
            text-align: center;
            color: white;
            margin-top: 30px;
            font-size: 0.9em;
        }
        .refresh-btn {
            background: white;
            color: #667eea;
            border: none;
            padding: 15px 30px;
            border-radius: 25px;
            font-size: 1em;
            font-weight: bold;
            cursor: pointer;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            transition: all 0.3s ease;
            display: block;
            margin: 20px auto;
        }
        .refresh-btn:hover {
            transform: scale(1.05);
            box-shadow: 0 7px 20px rgba(0,0,0,0.3);
        }
        .auto-refresh {
            text-align: center;
            color: white;
            margin: 10px 0;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🧬 Genomic Data Dashboard</h1>
        
        <div class="auto-refresh">Auto-refresh: <span id="countdown">5</span>s</div>
        <button class="refresh-btn" onclick="loadData()">🔄 Actualizar Ahora</button>
        
        <div class="stats-grid">
            <div class="stat-card fathers">
                <div class="stat-label">👨 Fathers</div>
                <div class="stat-value" id="fathers-count">-</div>
            </div>
            <div class="stat-card mothers">
                <div class="stat-label">👩 Mothers</div>
                <div class="stat-value" id="mothers-count">-</div>
            </div>
            <div class="stat-card children">
                <div class="stat-label">👶 Children</div>
                <div class="stat-value" id="children-count">-</div>
            </div>
            <div class="stat-card total">
                <div class="stat-label">📊 Total</div>
                <div class="stat-value" id="total-count">-</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h2>📈 Distribución de Registros</h2>
            <div class="bar-chart" id="bar-chart"></div>
        </div>
        
        <div class="footer">
            Última actualización: <span id="last-update">-</span>
        </div>
    </div>
    
    <script>
        let countdown = 5;
        
        function formatNumber(num) {
            return num.toLocaleString('es-ES');
        }
        
        function updateCountdown() {
            countdown--;
            document.getElementById('countdown').textContent = countdown;
            if (countdown <= 0) {
                loadData();
                countdown = 5;
            }
        }
        
        function loadData() {
            fetch('/api/stats')
                .then(response => response.json())
                .then(data => {
                    // Actualizar números
                    document.getElementById('fathers-count').textContent = formatNumber(data.fathers);
                    document.getElementById('mothers-count').textContent = formatNumber(data.mothers);
                    document.getElementById('children-count').textContent = formatNumber(data.children);
                    document.getElementById('total-count').textContent = formatNumber(data.total);
                    
                    // Actualizar gráfico de barras
                    const maxValue = Math.max(data.fathers, data.mothers, data.children);
                    const barChart = document.getElementById('bar-chart');
                    barChart.innerHTML = `
                        <div class="bar">
                            <div class="bar-label">Fathers</div>
                            <div class="bar-fill fathers" style="width: ${(data.fathers/maxValue)*100}%">
                                ${formatNumber(data.fathers)}
                            </div>
                        </div>
                        <div class="bar">
                            <div class="bar-label">Mothers</div>
                            <div class="bar-fill mothers" style="width: ${(data.mothers/maxValue)*100}%">
                                ${formatNumber(data.mothers)}
                            </div>
                        </div>
                        <div class="bar">
                            <div class="bar-label">Children</div>
                            <div class="bar-fill children" style="width: ${(data.children/maxValue)*100}%">
                                ${formatNumber(data.children)}
                            </div>
                        </div>
                    `;
                    
                    // Actualizar timestamp
                    const now = new Date();
                    document.getElementById('last-update').textContent = now.toLocaleString('es-ES');
                    
                    countdown = 5;
                })
                .catch(error => {
                    console.error('Error:', error);
                });
        }
        
        // Cargar datos al inicio
        loadData();
        
        // Auto-refresh cada segundo para el countdown
        setInterval(updateCountdown, 1000);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Página principal"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def api_stats():
    """API para obtener estadísticas completas"""
    stats = get_stats()
    
    return jsonify({
        'fathers': stats['fathers']['total_records'],
        'mothers': stats['mothers']['total_records'],
        'children': stats['children']['total_records'],
        'total': stats['fathers']['total_records'] + stats['mothers']['total_records'] + stats['children']['total_records'],
        'detailed': {
            'fathers': stats['fathers'],
            'mothers': stats['mothers'],
            'children': stats['children']
        },
        'batches_processed': stats['batches_processed'],
        'last_update': stats['last_update'].isoformat() if isinstance(stats['last_update'], datetime) else str(stats['last_update'])
    })

@app.route('/api/metrics', methods=['POST'])
def receive_metrics():
    """Recibir métricas avanzadas desde Spark"""
    try:
        data = request.get_json()
        member_type = data.get('member_type')
        
        with metrics_lock:
            if member_type in metrics_data:
                # Actualizar conteos acumulativos
                metrics_data[member_type]['total_records'] += data.get('total_records', 0)
                
                # Actualizar familias y personas únicas (usar máximo para evitar duplicados)
                metrics_data[member_type]['unique_families'] = max(
                    metrics_data[member_type]['unique_families'],
                    data.get('unique_families', 0)
                )
                metrics_data[member_type]['unique_persons'] = max(
                    metrics_data[member_type]['unique_persons'],
                    data.get('unique_persons', 0)
                )
                
                # Actualizar distribución de género
                gender_dist = data.get('gender_distribution', {})
                for gender, count in gender_dist.items():
                    if gender:
                        metrics_data[member_type]['gender_distribution'][gender] = \
                            metrics_data[member_type]['gender_distribution'].get(gender, 0) + count
                
                # Actualizar top cromosomas
                chrom_dist = data.get('top_chromosomes', {})
                for chrom, count in chrom_dist.items():
                    if chrom:
                        metrics_data[member_type]['top_chromosomes'][chrom] = \
                            metrics_data[member_type]['top_chromosomes'].get(chrom, 0) + count
                
                # Actualizar distribución de genotipos
                geno_dist = data.get('genotype_distribution', {})
                for geno, count in geno_dist.items():
                    if geno:
                        metrics_data[member_type]['genotype_distribution'][geno] = \
                            metrics_data[member_type]['genotype_distribution'].get(geno, 0) + count
                
                # Actualizar estadísticas de SNPs (promedio ponderado)
                snp_stats = data.get('snp_stats', {})
                if snp_stats:
                    metrics_data[member_type]['snp_stats']['avg'] = snp_stats.get('avg', 0)
                    metrics_data[member_type]['snp_stats']['max'] = max(
                        metrics_data[member_type]['snp_stats']['max'],
                        snp_stats.get('max', 0)
                    )
                    if metrics_data[member_type]['snp_stats']['min'] == 0:
                        metrics_data[member_type]['snp_stats']['min'] = snp_stats.get('min', 0)
                    else:
                        metrics_data[member_type]['snp_stats']['min'] = min(
                            metrics_data[member_type]['snp_stats']['min'],
                            snp_stats.get('min', 0)
                        )
                
                # Actualizar timestamp
                metrics_data['last_update'] = datetime.now()
                metrics_data['batches_processed'] += 1
        
        return jsonify({'status': 'success', 'message': f'Métricas de {member_type} actualizadas'}), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
