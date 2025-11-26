from flask import Flask, render_template, jsonify, request
from datetime import datetime
from threading import Lock
from collections import defaultdict, deque

app = Flask(__name__)

# Almacenamiento en memoria mejorado
metrics_data = {
    'fathers': {
        'total_records': 0,
        'unique_families': set(),
        'unique_persons': set(),
        'gender_distribution': defaultdict(int),
        'top_chromosomes': defaultdict(int),
        'genotype_distribution': defaultdict(int),
        'snp_stats': {'avg': 0, 'max': 0, 'min': 0, 'stddev': 0},
        'snp_ranges': defaultdict(int),
        'chromosome_types': {'sex_chromosomes': 0, 'autosomal': 0},
        'zygosity': {'heterozygous': 0, 'homozygous': 0, 'ratio': 0},
        'position_stats': {'avg': 0, 'max': 0, 'min': 0},
        'family_size_distribution': defaultdict(int),
        'families': {}
    },
    'mothers': {
        'total_records': 0,
        'unique_families': set(),
        'unique_persons': set(),
        'gender_distribution': defaultdict(int),
        'top_chromosomes': defaultdict(int),
        'genotype_distribution': defaultdict(int),
        'snp_stats': {'avg': 0, 'max': 0, 'min': 0, 'stddev': 0},
        'snp_ranges': defaultdict(int),
        'chromosome_types': {'sex_chromosomes': 0, 'autosomal': 0},
        'zygosity': {'heterozygous': 0, 'homozygous': 0, 'ratio': 0},
        'position_stats': {'avg': 0, 'max': 0, 'min': 0},
        'family_size_distribution': defaultdict(int),
        'families': {}
    },
    'children': {
        'total_records': 0,
        'unique_families': set(),
        'unique_persons': set(),
        'gender_distribution': defaultdict(int),
        'top_chromosomes': defaultdict(int),
        'genotype_distribution': defaultdict(int),
        'snp_stats': {'avg': 0, 'max': 0, 'min': 0, 'stddev': 0},
        'snp_ranges': defaultdict(int),
        'chromosome_types': {'sex_chromosomes': 0, 'autosomal': 0},
        'zygosity': {'heterozygous': 0, 'homozygous': 0, 'ratio': 0},
        'position_stats': {'avg': 0, 'max': 0, 'min': 0},
        'family_size_distribution': defaultdict(int),
        'families': {}
    },
    'last_update': datetime.now(),
    'batches_processed': 0,
    'processing_history': deque(maxlen=200)
}

metrics_lock = Lock()

@app.route('/')
def index():
    """Página principal con dashboard avanzado"""
    return render_template('index.html')

@app.route('/api/stats')
def api_stats():
    """API para estadísticas generales"""
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
            'unique_families': {
                'fathers': len(metrics_data['fathers']['unique_families']),
                'mothers': len(metrics_data['mothers']['unique_families']),
                'children': len(metrics_data['children']['unique_families']),
                'total': len(
                    metrics_data['fathers']['unique_families'] |
                    metrics_data['mothers']['unique_families'] |
                    metrics_data['children']['unique_families']
                )
            },
            'batches_processed': metrics_data['batches_processed'],
            'last_update': metrics_data['last_update'].isoformat()
        })

@app.route('/api/distributions')
def api_distributions():
    """API para distribuciones (género, cromosomas, genotipos)"""
    with metrics_lock:
        return jsonify({
            'gender': {
                'fathers': dict(metrics_data['fathers']['gender_distribution']),
                'mothers': dict(metrics_data['mothers']['gender_distribution']),
                'children': dict(metrics_data['children']['gender_distribution'])
            },
            'chromosomes': {
                'fathers': dict(sorted(metrics_data['fathers']['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10]),
                'mothers': dict(sorted(metrics_data['mothers']['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10]),
                'children': dict(sorted(metrics_data['children']['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10])
            },
            'genotypes': {
                'fathers': dict(sorted(metrics_data['fathers']['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10]),
                'mothers': dict(sorted(metrics_data['mothers']['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10]),
                'children': dict(sorted(metrics_data['children']['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10])
            }
        })

@app.route('/api/snp_stats')
def api_snp_stats():
    """API para estadísticas de SNPs"""
    with metrics_lock:
        return jsonify({
            'fathers': metrics_data['fathers']['snp_stats'],
            'mothers': metrics_data['mothers']['snp_stats'],
            'children': metrics_data['children']['snp_stats']
        })

@app.route('/api/advanced_metrics')
def api_advanced_metrics():
    """API para métricas avanzadas"""
    with metrics_lock:
        return jsonify({
            'snp_ranges': {
                'fathers': dict(metrics_data['fathers']['snp_ranges']),
                'mothers': dict(metrics_data['mothers']['snp_ranges']),
                'children': dict(metrics_data['children']['snp_ranges'])
            },
            'chromosome_types': {
                'fathers': metrics_data['fathers']['chromosome_types'],
                'mothers': dict(metrics_data['mothers']['chromosome_types']),
                'children': metrics_data['children']['chromosome_types']
            },
            'zygosity': {
                'fathers': metrics_data['fathers']['zygosity'],
                'mothers': metrics_data['mothers']['zygosity'],
                'children': metrics_data['children']['zygosity']
            },
            'position_stats': {
                'fathers': metrics_data['fathers']['position_stats'],
                'mothers': metrics_data['mothers']['position_stats'],
                'children': metrics_data['children']['position_stats']
            },
            'family_size_distribution': {
                'fathers': dict(metrics_data['fathers']['family_size_distribution']),
                'mothers': dict(metrics_data['mothers']['family_size_distribution']),
                'children': dict(metrics_data['children']['family_size_distribution'])
            }
        })

@app.route('/api/families')
def api_families():
    """API para datos por familia"""
    member_type = request.args.get('type', 'fathers')
    
    with metrics_lock:
        families = metrics_data.get(member_type, {}).get('families', {})
        return jsonify({
            'families': [
                {'family_id': fid, **fdata}
                for fid, fdata in list(families.items())[:50]  # Top 50 familias
            ],
            'total_families': len(families)
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
                # Actualizar conteos
                metrics_data[member_type]['total_records'] += data.get('total_records', 0)
                
                # Actualizar familias y personas únicas (usar sets)
                metrics_data[member_type]['unique_families'].add(str(data.get('unique_families', '')))
                
                # Actualizar distribuciones
                for gender, count in data.get('gender_distribution', {}).items():
                    metrics_data[member_type]['gender_distribution'][gender] += count
                
                for chrom, count in data.get('top_chromosomes', {}).items():
                    metrics_data[member_type]['top_chromosomes'][chrom] += count
                
                for geno, count in data.get('genotype_distribution', {}).items():
                    metrics_data[member_type]['genotype_distribution'][geno] += count
                
                # Actualizar SNP stats
                snp_stats = data.get('snp_stats', {})
                metrics_data[member_type]['snp_stats'] = snp_stats
                
                # Actualizar análisis avanzados
                for range_key, count in data.get('snp_ranges', {}).items():
                    metrics_data[member_type]['snp_ranges'][range_key] += count
                
                chrom_types = data.get('chromosome_types', {})
                metrics_data[member_type]['chromosome_types']['sex_chromosomes'] += chrom_types.get('sex_chromosomes', 0)
                metrics_data[member_type]['chromosome_types']['autosomal'] += chrom_types.get('autosomal', 0)
                
                zygosity = data.get('zygosity', {})
                metrics_data[member_type]['zygosity']['heterozygous'] += zygosity.get('heterozygous', 0)
                metrics_data[member_type]['zygosity']['homozygous'] += zygosity.get('homozygous', 0)
                metrics_data[member_type]['zygosity']['ratio'] = zygosity.get('ratio', 0)
                
                metrics_data[member_type]['position_stats'] = data.get('position_stats', {})
                
                for size_key, count in data.get('family_size_distribution', {}).items():
                    metrics_data[member_type]['family_size_distribution'][size_key] += count
                
                # Actualizar familias
                for family_data in data.get('families', []):
                    fid = family_data['family_id']
                    metrics_data[member_type]['families'][fid] = family_data
                
                # Actualizar timestamp y batches
                metrics_data['last_update'] = datetime.now()
                metrics_data['batches_processed'] += 1
                
                # Agregar a historial
                metrics_data['processing_history'].append({
                    'timestamp': datetime.now().isoformat(),
                    'member_type': member_type,
                    'batch_id': data.get('batch_id'),
                    'records': data.get('total_records', 0)
                })
        
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
