#!/usr/bin/env python3
"""
Dashboard Genomic Advanced - Análisis avanzado de datos genómicos
Incluye: Análisis MLL, Similitud familiar, Recombinación, Patrones genéticos
         + SNP Distribution + KNN Clustering
"""

from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
from threading import Lock
from collections import defaultdict, deque
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from functools import lru_cache
import statistics
import json

app = Flask(__name__)

# ==================== ALMACENAMIENTO MEJORADO ====================

genomic_data = {
    'fathers': {'records': deque(maxlen=5000), 'stats': {}, 'families': {}},
    'mothers': {'records': deque(maxlen=5000), 'stats': {}, 'families': {}},
    'children': {'records': deque(maxlen=5000), 'stats': {}, 'families': {}},
    'family_groups': defaultdict(lambda: {'father': None, 'mother': None, 'children': []}),
    'processing_timestamps': deque(maxlen=200),
    'total_processed': 0,
    'start_time': datetime.now()
}

data_lock = Lock()

# ==================== ANÁLISIS GENÉTICO AVANZADO ====================

class GenomicAnalyzer:
    """Analizador avanzado de datos genómicos"""
    
    # Cromosomas autosómicos y sexuales
    AUTOSOMAL = list(range(1, 23))
    SEX_CHROMOSOMES = ['X', 'Y', 'MT']
    CHROMOSOME_SIZES = {
        1: 249250621, 2: 242193529, 3: 198022430, 4: 191154276,
        5: 180915260, 6: 171115067, 7: 159138663, 8: 146364022,
        9: 141213431, 10: 135534747, 11: 135006516, 12: 133851895,
        13: 115169878, 14: 107349540, 15: 102531392, 16: 90354753,
        17: 81195210, 18: 78077248, 19: 59128983, 20: 63025520,
        21: 48129895, 22: 51304566, 'X': 155270560, 'Y': 59373566, 'MT': 16569
    }
    
    @staticmethod
    def normalize_snp_data(snp_data):
        """Convierte snp_data a diccionario, sin importar su forma original"""
        if isinstance(snp_data, dict):
            return snp_data
        elif isinstance(snp_data, (list, tuple)) and len(snp_data) >= 3:
            # Si es tupla/lista: [chromosome, position, genotype]
            return {
                'chromosome': str(snp_data[0]) if len(snp_data) > 0 else 'unknown',
                'position': int(snp_data[1]) if len(snp_data) > 1 else 0,
                'genotype': str(snp_data[2]) if len(snp_data) > 2 else ''
            }
        else:
            return {'chromosome': 'unknown', 'position': 0, 'genotype': ''}
    
    @staticmethod
    def calculate_mll_score(snp_records):
        """
        Calcula Multi-Locus Linkage (MLL): mide la vinculación genética entre múltiples loci
        Métrica: Frecuencia de haplotipo (combinación de alelos en loci cercanos)
        """
        if not snp_records:
            return {'score': 0, 'linkage_strength': 'LOW', 'haplotype_blocks': 0}
        
        # Agrupar por cromosoma
        chromosome_snps = defaultdict(list)
        for record in snp_records:
            if 'snp_data' in record:
                snp = GenomicAnalyzer.normalize_snp_data(record['snp_data'])
                chrom = snp.get('chromosome', 'unknown')
                pos = snp.get('position', 0)
                geno = snp.get('genotype', '')
                chromosome_snps[chrom].append((pos, geno))
        
        total_linkage = 0
        haplotype_blocks = 0
        
        for chrom, snps in chromosome_snps.items():
            if len(snps) > 1:
                snps_sorted = sorted(snps, key=lambda x: x[0])
                
                # Detectar bloques haplotípicos (SNPs cercanos con genotipo similar)
                block_size = 0
                prev_pos = 0
                
                for pos, geno in snps_sorted:
                    if prev_pos == 0 or (pos - prev_pos) < 100000:  # 100kb
                        block_size += 1
                    else:
                        if block_size > 1:
                            haplotype_blocks += 1
                        block_size = 1
                    prev_pos = pos
                
                if block_size > 1:
                    haplotype_blocks += 1
                
                linkage_score = len(snps) / max(1, len(snps_sorted))
                total_linkage += linkage_score
        
        mll_score = min(100, (total_linkage / max(1, len(chromosome_snps))) * 100)
        
        linkage_levels = ['LOW', 'MODERATE', 'HIGH', 'VERY_HIGH']
        strength_idx = min(3, int(mll_score / 25))
        
        return {
            'score': round(mll_score, 2),
            'linkage_strength': linkage_levels[strength_idx],
            'haplotype_blocks': haplotype_blocks,
            'total_snps': len(snp_records)
        }
    
    @staticmethod
    def calculate_genetic_similarity(parent_snps, child_snps):
        """
        Calcula similitud genética entre padre/madre e hijo
        Basado en concordancia de genotipos
        """
        if not parent_snps or not child_snps:
            return {'similarity': 0, 'inherited_snps': 0, 'shared_chromosomes': 0}
        
        shared_count = 0
        total_comparable = 0
        inherited_count = 0
        shared_chroms = set()
        
        for p_rec in parent_snps:
            if 'snp_data' not in p_rec:
                continue
            p_snp = GenomicAnalyzer.normalize_snp_data(p_rec['snp_data'])
            p_chrom = p_snp.get('chromosome')
            p_pos = p_snp.get('position')
            p_geno = p_snp.get('genotype', '')
            
            for c_rec in child_snps:
                if 'snp_data' not in c_rec:
                    continue
                c_snp = GenomicAnalyzer.normalize_snp_data(c_rec['snp_data'])
                c_chrom = c_snp.get('chromosome')
                c_pos = c_snp.get('position')
                c_geno = c_snp.get('genotype', '')
                
                if p_chrom == c_chrom and abs(p_pos - c_pos) < 1000:  # Mismo loci
                    total_comparable += 1
                    if p_geno and c_geno and len(p_geno) > 0 and len(c_geno) > 0:
                        if p_geno[0] == c_geno[0] or p_geno[0] == c_geno[1]:
                            shared_count += 1
                            inherited_count += 1
                            shared_chroms.add(p_chrom)
        
        similarity = (shared_count / max(1, total_comparable)) * 100 if total_comparable > 0 else 0
        
        return {
            'similarity': round(similarity, 2),
            'inherited_snps': inherited_count,
            'shared_chromosomes': len(shared_chroms),
            'total_comparable': total_comparable
        }
    
    @staticmethod
    def calculate_recombination_rate(snp_records):
        """
        Estima la tasa de recombinación cromosómica
        Métrica: Cambios de alelos entre SNPs en el mismo cromosoma
        """
        if not snp_records or len(snp_records) < 2:
            return {'recombination_events': 0, 'rate': 0, 'hotspots': []}
        
        chromosome_snps = defaultdict(list)
        
        for record in snp_records:
            if 'snp_data' in record:
                snp = GenomicAnalyzer.normalize_snp_data(record['snp_data'])
                chrom = snp.get('chromosome')
                pos = snp.get('position', 0)
                geno = snp.get('genotype', '')
                chromosome_snps[chrom].append({'pos': pos, 'geno': geno})
        
        recombination_events = 0
        hotspots = []
        
        for chrom, snps in chromosome_snps.items():
            snps_sorted = sorted(snps, key=lambda x: x['pos'])
            
            for i in range(1, len(snps_sorted)):
                prev_geno = snps_sorted[i-1]['geno']
                curr_geno = snps_sorted[i]['geno']
                
                if prev_geno and curr_geno and len(prev_geno) > 0 and len(curr_geno) > 0:
                    if prev_geno != curr_geno:
                        recombination_events += 1
                        hotspots.append({
                            'chromosome': chrom,
                            'position': snps_sorted[i]['pos'],
                            'type': 'recombination'
                        })
        
        rate = (recombination_events / max(1, len(snp_records))) * 100
        
        return {
            'recombination_events': recombination_events,
            'rate': round(rate, 2),
            'hotspots': hotspots[:10],  # Top 10
            'total_snps': len(snp_records)
        }
    
    @staticmethod
    def calculate_chromosome_distribution(snp_records):
        """Distribución de SNPs por cromosoma"""
        distribution = defaultdict(int)
        for record in snp_records:
            if 'snp_data' in record:
                snp = GenomicAnalyzer.normalize_snp_data(record['snp_data'])
                chrom = snp.get('chromosome', 'unknown')
                distribution[chrom] += 1
        return dict(distribution)
    
    @staticmethod
    def calculate_genetic_diversity(snp_records):
        """
        Métrica de diversidad genética: variedad de genotipos
        Basado en entropía de Shannon
        """
        if not snp_records:
            return {'diversity_index': 0, 'unique_genotypes': 0, 'entropy': 0}
        
        genotypes = defaultdict(int)
        for record in snp_records:
            if 'snp_data' in record:
                snp = GenomicAnalyzer.normalize_snp_data(record['snp_data'])
                geno = snp.get('genotype', '')
                genotypes[geno] += 1
        
        total = len(snp_records)
        entropy = 0
        
        for count in genotypes.values():
            if count > 0:
                p = count / total
                entropy -= p * np.log2(p)
        
        diversity_index = (entropy / np.log2(len(genotypes))) * 100 if genotypes else 0
        
        return {
            'diversity_index': round(diversity_index, 2),
            'unique_genotypes': len(genotypes),
            'entropy': round(entropy, 2),
            'genotype_distribution': dict(genotypes)
        }


# ==================== RUTAS API ====================

@app.route('/')
def index():
    """Dashboard principal"""
    return render_template('index_advanced.html')


@app.route('/api/genomic/mll')
def api_mll_analysis():
    """Análisis MLL avanzado"""
    with data_lock:
        analyzer = GenomicAnalyzer()
        
        result = {
            'fathers': analyzer.calculate_mll_score(list(genomic_data['fathers']['records'])),
            'mothers': analyzer.calculate_mll_score(list(genomic_data['mothers']['records'])),
            'children': analyzer.calculate_mll_score(list(genomic_data['children']['records'])),
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(result)


@app.route('/api/genomic/family-similarity')
def api_family_similarity():
    """Similitud familiar entre padres e hijos"""
    with data_lock:
        analyzer = GenomicAnalyzer()
        
        similarities = []
        for family_id, family_data in genomic_data['family_groups'].items():
            if family_data['father'] and family_data['children']:
                father_snps = list(genomic_data['fathers']['records'])
                for child in family_data['children'][:2]:  # Top 2 children
                    child_snps = list(genomic_data['children']['records'])
                    sim = analyzer.calculate_genetic_similarity(father_snps, child_snps)
                    similarities.append({
                        'family_id': family_id,
                        'relation': 'father-child',
                        **sim
                    })
        
        if not similarities:
            similarities = [{
                'family_id': 'N/A',
                'relation': 'N/A',
                'similarity': 0,
                'inherited_snps': 0,
                'shared_chromosomes': 0
            }]
        
        return jsonify({'similarities': similarities[:5], 'timestamp': datetime.now().isoformat()})


@app.route('/api/genomic/recombination')
def api_recombination():
    """Análisis de recombinación cromosómica"""
    with data_lock:
        analyzer = GenomicAnalyzer()
        
        result = {
            'fathers': analyzer.calculate_recombination_rate(list(genomic_data['fathers']['records'])),
            'mothers': analyzer.calculate_recombination_rate(list(genomic_data['mothers']['records'])),
            'children': analyzer.calculate_recombination_rate(list(genomic_data['children']['records'])),
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(result)


@app.route('/api/genomic/diversity')
def api_diversity():
    """Diversidad genética"""
    with data_lock:
        analyzer = GenomicAnalyzer()
        
        result = {
            'fathers': analyzer.calculate_genetic_diversity(list(genomic_data['fathers']['records'])),
            'mothers': analyzer.calculate_genetic_diversity(list(genomic_data['mothers']['records'])),
            'children': analyzer.calculate_genetic_diversity(list(genomic_data['children']['records'])),
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(result)


@app.route('/api/genomic/chromosome-distribution')
def api_chromosome_distribution():
    """Distribución de SNPs por cromosoma"""
    with data_lock:
        analyzer = GenomicAnalyzer()
        
        result = {
            'fathers': analyzer.calculate_chromosome_distribution(list(genomic_data['fathers']['records'])),
            'mothers': analyzer.calculate_chromosome_distribution(list(genomic_data['mothers']['records'])),
            'children': analyzer.calculate_chromosome_distribution(list(genomic_data['children']['records'])),
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(result)


@app.route('/api/processing/throughput')
def api_throughput():
    """Métricas de throughput de procesamiento"""
    with data_lock:
        now = datetime.now()
        last_min = now - timedelta(minutes=1)
        last_5min = now - timedelta(minutes=5)
        
        records_last_min = sum(1 for ts in genomic_data['processing_timestamps'] if ts > last_min)
        records_last_5min = sum(1 for ts in genomic_data['processing_timestamps'] if ts > last_5min)
        
        throughput_per_sec = records_last_min / 60 if records_last_min > 0 else 0
        
        return jsonify({
            'throughput_per_second': round(throughput_per_sec, 2),
            'records_last_minute': records_last_min,
            'records_last_5_minutes': records_last_5min,
            'total_processed': genomic_data['total_processed'],
            'uptime_seconds': (now - genomic_data['start_time']).total_seconds(),
            'timestamp': now.isoformat()
        })


@app.route('/api/metrics/update', methods=['POST'])
def update_metrics():
    """Endpoint para recibir métricas desde Spark"""
    # Usar ambos locks para actualizar ambas estructuras de datos
    with data_lock:
        data = request.json
        member_type = data.get('member_type')
        batch_records = data.get('records', [])
        batch_id = data.get('batch_id', 'unknown')
        total_records = data.get('total_records', len(batch_records))
        
        if member_type in genomic_data:
            for record in batch_records:
                genomic_data[member_type]['records'].append(record)
            
            genomic_data['processing_timestamps'].append(datetime.now())
            genomic_data['total_processed'] += total_records
    
    # Actualizar metrics_data bajo su propio lock
    with metrics_lock:
        if member_type in metrics_data:
            metrics_data[member_type]['total_records'] += total_records
            
            # Agregar a historial
            metrics_data['processing_history'].append({
                'timestamp': datetime.now().isoformat(),
                'member_type': member_type,
                'batch_id': batch_id,
                'records': total_records
            })
            
            # Actualizar last_update y batches_processed
            metrics_data['last_update'] = datetime.now()
            metrics_data['batches_processed'] += 1
        
        return jsonify({'status': 'ok', 'records_received': total_records})


@app.route('/api/status')
def status():
    """Estado general del sistema"""
    with data_lock:
        return jsonify({
            'fathers_count': len(genomic_data['fathers']['records']),
            'mothers_count': len(genomic_data['mothers']['records']),
            'children_count': len(genomic_data['children']['records']),
            'total_records': genomic_data['total_processed'],
            'unique_families': len(genomic_data['family_groups']),
            'uptime': (datetime.now() - genomic_data['start_time']).total_seconds(),
            'timestamp': datetime.now().isoformat()
        })


@app.route('/api/snp_distribution')
def api_snp_distribution():
    """Distribución de SNPs por cromosoma y rango"""
    with data_lock:
        chromosome_dist = defaultdict(int)
        genotype_dist = defaultdict(int)
        
        for member_type in ['fathers', 'mothers', 'children']:
            for record in genomic_data[member_type]['records']:
                if 'snp_data' in record:
                    snp = GenomicAnalyzer.normalize_snp_data(record['snp_data'])
                    chrom = snp.get('chromosome', 'unknown')
                    geno = snp.get('genotype', '')
                    chromosome_dist[str(chrom)] += 1
                    if geno:
                        genotype_dist[geno] += 1
        
        # Top 10 cromosomas
        top_chroms = dict(sorted(
            chromosome_dist.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10])
        
        return jsonify({
            'top_chromosomes': {
                'all': top_chroms
            },
            'genotype_distribution': {
                'all': dict(genotype_dist)
            }
        })


@app.route('/api/mendelian_consistency')
def api_mendelian_consistency():
    """Análisis de consistencia mendeliana en tríos padre-madre-hijo"""
    with data_lock:
        try:
            consistency_data = []
            
            # Agrupar por familia
            families = defaultdict(lambda: {'father': [], 'mother': [], 'children': []})
            
            for member_type in ['fathers', 'mothers', 'children']:
                for record in genomic_data[member_type]['records']:
                    family_id = record.get('family_id', 'unknown')
                    families[family_id][member_type].append(record)
            
            # Calcular consistencia mendeliana por familia
            for family_id, family_data in families.items():
                if family_data['father'] and family_data['mother'] and family_data['children']:
                    father_snps = extract_snp_dict(family_data['father'][0])
                    mother_snps = extract_snp_dict(family_data['mother'][0])
                    
                    violations = 0
                    total_checks = 0
                    
                    for child in family_data['children']:
                        child_snps = extract_snp_dict(child)
                        
                        # Comparar SNPs comunes
                        common_positions = set(father_snps.keys()) & set(mother_snps.keys()) & set(child_snps.keys())
                        
                        for pos in common_positions:
                            father_geno = father_snps[pos]
                            mother_geno = mother_snps[pos]
                            child_geno = child_snps[pos]
                            
                            # Verificar consistencia mendeliana
                            if not is_mendelian_consistent(father_geno, mother_geno, child_geno):
                                violations += 1
                            total_checks += 1
                    
                    if total_checks > 0:
                        consistency_pct = ((total_checks - violations) / total_checks) * 100
                        consistency_data.append({
                            'family_id': family_id,
                            'violations': violations,
                            'total_checks': total_checks,
                            'consistency_percentage': round(consistency_pct, 2),
                            'children_count': len(family_data['children'])
                        })
            
            # Ordenar por consistencia
            sorted_families = sorted(consistency_data, 
                                   key=lambda x: x['consistency_percentage'], 
                                   reverse=True)
            
            # Estadísticas
            if consistency_data:
                avg_consistency = sum(f['consistency_percentage'] for f in consistency_data) / len(consistency_data)
                families_with_violations = sum(1 for f in consistency_data if f['violations'] > 0)
            else:
                avg_consistency = 0
                families_with_violations = 0
            
            return jsonify({
                'families': sorted_families[:20],  # Top 20 familias
                'summary': {
                    'total_families_analyzed': len(consistency_data),
                    'average_consistency': round(avg_consistency, 2),
                    'families_with_violations': families_with_violations,
                    'families_perfect': len(consistency_data) - families_with_violations
                }
            })
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({'families': [], 'summary': {'total_families_analyzed': 0, 'average_consistency': 0, 'families_with_violations': 0, 'families_perfect': 0}})

def extract_snp_dict(record):
    """Extrae SNPs de un registro en formato {position: genotype}"""
    snp_dict = {}
    if 'snp_data' in record:
        snp = GenomicAnalyzer.normalize_snp_data(record['snp_data'])
        if snp.get('position') and snp.get('genotype'):
            snp_dict[snp['position']] = snp['genotype']
    return snp_dict

def is_mendelian_consistent(father_geno, mother_geno, child_geno):
    """Verifica si los genotipos siguen las leyes de Mendel"""
    try:
        # Para genotipos tipo "AA", "AB", "BB"
        possible_offspring = set()
        
        # Generar posibles alelos del hijo
        for f_allele in father_geno:
            for m_allele in mother_geno:
                # Ordenar para normalizar (AB = BA)
                offspring = ''.join(sorted([f_allele, m_allele]))
                possible_offspring.add(offspring)
        
        # Normalizar genotipo del hijo
        normalized_child = ''.join(sorted(child_geno)) if len(child_geno) >= 2 else child_geno
        
        return normalized_child in possible_offspring
    except:
        return True  # Si hay error, considerar consistente






























































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

@app.route('/api/stats')
def api_stats():
    """API para estadísticas generales"""
    with metrics_lock:
        return jsonify({
            'fathers': metrics_data['fathers']['total_records'],
            'mothers': metrics_data['mothers']['total_records'],
            'children': metrics_data['children']['total_records'],
            'total': sum([
                # metrics_data['fathers']['total_records'],
                # metrics_data['mothers']['total_records'],
                # metrics_data['children']['total_records']
                metrics_data[m]['total_records'] for m in ['fathers','mothers','children']
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
                # 'fathers': dict(metrics_data['fathers']['gender_distribution']),
                # 'mothers': dict(metrics_data['mothers']['gender_distribution']),
                # 'children': dict(metrics_data['children']['gender_distribution'])
                m: dict(metrics_data[m]['gender_distribution']) for m in ['fathers', 'mothers','children']
            },
            'chromosomes': {
                # 'fathers': dict(sorted(metrics_data['fathers']['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10]),
                # 'mothers': dict(sorted(metrics_data['mothers']['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10]),
                # 'children': dict(sorted(metrics_data['children']['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10])
                m : dict(sorted(metrics_data[m]['top_chromosomes'].items(), key=lambda x: x[1], reverse=True)[:10]) 
                for m in ['fathers', 'mothers','children']
            },
            'genotypes': {
                # 'fathers': dict(sorted(metrics_data['fathers']['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10]),
                # 'mothers': dict(sorted(metrics_data['mothers']['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10]),
                # 'children': dict(sorted(metrics_data['children']['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10])
                m : dict(sorted(metrics_data[m]['genotype_distribution'].items(), key=lambda x: x[1], reverse=True)[:10])
                for m in ['fathers','mothers','children']
            }
        })

@app.route('/api/snp_stats')
def api_snp_stats():
    """API para estadísticas de SNPs"""
    with metrics_lock:
        return jsonify({
            # 'fathers': metrics_data['fathers']['snp_stats'],
            # 'mothers': metrics_data['mothers']['snp_stats'],
            # 'children': metrics_data['children']['snp_stats']
            m : metrics_data[m]['snp_stats'] 
            for m in ['fathers','mothers','children']
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
    app.run(host='0.0.0.0', port=5000, debug=False)
