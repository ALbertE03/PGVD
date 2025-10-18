import pandas as pd
import numpy as np
from typing import Dict, List
import random


class Child:
    """Genera datos de hijos basados en herencia de los padres."""
    
    def __init__(self, child_genome_files: List[str] = None):
        self.child_genome_files = child_genome_files or []
        
        # Diccionario para almacenar distribuciones de cada hijo (1, 2, 3)
        self.children_profiles = {
            1: {'genotype_dist': {}, 'chromosome_dist': {}, 'position_ranges': {}, 'total_snps': 0, 'rsid_patterns': {}},
            2: {'genotype_dist': {}, 'chromosome_dist': {}, 'position_ranges': {}, 'total_snps': 0, 'rsid_patterns': {}},
            3: {'genotype_dist': {}, 'chromosome_dist': {}, 'position_ranges': {}, 'total_snps': 0, 'rsid_patterns': {}}
        }
        
        if child_genome_files:
            self._analyze_children_genomes()
    
    def _analyze_children_genomes(self):
        """Analiza los genomas reales de los 3 hijos para extraer distribuciones individuales."""
        try:
            print(f"\n{'='*70}")
            print(f"📊 ANALIZANDO GENOMAS DE LOS 3 HIJOS")
            print(f"{'='*70}")
            
            for idx, genome_file in enumerate(self.child_genome_files[:3], start=1):
                print(f"\nAnalizando Child {idx}: {genome_file}")
                
                # Cargar genoma completo del hijo
                df = pd.read_csv(genome_file)
                profile = self.children_profiles[idx]
                profile['total_snps'] = len(df)
                
                print(f"   Total SNPs: {profile['total_snps']:,}")
                
                # 1. Distribución de GENOTIPOS
                if 'genotype' in df.columns:
                    genotype_counts = df['genotype'].value_counts()
                    total = genotype_counts.sum()
                    profile['genotype_dist'] = {
                        gt: count/total for gt, count in genotype_counts.items()
                    }
                    print(f"    Genotipos únicos: {len(profile['genotype_dist'])}")
                    print(f"      Top 5: {list(profile['genotype_dist'].keys())[:5]}")
                
                # 2. Distribución de CROMOSOMAS
                if 'chromosome' in df.columns:
                    chrom_counts = df['chromosome'].value_counts()
                    total = chrom_counts.sum()
                    profile['chromosome_dist'] = {
                        str(ch): count/total for ch, count in chrom_counts.items()
                    }
                    print(f"   ✅ Cromosomas: {len(profile['chromosome_dist'])}")
                
                # 3. Rangos de POSICIÓN por cromosoma
                if 'chromosome' in df.columns and 'position' in df.columns:
                    profile['position_ranges'] = {}
                    for chrom in df['chromosome'].unique():
                        chrom_data = df[df['chromosome'] == chrom]['position']
                        profile['position_ranges'][str(chrom)] = {
                            'min': int(chrom_data.min()),
                            'max': int(chrom_data.max()),
                            'mean': float(chrom_data.mean()),
                            'std': float(chrom_data.std()) if len(chrom_data) > 1 else 1000000
                        }
                    print(f"   ✅ Rangos de posición calculados para {len(profile['position_ranges'])} cromosomas")
               
            print(f"\n{'='*70}")
            print(f"✅ ANÁLISIS COMPLETADO PARA LOS 3 HIJOS")
            print(f"{'='*70}\n")
                
        except Exception as e:
            print(f"❌ Error analizando genomas de hijos: {e}")
            import traceback
            traceback.print_exc()
 
    def generate(self, family_id: str, child_number: int, 
                father_genome: List[Dict], mother_genome: List[Dict]) -> Dict:
        """
        Genera datos de un hijo usando el perfil específico del Child 1, 2 o 3.
        Cada hijo tiene sus propias distribuciones estadísticas.
        
        Args:
            family_id: ID único de la familia
            child_number: Número del hijo (1, 2 o 3)
            father_genome: Genoma COMPLETO del padre (solo se usa para tamaño de referencia)
            mother_genome: Genoma COMPLETO de la madre (solo se usa para tamaño de referencia)
            
        Returns:
            Diccionario con datos del hijo y su genoma sintético basado en su perfil
        """
        # Validar número de hijo
        if child_number not in [1, 2, 3]:
            print(f"⚠️  Número de hijo inválido: {child_number}. Usando perfil 1 por defecto.")
            child_number = 1
        
        # Obtener el perfil específico del hijo
        profile = self.children_profiles[child_number]
        
        print(f"\n   🧬 Generando Child {child_number} con perfil específico...")
        print(f"      📊 Total SNPs del perfil: {profile['total_snps']:,}")
        
        # Generar genoma sintético usando las distribuciones del perfil
        child_genome = []
        
        # Listas para generación rápida
        genotypes_list = list(profile['genotype_dist'].keys())
        genotypes_weights = list(profile['genotype_dist'].values())
        
        chromosomes_list = list(profile['chromosome_dist'].keys())
        chromosomes_weights = list(profile['chromosome_dist'].values())
        
        # Generar SNPs sintéticos (mismo número que el perfil real)
        for i in range(profile['total_snps']):
            # 1. GENERAR CROMOSOMA sintético (según distribución del perfil)
            synthetic_chromosome = random.choices(chromosomes_list, weights=chromosomes_weights, k=1)[0]
            
            # 2. GENERAR POSICIÓN sintética (según rangos del cromosoma del perfil)
            if synthetic_chromosome in profile['position_ranges']:
                pos_range = profile['position_ranges'][synthetic_chromosome]
                # Generar posición usando distribución normal dentro del rango
                synthetic_position = int(np.random.normal(pos_range['mean'], pos_range['std']))
                # Asegurar que está dentro del rango válido
                synthetic_position = max(pos_range['min'], min(pos_range['max'], synthetic_position))
          
            
            # 4. GENERAR GENOTIPO sintético (según distribución del perfil)
            synthetic_genotype = random.choices(genotypes_list, weights=genotypes_weights, k=1)[0]
            
            # Agregar SNP completamente sintético
            child_genome.append({
                'chromosome': synthetic_chromosome,
                'position': synthetic_position,
                'genotype': synthetic_genotype
            })
        
        print(f"       Child {child_number}: {len(child_genome):,} SNPs generados con distribución específica")
        
        return {
            'family_id': family_id,
            'member_type': 'child',
            'child_number': child_number,
            'person_id': f"{family_id}_C{child_number}",
            'inherited_traits': {
                'profile_used': f'Child {child_number} distribution',
                'total_genotypes': len(profile['genotype_dist']),
                'total_chromosomes': len(profile['chromosome_dist'])
            },
            'date_generated': pd.Timestamp.now().isoformat(),
            'genome': child_genome,  
            'genome_size': len(child_genome),
            'total_snps': len(child_genome),
          
        }