import pandas as pd
import numpy as np
from typing import Dict, List
import random


class Mother:
    """Genera madres con genomas SINTÉTICOS."""
    
    def __init__(self, genome_file: str = None):
        self.genome_file = genome_file
        self.genome_df = None  # DataFrame para análisis
        self.genotype_distribution = {}  # Probabilidades de cada genotipo
        self.chromosome_distribution = {}  # Probabilidades de cada cromosoma
        self.position_template = []  # Posiciones (rsid, chr, pos) sin genotipos
        
        if genome_file:
            self._load_complete_genome()
    
    def _load_complete_genome(self):
        """Analiza el genoma real para extraer TODAS las distribuciones estadísticas."""
        try:
            print(f"Analizando genoma de la madre desde: {self.genome_file}")
            
            # Cargar el DataFrame completo para análisis
            self.genome_df = pd.read_csv(self.genome_file)
            self.total_snps = len(self.genome_df)

            
            print(f"Father: Analizando {self.total_snps:,} SNPs")
            
            # 1. Distribución de GENOTIPOS
            if 'genotype' in self.genome_df.columns:
                genotype_counts = self.genome_df['genotype'].value_counts()
                total = genotype_counts.sum()
                self.genotype_distribution = {
                    gt: count/total for gt, count in genotype_counts.items()
                }
              
            # 2. Distribución de CROMOSOMAS
            if 'chromosome' in self.genome_df.columns:
                chrom_counts = self.genome_df['chromosome'].value_counts()
                total = chrom_counts.sum()
                self.chromosome_distribution = {
                    str(ch): count/total for ch, count in chrom_counts.items()
                }

                self.snps_per_chromosome = dict(chrom_counts)
               
            # 3. Calcular rangos de posición por cromosoma
            self.position_ranges = {}
            if 'chromosome' in self.genome_df.columns and 'position' in self.genome_df.columns:
                for chrom in self.genome_df['chromosome'].unique():
                    chrom_data = self.genome_df[self.genome_df['chromosome'] == chrom]['position']
                    self.position_ranges[str(chrom)] = {
                        'min': int(chrom_data.min()),
                        'max': int(chrom_data.max()),
                        'mean': float(chrom_data.mean()),
                        'std': float(chrom_data.std()) if len(chrom_data) > 1 else 1000000
                    }
            
         
        except Exception as e:
            print(f"❌ Error analizando genoma del padre: {e}")
            import traceback
            traceback.print_exc()
            
    
    def generate(self, family_id: str) -> Dict:
        """
        Genera un NUEVO padre con genoma 100% SINTÉTICO.
        Todas las columnas (rsid, chromosome, position, genotype) son generadas
        siguiendo las distribuciones estadísticas del genoma real.
        
        Args:
            family_id: ID único de la familia
            
        Returns:
            Diccionario con datos del padre y su genoma completamente sintético
        """
        synthetic_genome = []
        
        # Listas para generación
        genotypes_list = list(self.genotype_distribution.keys())
        genotypes_weights = list(self.genotype_distribution.values())
        
        chromosomes_list = list(self.chromosome_distribution.keys())
        chromosomes_weights = list(self.chromosome_distribution.values())
        
    
        for i in range(self.total_snps):
            # 1. GENERAR CROMOSOMA sintético (según distribución)
            synthetic_chromosome = random.choices(chromosomes_list, weights=chromosomes_weights, k=1)[0]
            
            # 2. GENERAR POSICIÓN 
            if synthetic_chromosome in self.position_ranges:
                pos_range = self.position_ranges[synthetic_chromosome]
                # Generar posición usando distribución normal dentro del rango
                synthetic_position = int(np.random.normal(pos_range['mean'], pos_range['std']))
                # Asegurar que está dentro del rango válido
                synthetic_position = max(pos_range['min'], min(pos_range['max'], synthetic_position))
            
            # 4. GENERAR GENOTIPO
            synthetic_genotype = random.choices(genotypes_list, weights=genotypes_weights, k=1)[0]
            
            synthetic_genome.append({
              
                'chromosome': synthetic_chromosome,
                'position': synthetic_position,
                'genotype': synthetic_genotype
            })
        
        return {
            'family_id': family_id,
            'member_type': 'mother',
            'person_id': f"{family_id}_M",
            'gender': 'Female',
            'date_created': pd.Timestamp.now().isoformat(),
            'genome': synthetic_genome, 
            'genome_size': len(synthetic_genome),
        }