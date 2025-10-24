import pandas as pd
import numpy as np
from typing import Dict, List

class BaseGenomeGenerator:
    """Clase base para generadores de genomas sintéticos."""
    
    def __init__(self, genome_file: str = None, member_type: str = "unknown"):
        self.genome_file = genome_file
        self.member_type = member_type
        self.genome_df = None
        self.total_snps = 0
        self.genotype_distribution = {}
        self.chromosome_distribution = {}
        self.position_ranges = {}
        
        if genome_file:
            self._load_complete_genome()
    
    def _load_complete_genome(self):
        """Analiza un genoma real para extraer distribuciones estadísticas."""
        try:
            print(f"Analizando genoma para '{self.member_type}' desde: {self.genome_file}")
            
            self.genome_df = pd.read_csv(self.genome_file, low_memory=True)
            self.total_snps = len(self.genome_df)
            """indices_a_eliminar = self.genome_df[~self.genome_df['# rsid'].str.startswith('rs', na=False)].index
            self.genome_df = self.genome_df.drop(indices_a_eliminar)"""
            self.genome_df['position'].drop_duplicates(inplace=True)
            print(f"{self.member_type.capitalize()}: Analizando {self.total_snps:,} SNPs")
            
            if 'genotype' in self.genome_df.columns:
                genotype_counts = self.genome_df['genotype'].value_counts()
                self.genotype_distribution = (genotype_counts / genotype_counts.sum()).to_dict()
              
            if 'chromosome' in self.genome_df.columns:
                chrom_counts = self.genome_df['chromosome'].value_counts()
                self.chromosome_distribution = (chrom_counts / chrom_counts.sum()).to_dict()
                self.snps_per_chromosome = chrom_counts.to_dict()
               
            if 'chromosome' in self.genome_df.columns and 'position' in self.genome_df.columns:
                self.position_ranges = self.genome_df.groupby('chromosome')['position'].agg(['min', 'max', 'mean', 'std']).apply(
                    lambda r: r.to_dict(), axis=1
                ).to_dict()

                for chrom, stats in self.position_ranges.items():
                    if pd.isna(stats['std']):
                        self.position_ranges[chrom]['std'] = 1000000

        except Exception as e:
            print(f"❌ Error analizando genoma para '{self.member_type}': {e}")
            import traceback
            traceback.print_exc()

    def generate(self, family_id: str):
        """
        Debe ser implementado por las clases hijas.
        """
        raise NotImplementedError("El método 'generate' debe ser implementado por las subclases.")
