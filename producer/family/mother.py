import pandas as pd
import numpy as np
from .base_generator import BaseGenomeGenerator

class Mother(BaseGenomeGenerator):
    """Genera madres con genomas SINTÉTICOS"""
    
    def __init__(self, genome_file: str = None):
        super().__init__(genome_file, member_type="mother")
    
    
    def generate_with_interpolation(self,synthetic_chromosomes):
            from scipy.interpolate import interp1d

            synthetic_chromosomes_str = synthetic_chromosomes.astype(str)
            synthetic_positions = []
            
            for chrom in np.unique(synthetic_chromosomes_str):
                # Posiciones reales ordenadas para este cromosoma
                real_positions = np.sort(self.genome_df[self.genome_df['chromosome'].astype(str) == chrom]['position'].values)
                
                if len(real_positions) < 2:
                    count = (synthetic_chromosomes_str == chrom).sum()
                    samples = np.random.choice(real_positions, count, replace=True)
                    synthetic_positions.extend(samples)
                    continue
                
                # Creamos función de distribución acumulativa empírica
                ecdf = np.arange(len(real_positions)) / len(real_positions)
                
                # Interpolamos la CDF inversa
                inverse_cdf = interp1d(ecdf, real_positions, 
                                    bounds_error=False, 
                                    fill_value=(real_positions[0], real_positions[-1]))
                
                # Generamos muestras uniformes
                count = (synthetic_chromosomes_str == chrom).sum()
                uniform_samples = np.random.uniform(0, 1, count)
                
                # Aplicamos la CDF inversa
                samples = inverse_cdf(uniform_samples).astype(int)
                
                # Pequeña perturbación
                perturbation = np.random.randint(-1000, 1000, count)  # ±1000 bases
                perturbed_samples = samples + perturbation
                perturbed_samples = np.maximum(perturbed_samples, 1)
                
                synthetic_positions.extend(perturbed_samples)
            
            return np.array(synthetic_positions)
        
    def generate(self, family_id: str):
        """
        Genera una NUEVA madre con genoma 100% SINTÉTICO usando operaciones vectorizadas.
        """
        if self.total_snps == 0:
            print("❌ Error: No se pueden generar datos de la madre sin un genoma base cargado.")
            return

        variation_factor = np.random.beta(2, 2) * 0.6 + 0.7
        mutation_rate = min(np.random.exponential(0.03) + 0.01, 0.08)
        
        mother_info = {
            'family_id': family_id,
            'member_type': 'mother',
            'person_id': f"{family_id}_M",
            'gender': 'Female',
            'date_created': pd.Timestamp.now().isoformat(),
        }
        
        print(f"      🧬 Madre {mother_info['person_id']}: Iniciando generación vectorizada de {self.total_snps:,} SNPs (variación: {variation_factor:.3f}, mutación: {mutation_rate:.1%})")

        # Reutilizar la lógica de generación vectorizada (idéntica a la del padre)
        # --- 1. Generación Vectorizada de CROMOSOMAS ---
        chromosomes_list = np.array(list(self.chromosome_distribution.keys()))
        chromosomes_weights = np.array(list(self.chromosome_distribution.values()))
        noise = np.random.beta(8, 8, size=len(chromosomes_weights)) * 0.3
        noisy_weights = chromosomes_weights * noise
        noisy_weights = np.maximum(noisy_weights, 0.001)  
        noisy_weights /= np.sum(noisy_weights)
        synthetic_chromosomes = np.random.choice(chromosomes_list, size=self.total_snps, p=noisy_weights)

         # --- 2. Generación de POSICIONES ---
        
        synthetic_positions = self.generate_with_interpolation(synthetic_chromosomes)
        
        # --- 3. Generación Vectorizada de GENOTIPOS ---
        genotypes_list = np.array(list(self.genotype_distribution.keys()))
        genotypes_weights = np.array(list(self.genotype_distribution.values()))
        noise = np.random.gamma(2, 0.3, size=len(genotypes_weights)) + 0.4
        noisy_weights = genotypes_weights * noise
        noisy_weights /= np.sum(noisy_weights)
        synthetic_genotypes = np.random.choice(genotypes_list, size=self.total_snps, p=noisy_weights)

        # --- 4. Aplicación Vectorizada de MUTACIONES ---
        mutation_mask = np.random.rand(self.total_snps) < mutation_rate
        num_mutations = np.sum(mutation_mask)
        
        if num_mutations > 0:
            current_genotypes = synthetic_genotypes[mutation_mask]
            mutated_genotypes = np.array([
                np.random.choice([gt for gt in genotypes_list if gt != current_gt])
                for current_gt in current_genotypes
            ])
            synthetic_genotypes[mutation_mask] = mutated_genotypes

        # --- 5. Creación y envío de mensajes ---
        print(f"      🧬 Madre {mother_info['person_id']}: Ensamblando y enviando {self.total_snps:,} SNPs...")
        
        for i in range(self.total_snps):
            snp_message = {
                **mother_info,
                'total_snps': self.total_snps,
                'snp_data': {
                    'chromosome': synthetic_chromosomes[i],
                    'position': int(synthetic_positions[i]),
                    'genotype': synthetic_genotypes[i]
                },
                'timestamp': pd.Timestamp.now().isoformat()
            }
            yield snp_message

            if (i + 1) % 100000 == 0:
                progress = ((i + 1) / self.total_snps) * 100
                print(f"      📊 Madre: Enviados {i + 1:,}/{self.total_snps:,} SNPs ({progress:.1f}%)")
        
       