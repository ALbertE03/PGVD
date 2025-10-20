import pandas as pd
import numpy as np
from .base_generator import BaseGenomeGenerator

class Father(BaseGenomeGenerator):
    """Genera padres con genomas SINTÉTICOS"""
    
    def __init__(self, genome_file: str = None):
        super().__init__(genome_file, member_type="father")
    
    def generate(self, family_id: str):
        """
        Genera un NUEVO padre con genoma 100% SINTÉTICO usando operaciones vectorizadas de NumPy.
        """
        if self.total_snps == 0:
            print("❌ Error: No se pueden generar datos del padre sin un genoma base cargado.")
            return

        variation_factor = np.random.beta(2, 2) * 0.6 + 0.7
        mutation_rate = min(np.random.exponential(0.03) + 0.01, 0.08)
        
        father_info = {
            'family_id': family_id,
            'member_type': 'father',
            'person_id': f"{family_id}_F",
            'gender': 'Male',
            'date_created': pd.Timestamp.now().isoformat(),
        }
        
        print(f"      Padre {father_info['person_id']}: Iniciando generación vectorizada de {self.total_snps:,} SNPs (variación: {variation_factor:.3f}, mutación: {mutation_rate:.1%})")

        # --- 1. Generación Vectorizada de CROMOSOMAS ---
        chromosomes_list = np.array(list(self.chromosome_distribution.keys()))
        chromosomes_weights = np.array(list(self.chromosome_distribution.values()))
        noise = np.random.lognormal(0, 0.3, size=len(chromosomes_weights))
        noisy_weights = chromosomes_weights * noise
        noisy_weights /= np.sum(noisy_weights)
        synthetic_chromosomes = np.random.choice(chromosomes_list, size=self.total_snps, p=noisy_weights)

        # --- 2. Generación Vectorizada de POSICIONES ---
        pos_stats = pd.DataFrame(self.position_ranges).T
        # Convertir los nombres de los cromosomas a string para asegurar la coincidencia
        pos_stats.index = pos_stats.index.astype(str)
        synthetic_chromosomes_str = synthetic_chromosomes.astype(str)
        
        # Convertir a diccionarios para mapeo eficiente sin problemas de índice
        mean_dict = pos_stats['mean'].to_dict()
        std_dict = pos_stats['std'].to_dict()
        min_dict = pos_stats['min'].to_dict()
        max_dict = pos_stats['max'].to_dict()
        
        # Crear Series temporales y usar map con diccionarios
        chrom_series = pd.Series(synthetic_chromosomes_str)
        means = chrom_series.map(mean_dict).to_numpy()
        stds = chrom_series.map(std_dict).to_numpy()
        mins = chrom_series.map(min_dict).to_numpy(dtype=np.int64)
        maxs = chrom_series.map(max_dict).to_numpy(dtype=np.int64)

        # Aplicar estrategias de variación de forma vectorizada
        strategies = np.random.rand(self.total_snps)
        
        mask1 = strategies < 0.33
        varied_mean1 = means[mask1] * variation_factor
        varied_std1 = stds[mask1] * np.random.gamma(2, 0.3, size=np.sum(mask1))
        
        mask2 = (strategies >= 0.33) & (strategies < 0.66)
        varied_mean2 = means[mask2] * np.random.normal(variation_factor, 0.1, size=np.sum(mask2))
        varied_std2 = stds[mask2] * np.random.lognormal(0, 0.4, size=np.sum(mask2))

        mask3 = strategies >= 0.66
        varied_mean3 = means[mask3] * (variation_factor + np.random.normal(0, 0.2, size=np.sum(mask3)))
        varied_std3 = stds[mask3] * np.random.exponential(1.2, size=np.sum(mask3))

        synthetic_positions = np.zeros(self.total_snps, dtype=int)
        synthetic_positions[mask1] = np.random.normal(varied_mean1, varied_std1)
        synthetic_positions[mask2] = np.random.normal(varied_mean2, varied_std2)
        synthetic_positions[mask3] = np.random.normal(varied_mean3, varied_std3)
        
        np.clip(synthetic_positions, mins, maxs, out=synthetic_positions)

        noise_mask = np.random.rand(self.total_snps) < 0.3
        range_spans = maxs[noise_mask] - mins[noise_mask]
        additional_noise = np.random.laplace(0, 0.1 * range_spans).astype(int)
        synthetic_positions[noise_mask] += additional_noise
        np.clip(synthetic_positions, mins, maxs, out=synthetic_positions)

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
        print(f"      🧬 Padre {father_info['person_id']}: Ensamblando y enviando {self.total_snps:,} SNPs...")
        
        for i in range(self.total_snps):
            snp_message = {
                **father_info,
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
                print(f"      📊 Padre: Enviados {i + 1:,}/{self.total_snps:,} SNPs ({progress:.1f}%)")