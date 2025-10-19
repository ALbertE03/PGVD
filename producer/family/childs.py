import pandas as pd
import numpy as np
import random
from typing import Dict, List
from .base_generator import BaseGenomeGenerator

class Child(BaseGenomeGenerator):
    """
    Genera datos de hijos basados en perfiles individuales
    Cada hijo (1, 2, 3) tiene su propio perfil estadístico cargado de un archivo.
    """
    
    def __init__(self, child_genome_files: List[str] = None):
        # Este init es más complejo porque maneja múltiples perfiles
        self.child_genome_files = child_genome_files or []
        self.children_profiles = {}

        if child_genome_files:
            self._analyze_children_genomes()
    
    def _analyze_children_genomes(self):
        """Analiza y carga los perfiles para cada genoma de hijo proporcionado."""
        print(f"\n{'='*70}\n📊 ANALIZANDO GENOMAS DE HIJOS\n{'='*70}")
        for idx, genome_file in enumerate(self.child_genome_files, start=1):
            print(f"\nCargando perfil para Child {idx} desde {genome_file}...")
            profile_generator = BaseGenomeGenerator(genome_file, member_type=f"child_{idx}")
            if profile_generator.total_snps > 0:
                self.children_profiles[idx] = profile_generator
                print(f"   ✅ Perfil para Child {idx} cargado ({profile_generator.total_snps:,} SNPs)")
        print(f"\n{'='*70}\n✅ ANÁLISIS DE HIJOS COMPLETADO\n{'='*70}\n")

    def generate(self, family_id: str, child_number: int):
        """
        Genera un NUEVO hijo con genoma sintético usando operaciones vectorizadas
        y el perfil estadístico del número de hijo especificado.
        """
        if child_number not in self.children_profiles:
            print(f"❌ Error: Perfil para el hijo número {child_number} no encontrado.")
            return

        profile = self.children_profiles[child_number]
        
        variation_factor = np.random.beta(2, 2) * 0.6 + 0.7
        mutation_rate = min(np.random.exponential(0.03) + 0.01, 0.08)
        
        child_info = {
            'family_id': family_id,
            'member_type': 'child',
            'person_id': f"{family_id}_C{child_number}",
            'gender': 'Male', # Se puede aleatorizar si es necesario
            'date_created': pd.Timestamp.now().isoformat(),
        }
        
        print(f"      🧬 Hijo #{child_number} {child_info['person_id']}: Iniciando generación vectorizada de {profile.total_snps:,} SNPs")

        # Reutilizar la lógica de generación vectorizada con el perfil del hijo
        # --- 1. Generación Vectorizada de CROMOSOMAS ---
        chromosomes_list = np.array(list(profile.chromosome_distribution.keys()))
        chromosomes_weights = np.array(list(profile.chromosome_distribution.values()))
        noise = np.random.lognormal(0, 0.3, size=len(chromosomes_weights))
        noisy_weights = chromosomes_weights * noise
        noisy_weights /= np.sum(noisy_weights)
        synthetic_chromosomes = np.random.choice(chromosomes_list, size=profile.total_snps, p=noisy_weights)

        # --- 2. Generación Vectorizada de POSICIONES ---
        pos_stats = pd.DataFrame(profile.position_ranges).T
        pos_stats.index = pos_stats.index.astype(str)
        synthetic_chromosomes_str = synthetic_chromosomes.astype(str)
        mapped_stats = pos_stats.loc[synthetic_chromosomes_str]
        
        means = mapped_stats['mean'].to_numpy()
        stds = mapped_stats['std'].to_numpy()
        mins = mapped_stats['min'].to_numpy()
        maxs = mapped_stats['max'].to_numpy()

        strategies = np.random.rand(profile.total_snps)
        
        mask1 = strategies < 0.33
        varied_mean1 = means[mask1] * variation_factor
        varied_std1 = stds[mask1] * np.random.gamma(2, 0.3, size=np.sum(mask1))
        
        mask2 = (strategies >= 0.33) & (strategies < 0.66)
        varied_mean2 = means[mask2] * np.random.normal(variation_factor, 0.1, size=np.sum(mask2))
        varied_std2 = stds[mask2] * np.random.lognormal(0, 0.4, size=np.sum(mask2))

        mask3 = strategies >= 0.66
        varied_mean3 = means[mask3] * (variation_factor + np.random.normal(0, 0.2, size=np.sum(mask3)))
        varied_std3 = stds[mask3] * np.random.exponential(1.2, size=np.sum(mask3))

        synthetic_positions = np.zeros(profile.total_snps, dtype=int)
        synthetic_positions[mask1] = np.random.normal(varied_mean1, varied_std1)
        synthetic_positions[mask2] = np.random.normal(varied_mean2, varied_std2)
        synthetic_positions[mask3] = np.random.normal(varied_mean3, varied_std3)
        
        np.clip(synthetic_positions, mins, maxs, out=synthetic_positions)

        noise_mask = np.random.rand(profile.total_snps) < 0.3
        range_spans = maxs[noise_mask] - mins[noise_mask]
        additional_noise = np.random.laplace(0, 0.1 * range_spans).astype(int)
        synthetic_positions[noise_mask] += additional_noise
        np.clip(synthetic_positions, mins, maxs, out=synthetic_positions)

        # --- 3. Generación Vectorizada de GENOTIPOS ---
        genotypes_list = np.array(list(profile.genotype_distribution.keys()))
        genotypes_weights = np.array(list(profile.genotype_distribution.values()))
        noise = np.random.gamma(2, 0.3, size=len(genotypes_weights)) + 0.4
        noisy_weights = genotypes_weights * noise
        noisy_weights /= np.sum(noisy_weights)
        synthetic_genotypes = np.random.choice(genotypes_list, size=profile.total_snps, p=noisy_weights)

        # --- 4. Aplicación Vectorizada de MUTACIONES ---
        mutation_mask = np.random.rand(profile.total_snps) < mutation_rate
        num_mutations = np.sum(mutation_mask)
        
        if num_mutations > 0:
            current_genotypes = synthetic_genotypes[mutation_mask]
            mutated_genotypes = np.array([
                np.random.choice([gt for gt in genotypes_list if gt != current_gt])
                for current_gt in current_genotypes
            ])
            synthetic_genotypes[mutation_mask] = mutated_genotypes

        # --- 5. Creación y envío de mensajes ---
        print(f"      🧬 Hijo #{child_number}: Ensamblando y enviando {profile.total_snps:,} SNPs...")
        
        for i in range(profile.total_snps):
            snp_message = {
                **child_info,
                'total_snps': profile.total_snps,
                'snp_data': {
                    'chromosome': synthetic_chromosomes[i],
                    'position': int(synthetic_positions[i]),
                    'genotype': synthetic_genotypes[i]
                },
                'timestamp': pd.Timestamp.now().isoformat()
            }
            yield snp_message

            if (i + 1) % 100000 == 0:
                progress = ((i + 1) / profile.total_snps) * 100
                print(f"      📊 Hijo #{child_number}: Enviados {i + 1:,}/{profile.total_snps:,} SNPs ({progress:.1f}%)")
 
    def generate(self, family_id: str, child_number: int, 
                father_genome: List[Dict], mother_genome: List[Dict]):
        """
        Genera datos de un hijo usando el perfil específico del Child 1, 2 o 3 con VARIACIÓN ALEATORIA.
        Cada hijo tiene sus propias distribuciones estadísticas pero con ruido aleatorio
        para que cada hijo generado sea único y diferente.
        
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
        
        # 🎲 VARIACIÓN ALEATORIA EXTREMA: Cada hijo tiene su propia "firma genética"
        # Factor aleatorio con distribución NO UNIFORME para mayor diversidad
        # Usar beta distribution para generar factores más variados
        variation_factor = np.random.beta(2, 2) * 0.6 + 0.7  # Rango: 0.7-1.3 con distribución beta
        
        # Factor de mutación altamente variable (entre 1% y 8%)
        mutation_rate = np.random.exponential(0.03) + 0.01  # Distribución exponencial
        mutation_rate = min(mutation_rate, 0.08)  # Cap al 8%
        
        print(f"\n   🧬 Generando Child {child_number} con perfil específico...")
        print(f"      📊 Total SNPs del perfil: {profile['total_snps']:,}")
        print(f"      🎲 Factor de variación: {variation_factor:.3f}")
        print(f"      🧬 Tasa de mutación: {mutation_rate:.1%}")
        
        # Generar genoma sintético usando las distribuciones del perfil
        child_genome = []
        
        # Crear distribuciones variadas para GENOTIPOS con RUIDO EXTREMO
        genotypes_list = list(profile['genotype_dist'].keys())
        genotypes_weights = list(profile['genotype_dist'].values())
        
        # Añadir ruido aleatorio EXTREMO y NO UNIFORME a los pesos de genotipos
        noisy_genotype_weights = []
        for weight in genotypes_weights:
            # Usar distribución gamma para ruido más variado
            noise = np.random.gamma(2, 0.3) + 0.4  # Rango variable, no simétrico
            noisy_genotype_weights.append(weight * noise)
        
        # Normalizar para que sumen 1
        total_weight = sum(noisy_genotype_weights)
        genotypes_weights = [w / total_weight for w in noisy_genotype_weights]
        
        # Crear distribuciones variadas para CROMOSOMAS con RUIDO EXTREMO
        chromosomes_list = list(profile['chromosome_dist'].keys())
        chromosomes_weights = list(profile['chromosome_dist'].values())
        
        # Añadir ruido aleatorio EXTREMO a los pesos de cromosomas
        noisy_chromosome_weights = []
        for weight in chromosomes_weights:
            # Distribución más variada usando lognormal
            noise = np.random.lognormal(0, 0.3)  # Media=1, pero con cola larga
            noisy_chromosome_weights.append(weight * noise)
        
        # Normalizar
        total_weight = sum(noisy_chromosome_weights)
        chromosomes_weights = [w / total_weight for w in noisy_chromosome_weights]
        
        child_info = {
            'family_id': family_id,
            'member_type': 'child',
            'person_id': f"{family_id}_C{child_number}",
            'gender': 'Male',
            'date_created': pd.Timestamp.now().isoformat(),
        }
        
        print(f"      🧬 Hijo #{child_number} {child_info['person_id']}: Iniciando generación de {profile['total_snps']:,} SNPs")
        
        # Generar SNPs sintéticos (mismo número que el perfil real)
        for i in range(profile['total_snps']):
            # 1. GENERAR CROMOSOMA sintético con distribución variada
            synthetic_chromosome = random.choices(chromosomes_list, weights=chromosomes_weights, k=1)[0]
            
            # 2. GENERAR POSICIÓN con VARIACIÓN EXTREMA Y NO UNIFORME
            if synthetic_chromosome in profile['position_ranges']:
                pos_range = profile['position_ranges'][synthetic_chromosome]
                
                # 🎲 Variación NO UNIFORME en la media y desviación estándar
                # Usar diferentes estrategias aleatorias para cada SNP
                strategy = random.random()
                
                if strategy < 0.33:
                    # Estrategia 1: Variación moderada
                    varied_mean = pos_range['mean'] * variation_factor
                    varied_std = pos_range['std'] * np.random.gamma(2, 0.3)
                elif strategy < 0.66:
                    # Estrategia 2: Variación alta
                    varied_mean = pos_range['mean'] * np.random.normal(variation_factor, 0.1)
                    varied_std = pos_range['std'] * np.random.lognormal(0, 0.4)
                else:
                    # Estrategia 3: Variación extrema ocasional
                    varied_mean = pos_range['mean'] * (variation_factor + np.random.normal(0, 0.2))
                    varied_std = pos_range['std'] * np.random.exponential(1.2)
                
                # Generar posición usando distribución normal variada
                synthetic_position = int(np.random.normal(varied_mean, varied_std))
                
                # Asegurar que está dentro del rango válido
                synthetic_position = max(pos_range['min'], min(pos_range['max'], synthetic_position))
                
                # 🎲 Añadir ruido adicional EXTREMO con probabilidad variable
                if random.random() < 0.3:  # 30% de probabilidad de ruido extra
                    range_span = pos_range['max'] - pos_range['min']
                    additional_noise = int(np.random.laplace(0, 0.1 * range_span))  # Distribución Laplace
                    synthetic_position = max(pos_range['min'], min(pos_range['max'], synthetic_position + additional_noise))
            
            # 3. GENERAR GENOTIPO con distribución variada
            synthetic_genotype = random.choices(genotypes_list, weights=genotypes_weights, k=1)[0]
            
            # 🎲 MUTACIÓN ALEATORIA con tasa variable por individuo
            if random.random() < mutation_rate:
                # Seleccionar otro genotipo aleatorio
                mutation_candidates = [gt for gt in genotypes_list if gt != synthetic_genotype]
                if mutation_candidates:
                    synthetic_genotype = random.choice(mutation_candidates)
            
            # Agregar SNP completamente sintético
            child_genome.append({
                'chromosome': synthetic_chromosome,
                'position': synthetic_position,
                'genotype': synthetic_genotype
            })
            snp_message = {
                **child_info,
                'total_snps': profile['total_snps'],
                'snp_data': {
                    'chromosome': synthetic_chromosome,
                    'position': synthetic_position,
                    'genotype': synthetic_genotype
                },
                'timestamp': pd.Timestamp.now().isoformat()
                }
            yield snp_message

            if (i + 1) % 100000 == 0:
                progress = ((i + 1) / profile['total_snps']) * 100
                print(f"      📊 Hijo #{child_number}: {i + 1:,}/{profile['total_snps']:,} SNPs ({progress:.1f}%)")
        
        print(f"       Child {child_number}: {len(child_genome):,} SNPs generados con distribución específica y variación aleatoria")
        