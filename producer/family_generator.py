#!/usr/bin/env python3
"""
Generador de familias completas con ID único y distribuciones realistas.
Se enfoca únicamente en la generación de datos, sin lógica de Kafka o hilos.
"""

import time
import uuid
from typing import Dict, Iterator, Tuple
import random
from family.father import Father
from family.mother import Mother
from family.childs import Child

class FamilyGenerator:
    """
    Generador de familias completas con herencia genética.
    Esta clase está dedicada únicamente a la lógica de generación de datos.
    """
    
    def __init__(self, genome_paths: Dict[str, str] = None):
        """
        Inicializa el generador de familias.
        
        Args:
            genome_paths: Diccionario con rutas a genomas reales
                         {'father': path, 'mother': path, 'children': [path1, path2, path3]}
        """
        print("🧬 Inicializando los generadores de perfiles genéticos...")
        
        if genome_paths:
            father_file = genome_paths.get('father')
            mother_file = genome_paths.get('mother')
            children_files = genome_paths.get('children', [])
            
            self.father_generator = Father(genome_file=father_file)
            self.mother_generator = Mother(genome_file=mother_file)
            self.child_generator = Child(child_genome_files=children_files)
        
        print("✅ Generador de familias listo")
        print("="*80 + "\n")
    
    def generate_complete_family(self) -> Iterator[Tuple[str, Dict]]:
        """
        Genera una familia completa con un ID único.
        Produce los SNPs de todos los miembros de forma intercalada.
        
        Yields:
            Tupla con (tipo_miembro, mensaje_snp) o ('completion', token_final).
        """
        family_id = f"FAM_{uuid.uuid4().hex[:12].upper()}"
        num_children = random.randint(1, 3)
        start_time = time.time()
        
        print(f"\n🏠 Generando familia: {family_id} con {num_children} hijo(s)")
        
        # Crear iteradores para cada miembro de la familia
        father_iter = self.father_generator.generate(family_id)
        mother_iter = self.mother_generator.generate(family_id)
        children_iters = [
            self.child_generator.generate(family_id, i) 
            for i in range(1, num_children + 1)
        ]
        
        # Listas para gestionar todos los generadores
        all_iterators = [father_iter, mother_iter] + children_iters
        member_types = ['father', 'mother'] + ['child'] * num_children
        snps_count = [0] * len(all_iterators)
        
        # Generar intercaladamente usando round-robin
        active = True
        while active:
            active = False
            for i, iterator in enumerate(all_iterators):
                try:
                    snp_message = next(iterator)
                    yield (member_types[i], snp_message)
                    snps_count[i] += 1
                    active = True
                except StopIteration:
                    continue  # Este generador ha terminado
        
        end_time = time.time()
        total_snps = sum(snps_count)
        duration = end_time - start_time
        
        print(f"✅ Familia {family_id} completada en {duration:.2f}s. Total: {total_snps:,} SNPs")
        
        # Token de finalización con metadatos completos
        completion_token = {
            'family_id': family_id,
            'message_type': 'FAMILY_COMPLETE',
            'total_members': 2 + num_children,
            'num_children': num_children,
            'total_snps': total_snps,
            'generation_start_time': start_time,
            'generation_end_time': end_time,
            'generation_duration_secs': duration,
            'snps_per_member': {
                'father': snps_count[0],
                'mother': snps_count[1],
                'children': snps_count[2:]
            }
        }
        yield ('completion', completion_token)
