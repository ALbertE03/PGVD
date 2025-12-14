"""
Modelos de datos para los mensajes del productor.
Define las estructuras de los datos que se envían a Kafka.
"""

from typing import TypedDict


class SNPMessage(TypedDict):
    """Mensaje individual de un SNP (Single Nucleotide Polymorphism)."""
    family_id: str
    member_type: str  # 'father', 'mother', 'child'
    person_id: str
    gender: str
    date_created: str
    total_snps: int
    snp_data: dict  # {'chromosome': str, 'position': int, 'genotype': str}
    timestamp: str


class FamilyCompletionToken(TypedDict):
    """Token que indica la finalización de la generación de una familia."""
    family_id: str
    message_type: str  # Siempre 'FAMILY_COMPLETE'
    total_members: int
    num_children: int
    total_snps: int
    generation_start_time: float
    generation_end_time: float
    generation_duration_secs: float
    snps_per_member: dict  # {'father': int, 'mother': int, 'children': List[int]}
