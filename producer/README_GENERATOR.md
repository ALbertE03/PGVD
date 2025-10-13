# Genomic Data Generator

Generador de datos genómicos sintéticos 
## Características

- **51 campos genómicos**
- **8 categorías** de métricas genómicas
- **Datos realistas** basados en el proyecto 1000 Genomes
- **Streaming de alto rendimiento** hacia Kafka
- **Multi-threading** para generación paralela
- **QC integrado** con métricas de calidad

## Categorías de Datos Generados

### 1. Calidad de Secuenciación (5 campos)
- Read Length (75-250 bp)
- GC Content (30-70%)
- Phred Q20/Q30 scores
- Duplication Rate

### 2.  Métricas de Alineamiento (5 campos)
- Mapping Quality
- Properly Paired %
- On-Target %
- Insert Size (media y desviación)

### 3. Cobertura (6 campos)
- Mean Coverage Depth
- % Bases at 1x, 10x, 20x, 30x
- Coverage Uniformity

### 4. Variantes Genéticas (5 campos)
- SNPs (~3.5M por muestra)
- INDELs (~500K por muestra)
- Structural Variants (~8K por muestra)
- Ti/Tv Ratio (~2.05)
- Novel Variants

### 5.  Información Demográfica (5 campos)
- Gender (Male/Female)
- Age (18-85 años)
- Family ID
- Relationship
- Phenotype (Case/Control)

### 6. Información Técnica (5 campos)
- Run Date
- Sequencer ID
- Flow Cell ID
- Lane Number
- Barcode

### 7.  Pipeline Bioinformático (5 campos)
- Processing Date
- Pipeline Version
- Reference Genome (GRCh37/GRCh38)
- Aligner (BWA-MEM, Bowtie2, etc.)
- Variant Caller (GATK, FreeBayes, etc.)

### 8. Contaminación (2 campos)
- Contamination Estimate
- Freemix Score


## Ejemplo de Salida

```json
{
  "Sample": "T0_GBR12345",
  "Population": "GBR",
  "Center": "WUGSC",
  "Platform": "ILLUMINA",
  "Total Sequence": "18,234,567,890",
  "Aligned Non Duplicated Coverage": 7.82,
  "Passed QC": 1,
  
  "Read_Length": 150,
  "GC_Content_Pct": 48.23,
  "Phred_Q20_Pct": 93.45,
  "Phred_Q30_Pct": 87.12,
  "Duplication_Rate_Pct": 12.34,
  
  "Mapping_Quality": 58.76,
  "Properly_Paired_Pct": 95.67,
  "On_Target_Pct": 82.34,
  "Mean_Insert_Size": 342,
  "Insert_Size_StdDev": 68,
  
  "Mean_Coverage_Depth": 7.95,
  "Pct_Bases_1x": 98.45,
  "Pct_Bases_10x": 87.23,
  "Pct_Bases_20x": 76.89,
  "Pct_Bases_30x": 65.34,
  "Coverage_Uniformity": 0.87,
  
  "Num_SNPs": 3456789,
  "Num_INDELs": 456234,
  "Num_Structural_Variants": 8234,
  "TiTv_Ratio": 2.03,
  "Novel_Variants": 12456,
  
  "Gender": "Female",
  "Age": 42,
  "Family_ID": "FAM1234",
  "Relationship": "Proband",
  "Phenotype": "Case",
  
  "Run_Date": "2024-03-15",
  "Sequencer_ID": "SEQ5678",
  "Flow_Cell_ID": "FC123456",
  "Lane_Number": 3,
  "Barcode": "ATCG5678",
  
  "Processing_Date": "2024-08-20",
  "Pipeline_Version": "v3.4.12",
  "Reference_Genome": "GRCh38",
  "Aligner_Used": "BWA-MEM",
  "Variant_Caller": "GATK",
  
  "Contamination_Estimate": 0.0123,
  "Freemix_Score": 0.0089
}
```

## Rendimiento

- **Generación**: ~10,000-50,000 muestras/segundo (dependiendo del hardware)
- **Multi-threading**: Escalable hasta 8+ threads
- **Kafka**: Soporte para particionado y alta throughput
