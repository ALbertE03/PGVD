import time
import random
import pandas as pd
import threading
from datetime import datetime, timedelta

class GenomicGenerator:

    def __init__(self, num_threads: int = 4,path_to_csv:str="data/20130606_sample_info.csv"):
        self.data=pd.read_csv(path_to_csv)
        self.num_threads=num_threads
        self.lock = threading.Lock()
        self.__process_data()

    def __process_data(self):
        try:
            self.data.drop(columns=[f"Unnamed: {x}" for x in range(13,19)],inplace=True)
            row1 = self.data.iloc[[0]]
            self.data.columns =  row1.to_numpy().flatten()
            self.data.drop(self.data.index[0], inplace=True)
            self.data.dropna(inplace=True)
            self.data.columns = list(self.data.columns[:6])+[ f"step2_{x}" for x in self.data.columns[6:]]
        except Exception as e:
            print(f"Error processing data: {e}")
            import traceback
            traceback.print_exc()

    def generate_threaded_kafka_stream(self, kafka_producer, kafka_topic, partition_number=None,records_per_thread=1000):
        """
        Generate and send data directly to Kafka
        Each thread sends its data immediately without waiting for others.
        
        Args:
            kafka_producer: The KafkaProducer instance
            kafka_topic: Topic to send to
            partition_number: Specific partition (optional)
            records_per_thread: Records each thread should generate continuously
        """
        threads = []
        self.total_sent = 0
        self.should_stop = False
        
        def streaming_worker(thread_id):
            """Worker that generates and sends data continuously"""
            thread_sent = 0
            thread_start = time.time()
            batch_size = records_per_thread 
            
            print(f"THREAD-{thread_id} started - streaming mode activated!")
            
            while not self.should_stop:
                try:
                    batch_samples = self._generate_sample_batch_fast(batch_size, thread_id)
                    
                    for _, row in batch_samples.iterrows():
                        if self.should_stop:
                            break
                        record = {
                            **row.to_dict(),
                            'thread_id': thread_id,
                            'partition': partition_number,
                            'timestamp': time.time(),
                            'generation_batch': int(time.time())
                        }
                        
                        if partition_number is not None:
                            kafka_producer.send(kafka_topic, value=record, partition=partition_number)
                        else:
                            kafka_producer.send(kafka_topic, value=record)
                        
                        thread_sent += 1
                        
                        with self.lock:
                            self.total_sent += 1
                    
        
                    if thread_sent % 1000 == 0:
                        elapsed = time.time() - thread_start
                        rate = thread_sent / elapsed if elapsed > 0 else 0
                        print(f"THREAD-{thread_id}: {thread_sent:,} sent ({rate:.0f}/sec)")
                
                        
                except Exception as e:
                    print(f"THREAD-{thread_id} ERROR: {e}")
                    time.sleep(0.1)
            
            print(f"THREAD-{thread_id} finished: {thread_sent:,} records sent")
        

        for i in range(self.num_threads):
            thread = threading.Thread(
                target=streaming_worker,
                args=(i,),
                daemon=True,
                name=f"KafkaStreamer-{i}"
            )
            threads.append(thread)
            thread.start()
            print(f"Thread-{i} launched for streaming")
        
        return threads  
    
    def stop_streaming(self):
        """Stop all streaming threads"""
        self.should_stop = True
        print("Stopping all streaming threads...")

    def _generate_sample_batch_fast(self, batch_size: int, thread_id: int):
        """
         batch generation 
        """
        populations = self.data['Population'].unique()
        centers = self.data['Center'].dropna().unique()
        platforms = self.data['Platform'].dropna().unique()
        
        coverage_data = pd.to_numeric(self.data['Aligned Non Duplicated Coverage'], errors='coerce').dropna()
        coverage_mean = coverage_data.mean() if len(coverage_data) > 0 else 8.0
        coverage_std = coverage_data.std() if len(coverage_data) > 0 else 3.0
        
        sequence_data = self.data['Total Sequence'].str.replace(',', '').astype(float, errors='ignore')
        sequence_data = pd.to_numeric(sequence_data, errors='coerce').dropna()
        seq_mean = sequence_data.mean() if len(sequence_data) > 0 else 20000000000
        seq_std = sequence_data.std() if len(sequence_data) > 0 else 5000000000
        
        read_lengths = [75, 100, 150, 250]
        reference_genomes = ['GRCh37', 'GRCh38']
        aligners = ['BWA-MEM', 'BWA-ALN', 'Bowtie2', 'STAR']
        variant_callers = ['GATK', 'FreeBayes', 'BCFtools', 'DeepVariant']
        genders = ['Male', 'Female']
        phenotypes = ['Control', 'Case', 'Unknown']
        
        batch_data = []
        
        for i in range(batch_size):
            pop = random.choice(populations)
            sample_id = f"T{thread_id}_{pop}{random.randint(10000, 99999)}"
            
            coverage = max(0.1, random.normalvariate(coverage_mean, coverage_std))
            total_sequence = max(1000000, int(random.normalvariate(seq_mean, seq_std)))
            qc_pass = random.choices([0, 1], weights=[0.1, 0.9])[0]
            
            step2_coverage_pct = random.uniform(0.75, 0.95) if qc_pass else random.uniform(0.3, 0.75)
            step2_qc = random.choices([0, 1], weights=[0.15, 0.85])[0] if qc_pass else 0
            step2_total_seq = max(1000000, int(total_sequence * random.uniform(0.1, 0.8)))
            
            # 1. Sequencing Quality Metrics
            read_length = random.choice(read_lengths)
            gc_content = round(random.normalvariate(50, 5), 2)  # Normal distribution around 50%
            gc_content = max(30, min(70, gc_content))  # Clamp between 30-70%
            phred_q20 = round(random.uniform(85, 99), 2)  # % bases with Q>=20
            phred_q30 = round(random.uniform(75, 95), 2)  # % bases with Q>=30
            duplication_rate = round(random.uniform(5, 25), 2) if qc_pass else round(random.uniform(25, 60), 2)
            
            # 2. Alignment Metrics
            mapping_quality = round(random.uniform(55, 60), 2) if qc_pass else round(random.uniform(30, 55), 2)
            properly_paired_pct = round(random.uniform(90, 99), 2) if qc_pass else round(random.uniform(60, 90), 2)
            on_target_pct = round(random.uniform(70, 95), 2) if qc_pass else round(random.uniform(30, 70), 2)
            mean_insert_size = int(random.normalvariate(350, 50))  # Typical paired-end insert size
            mean_insert_size = max(150, min(600, mean_insert_size))
            insert_size_std = int(mean_insert_size * random.uniform(0.15, 0.25))
            
            # 3. Coverage Metrics
            mean_coverage_depth = round(coverage * random.uniform(0.9, 1.1), 2)
            pct_bases_1x = round(random.uniform(95, 99.9), 2) if qc_pass else round(random.uniform(70, 95), 2)
            pct_bases_10x = round(random.uniform(85, 98), 2) if qc_pass else round(random.uniform(50, 85), 2)
            pct_bases_20x = round(random.uniform(75, 95), 2) if qc_pass else round(random.uniform(30, 75), 2)
            pct_bases_30x = round(random.uniform(60, 90), 2) if qc_pass else round(random.uniform(20, 60), 2)
            coverage_uniformity = round(random.uniform(0.75, 0.95), 2) if qc_pass else round(random.uniform(0.4, 0.75), 2)
            
            # 4. Variant Metrics
            # SNPs typically 3-4 million per individual
            num_snps = int(random.normalvariate(3500000, 500000))
            num_snps = max(2000000, min(5000000, num_snps))
            
            # INDELs typically 10-15% of SNPs
            num_indels = int(num_snps * random.uniform(0.10, 0.15))
            
            # Structural variants - much rarer
            num_structural_variants = int(random.normalvariate(8000, 2000))
            num_structural_variants = max(3000, min(15000, num_structural_variants))
            
            # Ti/Tv ratio should be around 2.0-2.1 for whole genome
            titv_ratio = round(random.normalvariate(2.05, 0.1), 2)
            titv_ratio = max(1.8, min(2.3, titv_ratio))
            
            # Novel variants (not in dbSNP) - typically 0.1-1%
            novel_variants = int(num_snps * random.uniform(0.001, 0.01))
            
            # 5. Demographic/Clinical Information
            gender = random.choice(genders)
            age = random.randint(18, 85)
            family_id = f"FAM{random.randint(1000, 9999)}" if random.random() > 0.7 else None
            relationship = random.choice(['Proband', 'Father', 'Mother', 'Sibling', 'Child', 'Unrelated']) if family_id else 'Unrelated'
            phenotype = random.choice(phenotypes)
            
            # 6. Technical Information
            run_date = (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
            sequencer_id = f"SEQ{random.randint(1000, 9999)}"
            flow_cell_id = f"FC{random.randint(100000, 999999)}"
            lane_number = random.randint(1, 8)
            barcode = f"ATCG{random.randint(1000, 9999)}"
            
            # 7. Pipeline Metrics
            processing_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            pipeline_version = f"v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 20)}"
            reference_genome = random.choice(reference_genomes)
            aligner_used = random.choice(aligners)
            variant_caller = random.choice(variant_callers)
            
            # 8. Contamination Metrics
            contamination_estimate = round(random.uniform(0, 0.05), 4) if qc_pass else round(random.uniform(0.05, 0.15), 4)
            freemix_score = round(random.uniform(0, 0.03), 4) if qc_pass else round(random.uniform(0.03, 0.10), 4)
            
            batch_data.append({
                'Sample': sample_id,
                'Population': pop,
                'Center': random.choice(centers),
                'Platform': random.choice(platforms),
                'Total Sequence': f"{total_sequence:,}",
                'Aligned Non Duplicated Coverage': round(coverage, 2),
                'Passed QC': qc_pass,
                'step2_Center': random.choice(centers),
                'step2_Platform': random.choice(platforms),
                'step2_Total Sequence': f"{step2_total_seq:,}",
                'step2_% Targets Covered to 20x or greater': round(step2_coverage_pct, 2),
                'step2_Passed QC': step2_qc,
                'step2_In Final Phase Variant Calling': step2_qc,
                
                # 1. Sequencing Quality Metrics
                'Read_Length': read_length,
                'GC_Content_Pct': gc_content,
                'Phred_Q20_Pct': phred_q20,
                'Phred_Q30_Pct': phred_q30,
                'Duplication_Rate_Pct': duplication_rate,
                
                # 2. Alignment Metrics
                'Mapping_Quality': mapping_quality,
                'Properly_Paired_Pct': properly_paired_pct,
                'On_Target_Pct': on_target_pct,
                'Mean_Insert_Size': mean_insert_size,
                'Insert_Size_StdDev': insert_size_std,
                
                # 3. Coverage Metrics
                'Mean_Coverage_Depth': mean_coverage_depth,
                'Pct_Bases_1x': pct_bases_1x,
                'Pct_Bases_10x': pct_bases_10x,
                'Pct_Bases_20x': pct_bases_20x,
                'Pct_Bases_30x': pct_bases_30x,
                'Coverage_Uniformity': coverage_uniformity,
                
                # 4. Variant Metrics
                'Num_SNPs': num_snps,
                'Num_INDELs': num_indels,
                'Num_Structural_Variants': num_structural_variants,
                'TiTv_Ratio': titv_ratio,
                'Novel_Variants': novel_variants,
                
                # 5. Demographic/Clinical Information
                'Gender': gender,
                'Age': age,
                'Family_ID': family_id,
                'Relationship': relationship,
                'Phenotype': phenotype,
                
                # 6. Technical Information
                'Run_Date': run_date,
                'Sequencer_ID': sequencer_id,
                'Flow_Cell_ID': flow_cell_id,
                'Lane_Number': lane_number,
                'Barcode': barcode,
                
                # 7. Pipeline Metrics
                'Processing_Date': processing_date,
                'Pipeline_Version': pipeline_version,
                'Reference_Genome': reference_genome,
                'Aligner_Used': aligner_used,
                'Variant_Caller': variant_caller,
                
                # 8. Contamination Metrics
                'Contamination_Estimate': contamination_estimate,
                'Freemix_Score': freemix_score
            })
        
        return pd.DataFrame(batch_data)
    
    
    