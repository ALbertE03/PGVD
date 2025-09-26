import time
import random
import pandas as pd
import threading
import queue
class GenomicGenerator:


    def __init__(self, num_threads: int = 4,path_to_csv:str="data/20130606_sample_info.csv"):
        self.data=pd.read_csv(path_to_csv)
        self.num_threads=num_threads
        self.lock = threading.Lock()
        self.queue = queue.Queue()
        self.total_generated = 0
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
            print(f"❌ Error processing data: {e}")
            import traceback
            traceback.print_exc()

    def generate_threaded_samples(self, total_samples: int = 10000, samples_per_thread: int = 1000):
        """
        🚀 Generate MASSIVE samples using high-performance concurrent threads
        
        Args:
            total_samples: Total number of samples to generate
            samples_per_thread: How many samples each thread should generate
        """
      
        results_queue = queue.Queue()
        threads = []
        samples_per_actual_thread = total_samples // self.num_threads
        
        def high_performance_worker(thread_id, num_to_generate):
            """High-performance worker function for each thread"""
            thread_start = time.time()
           
            try:
                batch_size = min(500, num_to_generate)  
                all_thread_samples = []
                generated_count = 0
                
                while generated_count < num_to_generate:
                    remaining = num_to_generate - generated_count
                    current_batch = min(batch_size, remaining)
                    
                    batch_samples = self._generate_sample_batch_fast(current_batch, thread_id)
                    with self.lock:
                        self.total_generated += current_batch
                    all_thread_samples.append(batch_samples)
                    generated_count += current_batch
                    
                    if generated_count % 1000 == 0:
                        elapsed = time.time() - thread_start
                        rate = generated_count / elapsed if elapsed > 0 else 0
                        print(f"🔥 THREAD-{thread_id}: {generated_count}/{num_to_generate} ({rate:.0f}/sec)")
                
                combined_thread_data = pd.concat(all_thread_samples, ignore_index=True)
                
                thread_time = time.time() - thread_start
                thread_rate = len(combined_thread_data) / thread_time if thread_time > 0 else 0
                
                results_queue.put({
                    'thread_id': thread_id,
                    'samples': combined_thread_data,
                    'count': len(combined_thread_data),
                    'time': thread_time,
                    'rate': thread_rate
                })
                
            except Exception as e:
                print(f"❌ THREAD-{thread_id} ERROR: {e}")
                import traceback
                traceback.print_exc()
                results_queue.put({
                    'thread_id': thread_id,
                    'samples': pd.DataFrame(),
                    'count': 0,
                    'error': str(e)
                })
        
        start_time = time.time()
        print(f"🚀 LAUNCHING {self.num_threads} CONCURRENT THREADS...")
        
        for i in range(self.num_threads):
            samples_for_this_thread = samples_per_actual_thread
            if i == self.num_threads - 1: 
                samples_for_this_thread = total_samples - (samples_per_actual_thread * (self.num_threads - 1))
            
            thread = threading.Thread(
                target=high_performance_worker,
                args=(i, samples_for_this_thread),
                daemon=True,
                name=f"GenomicWorker-{i}"
            )
            threads.append(thread)
            thread.start()
            print(f"🧵 Thread-{i} launched for {samples_for_this_thread} samples")
        
        all_samples = []
        completed_threads = 0
        total_generated = 0
        
        print(f"📊 MONITORING {self.num_threads} CONCURRENT THREADS...")
        
        while completed_threads < self.num_threads:
            try:
               
                timeout = max(60, total_samples // 1000)  
                result = results_queue.get(timeout=timeout)
                completed_threads += 1
                
                if 'error' in result:
                    print(f"⚠️ THREAD-{result['thread_id']} ERROR: {result['error']}")
                else:
                    all_samples.append(result['samples'])
                    total_generated += result['count']
                    
                
            except queue.Empty:
                print("⏰ TIMEOUT - Some threads may still be working...")
                break

        print("🔄 Waiting for all threads to complete...")
        for thread in threads:
            thread.join(timeout=30)  
        
        if all_samples:
            combined_samples = pd.concat(all_samples, ignore_index=True)
            execution_time = time.time() - start_time
            total_rate = len(combined_samples) / execution_time if execution_time > 0 else 0

            
            return combined_samples
        else:
            print("❌ NO SAMPLES GENERATED - All threads failed!")
            return pd.DataFrame()
    
    def _generate_sample_batch_fast(self, batch_size: int, thread_id: int):
        """
        ⚡ Fast batch generation optimized for high performance
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
            
            batch_data.append({
                'Sample': sample_id,
                'Population': pop,
                'Center': random.choice(centers),
                'Platform': random.choice(platforms),
                'Total Sequence': f"{total_sequence:,}",
                'Aligned Non Duplicated Coverage': round(coverage, 2),
                'step2_Passed QC': qc_pass,
                'step2_Center': random.choice(centers),
                'step2_Platform': random.choice(platforms),
                'step2_Total Sequence': f"{step2_total_seq:,}",
                'step2_% Targets Covered to 20x or greater': round(step2_coverage_pct, 2),
                'step2_Passed QC': step2_qc,
                'step2_In Final Phase Variant Calling': step2_qc
            })
        
        return pd.DataFrame(batch_data, columns=self.data.columns)
    
    def generate_kafka_records(self, num_records: int = 1000) -> list:
        """
        🚀 Generate records optimized for Kafka producer (returns list of dicts)
        
        Args:
            num_records: Number of records to generate
            
        Returns:
            List of dictionaries ready for Kafka
        """
     
        
        genomic_df = self.generate_threaded_samples(total_samples=num_records)
        
        kafka_records = []
        for _, row in genomic_df.iterrows():
            record = {
                **row.to_dict(),
            }
            kafka_records.append(record)
        
        return kafka_records
    