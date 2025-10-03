import time
import random
import pandas as pd
import threading
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

    def generate_threaded_kafka_stream(self, kafka_producer, kafka_topic, partition_number=None, records_per_thread=10000):
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
            batch_size = 1000 
            
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
        Fast batch generation optimized for high performance
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
    
    
    