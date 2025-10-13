#!/usr/bin/env python3
"""
Genomic Data Consumer Dashboard
Consume datos de Kafka y los visualiza en tiempo real con Gradio
"""

import os
import json
import time
import threading
from datetime import datetime
from collections import deque
from typing import Dict, List, Tuple
import pandas as pd
import gradio as gr
import plotly.graph_objects as go
import plotly.express as px
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from hdfs import InsecureClient
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GenomicConsumer:
    """Consumer de datos genómicos con buffer en memoria y persistencia en HDFS"""
    
    def __init__(self, buffer_size: int = 10000):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'genomic-data')
        self.hdfs_url = os.getenv('HDFS_NAMENODE_URL', 'http://namenode:9870')
        self.hdfs_user = os.getenv('HDFS_USER', 'root')
        
        # Buffer circular para datos en memoria
        self.buffer = deque(maxlen=buffer_size)
        self.buffer_lock = threading.Lock()
        
        # Métricas de consumo
        self.total_consumed = 0
        self.consumption_rate = 0
        self.start_time = time.time()
        self.last_update = time.time()
        self.last_count = 0
        
        # Estado del consumidor
        self.is_running = False
        self.consumer_thread = None
        self.hdfs_client = None
        
        # Estadísticas agregadas
        self.stats = {
            'by_population': {},
            'by_platform': {},
            'by_qc_status': {'Passed': 0, 'Failed': 0},
            'coverage_distribution': [],
            'variant_counts': []
        }
        
    def connect_kafka(self) -> bool:
        """Conecta al broker de Kafka con reintentos"""
        logger.info(f"Conectando a Kafka broker: {self.kafka_broker}")
        
        for attempt in range(30):
            try:
                self.consumer = KafkaConsumer(
                    self.kafka_topic,
                    bootstrap_servers=[self.kafka_broker],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',  # Consumir solo mensajes nuevos
                    enable_auto_commit=True,
                    group_id='genomic-dashboard-consumer',
                    max_poll_records=500,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000
                )
                logger.info("✅ Conectado a Kafka exitosamente")
                return True
                
            except NoBrokersAvailable:
                logger.warning(f"Intento {attempt + 1}/30: Kafka no disponible, reintentando...")
                time.sleep(2)
        
        logger.error("❌ No se pudo conectar a Kafka después de 30 intentos")
        return False
    
    def connect_hdfs(self) -> bool:
        """Conecta al cliente HDFS"""
        try:
            self.hdfs_client = InsecureClient(self.hdfs_url, user=self.hdfs_user)
            
            # Crear directorio base si no existe
            hdfs_path = '/genomic-data'
            if not self.hdfs_client.status(hdfs_path, strict=False):
                self.hdfs_client.makedirs(hdfs_path)
                logger.info(f"📁 Creado directorio HDFS: {hdfs_path}")
            
            logger.info("✅ Conectado a HDFS exitosamente")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ No se pudo conectar a HDFS: {e}")
            return False
    
    def start_consuming(self):
        """Inicia el consumo de mensajes en un thread separado"""
        if self.is_running:
            logger.warning("El consumidor ya está ejecutándose")
            return
        
        if not self.connect_kafka():
            logger.error("No se pudo iniciar el consumidor: fallo en conexión a Kafka")
            return
        
        # Intentar conectar a HDFS (no crítico)
        self.connect_hdfs()
        
        self.is_running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()
        logger.info("🚀 Consumer iniciado")
    
    def stop_consuming(self):
        """Detiene el consumo de mensajes"""
        self.is_running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        if hasattr(self, 'consumer'):
            self.consumer.close()
        logger.info("🛑 Consumer detenido")
    
    def _consume_loop(self):
        """Loop principal de consumo"""
        batch_buffer = []
        batch_size = 100
        last_hdfs_write = time.time()
        hdfs_write_interval = 60  # Escribir a HDFS cada 60 segundos
        
        logger.info("📡 Iniciando loop de consumo...")
        
        try:
            for message in self.consumer:
                if not self.is_running:
                    break
                
                record = message.value
                
                # Agregar al buffer en memoria
                with self.buffer_lock:
                    self.buffer.append(record)
                    self.total_consumed += 1
                
                # Actualizar estadísticas
                self._update_stats(record)
                
                # Agregar al batch para HDFS
                batch_buffer.append(record)
                
                # Escribir a HDFS periódicamente
                if len(batch_buffer) >= batch_size or \
                   (time.time() - last_hdfs_write) > hdfs_write_interval:
                    self._write_to_hdfs(batch_buffer)
                    batch_buffer = []
                    last_hdfs_write = time.time()
                
                # Actualizar tasa de consumo cada 5 segundos
                if time.time() - self.last_update >= 5:
                    elapsed = time.time() - self.last_update
                    self.consumption_rate = (self.total_consumed - self.last_count) / elapsed
                    self.last_count = self.total_consumed
                    self.last_update = time.time()
                    
                    logger.info(f"📊 Consumidos: {self.total_consumed:,} | Tasa: {self.consumption_rate:.0f} msg/s")
        
        except Exception as e:
            logger.error(f"Error en loop de consumo: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # Escribir batch final a HDFS
            if batch_buffer and self.hdfs_client:
                self._write_to_hdfs(batch_buffer)
    
    def _update_stats(self, record: Dict):
        """Actualiza estadísticas agregadas"""
        try:
            # Por población
            pop = record.get('Population', 'Unknown')
            self.stats['by_population'][pop] = self.stats['by_population'].get(pop, 0) + 1
            
            # Por plataforma
            platform = record.get('Platform', 'Unknown')
            self.stats['by_platform'][platform] = self.stats['by_platform'].get(platform, 0) + 1
            
            # Por QC status
            qc_passed = record.get('Passed QC', 0)
            if qc_passed:
                self.stats['by_qc_status']['Passed'] += 1
            else:
                self.stats['by_qc_status']['Failed'] += 1
            
            # Distribución de coverage
            coverage = record.get('Aligned Non Duplicated Coverage', 0)
            if coverage > 0:
                self.stats['coverage_distribution'].append(coverage)
                # Mantener solo últimos 1000 valores
                if len(self.stats['coverage_distribution']) > 1000:
                    self.stats['coverage_distribution'].pop(0)
            
            # Conteo de variantes
            num_snps = record.get('Num_SNPs', 0)
            if num_snps > 0:
                self.stats['variant_counts'].append(num_snps)
                if len(self.stats['variant_counts']) > 1000:
                    self.stats['variant_counts'].pop(0)
        
        except Exception as e:
            logger.warning(f"Error actualizando estadísticas: {e}")
    
    def _write_to_hdfs(self, records: List[Dict]):
        """Escribe batch de registros a HDFS"""
        if not self.hdfs_client or not records:
            return
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            hdfs_path = f'/genomic-data/batch_{timestamp}.json'
            
            with self.hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
                json.dump(records, writer, indent=2)
            
            logger.info(f"💾 Escritos {len(records)} registros a HDFS: {hdfs_path}")
        
        except Exception as e:
            logger.error(f"Error escribiendo a HDFS: {e}")
    
    def get_buffer_df(self, limit: int = 1000) -> pd.DataFrame:
        """Retorna el buffer como DataFrame"""
        with self.buffer_lock:
            data = list(self.buffer)[-limit:]
        
        if not data:
            return pd.DataFrame()
        
        return pd.DataFrame(data)
    
    def get_metrics(self) -> Dict:
        """Retorna métricas actuales del consumidor"""
        elapsed = time.time() - self.start_time
        avg_rate = self.total_consumed / elapsed if elapsed > 0 else 0
        
        return {
            'total_consumed': self.total_consumed,
            'buffer_size': len(self.buffer),
            'consumption_rate': self.consumption_rate,
            'avg_consumption_rate': avg_rate,
            'elapsed_time': elapsed,
            'is_running': self.is_running
        }


# Instancia global del consumer
consumer = GenomicConsumer(buffer_size=10000)


def create_dashboard():
    """Crea el dashboard de Gradio"""
    
    def get_metrics_display():
        """Retorna las métricas formateadas para display"""
        metrics = consumer.get_metrics()
        
        elapsed_min = int(metrics['elapsed_time'] / 60)
        elapsed_sec = int(metrics['elapsed_time'] % 60)
        
        status = "🟢 Activo" if metrics['is_running'] else "🔴 Inactivo"
        
        text = f"""
        ### Estado del Consumer: {status}
        
        **Mensajes Consumidos:** {metrics['total_consumed']:,}  
        **Buffer en Memoria:** {metrics['buffer_size']:,} registros  
        **Tasa Actual:** {metrics['consumption_rate']:.0f} msg/s  
        **Tasa Promedio:** {metrics['avg_consumption_rate']:.0f} msg/s  
        **Tiempo Transcurrido:** {elapsed_min}m {elapsed_sec}s
        """
        
        return text
    
    def get_latest_data(limit: int = 100):
        """Retorna los últimos N registros"""
        df = consumer.get_buffer_df(limit=limit)
        
        if df.empty:
            return pd.DataFrame({'Mensaje': ['No hay datos disponibles aún...']})
        
        # Seleccionar columnas relevantes para mostrar
        display_cols = [
            'Sample', 'Population', 'Platform', 'Passed QC',
            'Aligned Non Duplicated Coverage', 'Num_SNPs', 'Num_INDELs',
            'Mean_Coverage_Depth', 'GC_Content_Pct', 'Mapping_Quality'
        ]
        
        available_cols = [col for col in display_cols if col in df.columns]
        
        return df[available_cols].head(limit)
    
    def create_population_chart():
        """Gráfico de distribución por población"""
        stats = consumer.stats['by_population']
        
        if not stats:
            return go.Figure().add_annotation(
                text="No hay datos aún",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
        
        fig = go.Figure(data=[
            go.Bar(
                x=list(stats.keys()),
                y=list(stats.values()),
                marker_color='#1f77b4'
            )
        ])
        
        fig.update_layout(
            title='Distribución por Población',
            xaxis_title='Población',
            yaxis_title='Cantidad de Muestras',
            height=400
        )
        
        return fig
    
    def create_qc_chart():
        """Gráfico de QC Status"""
        stats = consumer.stats['by_qc_status']
        
        if stats['Passed'] == 0 and stats['Failed'] == 0:
            return go.Figure().add_annotation(
                text="No hay datos aún",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
        
        fig = go.Figure(data=[
            go.Pie(
                labels=['Passed QC', 'Failed QC'],
                values=[stats['Passed'], stats['Failed']],
                marker_colors=['#2ecc71', '#e74c3c'],
                hole=0.3
            )
        ])
        
        fig.update_layout(
            title='Estado de Control de Calidad',
            height=400
        )
        
        return fig
    
    def create_coverage_chart():
        """Histograma de distribución de coverage"""
        coverage_data = consumer.stats['coverage_distribution']
        
        if not coverage_data:
            return go.Figure().add_annotation(
                text="No hay datos aún",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
        
        fig = go.Figure(data=[
            go.Histogram(
                x=coverage_data,
                nbinsx=30,
                marker_color='#3498db'
            )
        ])
        
        fig.update_layout(
            title='Distribución de Coverage',
            xaxis_title='Coverage',
            yaxis_title='Frecuencia',
            height=400
        )
        
        return fig
    
    def create_platform_chart():
        """Gráfico de distribución por plataforma"""
        stats = consumer.stats['by_platform']
        
        if not stats:
            return go.Figure().add_annotation(
                text="No hay datos aún",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
        
        fig = go.Figure(data=[
            go.Bar(
                x=list(stats.keys()),
                y=list(stats.values()),
                marker_color='#9b59b6'
            )
        ])
        
        fig.update_layout(
            title='Distribución por Plataforma de Secuenciación',
            xaxis_title='Plataforma',
            yaxis_title='Cantidad de Muestras',
            height=400
        )
        
        return fig
    
    def refresh_data():
        """Actualiza todos los componentes del dashboard"""
        return (
            get_metrics_display(),
            get_latest_data(100),
            create_population_chart(),
            create_qc_chart(),
            create_coverage_chart(),
            create_platform_chart()
        )
    
    # Crear interfaz de Gradio
    with gr.Blocks(title="Genomic Data Dashboard", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# 🧬 Genomic Data Consumer Dashboard")
        gr.Markdown("Dashboard en tiempo real para análisis de datos genómicos desde Kafka")
        
        with gr.Row():
            with gr.Column(scale=1):
                metrics_display = gr.Markdown(get_metrics_display())
                refresh_btn = gr.Button("🔄 Actualizar Datos", variant="primary")
        
        with gr.Tabs():
            with gr.Tab("📊 Visualizaciones"):
                with gr.Row():
                    with gr.Column():
                        population_chart = gr.Plot(create_population_chart())
                    with gr.Column():
                        qc_chart = gr.Plot(create_qc_chart())
                
                with gr.Row():
                    with gr.Column():
                        coverage_chart = gr.Plot(create_coverage_chart())
                    with gr.Column():
                        platform_chart = gr.Plot(create_platform_chart())
            
            with gr.Tab("📋 Datos Recientes"):
                data_table = gr.Dataframe(
                    get_latest_data(100),
                    label="Últimos 100 registros",
                    wrap=True
                )
            
            with gr.Tab("ℹ️ Información"):
                gr.Markdown("""
                ## Acerca de este Dashboard
                
                Este dashboard consume datos genómicos en tiempo real desde Apache Kafka y los visualiza
                de manera interactiva usando Gradio.
                
                ### Características:
                - ✅ Consumo de datos en tiempo real desde Kafka
                - ✅ Buffer en memoria con los últimos 10,000 registros
                - ✅ Persistencia de datos en HDFS
                - ✅ Visualizaciones interactivas con Plotly
                - ✅ Métricas de rendimiento en tiempo real
                
                ### Componentes:
                - **Kafka Topic:** genomic-data
                - **HDFS Storage:** /genomic-data/
                - **Consumer Group:** genomic-dashboard-consumer
                
                ### Métricas Visualizadas:
                - Distribución por población genética
                - Estado de control de calidad (QC)
                - Distribución de coverage de secuenciación
                - Distribución por plataforma de secuenciación
                
                **Tip:** Usa el botón "Actualizar Datos" para refrescar las visualizaciones.
                """)
        
        # Configurar el refresh button
        refresh_btn.click(
            fn=refresh_data,
            outputs=[
                metrics_display,
                data_table,
                population_chart,
                qc_chart,
                coverage_chart,
                platform_chart
            ]
        )
        
        # Auto-refresh cada 5 segundos
        demo.load(
            fn=refresh_data,
            outputs=[
                metrics_display,
                data_table,
                population_chart,
                qc_chart,
                coverage_chart,
                platform_chart
            ],
            every=5
        )
    
    return demo


if __name__ == "__main__":
    logger.info("🚀 Iniciando Genomic Data Consumer Dashboard...")
    
    # Iniciar el consumer
    consumer.start_consuming()
    
    # Esperar un poco para que empiece a consumir datos
    time.sleep(3)
    
    # Crear y lanzar el dashboard
    demo = create_dashboard()
    
    try:
        demo.launch(
            server_name="0.0.0.0",
            server_port=7860,
            share=False,
            show_error=True
        )
    except KeyboardInterrupt:
        logger.info("Recibido signal de interrupción...")
    finally:
        consumer.stop_consuming()
        logger.info("Dashboard cerrado")
