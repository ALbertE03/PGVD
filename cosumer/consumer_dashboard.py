#!/usr/bin/env python3
"""
🧬 DASHBOARD DE ANÁLISIS GENÓMICO EN TIEMPO REAL
Consume datos de Kafka y visualiza el progreso de generación de familias
"""

import gradio as gr
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import threading
import time
from collections import defaultdict, Counter
from datetime import datetime
import queue

# ==================== CONFIGURACIÓN ====================
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['genomic-fathers', 'genomic-mothers', 'genomic-children']

# ==================== ANALIZADOR DE DATOS ====================
class GenomicAnalyzer:
    def __init__(self):
        # Datos por familia
        self.families = defaultdict(lambda: {
            'father': {'snps': [], 'chromosomes': Counter(), 'genotypes': Counter(), 'start_time': None, 'count': 0},
            'mother': {'snps': [], 'chromosomes': Counter(), 'genotypes': Counter(), 'start_time': None, 'count': 0},
            'children': defaultdict(lambda: {'snps': [], 'chromosomes': Counter(), 'genotypes': Counter(), 'start_time': None, 'count': 0}),
            'status': 'in_progress',
            'start_time': None,
            'end_time': None,
            'num_children': 0,
            'children_ids': set()
        })
        
        # Estadísticas globales
        self.total_messages = 0
        self.messages_by_type = Counter()
        self.completed_families = []
        self.start_time = datetime.now()
        self.snps_history = []  # Para gráfico temporal
        self.families_timeline = []  # Timeline de familias
        
        # Lock para thread-safe
        self.lock = threading.Lock()
        
        # Buffer para procesamiento en lote
        self.message_buffer = []
        self.buffer_lock = threading.Lock()
        self.MAX_BUFFER_SIZE = 1000
        
    def add_to_buffer(self, data, topic):
        """Agrega mensaje al buffer para procesamiento por lotes"""
        with self.buffer_lock:
            self.message_buffer.append((data, topic))
            
            # Procesar cuando el buffer está lleno
            if len(self.message_buffer) >= self.MAX_BUFFER_SIZE:
                self._process_buffer()
    
    def _process_buffer(self):
        """Procesa todos los mensajes en el buffer de una sola vez"""
        if not self.message_buffer:
            return
        
        # Obtener mensajes y limpiar buffer
        messages = self.message_buffer.copy()
        self.message_buffer.clear()
        
        # Procesar en lote (fuera del buffer_lock)
        current_time = datetime.now()
        cutoff_time = current_time.timestamp() - 60
        
        with self.lock:
            # Procesar todos los mensajes
            for data, topic in messages:
                self.total_messages += 1
                
                # Agregar a histórico
                self.snps_history.append({
                    'timestamp': current_time,
                    'count': 1
                })
                
                # Verificar si es token de completación
                if data.get('message_type') == 'FAMILY_COMPLETE':
                    family_id = data['family_id']
                    self.families[family_id]['status'] = 'completed'
                    self.families[family_id]['end_time'] = current_time
                    
                    completion_info = {
                        'family_id': family_id,
                        'timestamp': current_time,
                        'total_snps': data.get('total_snps', 0),
                        'num_children': data.get('num_children', 0),
                        'father_snps': data.get('father_snps', 0),
                        'mother_snps': data.get('mother_snps', 0),
                        'children_snps': data.get('children_snps', 0)
                    }
                    self.completed_families.append(completion_info)
                    
                    self.families_timeline.append({
                        'timestamp': current_time,
                        'family_id': family_id,
                        'status': 'completed',
                        'total_snps': data.get('total_snps', 0)
                    })
                    
                    continue
                
                # Extraer información del mensaje
                family_id = data.get('family_id', 'UNKNOWN')
                person_id = data.get('person_id', '')
                
                # Los datos vienen en estructura anidada snp_data
                snp_data = data.get('snp_data', {})
                chromosome = snp_data.get('chromosome', data.get('chromosome', ''))
                genotype = snp_data.get('genotype', data.get('genotype', ''))
                
                # Inicializar familia si es nueva
                if self.families[family_id]['start_time'] is None:
                    self.families[family_id]['start_time'] = current_time
                
                # Determinar tipo de miembro
                if '_F' in person_id:
                    member_data = self.families[family_id]['father']
                    self.messages_by_type['Padre'] += 1
                elif '_M' in person_id:
                    member_data = self.families[family_id]['mother']
                    self.messages_by_type['Madre'] += 1
                elif '_C' in person_id:
                    child_num = person_id.split('_C')[1]
                    member_data = self.families[family_id]['children'][child_num]
                    self.messages_by_type[f'Hijo {child_num}'] += 1
                    self.families[family_id]['children_ids'].add(person_id)
                    self.families[family_id]['num_children'] = len(self.families[family_id]['children_ids'])
                else:
                    continue
                
                # Inicializar tiempo de inicio del miembro
                if member_data['start_time'] is None:
                    member_data['start_time'] = current_time
                
                # Actualizar contadores
                member_data['count'] += 1
                member_data['chromosomes'][chromosome] += 1
                member_data['genotypes'][genotype] += 1
            
            # Limpiar histórico antiguo (una sola vez al final)
            self.snps_history = [h for h in self.snps_history if h['timestamp'].timestamp() > cutoff_time]
    
    def process_message(self, data, topic):
        """Procesa un mensaje de Kafka (mantener para compatibilidad)"""
        self.add_to_buffer(data, topic)
    
    def flush_buffer(self):
        """Forzar procesamiento del buffer restante"""
        with self.buffer_lock:
            if self.message_buffer:
                self._process_buffer()
    
    def get_summary(self):
        """Genera resumen de estadísticas"""
        with self.lock:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            
            return {
                'total_messages': self.total_messages,
                'messages_per_second': self.total_messages / elapsed if elapsed > 0 else 0,
                'families_in_progress': len([f for f, data in self.families.items() if data['status'] == 'in_progress']),
                'families_completed': len(self.completed_families),
                'elapsed_seconds': elapsed
            }

# Instancia global del analizador
analyzer = GenomicAnalyzer()

# ==================== CONSUMER DE KAFKA ====================
consumer_running = False
consumer_thread = None

def start_kafka_consumer():
    """Inicia el consumer de Kafka en un hilo separado"""
    global consumer_running
    
    print("🚀 Iniciando consumer de Kafka...")
    
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='genomic-dashboard-group',
            max_poll_records=1000,  # Leer hasta 1000 mensajes por poll
            fetch_min_bytes=1024,  # Esperar al menos 1KB
            fetch_max_wait_ms=500   # Máximo 500ms de espera
        )
        
        print(f"✅ Consumer conectado a {KAFKA_BROKER}")
        print(f"📡 Escuchando topics: {TOPICS}")
        print(f"⚡ Optimizado: max_poll_records=1000, batch processing activado")
        
        consumer_running = True
        
        while consumer_running:
            messages = consumer.poll(timeout_ms=1000, max_records=1000)
            
            # Procesar todos los mensajes del poll
            for topic_partition, records in messages.items():
                for message in records:
                    analyzer.add_to_buffer(message.value, message.topic)
            
            # Forzar procesamiento cada 500ms si hay datos pendientes
            analyzer.flush_buffer()
            
            time.sleep(0.01)
        
        # Procesar mensajes restantes antes de cerrar
        analyzer.flush_buffer()
        consumer.close()
        print("⏹️  Consumer detenido")
        
    except Exception as e:
        print(f"❌ Error en consumer: {e}")
        import traceback
        traceback.print_exc()

def toggle_consumer():
    """Inicia o detiene el consumer"""
    global consumer_running, consumer_thread
    
    if not consumer_running:
        consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
        consumer_thread.start()
        return "⏹️ Detener Consumer", "🟢 Consumer activo - Recibiendo datos..."
    else:
        consumer_running = False
        if consumer_thread:
            consumer_thread.join(timeout=2)
        return "▶️ Iniciar Consumer", "🔴 Consumer detenido"

# ==================== FUNCIONES DE VISUALIZACIÓN ====================

def get_statistics_summary():
    """Genera resumen de estadísticas en HTML"""
    summary = analyzer.get_summary()
    
    # Calcular throughput de SNPs por segundo
    snps_per_second = summary['messages_per_second']
    
    html = f"""
    <div style="font-family: Arial; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; color: white;">
        <h2 style="margin: 0; text-align: center;">📊 ESTADÍSTICAS GLOBALES</h2>
        <hr style="border-color: rgba(255,255,255,0.3);">
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-top: 20px;">
            <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px;">
                <h3 style="margin: 0; font-size: 16px;">📨 Total Mensajes</h3>
                <p style="font-size: 32px; font-weight: bold; margin: 10px 0;">{summary['total_messages']:,}</p>
            </div>
            <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px;">
                <h3 style="margin: 0; font-size: 16px;">⚡ Throughput</h3>
                <p style="font-size: 32px; font-weight: bold; margin: 10px 0;">{snps_per_second:,.0f}</p>
                <p style="font-size: 14px; margin: 0;">SNPs/segundo</p>
            </div>
            <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px;">
                <h3 style="margin: 0; font-size: 16px;">🏠 En Progreso</h3>
                <p style="font-size: 32px; font-weight: bold; margin: 10px 0;">{summary['families_in_progress']}</p>
            </div>
            <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px;">
                <h3 style="margin: 0; font-size: 16px;">✅ Completadas</h3>
                <p style="font-size: 32px; font-weight: bold; margin: 10px 0;">{summary['families_completed']}</p>
            </div>
        </div>
        <div style="text-align: center; margin-top: 20px; font-size: 14px; opacity: 0.8;">
            ⏱️ Tiempo transcurrido: {summary['elapsed_seconds']/60:.1f} minutos
        </div>
    </div>
    """
    
    return html

def plot_messages_distribution():
    """Gráfico de distribución de mensajes por tipo"""
    if not analyzer.messages_by_type:
        fig = go.Figure()
        fig.add_annotation(text="No hay datos aún", showarrow=False, font=dict(size=20))
        return fig
    
    # Crear figura con subplots
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "bar"}, {"type": "pie"}]],
        subplot_titles=("SNPs por Tipo de Miembro", "Distribución Porcentual")
    )
    
    # Datos
    types = list(analyzer.messages_by_type.keys())
    counts = list(analyzer.messages_by_type.values())
    
    # Colores
    colors_map = {
        'Padre': 'steelblue',
        'Madre': 'coral',
    }
    colors = []
    for t in types:
        if 'Padre' in t:
            colors.append('steelblue')
        elif 'Madre' in t:
            colors.append('coral')
        else:
            colors.append('lightgreen')
    
    # Gráfico de barras
    fig.add_trace(
        go.Bar(x=types, y=counts, marker_color=colors, name="SNPs", showlegend=False),
        row=1, col=1
    )
    
    # Gráfico de pastel
    fig.add_trace(
        go.Pie(labels=types, values=counts, marker_colors=colors, showlegend=True),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text="📊 Distribución de SNPs por Tipo de Miembro",
        title_font_size=18,
        height=400
    )
    
    return fig

def plot_throughput_timeline():
    """Gráfico de throughput en tiempo real"""
    if not analyzer.snps_history:
        fig = go.Figure()
        fig.add_annotation(text="Esperando datos...", showarrow=False, font=dict(size=20))
        return fig
    
    # Agrupar por segundo
    throughput_data = defaultdict(int)
    for entry in analyzer.snps_history:
        second = entry['timestamp'].replace(microsecond=0)
        throughput_data[second] += entry['count']
    
    # Ordenar por tiempo
    sorted_times = sorted(throughput_data.keys())
    sorted_counts = [throughput_data[t] for t in sorted_times]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=sorted_times,
        y=sorted_counts,
        mode='lines+markers',
        name='SNPs/segundo',
        line=dict(color='#667eea', width=3),
        fill='tozeroy',
        fillcolor='rgba(102, 126, 234, 0.2)'
    ))
    
    fig.update_layout(
        title="⚡ Throughput de SNPs en Tiempo Real (últimos 60s)",
        xaxis_title="Tiempo",
        yaxis_title="SNPs por segundo",
        height=400,
        hovermode='x unified'
    )
    
    return fig

def get_families_in_progress():
    """Tabla HTML de familias en progreso"""
    with analyzer.lock:
        active_families = [(fid, data) for fid, data in analyzer.families.items() 
                          if data['status'] == 'in_progress']
    
    if not active_families:
        return "<div style='text-align: center; padding: 40px; font-size: 18px; color: #888;'>No hay familias en progreso</div>"
    
    html = """
    <div style="font-family: Arial; padding: 20px;">
        <h2 style="color: #667eea;">🏠 Familias en Progreso</h2>
        <table style="width: 100%; border-collapse: collapse; margin-top: 20px;">
            <thead>
                <tr style="background: #667eea; color: white;">
                    <th style="padding: 12px; text-align: left;">Family ID</th>
                    <th style="padding: 12px; text-align: center;">👨 Padre</th>
                    <th style="padding: 12px; text-align: center;">👩 Madre</th>
                    <th style="padding: 12px; text-align: center;">👶 Hijos</th>
                    <th style="padding: 12px; text-align: center;">📊 Total</th>
                    <th style="padding: 12px; text-align: center;">🧬 Cromosomas</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for idx, (fid, fdata) in enumerate(active_families[:10]):  # Mostrar primeras 10
        bg_color = "#f8f9fa" if idx % 2 == 0 else "white"
        
        father_snps = fdata['father']['count']
        mother_snps = fdata['mother']['count']
        children_snps = sum(c['count'] for c in fdata['children'].values())
        total_snps = father_snps + mother_snps + children_snps
        num_children = len(fdata['children_ids'])
        
        # Cromosomas únicos
        all_chroms = set()
        all_chroms.update(fdata['father']['chromosomes'].keys())
        all_chroms.update(fdata['mother']['chromosomes'].keys())
        for c in fdata['children'].values():
            all_chroms.update(c['chromosomes'].keys())
        
        html += f"""
            <tr style="background: {bg_color};">
                <td style="padding: 10px; border-bottom: 1px solid #ddd; font-family: monospace;">{fid[-12:]}...</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{father_snps:,}</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{mother_snps:,}</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{num_children} ({children_snps:,})</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center; font-weight: bold;">{total_snps:,}</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{len(all_chroms)}</td>
            </tr>
        """
    
    html += """
            </tbody>
        </table>
    </div>
    """
    
    return html

def get_completed_families_table():
    """Tabla de familias completadas"""
    if not analyzer.completed_families:
        return "<div style='text-align: center; padding: 40px; font-size: 18px; color: #888;'>No hay familias completadas aún</div>"
    
    html = """
    <div style="font-family: Arial; padding: 20px;">
        <h2 style="color: #28a745;">✅ Familias Completadas</h2>
        <table style="width: 100%; border-collapse: collapse; margin-top: 20px;">
            <thead>
                <tr style="background: #28a745; color: white;">
                    <th style="padding: 12px; text-align: left;">Family ID</th>
                    <th style="padding: 12px; text-align: center;">👨 Padre</th>
                    <th style="padding: 12px; text-align: center;">👩 Madre</th>
                    <th style="padding: 12px; text-align: center;">👶 Hijos</th>
                    <th style="padding: 12px; text-align: center;">📊 Total SNPs</th>
                    <th style="padding: 12px; text-align: center;">⏱️ Timestamp</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for idx, comp in enumerate(analyzer.completed_families[-20:]):  # Últimas 20
        bg_color = "#f8f9fa" if idx % 2 == 0 else "white"
        
        html += f"""
            <tr style="background: {bg_color};">
                <td style="padding: 10px; border-bottom: 1px solid #ddd; font-family: monospace;">{comp['family_id'][-12:]}...</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{comp['father_snps']:,}</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{comp['mother_snps']:,}</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{comp['num_children']} ({comp['children_snps']:,})</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center; font-weight: bold; color: #28a745;">{comp['total_snps']:,}</td>
                <td style="padding: 10px; border-bottom: 1px solid #ddd; text-align: center;">{comp['timestamp'].strftime('%H:%M:%S')}</td>
            </tr>
        """
    
    html += """
            </tbody>
        </table>
    </div>
    """
    
    return html

def plot_chromosome_distribution():
    """Gráfico de distribución de SNPs por cromosoma"""
    with analyzer.lock:
        if not analyzer.families:
            fig = go.Figure()
            fig.add_annotation(text="No hay datos de cromosomas aún", showarrow=False, font=dict(size=20))
            return fig
        
        # Obtener primera familia con datos
        family_id = list(analyzer.families.keys())[0]
        family_data = analyzer.families[family_id]
    
    # Recopilar datos de cromosomas
    all_chromosomes = set()
    chromosome_data = {}
    
    # Padre
    if family_data['father']['chromosomes']:
        chromosome_data['Padre'] = family_data['father']['chromosomes']
        all_chromosomes.update(family_data['father']['chromosomes'].keys())
    
    # Madre
    if family_data['mother']['chromosomes']:
        chromosome_data['Madre'] = family_data['mother']['chromosomes']
        all_chromosomes.update(family_data['mother']['chromosomes'].keys())
    
    # Hijos
    for child_num, cdata in family_data['children'].items():
        if cdata['chromosomes']:
            chromosome_data[f'Hijo {child_num}'] = cdata['chromosomes']
            all_chromosomes.update(cdata['chromosomes'].keys())
    
    if not all_chromosomes:
        fig = go.Figure()
        fig.add_annotation(text="Esperando datos de cromosomas...", showarrow=False, font=dict(size=20))
        return fig
    
    # Ordenar cromosomas
    def chrom_sort_key(c):
        if c.isdigit():
            return (0, int(c))
        else:
            return (1, c)
    
    sorted_chroms = sorted(list(all_chromosomes), key=chrom_sort_key)
    
    # Crear gráfico
    fig = go.Figure()
    
    colors = {
        'Padre': 'steelblue',
        'Madre': 'coral',
    }
    
    for idx, (member, chroms) in enumerate(chromosome_data.items()):
        counts = [chroms.get(c, 0) for c in sorted_chroms]
        color = colors.get(member, f'rgb({100 + idx*30}, {200 - idx*30}, {100 + idx*20})')
        
        fig.add_trace(go.Bar(
            name=member,
            x=sorted_chroms,
            y=counts,
            marker_color=color
        ))
    
    fig.update_layout(
        title=f"🧬 Distribución de SNPs por Cromosoma - Familia {family_id[-12:]}",
        xaxis_title="Cromosoma",
        yaxis_title="Número de SNPs",
        barmode='group',
        height=500,
        hovermode='x unified'
    )
    
    return fig

def plot_genotype_distribution():
    """Gráfico de top genotipos"""
    with analyzer.lock:
        if not analyzer.families:
            fig = go.Figure()
            fig.add_annotation(text="No hay datos de genotipos aún", showarrow=False, font=dict(size=20))
            return fig
        
        family_id = list(analyzer.families.keys())[0]
        family_data = analyzer.families[family_id]
    
    # Recopilar top genotipos del padre
    if not family_data['father']['genotypes']:
        fig = go.Figure()
        fig.add_annotation(text="Esperando datos de genotipos...", showarrow=False, font=dict(size=20))
        return fig
    
    top_genotypes = family_data['father']['genotypes'].most_common(15)
    genotypes = [g[0] for g in top_genotypes]
    counts = [g[1] for g in top_genotypes]
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=genotypes,
        x=counts,
        orientation='h',
        marker_color='steelblue',
        text=counts,
        textposition='outside',
        texttemplate='%{text:,}'
    ))
    
    fig.update_layout(
        title=f"🧬 Top 15 Genotipos - Padre (Familia {family_id[-12:]})",
        xaxis_title="Frecuencia",
        yaxis_title="Genotipo",
        height=600,
        showlegend=False
    )
    
    return fig

def plot_families_timeline():
    """Timeline de familias completadas"""
    if not analyzer.completed_families:
        fig = go.Figure()
        fig.add_annotation(text="Esperando familias completadas...", showarrow=False, font=dict(size=20))
        return fig
    
    df = pd.DataFrame(analyzer.completed_families)
    df['family_short'] = df['family_id'].str[-8:]
    df['minutes'] = (df['timestamp'] - analyzer.start_time).dt.total_seconds() / 60
    
    # Acumulativo
    df['cumulative'] = range(1, len(df) + 1)
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Timeline Acumulativo", "Total SNPs por Familia")
    )
    
    # Timeline
    fig.add_trace(
        go.Scatter(
            x=df['minutes'],
            y=df['cumulative'],
            mode='lines+markers',
            name='Familias',
            line=dict(color='#28a745', width=3),
            marker=dict(size=10),
            fill='tozeroy'
        ),
        row=1, col=1
    )
    
    # SNPs
    fig.add_trace(
        go.Bar(
            x=df['family_short'],
            y=df['total_snps'],
            name='SNPs',
            marker_color='coral',
            text=df['total_snps'],
            texttemplate='%{text:,}',
            textposition='outside'
        ),
        row=2, col=1
    )
    
    fig.update_xaxes(title_text="Tiempo (minutos)", row=1, col=1)
    fig.update_yaxes(title_text="Familias Completadas", row=1, col=1)
    fig.update_xaxes(title_text="Family ID", row=2, col=1)
    fig.update_yaxes(title_text="Total SNPs", row=2, col=1)
    
    fig.update_layout(
        title_text="📈 Timeline de Familias Completadas",
        height=700,
        showlegend=False
    )
    
    return fig

def update_all_data():
    """Actualiza todos los componentes del dashboard"""
    return (
        get_statistics_summary(),
        plot_messages_distribution(),
        plot_throughput_timeline(),
        get_families_in_progress(),
        plot_chromosome_distribution(),
        plot_genotype_distribution(),
        get_completed_families_table(),
        plot_families_timeline()
    )

# ==================== INTERFAZ GRADIO ====================

def create_dashboard():
    """Crea el dashboard de Gradio"""
    
    with gr.Blocks(title="🧬 Genomic Dashboard", theme=gr.themes.Soft()) as dashboard:
        gr.Markdown("""
        # 🧬 DASHBOARD DE ANÁLISIS GENÓMICO EN TIEMPO REAL
        ### Monitoreo de generación de familias genómicas con Kafka
        """)
        
        with gr.Row():
            start_btn = gr.Button("▶️ Iniciar Consumer", variant="primary", size="lg")
            status_text = gr.Textbox(label="Estado", value="🔴 Consumer detenido", interactive=False)
        
        # Tab de estadísticas globales
        with gr.Tab("📊 Estadísticas"):
            stats_html = gr.HTML(label="Resumen")
            
            with gr.Row():
                messages_plot = gr.Plot(label="Distribución de Mensajes")
                throughput_plot = gr.Plot(label="Throughput")
        
        # Tab de familias en progreso
        with gr.Tab("🏠 Familias en Progreso"):
            families_progress_html = gr.HTML(label="Familias Activas")
            chromosome_plot = gr.Plot(label="Distribución Cromosómica")
        
        # Tab de análisis de genotipos
        with gr.Tab("🧬 Genotipos"):
            genotype_plot = gr.Plot(label="Top Genotipos")
        
        # Tab de familias completadas
        with gr.Tab("✅ Familias Completadas"):
            completed_html = gr.HTML(label="Tabla de Completadas")
            timeline_plot = gr.Plot(label="Timeline")
        
        # Botón de actualización
        with gr.Row():
            refresh_btn = gr.Button("🔄 Actualizar Dashboard", variant="secondary", size="lg")
        
        # Eventos
        start_btn.click(
            fn=toggle_consumer,
            outputs=[start_btn, status_text]
        )
        
        refresh_btn.click(
            fn=update_all_data,
            outputs=[
                stats_html,
                messages_plot,
                throughput_plot,
                families_progress_html,
                chromosome_plot,
                genotype_plot,
                completed_html,
                timeline_plot
            ]
        )
        
        # Auto-refresh cada 5 segundos
        dashboard.load(
            fn=update_all_data,
            outputs=[
                stats_html,
                messages_plot,
                throughput_plot,
                families_progress_html,
                chromosome_plot,
                genotype_plot,
                completed_html,
                timeline_plot
            ],
            every=5
        )
    
    return dashboard

# ==================== MAIN ====================

if __name__ == "__main__":
    print("="*80)
    print("🧬 GENOMIC DASHBOARD - INICIANDO")
    print("="*80)
    
    dashboard = create_dashboard()
    dashboard.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False
    )
