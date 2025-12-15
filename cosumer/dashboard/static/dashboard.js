// Global chart instances
let charts = {};
let currentGenderType = 'fathers';
let currentChromType = 'fathers';
let currentGenoType = 'fathers';
let currentSnpRangeType = 'fathers';
let currentFamilySizeType = 'fathers';
let individualGeneticsChart;
let populationGeneticsChart;
let populationDiversityChart;


// Get dark theme colors (fixed palette)
function getThemeColors() {
    return {
        gridColor: '#2d3748',
        textColor: '#e2e8f0',
        blue: '#3b82f6',
        red: '#ef4444',
        green: '#10b981',
        amber: '#f59e0b',
        purple: '#8b5cf6',
        cyan: '#06b6d4',
        pink: '#ec4899',
    };
}

// Initialize all charts
function initializeCharts() {
    const colors = getThemeColors();
    
    // Processing Rate - Line Chart
    charts.processing = new Chart(document.getElementById('processingChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Fathers',
                    data: [],
                    borderColor: colors.blue,
                    backgroundColor: colors.blue + '20',
                    tension: 0.4,
                    borderWidth: 3
                },
                {
                    label: 'Mothers',
                    data: [],
                    borderColor: colors.red,
                    backgroundColor: colors.red + '20',
                    tension: 0.4,
                    borderWidth: 3
                },
                {
                    label: 'Children',
                    data: [],
                    borderColor: colors.green,
                    backgroundColor: colors.green + '20',
                    tension: 0.4,
                    borderWidth: 3
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: {
                y: { 
                    beginAtZero: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: { 
                        usePointStyle: true, 
                        padding: 20,
                        color: colors.textColor
                    }
                }
            }
        }
    });

    // CPU Chart
    charts.cpu = new Chart(document.getElementById('cpuChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'CPU %',
                data: [],
                borderColor: colors.amber,
                backgroundColor: colors.amber + '20',
                tension: 0.3,
                fill: true,
                borderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { 
                y: { 
                    beginAtZero: true, 
                    max: 100, 
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                x: { 
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: { 
                legend: { 
                    display: false 
                } 
            },
            animation: false
        }
    });

    // RAM Chart
    charts.ram = new Chart(document.getElementById('ramChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'RAM %',
                data: [],
                borderColor: colors.purple,
                backgroundColor: colors.purple + '20',
                tension: 0.3,
                fill: true,
                borderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { 
                y: { 
                    beginAtZero: true, 
                    max: 100, 
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                x: { 
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: { 
                legend: { 
                    display: false 
                } 
            },
            animation: false
        }
    });

    // Disk Chart - Doughnut con valores en GB
    charts.disk = new Chart(document.getElementById('diskChart'), {
        type: 'doughnut',
        data: {
            labels: ['Used (GB)', 'Free (GB)'],
            datasets: [{
                data: [0, 100],
                backgroundColor: [colors.red, colors.green],
                borderWidth: 0,
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '65%',
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: {
                        font: { size: 11 },
                        padding: 10,
                        color: colors.textColor
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': ' + context.raw.toFixed(2) + ' GB';
                        }
                    }
                }
            }
        }
    });

    // Task Completion Times Chart - Line Chart (conexas por tipo) - Optimizado
    charts.taskTimes = new Chart(document.getElementById('taskTimesChart'), {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Fathers',
                    data: [],
                    borderColor: colors.blue,
                    backgroundColor: colors.blue + '20',
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    pointBackgroundColor: colors.blue,
                    pointBorderColor: colors.blue,
                    pointHoverBackgroundColor: colors.blue,
                    pointHoverBorderColor: '#ffffff',
                    pointHoverBorderWidth: 2,
                    fill: false,
                    segment: {
                        borderColor: ctx => ctx.p0.skip || ctx.p1.skip ? 'transparent' : undefined
                    }
                },
                {
                    label: 'Mothers',
                    data: [],
                    borderColor: colors.red,
                    backgroundColor: colors.red + '20',
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    pointBackgroundColor: colors.red,
                    pointBorderColor: colors.red,
                    pointHoverBackgroundColor: colors.red,
                    pointHoverBorderColor: '#ffffff',
                    pointHoverBorderWidth: 2,
                    fill: false,
                    segment: {
                        borderColor: ctx => ctx.p0.skip || ctx.p1.skip ? 'transparent' : undefined
                    }
                },
                {
                    label: 'Children',
                    data: [],
                    borderColor: colors.green,
                    backgroundColor: colors.green + '20',
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    pointBackgroundColor: colors.green,
                    pointBorderColor: colors.green,
                    pointHoverBackgroundColor: colors.green,
                    pointHoverBorderColor: '#ffffff',
                    pointHoverBorderWidth: 2,
                    fill: false,
                    segment: {
                        borderColor: ctx => ctx.p0.skip || ctx.p1.skip ? 'transparent' : undefined
                    }
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: false, // Deshabilitar animaciones para mejor performance
            interaction: {
                mode: 'nearest',
                intersect: false,
                axis: 'x'
            },
            parsing: false, // Deshabilitar parsing automático
            normalized: true, // Los datos ya vienen normalizados
            scales: {
                y: { 
                    beginAtZero: true,
                    grid: { 
                        color: colors.gridColor,
                        drawTicks: false
                    },
                    ticks: { 
                        color: colors.textColor,
                        maxTicksLimit: 8
                    },
                    title: {
                        display: true,
                        text: 'Tiempo (ms)',
                        color: colors.textColor,
                        font: { size: 12, weight: 'bold' }
                    }
                },
                x: {
                    type: 'linear',
                    grid: { 
                        display: false
                    },
                    ticks: { 
                        color: colors.textColor,
                        maxTicksLimit: 15,
                        autoSkip: true
                    },
                    title: {
                        display: true,
                        text: 'Número de Tarea',
                        color: colors.textColor,
                        font: { size: 12, weight: 'bold' }
                    }
                }
            },
            plugins: {
                legend: { 
                    display: true,
                    position: 'top',
                    labels: { 
                        usePointStyle: true, 
                        padding: 20,
                        color: colors.textColor,
                        font: { size: 13, weight: '600' }
                    }
                },
                tooltip: {
                    enabled: true,
                    mode: 'nearest',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.9)',
                    titleColor: colors.textColor,
                    bodyColor: colors.textColor,
                    borderColor: 'rgba(255, 255, 255, 0.3)',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: true,
                    callbacks: {
                        title: function(context) {
                            const taskNum = Math.round(context[0].parsed.x) + 1;
                            return 'Tarea #' + taskNum;
                        },
                        label: function(context) {
                            const label = context.dataset.label || '';
                            const value = context.parsed.y;
                            return label + ': ' + value.toFixed(2) + ' ms';
                        }
                    }
                },
                decimation: {
                    enabled: true,
                    algorithm: 'lttb',
                    samples: 500
                }
            }
        },
        plugins: [{
            id: 'verticalLineOnHover',
            afterDraw: (chart) => {
                if (chart.tooltip?._active?.length) {
                    const ctx = chart.ctx;
                    const activePoint = chart.tooltip._active[0];
                    const x = activePoint.element.x;
                    const topY = chart.scales.y.top;
                    const bottomY = chart.scales.y.bottom;

                    // Dibujar línea vertical blanca
                    ctx.save();
                    ctx.beginPath();
                    ctx.moveTo(x, topY);
                    ctx.lineTo(x, bottomY);
                    ctx.lineWidth = 1;
                    ctx.strokeStyle = 'rgba(255, 255, 255, 0.5)';
                    ctx.setLineDash([5, 5]);
                    ctx.stroke();
                    ctx.restore();
                }
            }
        }]
    });

}

// Update all data
async function updateDashboard() {
    try {
        // Update stats
        const stats = await fetch('/api/stats').then(r => r.json());
        document.getElementById('fathers-count').textContent = formatNumber(stats.fathers);
        document.getElementById('mothers-count').textContent = formatNumber(stats.mothers);
        document.getElementById('children-count').textContent = formatNumber(stats.children);
        document.getElementById('total-count').textContent = formatNumber(stats.total);
        document.getElementById('batches-count').textContent = stats.batches_processed;
        document.getElementById('families-completed-count').textContent = formatNumber(stats.families_completed);
        document.getElementById('last-update').textContent = new Date(stats.last_update).toLocaleString('es-ES');

        // Update processing history
        const history = await fetch('/api/processing_history').then(r => r.json());
        updateProcessingChart(history.history);

        // Update Cluster Stats
        const cluster = await fetch('/api/cluster_stats').then(r => r.json());
        updateClusterCharts(cluster);
        if (cluster.spark) {
            updateSparkTopology(cluster.spark);
        }
        if (cluster.hdfs) {
            updateHdfsMetrics(cluster.hdfs);
        }
        
        // Update Spark Jobs Performance Metrics
        updateSparkJobs();

        // Update Task Completion Times
        updateTaskTimes();

    } catch (error) {
        console.error('Error updating dashboard:', error);
    }
}

function updateProcessingChart(history) {
    const fathersData = history.filter(h => h.member_type === 'fathers').map(h => h.records);
    const mothersData = history.filter(h => h.member_type === 'mothers').map(h => h.records);
    const childrenData = history.filter(h => h.member_type === 'children').map(h => h.records);
    const labels = history.filter(h => h.member_type === 'fathers').map((h, i) => `B${i + 1}`);

    charts.processing.data.labels = labels;
    charts.processing.data.datasets[0].data = fathersData;
    charts.processing.data.datasets[1].data = mothersData;
    charts.processing.data.datasets[2].data = childrenData;
    charts.processing.update();
}

function updateClusterCharts(data) {
    // Helper to convert bytes to GB
    function bytesToGB(bytes) {
        return bytes / (1024 * 1024 * 1024);
    }

    // Current values text - con validación para evitar NaN
    const cpuValue = (data.current && data.current.cpu && !isNaN(data.current.cpu.value)) 
        ? data.current.cpu.value.toFixed(1) 
        : '0.0';
    const ramValue = (data.current && data.current.ram && !isNaN(data.current.ram.value)) 
        ? data.current.ram.value.toFixed(1) 
        : '0.0';
    const diskValue = (data.current && data.current.disk && !isNaN(data.current.disk.value)) 
        ? data.current.disk.value.toFixed(1) 
        : '0.0';
    
    document.getElementById('cpu-val').textContent = cpuValue;
    document.getElementById('ram-val').textContent = ramValue;
    document.getElementById('disk-val').textContent = diskValue;

    // Update CPU Graph
    if (data.cpu && data.cpu.length > 0) {
        const labels = data.cpu.map(d => new Date(d.timestamp).toLocaleTimeString());
        const values = data.cpu.map(d => isNaN(d.value) ? 0 : d.value);
        charts.cpu.data.labels = labels;
        charts.cpu.data.datasets[0].data = values;
        charts.cpu.update();
    }

    // Update RAM Graph
    if (data.ram && data.ram.length > 0) {
        const labels = data.ram.map(d => new Date(d.timestamp).toLocaleTimeString());
        const values = data.ram.map(d => isNaN(d.value) ? 0 : d.value);
        charts.ram.data.labels = labels;
        charts.ram.data.datasets[0].data = values;
        charts.ram.update();
    }

    // Update Disk Graph with GB values
    if (data.hdfs && data.hdfs.capacity > 0) {
        const usedGB = bytesToGB(data.hdfs.used);
        const freeGB = bytesToGB(data.hdfs.remaining);
        charts.disk.data.labels = [`Used (${usedGB.toFixed(2)} GB)`, `Free (${freeGB.toFixed(2)} GB)`];
        charts.disk.data.datasets[0].data = [usedGB, freeGB];
        charts.disk.update();
    } else if (data.current.disk) {
        // Fallback si no hay datos de HDFS
        charts.disk.data.datasets[0].data = [data.current.disk.value, 100 - data.current.disk.value];
        charts.disk.update();
    }
}

function updateSparkTopology(sparkData) {
    const workersContainer = document.getElementById('spark-topology');
    const mastersContainer = document.getElementById('spark-masters');

    // --- Render Masters ---
    if (sparkData.masters && sparkData.masters.length > 0) {
        let mastersHtml = '';
        sparkData.masters.forEach(m => {
            const isAlive = m.status === 'ALIVE';
            const statusColor = isAlive ? '#10b981' : (m.status === 'STANDBY' ? '#f59e0b' : '#ef4444');
            const isEmergency = m.type === 'WORKER_AS_MASTER';
            const typeLabel = isEmergency ? 'Emergency Master (Worker Failover)' : 'Master';

            mastersHtml += `
                <div style="background:#1a1f2e; border: 2px solid #6b7280; border-top: 4px solid ${statusColor}; border-radius:12px; padding:20px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); backdrop-filter: blur(10px);">
                    <div style="font-weight:700; font-size:1.1em; margin-bottom:5px; color:#e2e8f0;">${m.name}</div>
                    <div style="font-size:0.8em; color:#94a3b8; margin-bottom:4px; font-weight:500;">${typeLabel}</div>
                    <div style="font-size:0.85em; color:#94a3b8; margin-bottom:8px; font-family:monospace;">${m.url || 'No URL'}</div>
                    <div style="font-size:0.9em; font-weight:700; color:${statusColor}; letter-spacing:0.05em; text-shadow: 0 0 10px ${statusColor};">${m.status}</div>
                    ${isAlive ? `<div style="font-size:0.8em; color:#94a3b8; margin-top:8px; border-top:1px solid #6b7280; padding-top:8px;">Workers: <strong style="color:#e2e8f0;">${m.workers_count || 0}</strong></div>` : ''}
                </div>
            `;
        });
        mastersContainer.innerHTML = mastersHtml;
    } else {
        mastersContainer.innerHTML = '<div style="color:#ffffff; font-style:italic;">No masters found</div>';
    }

    // --- Render Workers ---
    if (!sparkData.workers || sparkData.workers.length === 0) {
        workersContainer.innerHTML = '<div style="grid-column: 1/-1; text-align: center; padding: 30px; color:#ffffff; background:#1a1f2e; border-radius:12px; border:2px solid #6b7280;">No Workers Registered (Check Active Master)</div>';
        return;
    }

    let html = '';

    // Workers
    sparkData.workers.sort((a, b) => a.id.localeCompare(b.id)).forEach(w => {
        // Usar valores calculados con seguridad
        const cores = parseInt(w.cores) || 0;
        const coresUsed = parseInt(w.cores_used) || 0;
        const coresFree = parseInt(w.coresfree) || cores - coresUsed;
        const memory = parseInt(w.memory) || 0;
        const memoryUsed = parseInt(w.memory_used) || 0;
        const memoryFree = parseInt(w.memoryfree) || memory - memoryUsed;
        
        // Calcular porcentajes de forma segura
        const coreUsage = cores > 0 ? ((coresUsed / cores) * 100) : 0;
        const memUsage = memory > 0 ? ((memoryUsed / memory) * 100) : 0;
        
        const statusColor = w.state === 'ALIVE' ? '#10b981' : '#ef4444';
        const cpuColor = coreUsage > 80 ? '#ef4444' : (coreUsage > 50 ? '#f59e0b' : '#3b82f6');
        const memColor = memUsage > 80 ? '#ef4444' : (memUsage > 50 ? '#f59e0b' : '#8b5cf6');

        html += `
            <div style="background:#1a1f2e; border: 2px solid #6b7280; border-left: 4px solid ${statusColor}; border-radius:12px; padding:20px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); backdrop-filter: blur(10px);">
                <div style="font-weight:600; font-size:0.95em; margin-bottom:5px; word-break:break-all; color:#e2e8f0;">${w.id}</div>
                <div style="font-size:0.85em; color:#94a3b8; margin-bottom:15px; font-family:monospace;">${w.host}:${w.port}</div>
                
                <div style="margin-bottom:12px;">
                    <div style="display:flex; justify-content:space-between; font-size:0.8em; margin-bottom:4px; color:#94a3b8;">
                        <span style="font-weight:500;">Cores</span>
                        <span style="font-weight:600; color:#e2e8f0;">${coresUsed}/${cores} (${coreUsage.toFixed(1)}%)</span>
                    </div>
                    <div style="height:6px; background:#0f1419; border-radius:3px; overflow:hidden;">
                        <div style="height:100%; width:${coreUsage}%; background:${cpuColor}; transition: width 0.3s; box-shadow: 0 0 10px ${cpuColor};"></div>
                    </div>
                    <div style="font-size:0.75em; color:#9ca3af; margin-top:2px;">Free: ${coresFree} cores</div>
                </div>
                
                <div>
                    <div style="display:flex; justify-content:space-between; font-size:0.8em; margin-bottom:4px; color:#94a3b8;">
                        <span style="font-weight:500;">Memory</span>
                        <span style="font-weight:600; color:#e2e8f0;">${memoryUsed}/${memory} MB (${memUsage.toFixed(1)}%)</span>
                    </div>
                    <div style="height:6px; background:#0f1419; border-radius:3px; overflow:hidden;">
                        <div style="height:100%; width:${memUsage}%; background:${memColor}; transition: width 0.3s; box-shadow: 0 0 10px ${memColor};"></div>
                    </div>
                    <div style="font-size:0.75em; color:#9ca3af; margin-top:2px;">Free: ${memoryFree} MB</div>
                </div>
                
                <div style="margin-top:15px; padding-top:12px; border-top:1px solid #6b7280; font-size:0.8em; color:#94a3b8; display:flex; justify-content:space-between; align-items:center;">
                    <span>Status</span>
                    <span style="color:${statusColor}; font-weight:700; background:${statusColor}25; padding:2px 8px; border-radius:4px; box-shadow: 0 0 10px ${statusColor}50;">${w.state}</span>
                </div>
            </div>
        `;
    });

    workersContainer.innerHTML = html;
}

function updateSparkJobs() {
    fetch('/api/spark_jobs')
        .then(r => r.json())
        .then(data => {
            if (data.error) {
                console.warn('Spark Jobs API error:', data.error);
                return;
            }

            // Helper function to format bytes
            function formatBytes(bytes) {
                if (!bytes || bytes === 0) return '0 B';
                const k = 1024;
                const sizes = ['B', 'KB', 'MB', 'GB'];
                const i = Math.floor(Math.log(bytes) / Math.log(k));
                return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
            }

            // Helper function to format duration
            function formatDuration(ms) {
                if (ms === null || ms === undefined) return 'N/A';
                if (ms === 0) return '< 0.01s';
                const seconds = (ms / 1000);
                if (seconds < 1) return `${ms.toFixed(0)}ms`;
                if (seconds < 60) return `${seconds.toFixed(2)}s`;
                const minutes = Math.floor(seconds / 60);
                const remainingSecs = (seconds % 60).toFixed(0);
                return `${minutes}m ${remainingSecs}s`;
            }

            // Update Application Info
            const appInfo = document.getElementById('spark-app-info');
            if (appInfo && data.appName) {
                appInfo.innerHTML = `
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="color:#3b82f6; font-weight:600;">Application:</span>
                            <span style="color:#e2e8f0; margin-left:8px;">${data.appName}</span>
                        </div>
                        <div>
                            <span style="color:#3b82f6; font-weight:600;">App ID:</span>
                            <span style="color:#94a3b8; margin-left:8px; font-family:monospace; font-size:0.85em;">${data.appId || 'N/A'}</span>
                        </div>
                    </div>
                `;
            }

            // Update Jobs Table
            const jobsTable = document.getElementById('spark-jobs-table');
            if (jobsTable && data.jobs && data.jobs.length > 0) {
                let tableHtml = `
                    <table style="width:100%; border-collapse:collapse; font-size:0.85em;">
                        <thead>
                            <tr style="background:#0f1419; border-bottom:2px solid #2d3748;">
                                <th style="padding:12px; text-align:left; color:#3b82f6; font-weight:600;">Job ID</th>
                                <th style="padding:12px; text-align:left; color:#3b82f6; font-weight:600;">Estado</th>
                                <th style="padding:12px; text-align:left; color:#3b82f6; font-weight:600;">Nombre</th>
                                <th style="padding:12px; text-align:center; color:#3b82f6; font-weight:600;">Tareas</th>
                                <th style="padding:12px; text-align:center; color:#3b82f6; font-weight:600;">Completadas</th>
                                <th style="padding:12px; text-align:center; color:#3b82f6; font-weight:600;">Fallidas</th>
                                <th style="padding:12px; text-align:right; color:#3b82f6; font-weight:600;">Duración</th>
                            </tr>
                        </thead>
                        <tbody>
                `;

                // La API ya retorna solo 5 jobs, pero por si acaso
                data.jobs.forEach((job, index) => {
                    const statusColors = {
                        'SUCCEEDED': '#10b981',
                        'RUNNING': '#3b82f6',
                        'FAILED': '#ef4444',
                        'UNKNOWN': '#6b7280'
                    };
                    const statusColor = statusColors[job.status] || statusColors['UNKNOWN'];
                    const bgColor = index % 2 === 0 ? '#1a1f2e' : '#0f1419';
                    const progress = job.numTasks > 0 ? (job.numCompletedTasks / job.numTasks * 100).toFixed(0) : 0;

                    tableHtml += `
                        <tr style="background:${bgColor}; border-bottom:1px solid #2d3748;">
                            <td style="padding:12px; color:#e2e8f0; font-weight:600;">${job.jobId}</td>
                            <td style="padding:12px;">
                                <span style="display:inline-block; padding:4px 10px; border-radius:4px; font-size:0.8em; font-weight:700; color:${statusColor}; background:${statusColor}25; border:1px solid ${statusColor};">
                                    ${job.status}
                                </span>
                            </td>
                            <td style="padding:12px; color:#94a3b8; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${job.name}">${job.name}</td>
                            <td style="padding:12px; text-align:center; color:#e2e8f0;">${job.numTasks}</td>
                            <td style="padding:12px; text-align:center;">
                                <div style="color:#10b981; font-weight:600;">${job.numCompletedTasks}</div>
                                <div style="font-size:0.75em; color:#6b7280;">${progress}%</div>
                            </td>
                            <td style="padding:12px; text-align:center; color:${job.numFailedTasks > 0 ? '#ef4444' : '#6b7280'}; font-weight:${job.numFailedTasks > 0 ? '700' : '400'};">${job.numFailedTasks}</td>
                            <td style="padding:12px; text-align:right; color:#e2e8f0; font-family:monospace;">${formatDuration(job.duration)}</td>
                        </tr>
                    `;
                });

                tableHtml += `
                        </tbody>
                    </table>
                    <div style="text-align:center; margin-top:10px; color:#6b7280; font-size:0.85em;">
                        Mostrando los últimos 5 jobs más recientes
                    </div>
                `;
                jobsTable.innerHTML = tableHtml;
            } else if (jobsTable) {
                jobsTable.innerHTML = '<div style="color:#94a3b8; font-style:italic; padding:20px; text-align:center;">No jobs available</div>';
            }

            // Update Executors Table
            const executorsTable = document.getElementById('spark-executors-table');
            if (executorsTable && data.executors && data.executors.length > 0) {
                let tableHtml = `
                    <table style="width:100%; border-collapse:collapse; font-size:0.85em;">
                        <thead>
                            <tr style="background:#0f1419; border-bottom:2px solid #2d3748;">
                                <th style="padding:12px; text-align:left; color:#10b981; font-weight:600;">Executor ID</th>
                                <th style="padding:12px; text-align:left; color:#10b981; font-weight:600;">Host:Port</th>
                                <th style="padding:12px; text-align:center; color:#10b981; font-weight:600;">Cores</th>
                                <th style="padding:12px; text-align:center; color:#10b981; font-weight:600;">Tareas Activas</th>
                                <th style="padding:12px; text-align:center; color:#10b981; font-weight:600;">Completadas</th>
                                <th style="padding:12px; text-align:center; color:#10b981; font-weight:600;">Fallidas</th>
                                <th style="padding:12px; text-align:right; color:#10b981; font-weight:600;">Memoria (On-Heap)</th>
                                <th style="padding:12px; text-align:right; color:#10b981; font-weight:600;">GC Time</th>
                            </tr>
                        </thead>
                        <tbody>
                `;

                data.executors.forEach((exec, index) => {
                    const bgColor = index % 2 === 0 ? '#1a1f2e' : '#0f1419';
                    const memPercent = exec.totalOnHeapMemory > 0 ? ((exec.usedOnHeapMemory / exec.totalOnHeapMemory) * 100).toFixed(1) : 0;
                    const gcPercent = exec.totalDuration > 0 ? ((exec.totalGCTime / exec.totalDuration) * 100).toFixed(1) : 0;
                    const statusColor = exec.isActive ? '#10b981' : '#ef4444';
                    
                    // Determinar nivel de alerta de memoria
                    let memColor = '#8b5cf6';
                    let memWarning = '';
                    if (memPercent > 90) {
                        memColor = '#ef4444';
                        memWarning = '⚠️ CRÍTICO';
                    } else if (memPercent > 75) {
                        memColor = '#f59e0b';
                        memWarning = '⚠️ ALTO';
                    }

                    tableHtml += `
                        <tr style="background:${bgColor}; border-bottom:1px solid #2d3748;">
                            <td style="padding:12px; color:#e2e8f0; font-weight:600;">
                                <div style="display:flex; align-items:center; gap:8px;">
                                    <span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:${statusColor};"></span>
                                    ${exec.id}
                                </div>
                            </td>
                            <td style="padding:12px; color:#94a3b8; font-family:monospace; font-size:0.85em;">${exec.hostPort}</td>
                            <td style="padding:12px; text-align:center; color:#e2e8f0;">${exec.totalCores}</td>
                            <td style="padding:12px; text-align:center;">
                                <span style="color:#3b82f6; font-weight:600;">${exec.activeTasks}</span>
                                <span style="color:#6b7280;"> / ${exec.maxTasks}</span>
                            </td>
                            <td style="padding:12px; text-align:center; color:#10b981; font-weight:600;">${exec.completedTasks}</td>
                            <td style="padding:12px; text-align:center; color:${exec.failedTasks > 0 ? '#ef4444' : '#6b7280'}; font-weight:${exec.failedTasks > 0 ? '700' : '400'};">${exec.failedTasks}</td>
                            <td style="padding:12px; text-align:right;">
                                <div style="color:${memColor}; font-weight:600; font-family:monospace;">${formatBytes(exec.usedOnHeapMemory)} / ${formatBytes(exec.totalOnHeapMemory)}</div>
                                <div style="font-size:0.75em; color:${memColor}; font-weight:${memWarning ? '700' : '400'};">${memPercent}% ${memWarning}</div>
                            </td>
                            <td style="padding:12px; text-align:right;">
                                <div style="color:${gcPercent > 20 ? '#ef4444' : '#f59e0b'}; font-weight:600;">${gcPercent}%</div>
                                <div style="font-size:0.75em; color:#6b7280;">${(exec.totalGCTime / 1000).toFixed(2)}s</div>
                            </td>
                        </tr>
                    `;
                });

                tableHtml += `
                        </tbody>
                    </table>
                `;
                executorsTable.innerHTML = tableHtml;
                
                // Actualizar gráfico de comparación de executors

            } else if (executorsTable) {
                executorsTable.innerHTML = '<div style="color:#94a3b8; font-style:italic; padding:20px; text-align:center;">No executors available</div>';
            }
        })
        .catch(error => {
            console.error('Error fetching Spark jobs:', error);
        });
}



function updateHdfsMetrics(hdfs) {
    // Helper function to format bytes to GB/TB
    function formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    // Update overview stats
    const statusEl = document.getElementById('hdfs-status');
    if (statusEl) {
        statusEl.textContent = hdfs.status || '-';
        // Color verde si es "active", rojo si no
        const isActive = (hdfs.status || '').toLowerCase() === 'active';
        statusEl.style.color = isActive ? '#10b981' : '#ef4444';
        statusEl.style.textShadow = isActive ? '0 0 10px rgba(16,185,129,0.5)' : '0 0 10px rgba(239,68,68,0.5)';
    }

    const capacityEl = document.getElementById('hdfs-capacity');
    if (capacityEl) capacityEl.textContent = formatBytes(hdfs.capacity || 0);

    const usedEl = document.getElementById('hdfs-used');
    if (usedEl) usedEl.textContent = formatBytes(hdfs.used || 0);

    const remainingEl = document.getElementById('hdfs-remaining');
    if (remainingEl) remainingEl.textContent = formatBytes(hdfs.remaining || 0);

    const percentageEl = document.getElementById('hdfs-percentage');
    if (percentageEl) percentageEl.textContent = (hdfs.percentage || 0).toFixed(1) + '%';

    // Update data stats
    const filesEl = document.getElementById('hdfs-files');
    if (filesEl) filesEl.textContent = formatNumber(hdfs.total_files || 0);

    const blocksEl = document.getElementById('hdfs-blocks');
    if (blocksEl) blocksEl.textContent = formatNumber(hdfs.total_blocks || 0);

    // Update nodes
    const liveNodesEl = document.getElementById('hdfs-live-nodes');
    if (liveNodesEl) liveNodesEl.textContent = hdfs.live_nodes || 0;

    const deadNodesEl = document.getElementById('hdfs-dead-nodes');
    if (deadNodesEl) deadNodesEl.textContent = hdfs.dead_nodes || 0;

    // Update health
    const underReplicatedEl = document.getElementById('hdfs-under-replicated');
    if (underReplicatedEl) {
        underReplicatedEl.textContent = hdfs.under_replicated || 0;
        underReplicatedEl.style.color = (hdfs.under_replicated || 0) > 0 ? '#f59e0b' : '#10b981';
    }

    const corruptEl = document.getElementById('hdfs-corrupt');
    if (corruptEl) {
        corruptEl.textContent = hdfs.corrupt_blocks || 0;
        corruptEl.style.color = (hdfs.corrupt_blocks || 0) > 0 ? '#ef4444' : '#10b981';
    }

    const missingEl = document.getElementById('hdfs-missing');
    if (missingEl) {
        missingEl.textContent = hdfs.missing_blocks || 0;
        missingEl.style.color = (hdfs.missing_blocks || 0) > 0 ? '#ef4444' : '#10b981';
    }

    // Update safemode
    const safemodeEl = document.getElementById('hdfs-safemode');
    if (safemodeEl) {
        safemodeEl.textContent = hdfs.safemode ? 'ON' : 'OFF';
        safemodeEl.style.color = hdfs.safemode ? '#f59e0b' : '#10b981';
    }

    // Update DataNodes
    const datanodesContainer = document.getElementById('hdfs-datanodes');
    if (datanodesContainer && hdfs.datanodes && hdfs.datanodes.length > 0) {
        let html = '';
        hdfs.datanodes.forEach(node => {
            const usagePercent = node.capacity > 0 ? (node.used / node.capacity * 100) : 0;
            const statusColor = node.state === 'In Service' ? '#10b981' : '#f59e0b';
            
            html += `
                <div style="background:#1a1f2e; border: 2px solid #6b7280; border-left: 4px solid ${statusColor}; border-radius:12px; padding:20px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); backdrop-filter: blur(10px);">
                    <div style="font-weight:600; font-size:0.95em; margin-bottom:5px; word-break:break-all; color:#e2e8f0;">${node.name}</div>
                    <div style="font-size:0.85em; color:#94a3b8; margin-bottom:15px; font-family:monospace;">State: ${node.state}</div>
                    
                    <div style="margin-bottom:12px;">
                        <div style="display:flex; justify-content:space-between; font-size:0.8em; margin-bottom:4px; color:#9ca3af;">
                            <span style="font-weight:500;">Storage</span>
                            <span style="font-weight:600; color:#e2e8f0;">${formatBytes(node.used)} / ${formatBytes(node.capacity)}</span>
                        </div>
                        <div style="height:6px; background:#0f1419; border-radius:3px; overflow:hidden;">
                            <div style="height:100%; width:${usagePercent}%; background:#3b82f6; box-shadow: 0 0 10px #3b82f6;"></div>
                        </div>
                    </div>
                    
                    <div style="font-size:0.8em; color:#9ca3af; display:flex; justify-content:space-between;">
                        <span>Remaining:</span>
                        <span style="font-weight:600; color:#e2e8f0;">${formatBytes(node.remaining)}</span>
                    </div>
                </div>
            `;
        });
        datanodesContainer.innerHTML = html;
    } else if (datanodesContainer) {
        datanodesContainer.innerHTML = '<div style="color:#ffffff; font-style:italic;">No DataNodes found</div>';
    }
}

// Load families table
async function loadFamilies(type) {
    document.querySelectorAll('.families-table .tab').forEach(t => t.classList.remove('active'));
    event.target.classList.add('active');

    const content = document.getElementById('families-content');
    content.innerHTML = '<div class="loading">Loading families...</div>';

    try {
        const data = await fetch(`/api/families?type=${type}`).then(r => r.json());

        if (data.families.length === 0) {
            content.innerHTML = '<p style="text-align:center;padding:20px;">No family data available yet.</p>';
            return;
        }

        let html = `
            <table>
                <thead>
                    <tr>
                        <th>Family ID</th>
                        <th>Members</th>
                        <th>Unique Persons</th>
                        <th>Avg SNPs</th>
                        <th>Max SNPs</th>
                        <th>Min SNPs</th>
                        <th>Genders</th>
                    </tr>
                </thead>
                <tbody>
        `;

        data.families.forEach(f => {
            html += `
                <tr>
                    <td><strong>${f.family_id}</strong></td>
                    <td>${f.members_count}</td>
                    <td>${f.unique_persons}</td>
                    <td>${formatNumber(Math.round(f.avg_snps))}</td>
                    <td>${formatNumber(f.max_snps)}</td>
                    <td>${formatNumber(f.min_snps)}</td>
                    <td>${f.genders.join(', ')}</td>
                </tr>
            `;
        });

        html += `
                </tbody>
            </table>
            <p style="text-align:center;margin-top:15px;color:#666;">
                Showing ${data.families.length} of ${data.total_families} families
            </p>
        `;

        content.innerHTML = html;
    } catch (error) {
        console.error('Error loading families:', error);
        content.innerHTML = '<p style="text-align:center;padding:20px;color:red;">Error loading families</p>';
    }
}

function formatNumber(num) {
    return num.toLocaleString('es-ES');
}

// Update task completion times chart - Optimizado para grandes volúmenes
async function updateTaskTimes() {
    try {
        const data = await fetch('/api/task_times').then(r => r.json());
        const taskTimes = data.task_times;
        
        if (!taskTimes || taskTimes.length === 0) {
            return;
        }
        
        // Limitar cantidad de puntos mostrados para mejor performance
        const maxPoints = 50; // Mostrar solo los últimos 50 puntos de cada tipo
        
        // Separar por tipo pero en líneas conexas
        const fathersTimes = [];
        const mothersTimes = [];
        const childrenTimes = [];
        
        let fathersIdx = 0;
        let mothersIdx = 0;
        let childrenIdx = 0;
        
        taskTimes.forEach((task) => {
            if (task.member_type === 'fathers') {
                fathersTimes.push({x: fathersIdx++, y: task.processing_time});
            } else if (task.member_type === 'mothers') {
                mothersTimes.push({x: mothersIdx++, y: task.processing_time});
            } else if (task.member_type === 'children') {
                childrenTimes.push({x: childrenIdx++, y: task.processing_time});
            }
        });
        
        // Aplicar límite y hacer downsampling si es necesario
        const downsample = (data, limit) => {
            if (data.length <= limit) return data;
            const step = Math.ceil(data.length / limit);
            return data.filter((_, i) => i % step === 0).slice(-limit);
        };
        
        // Actualizar gráfica con datos limitados
        charts.taskTimes.data.datasets[0].data = downsample(fathersTimes, maxPoints);
        charts.taskTimes.data.datasets[1].data = downsample(mothersTimes, maxPoints);
        charts.taskTimes.data.datasets[2].data = downsample(childrenTimes, maxPoints);
        
        // Actualizar sin animación para mejor performance
        charts.taskTimes.update('none');
        
    } catch (error) {
        console.error('Error updating task times:', error);
    }
}

// ============== MÉTRICAS AVANZADAS DE STREAMING GENÉTICO ==============

let geneticCharts = {};

function initializeGeneticCharts() {
    const colors = getThemeColors();
    
    // Gráfico de Tasa de Mutación en Tiempo Real
    geneticCharts.mutationRate = new Chart(document.getElementById('mutationRateChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Tasa de Mutación (por segundo)',
                data: [],
                borderColor: colors.cyan,
                backgroundColor: colors.cyan + '20',
                tension: 0.4,
                borderWidth: 3,
                pointRadius: 3,
                pointBackgroundColor: colors.cyan,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: colors.textColor, padding: 15 }
                }
            }
        }
    });
    
    // // Gráfico de Distribución de Genotipos (Pie)
    // geneticCharts.genotypeDistribution = new Chart(document.getElementById('genotypeDistributionChart'), {
    //     type: 'doughnut',
    //     data: {
    //         labels: ['Dominante', 'Recesivo', 'Heterocigoto'],
    //         datasets: [{
    //             data: [0, 0, 0],
    //             backgroundColor: [colors.green, colors.red, colors.blue],
    //             borderWidth: 2,
    //             borderColor: colors.gridColor,
    //             hoverOffset: 10
    //         }]
    //     },
    //     options: {
    //         responsive: true,
    //         maintainAspectRatio: true,
    //         cutout: '60%',
    //         plugins: {
    //             legend: {
    //                 position: 'bottom',
    //                 labels: { color: colors.textColor, padding: 15 }
    //             },
    //             tooltip: {
    //                 callbacks: {
    //                     label: function(context) {
    //                         return context.label + ': ' + context.raw;
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // });
    
    // Gráfico de Top Genes (Bar Chart)
    geneticCharts.topGenes = new Chart(document.getElementById('topGenesChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Frecuencia de Genes',
                data: [],
                backgroundColor: [colors.purple, colors.amber, colors.cyan, colors.pink, colors.green],
                borderRadius: 8,
                borderWidth: 0
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            }
        }
    });
    
    // // Gráfico de Top Variantes (Horizontal Bar)
    // geneticCharts.topVariants = new Chart(document.getElementById('topVariantsChart'), {
    //     type: 'bar',
    //     data: {
    //         labels: [],
    //         datasets: [{
    //             label: 'Frecuencia de Variantes',
    //             data: [],
    //             backgroundColor: [colors.amber, colors.cyan, colors.pink, colors.red, colors.green],
    //             borderRadius: 8,
    //             borderWidth: 0
    //         }]
    //     },
    //     options: {
    //         indexAxis: 'y',
    //         responsive: true,
    //         maintainAspectRatio: true,
    //         animation: false,
    //         plugins: {
    //             legend: { display: false }
    //         },
    //         scales: {
    //             x: {
    //                 grid: { color: colors.gridColor },
    //                 ticks: { color: colors.textColor }
    //             },
    //             y: {
    //                 grid: { display: false },
    //                 ticks: { color: colors.textColor }
    //             }
    //         }
    //     }
    // });

    // NEW: Gráfico de Ventanas de Tiempo (Line multi-window)
    geneticCharts.timeWindows = new Chart(document.getElementById('timeWindowsChart'), {
        type: 'line',
        data: {
            labels: ['Fathers', 'Mothers', 'Children'],
            datasets: [
                {
                    label: '1 Minuto',
                    data: [0, 0, 0],
                    borderColor: colors.cyan,
                    backgroundColor: colors.cyan + '20',
                    tension: 0.4,
                    borderWidth: 2,
                    pointRadius: 4
                },
                {
                    label: '5 Minutos',
                    data: [0, 0, 0],
                    borderColor: colors.purple,
                    backgroundColor: colors.purple + '20',
                    tension: 0.4,
                    borderWidth: 2,
                    pointRadius: 4
                },
                {
                    label: '15 Minutos',
                    data: [0, 0, 0],
                    borderColor: colors.amber,
                    backgroundColor: colors.amber + '20',
                    tension: 0.4,
                    borderWidth: 2,
                    pointRadius: 4
                },
                {
                    label: '1 Hora',
                    data: [0, 0, 0],
                    borderColor: colors.pink,
                    backgroundColor: colors.pink + '20',
                    tension: 0.4,
                    borderWidth: 2,
                    pointRadius: 4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: colors.textColor, padding: 15 }
                }
            }
        }
    });

    // NEW: Gráfico de Hotspots (Top 10 positions)
    geneticCharts.hotspots = new Chart(document.getElementById('hotspotsChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Frecuencia de Hotspots',
                data: [],
                backgroundColor: colors.red,
                borderRadius: 8,
                borderWidth: 0
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            }
        }
    });

    // NEW: Gráfico de Distribución de Cromosomas (Stacked Bar)
    geneticCharts.chromosomeDistribution = new Chart(document.getElementById('chromosomeDistributionChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: []
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            indexAxis: 'y',
            scales: {
                x: {
                    stacked: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor }
                },
                y: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: colors.textColor, padding: 15 }
                }
            }
        }
    });

    // NEW: Gráfico de Tendencias de Genotipos (Grouped Bar)
    geneticCharts.genotypeTrends = new Chart(document.getElementById('genotypeTrendsChart'), {
        type: 'bar',
        data: {
            labels: ['Fathers', 'Mothers', 'Children'],
            datasets: [
                {
                    label: 'Dominante (%)',
                    data: [0, 0, 0],
                    backgroundColor: colors.green,
                    borderRadius: 4
                },
                {
                    label: 'Recesivo (%)',
                    data: [0, 0, 0],
                    backgroundColor: colors.red,
                    borderRadius: 4
                },
                {
                    label: 'Heterocigoto (%)',
                    data: [0, 0, 0],
                    backgroundColor: colors.blue,
                    borderRadius: 4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor, callback: function(v) { return v + '%'; } }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: colors.textColor, padding: 15 }
                }
            }
        }
    });
}

function updateGeneticMetrics() {
    try {
        // Obtener métricas genéticas
        fetch('/api/genetic_metrics')
            .then(res => res.json())
            .then(metrics => {
                const colors = getThemeColors();
                
                // Actualizar tasa de mutación
                if (geneticCharts.mutationRate) {
                    const history = metrics.mutation_rate_history || [];
                    geneticCharts.mutationRate.data.labels = history.map((_, i) => i);
                    geneticCharts.mutationRate.data.datasets[0].data = history;
                    geneticCharts.mutationRate.update('none');
                }
                
                // Actualizar distribución de genotipos
                if (geneticCharts.genotypeDistribution) {
                    const dist = metrics.genotype_distribution || {};
                    geneticCharts.genotypeDistribution.data.datasets[0].data = [
                        dist.dominant || 0,
                        dist.recessive || 0,
                        dist.heterozygous || 0
                    ];
                    geneticCharts.genotypeDistribution.update('none');
                }
                
                // Actualizar Top Genes
                if (geneticCharts.topGenes && metrics.top_genes) {
                    geneticCharts.topGenes.data.labels = metrics.top_genes.map(g => g.gene);
                    geneticCharts.topGenes.data.datasets[0].data = metrics.top_genes.map(g => g.count);
                    geneticCharts.topGenes.update('none');
                }
                
                // Actualizar Top Variantes
                if (geneticCharts.topVariants && metrics.top_variants) {
                    geneticCharts.topVariants.data.labels = metrics.top_variants.map(v => v.type);
                    geneticCharts.topVariants.data.datasets[0].data = metrics.top_variants.map(v => v.count);
                    geneticCharts.topVariants.update('none');
                }
                
                // Actualizar indicadores de métricas avanzadas
                updateGeneticIndicators(metrics);
            })
            .catch(err => console.error('Error fetching genetic metrics:', err));
    } catch (error) {
        console.error('Error updating genetic metrics:', error);
    }
}

function updateGeneticIndicators(metrics) {
    // Actualizar indicador de tasa de mutación
    const mutRateEl = document.getElementById('mutationRateIndicator');
    if (mutRateEl) {
        mutRateEl.textContent = (metrics.mutation_rate_percent || 0).toFixed(2) + '%';
        mutRateEl.className = metrics.mutation_rate_percent > 5 ? 'metric-high' : metrics.mutation_rate_percent > 2 ? 'metric-medium' : 'metric-low';
    }
    
    // Actualizar indicador de anomalías
    const anomalyEl = document.getElementById('anomalyCountIndicator');
    if (anomalyEl) {
        anomalyEl.textContent = metrics.anomaly_count || 0;
    }
    
    // Actualizar indicador de variantes procesadas
    const variantsEl = document.getElementById('totalVariantsIndicator');
    if (variantsEl) {
        variantsEl.textContent = metrics.total_variants_processed || 0;
    }
    
    // Obtener tendencias
    fetch('/api/genetic_trends')
        .then(res => res.json())
        .then(trends => {
            // Actualizar tendencia de tasa de mutación
            const trendEl = document.getElementById('mutationTrendIndicator');
            if (trendEl) {
                const changePercent = trends.rate_change_percent || 0;
                trendEl.textContent = (changePercent > 0 ? '+' : '') + changePercent.toFixed(2) + '%';
                trendEl.className = 'trend-' + (trends.trend_direction || 'stable');
            }
            
            // Actualizar diversidad genética
            const diversityEl = document.getElementById('geneticDiversityIndicator');
            if (diversityEl) {
                diversityEl.textContent = (trends.genetic_diversity * 100).toFixed(2) + '%';
            }
            
            // Mostrar distribución de genotipos
            Object.keys(trends.genotype_percentages || {}).forEach(gtype => {
                const el = document.getElementById(`genotype${gtype.charAt(0).toUpperCase() + gtype.slice(1)}Percent`);
                if (el) {
                    el.textContent = (trends.genotype_percentages[gtype] || 0).toFixed(2) + '%';
                }
            });
        })
        .catch(err => console.error('Error fetching genetic trends:', err));
    
    // NEW: Actualizar gráficos de ventanas de tiempo y hotspots
    updateTimeWindowMetrics();
    updateHotspotsMetrics();
    updateChromosomeDistributionMetrics();
    updateGenotypeTrendsMetrics();
}

// NEW: Actualizar métricas de ventanas de tiempo
function updateTimeWindowMetrics() {
    try {
        fetch('/api/time_windows')
            .then(res => res.json())
            .then(data => {
                if (geneticCharts.timeWindows && data.windows) {
                    const windows = data.windows;
                    geneticCharts.timeWindows.data.datasets[0].data = [
                        windows['1_minute']['fathers'] || 0,
                        windows['1_minute']['mothers'] || 0,
                        windows['1_minute']['children'] || 0
                    ];
                    geneticCharts.timeWindows.data.datasets[1].data = [
                        windows['5_minutes']['fathers'] || 0,
                        windows['5_minutes']['mothers'] || 0,
                        windows['5_minutes']['children'] || 0
                    ];
                    geneticCharts.timeWindows.data.datasets[2].data = [
                        windows['15_minutes']['fathers'] || 0,
                        windows['15_minutes']['mothers'] || 0,
                        windows['15_minutes']['children'] || 0
                    ];
                    geneticCharts.timeWindows.data.datasets[3].data = [
                        windows['1_hour']['fathers'] || 0,
                        windows['1_hour']['mothers'] || 0,
                        windows['1_hour']['children'] || 0
                    ];
                    geneticCharts.timeWindows.update('none');
                }
            })
            .catch(err => console.error('Error fetching time windows:', err));
    } catch (error) {
        console.error('Error updating time windows:', error);
    }
}

// NEW: Actualizar métricas de hotspots
function updateHotspotsMetrics() {
    try {
        fetch('/api/hotspots')
            .then(res => res.json())
            .then(data => {
                if (geneticCharts.hotspots && data.position_hotspots) {
                    const allHotspots = [];
                    Object.values(data.position_hotspots).forEach(memberHotspots => {
                        allHotspots.push(...memberHotspots);
                    });
                    
                    // Agrupar y ordenar top 10
                    const topHotspots = allHotspots.slice(0, 10);
                    geneticCharts.hotspots.data.labels = topHotspots.map(h => `Chr${h.chromosome}:${h.position}`);
                    geneticCharts.hotspots.data.datasets[0].data = topHotspots.map(h => h.frequency);
                    geneticCharts.hotspots.update('none');
                }
            })
            .catch(err => console.error('Error fetching hotspots:', err));
    } catch (error) {
        console.error('Error updating hotspots:', error);
    }
}

// NEW: Actualizar distribución de cromosomas
function updateChromosomeDistributionMetrics() {
    try {
        fetch('/api/chromosome_stats')
            .then(res => res.json())
            .then(data => {
                if (geneticCharts.chromosomeDistribution && data.chromosome_distribution) {
                    const chrDist = data.chromosome_distribution;
                    const colors = getThemeColors();
                    const colorArray = [colors.cyan, colors.purple, colors.amber, colors.pink, colors.green, colors.red, colors.blue];
                    
                    // Recolectar todos los cromosomas únicos
                    const allChromosomes = new Set();
                    Object.values(chrDist).forEach(memberChrs => {
                        Object.keys(memberChrs).forEach(chr => allChromosomes.add(chr));
                    });
                    
                    const chromosomes = Array.from(allChromosomes).sort();
                    const memberTypes = ['fathers', 'mothers', 'children'];
                    
                    // Construir datasets para cada tipo de miembro
                    const datasets = memberTypes.map((member, idx) => ({
                        label: member.charAt(0).toUpperCase() + member.slice(1),
                        data: chromosomes.map(chr => {
                            const count = Object.values(chrDist[member] || {}).reduce((sum, genotypes) => {
                                return sum + (genotypes[chr] || 0);
                            }, 0);
                            return count;
                        }),
                        backgroundColor: colorArray[idx % colorArray.length]
                    }));
                    
                    geneticCharts.chromosomeDistribution.data.labels = chromosomes;
                    geneticCharts.chromosomeDistribution.data.datasets = datasets;
                    geneticCharts.chromosomeDistribution.update('none');
                }
            })
            .catch(err => console.error('Error fetching chromosome distribution:', err));
    } catch (error) {
        console.error('Error updating chromosome distribution:', error);
    }
}

// NEW: Actualizar tendencias de genotipos
function updateGenotypeTrendsMetrics() {
    try {
        fetch('/api/genotype_trends')
            .then(res => res.json())
            .then(data => {
                if (geneticCharts.genotypeTrends && data.genotype_trends) {
                    const trends = data.genotype_trends;
                    const memberTypes = ['fathers', 'mothers', 'children'];
                    
                    const dominantData = memberTypes.map(m => {
                        const percentages = trends[m]?.percentages || {};
                        return percentages.dominant || 0;
                    });
                    
                    const recessiveData = memberTypes.map(m => {
                        const percentages = trends[m]?.percentages || {};
                        return percentages.recessive || 0;
                    });
                    
                    const heteroData = memberTypes.map(m => {
                        const percentages = trends[m]?.percentages || {};
                        return percentages.heterozygous || 0;
                    });
                    
                    geneticCharts.genotypeTrends.data.datasets[0].data = dominantData;
                    geneticCharts.genotypeTrends.data.datasets[1].data = recessiveData;
                    geneticCharts.genotypeTrends.data.datasets[2].data = heteroData;
                    geneticCharts.genotypeTrends.update('none');
                }
            })
            .catch(err => console.error('Error fetching genotype trends:', err));
    } catch (error) {
        console.error('Error updating genotype trends:', error);
    }
}

// NEW: Actualizar orientación genética
function updateGeneticOrientationMetrics() {
    try {
        fetch('/api/genetic_orientation')
            .then(res => res.json())
            .then(data => {
                if (data.genetic_orientation) {
                    const orientation = data.genetic_orientation;
                    
                    // Actualizar indicadores de orientación genética
                    const memberTypes = ['fathers', 'mothers', 'children'];
                    memberTypes.forEach(member => {
                        const memberData = orientation[member] || {};
                        const domEl = document.getElementById(`${member}DominantPercent`);
                        const recEl = document.getElementById(`${member}RecessivePercent`);
                        const hetEl = document.getElementById(`${member}HeterozygousPercent`);
                        const divEl = document.getElementById(`${member}DiversityIndicator`);
                        
                        if (domEl) domEl.textContent = (memberData.dominant_pct || 0).toFixed(2) + '%';
                        if (recEl) recEl.textContent = (memberData.recessive_pct || 0).toFixed(2) + '%';
                        if (hetEl) hetEl.textContent = (memberData.heterozygous_pct || 0).toFixed(2) + '%';
                        if (divEl) divEl.textContent = (memberData.genetic_diversity || 0).toFixed(3);
                    });
                }
            })
            .catch(err => console.error('Error fetching genetic orientation:', err));
    } catch (error) {
        console.error('Error updating genetic orientation:', error);
    }
}


function initializeGeneticsCharts() {

    // 🧬 INDIVIDUAL: Homo vs Hetero
    const ctxIndividual = document
        .getElementById('individualGeneticsChart')
        .getContext('2d');

    individualGeneticsChart = new Chart(ctxIndividual, {
        type: 'doughnut',
        data: {
            labels: ['Homozygous', 'Heterozygous'],
            datasets: [{
                data: [0, 0]
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'bottom' }
            }
        }
    });

    // 🌍 POBLACIÓN: Homo vs Hetero
    const ctxPopulation = document
        .getElementById('populationGeneticsChart')
        .getContext('2d');

    populationGeneticsChart = new Chart(ctxPopulation, {
        type: 'bar',
        data: {
            labels: ['Homozygous', 'Heterozygous'],
            datasets: [{
                label: 'Population (%)',
                data: [0, 0]
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: { beginAtZero: true , max :100}
            }
        }
    });

    // 📊 DIVERSIDAD GENÉTICA
    const ctxDiversity = document
        .getElementById('populationDiversityChart')
        .getContext('2d');

    populationDiversityChart = new Chart(ctxDiversity, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Genetic Diversity (He)',
                data: []
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    min: 0,
                    max: 1
                }
            }
        }
    });
}

function populateIndividualSelector(data) {
    const selector = document.getElementById('individualSelector');

    if (selector.options.length > 1) return;

    Object.keys(data).forEach(personId => {
        const opt = document.createElement('option');
        opt.value = personId;
        opt.textContent = personId;
        selector.appendChild(opt);
    });
}


async function updateIndividualGeneticsChart() {
    try {
        const res = await fetch('/api/genetics/individual');
        const data = await res.json();

        console.log(
            'Datos recibidos de /api/genetics/individual:',
            data
        );
        
        populateIndividualSelector(data);

        const personId =
            document.getElementById('individualSelector').value;

        if (!personId || !data[personId]) return;

        const individual = data[personId];

        individualGeneticsChart.data.datasets[0].data = [
            individual.homozygous_pct || 0,
            individual.heterozygous_pct || 0
        ];

        individualGeneticsChart.update();
    } catch (err) {
        console.error(
            'Error fetching individual genetics:',
            err
        );
    }
}


async function updatePopulationGeneticsChart() {
    try {
        const res = await fetch('/api/genetics/population');
        const data = await res.json();

        console.log(
            'Datos recibidos de /api/genetics/population:',
            data
        );

        const group = document.getElementById('populationGroupSelector').value;

        if (!data[group] || !data[group].percentages) return;

        const pct = data[group].percentages;

        populationGeneticsChart.data.datasets[0].data = [
            pct.Homozygous || 0,
            pct.Heterozygous || 0
        ];

        populationGeneticsChart.update();
    } catch (err) {
        console.error(
            'Error fetching population genetics:',
            err
        );
    }
}

// async function updateGeneticIndicators() {
//     try {
//         const res = await fetch('/api/genetics/population');
//         const data = await res.json();

//         const group = document.getElementById('populationGroupSelector').value;

//         if (!data[group] || !data[group].percentages) return;

//         const pct = data[group].percentages;

//         // Actualiza los indicadores
//         document.getElementById('genotypeHomoPercent').textContent = (pct.Homozygous || 0) + '%';
//         document.getElementById('genotypeHeteroPercent').textContent = (pct.Heterozygous || 0) + '%';
//         document.getElementById('geneticDiversityIndicator').textContent = ((pct.Diversity || 0) * 100).toFixed(1) + '%';

//     } catch (err) {
//         console.error('Error updating genetic indicators:', err);
//     }
// }

async function updatePopulationDiversityChart() {
    try {
        const res = await fetch('/api/genetics/diversity');
        const data = await res.json();

        console.log('📊 Datos diversidad genética:', data);

        const group =
            document.getElementById('populationGroupSelector')?.value || 'fathers';

        if (!data[group] || data[group].length === 0) return;

        populationDiversityChart.data.labels = data[group].map(
            d => new Date(d.timestamp).toLocaleTimeString()
        );

        populationDiversityChart.data.datasets[0].data = data[group].map(
            d => d.He
        );

        populationDiversityChart.update();
    } catch (err) {
        console.error('Error updating diversity chart:', err);
    }
}



// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initializeCharts();
    initializeGeneticCharts();
    initializeGeneticsCharts();

    document.getElementById('individualSelector')
        .addEventListener('change', updateIndividualGeneticsChart);

    document.getElementById('populationGroupSelector')
        .addEventListener('change', () => {
            populationDiversityChart.data.labels = [];
            populationDiversityChart.data.datasets[0].data = [];
            populationDiversityChart.update();

            updatePopulationGeneticsChart();
            updatePopulationDiversityChart();
            //updateGeneticIndicators();
    });

    console.log(individualGeneticsChart, populationGeneticsChart, populationDiversityChart);

    updateDashboard();
    updateGeneticMetrics();
    updateGeneticOrientationMetrics();
    
    // Primera carga
    //updateGeneticIndicators();
    updateIndividualGeneticsChart();
    updatePopulationGeneticsChart();
    updatePopulationDiversityChart();

    loadFamilies('fathers');

    // Auto-refresh every 3 seconds
    setInterval(updateDashboard, 3000);
    setInterval(updateGeneticMetrics, 2000);
    setInterval(updateGeneticOrientationMetrics, 2000);
    
    
    // Refrescar cada 5 segundos
    //setInterval(updateGeneticIndicators, 5000);
    setInterval(updateIndividualGeneticsChart, 5000);
    setInterval(updatePopulationGeneticsChart, 5000);
    setInterval(updatePopulationDiversityChart, 5000);
    
});
