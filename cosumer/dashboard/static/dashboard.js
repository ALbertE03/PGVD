// Global chart instances
let charts = {};
let currentGenderType = 'fathers';
let currentChromType = 'all';
let currentGenoType = 'all';
let currentSnpRangeType = 'fathers';
let currentFamilySizeType = 'fathers';

// ========== Funciones Reutilizables ==========
/**
 * Actualiza una gráfica de barras sin animación para mejor performance
 * @param {Chart} chart - Instancia de Chart.js
 * @param {Array} labels - Etiquetas del eje
 * @param {Array} datasets - Datasets de la gráfica
 */
function updateBarChart(chart, labels, datasets) {
    if (!chart) return;
    chart.data.labels = labels;
    chart.data.datasets = datasets;
    chart.update('none'); // Sin animación como Detected Genes
}

/**
 * Filtra datos genéticos (ahora solo retorna los datos sin filtrar)
 * @param {Array} data - Array de datos genéticos
 * @returns {Array} Datos sin filtrar
 */
function filterBySelectedFamilies(data) {
    return data || [];  // Retornar todos los datos sin filtro
}

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

    // Chromosome Distribution Chart
    charts.chromosome = new Chart(document.getElementById('chromosomeChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Variants Count',
                data: [],
                backgroundColor: colors.blue + '80',
                borderColor: colors.blue,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
                duration: 750,
                easing: 'easeInOutQuart'
            },
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
                    display: true,
                    position: 'top',
                    labels: { color: colors.textColor }
                }
            }
        }
    });

    // Mutation Rate Chart - Variantes por segundo en ventana de 60s
    charts.mutation = new Chart(document.getElementById('mutationChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Mutation Rate (variants/sec)',
                data: [],
                borderColor: colors.cyan,
                backgroundColor: colors.cyan + '20',
                tension: 0.4,
                borderWidth: 3,
                fill: true,
                pointRadius: 3,
                pointBackgroundColor: colors.cyan
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            scales: {
                y: { 
                    beginAtZero: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor },
                    title: {
                        display: true,
                        text: 'Mutation Rate (%)',
                        color: colors.textColor
                    }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: colors.textColor },
                    title: {
                        display: true,
                        text: 'Time',
                        color: colors.textColor
                    }
                }
            },
            plugins: {
                legend: { 
                    position: 'top',
                    labels: { 
                        color: colors.textColor,
                        padding: 15,
                        usePointStyle: true
                    }
                }
            }
        }
    });

    // Heterozygosity Population Chart
    charts.heteroPop = new Chart(document.getElementById('heteroPopChart'), {
        type: 'bar',
        data: {
            labels: ['Fathers', 'Mothers', 'Children'],
            datasets: [
                {
                    label: 'Heterozygous %',
                    data: [0, 0, 0],
                    backgroundColor: colors.green + '80',
                    borderColor: colors.green,
                    borderWidth: 2
                },
                {
                    label: 'Homozygous %',
                    data: [0, 0, 0],
                    backgroundColor: colors.blue + '80',
                    borderColor: colors.blue,
                    borderWidth: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
                duration: 750,
                easing: 'easeInOutQuart'
            },
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
                    position: 'bottom',
                    labels: { 
                        color: colors.textColor,
                        padding: 15
                    }
                }
            }
        }
    });

    // Genotype Trends Chart (cambiado a barras horizontales)
    charts.genotype = new Chart(document.getElementById('genotypeChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: []
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            animation: {
                duration: 750,
                easing: 'easeInOutQuart'
            },
            scales: {
                x: { 
                    beginAtZero: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor },
                    title: {
                        display: true,
                        text: 'Count',
                        color: colors.textColor
                    }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: { 
                    position: 'top',
                    labels: { 
                        color: colors.textColor,
                        padding: 15,
                        usePointStyle: true
                    }
                }
            }
        }
    });

    // Heredabilidad Chart
    charts.heritability = new Chart(document.getElementById('heritabilityChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: []
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            scales: {
                x: { 
                    beginAtZero: true,
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textColor },
                    title: {
                        display: true,
                        text: '% Heredabilidad',
                        color: colors.textColor
                    }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: colors.textColor }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });

    // Variantes Raras Chart
    charts.rareVariants = new Chart(document.getElementById('rareVariantsChart'), {
        type: 'doughnut',
        data: {
            labels: [],
            datasets: [{
                data: [],
                backgroundColor: []
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { 
                        color: colors.textColor,
                        padding: 12,
                        font: { size: 11 }
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': ' + context.parsed + ' variantes';
                        }
                    }
                }
            }
        }
    });

    // Gene Detection Chart
    charts.gene = new Chart(document.getElementById('geneChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Fathers',
                    data: [],
                    backgroundColor: colors.blue + '80',
                    borderColor: colors.blue,
                    borderWidth: 2
                },
                {
                    label: 'Mothers',
                    data: [],
                    backgroundColor: colors.red + '80',
                    borderColor: colors.red,
                    borderWidth: 2
                },
                {
                    label: 'Children',
                    data: [],
                    backgroundColor: colors.green + '80',
                    borderColor: colors.green,
                    borderWidth: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
                duration: 750,
                easing: 'easeInOutQuart'
            },
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
                        color: colors.textColor,
                        padding: 15
                    }
                }
            }
        }
    });

    // Genetic Interaction Network Chart - con nodos Y conexiones
    charts.network = new Chart(document.getElementById('networkChart'), {
        type: 'scatter',
        data: {
            datasets: []
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: false,
            scales: {
                x: {
                    type: 'linear',
                    grid: { color: colors.gridColor },
                    ticks: { display: false },
                    display: false
                },
                y: {
                    type: 'linear',
                    grid: { color: colors.gridColor },
                    ticks: { display: false },
                    display: false
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'bottom',
                    labels: {
                        color: colors.textColor,
                        usePointStyle: true,
                        padding: 15
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            if (context.raw.gene) {
                                return `${context.raw.gene}: ${context.raw.count} variantes`;
                            }
                            return '';
                        }
                    }
                }
            }
        }
    });

    // Risk Score Chart (Bar)
    charts.riskScore = new Chart(document.getElementById('riskScoreChart'), {
        type: 'bar',
        data: { labels: [], datasets: [] },
        options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: colors.cardBg,
                    titleColor: colors.textColor,
                    bodyColor: colors.textSecondary,
                    borderColor: colors.borderColor,
                    borderWidth: 1
                }
            },
            scales: {
                x: { 
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textSecondary }
                },
                y: { 
                    grid: { color: colors.gridColor },
                    ticks: { color: colors.textSecondary },
                    beginAtZero: true,
                    max: 5
                }
            }
        }
    });

    // Cohort Comparison Chart
    charts.cohortComparison = new Chart(document.getElementById('cohortComparisonChart'), {
        type: 'radar',
        data: { labels: [], datasets: [] },
        options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { color: colors.textColor, padding: 20, font: { size: 12 } }
                },
                tooltip: {
                    backgroundColor: colors.cardBg,
                    titleColor: colors.textColor,
                    bodyColor: colors.textSecondary,
                    borderColor: colors.borderColor,
                    borderWidth: 1
                }
            },
            scales: {
                r: {
                    grid: { color: colors.gridColor },
                    angleLines: { color: colors.gridColor },
                    pointLabels: { color: colors.textColor, font: { size: 11 } },
                    ticks: { color: colors.textSecondary, backdropColor: 'transparent' },
                    beginAtZero: true
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
                mode: 'index',
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
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(0, 0, 0, 0.95)',
                    titleColor: '#ffffff',
                    bodyColor: '#e2e8f0',
                    borderColor: 'rgba(59, 130, 246, 0.8)',
                    borderWidth: 2,
                    padding: 15,
                    displayColors: true,
                    boxWidth: 12,
                    boxHeight: 12,
                    usePointStyle: true,
                    callbacks: {
                        title: function(context) {
                            if (context.length > 0) {
                                const taskNum = Math.round(context[0].parsed.x) + 1;
                                return '📊 Tarea #' + taskNum;
                            }
                            return '';
                        },
                        label: function(context) {
                            const label = context.dataset.label || '';
                            const value = context.parsed.y;
                            
                            // Agregar emoji según el tipo
                            let emoji = '•';
                            if (label === 'Fathers') emoji = '👨';
                            else if (label === 'Mothers') emoji = '👩';
                            else if (label === 'Children') emoji = '👶';
                            
                            return emoji + ' ' + label + ': ' + value.toFixed(2) + ' ms';
                        },
                        afterBody: function(context) {
                            // Calcular promedio de los 3 valores mostrados
                            if (context.length === 3) {
                                const avg = context.reduce((sum, ctx) => sum + ctx.parsed.y, 0) / context.length;
                                return '\n⚡ Promedio: ' + avg.toFixed(2) + ' ms';
                            }
                            return '';
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

        // Update genetic analysis charts
        updateGeneticCharts();

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

// Chromosome Distribution
function setChromType(type) {
    currentChromType = type;
    // Actualizar botones activos
    document.querySelectorAll('.chart-container:nth-of-type(1) .filter-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.textContent.toLowerCase() === type || (type === 'all' && btn.textContent === 'All')) {
            btn.classList.add('active');
        }
    });
    updateGeneticCharts();
}

// Genotype Type
function setGenoType(type) {
    currentGenoType = type;
    // Actualizar botones activos
    document.querySelectorAll('.chart-container:nth-of-type(3) .filter-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.textContent.toLowerCase() === type || (type === 'all' && btn.textContent === 'All')) {
            btn.classList.add('active');
        }
    });
    updateGeneticCharts();
}

// Update Genetic Charts
async function updateGeneticCharts() {
    try {
        const response = await fetch('/api/genetic_analysis');
        const data = await response.json();
        const colors = getThemeColors();
        
        // ========== Update Chromosome Distribution ==========
        if (currentChromType === 'all') {
            // Mostrar todos los tipos combinados
            const allChroms = {};
            ['fathers', 'mothers', 'children'].forEach(type => {
                const dist = data.chromosome_distribution[type] || {};
                Object.keys(dist).forEach(chr => {
                    if (!allChroms[chr]) allChroms[chr] = { fathers: 0, mothers: 0, children: 0 };
                    allChroms[chr][type] = dist[chr]?.total || 0;
                });
            });
            
            const chromLabels = Object.keys(allChroms).sort((a, b) => {
                const numA = parseInt(a) || (a === 'X' ? 23 : (a === 'Y' ? 24 : (a === 'MT' ? 25 : 99)));
                const numB = parseInt(b) || (b === 'X' ? 23 : (b === 'Y' ? 24 : (b === 'MT' ? 25 : 99)));
                return numA - numB;
            });
            
            // Usar función reutilizable
            updateBarChart(charts.chromosome, chromLabels, [
                {
                    label: 'Fathers',
                    data: chromLabels.map(chr => allChroms[chr].fathers),
                    backgroundColor: colors.blue + '80',
                    borderColor: colors.blue,
                    borderWidth: 2
                },
                {
                    label: 'Mothers',
                    data: chromLabels.map(chr => allChroms[chr].mothers),
                    backgroundColor: colors.red + '80',
                    borderColor: colors.red,
                    borderWidth: 2
                },
                {
                    label: 'Children',
                    data: chromLabels.map(chr => allChroms[chr].children),
                    backgroundColor: colors.green + '80',
                    borderColor: colors.green,
                    borderWidth: 2
                }
            ]);
        } else {
            // Mostrar solo un tipo
            const chromDist = data.chromosome_distribution[currentChromType] || {};
            const chromLabels = Object.keys(chromDist).sort((a, b) => {
                const numA = parseInt(a) || (a === 'X' ? 23 : (a === 'Y' ? 24 : (a === 'MT' ? 25 : 99)));
                const numB = parseInt(b) || (b === 'X' ? 23 : (b === 'Y' ? 24 : (b === 'MT' ? 25 : 99)));
                return numA - numB;
            });
            const chromData = chromLabels.map(chr => chromDist[chr]?.total || 0);
            
            // Usar función reutilizable
            updateBarChart(charts.chromosome, chromLabels, [{
                label: 'Variants Count',
                data: chromData,
                backgroundColor: colors.blue + '80',
                borderColor: colors.blue,
                borderWidth: 2
            }]);
        }
        
        // ========== Update Heterozygosity Population ==========
        const heteroFathers = data.heterozygosity_population.fathers || {};
        const heteroMothers = data.heterozygosity_population.mothers || {};
        const heteroChildren = data.heterozygosity_population.children || {};
        
        // Usar función reutilizable
        updateBarChart(charts.heteroPop, ['Fathers', 'Mothers', 'Children'], [
            {
                label: 'Heterozygous %',
                data: [
                    heteroFathers.heterozygous_pct || 0,
                    heteroMothers.heterozygous_pct || 0,
                    heteroChildren.heterozygous_pct || 0
                ],
                backgroundColor: colors.green + '80',
                borderColor: colors.green,
                borderWidth: 2
            },
            {
                label: 'Homozygous %',
                data: [
                    heteroFathers.homozygous_pct || 0,
                    heteroMothers.homozygous_pct || 0,
                    heteroChildren.homozygous_pct || 0
                ],
                backgroundColor: colors.blue + '80',
                borderColor: colors.blue,
                borderWidth: 2
            }
        ]);
        
        // ========== Update Genotype Trends ==========
        if (currentGenoType === 'all') {
            // Mostrar todos combinados
            const allGenotypes = {};
            ['fathers', 'mothers', 'children'].forEach(type => {
                const trends = data.genotype_trends[type] || {};
                const dist = trends.distribution || {};
                Object.keys(dist).forEach(geno => {
                    if (!allGenotypes[geno]) allGenotypes[geno] = { fathers: 0, mothers: 0, children: 0 };
                    allGenotypes[geno][type] = dist[geno] || 0;
                });
            });
            
            const genoLabels = Object.keys(allGenotypes);
            // Usar función reutilizable
            updateBarChart(charts.genotype, genoLabels, [
                {
                    label: 'Fathers',
                    data: genoLabels.map(g => allGenotypes[g].fathers),
                    backgroundColor: colors.blue + '80',
                    borderColor: colors.blue,
                    borderWidth: 2
                },
                {
                    label: 'Mothers',
                    data: genoLabels.map(g => allGenotypes[g].mothers),
                    backgroundColor: colors.red + '80',
                    borderColor: colors.red,
                    borderWidth: 2
                },
                {
                    label: 'Children',
                    data: genoLabels.map(g => allGenotypes[g].children),
                    backgroundColor: colors.green + '80',
                    borderColor: colors.green,
                    borderWidth: 2
                }
            ]);
        } else {
            // Mostrar solo un tipo
            const genoTrends = data.genotype_trends[currentGenoType] || {};
            const genoLabels = Object.keys(genoTrends.distribution || {});
            const genoData = Object.values(genoTrends.distribution || {});
            
            // Usar función reutilizable
            updateBarChart(charts.genotype, genoLabels, [{
                label: 'Count',
                data: genoData,
                backgroundColor: colors.purple + '80',
                borderColor: colors.purple,
                borderWidth: 2
            }]);
        }
        
        // ========== Update Heredabilidad ==========
        updateHeritability(data);
        
        // ========== Update Variantes Raras ==========
        updateRareVariants(data);
        
        // ========== Update Risk Score ==========
        updateRiskScore(data);
        
        // ========== Update Cohort Comparison ==========
        updateCohortComparison(data);
        
        // ========== Update Gene Detection (con filtro de familias) ==========
        // Aplicar filtro de familias
        const genesFathers = filterBySelectedFamilies(data.genetic_data.fathers || []);
        const genesMothers = filterBySelectedFamilies(data.genetic_data.mothers || []);
        const genesChildren = filterBySelectedFamilies(data.genetic_data.children || []);
        
        const geneCounts = {};
        [...genesFathers, ...genesMothers, ...genesChildren].forEach(snp => {
            if (snp && snp.gene && snp.gene !== 'Unknown') {
                if (!geneCounts[snp.gene]) {
                    geneCounts[snp.gene] = { fathers: 0, mothers: 0, children: 0 };
                }
            }
        });
        
        genesFathers.forEach(snp => {
            if (snp && snp.gene && geneCounts[snp.gene]) geneCounts[snp.gene].fathers++;
        });
        genesMothers.forEach(snp => {
            if (snp && snp.gene && geneCounts[snp.gene]) geneCounts[snp.gene].mothers++;
        });
        genesChildren.forEach(snp => {
            if (snp && snp.gene && geneCounts[snp.gene]) geneCounts[snp.gene].children++;
        });
        
        const geneLabels = Object.keys(geneCounts).slice(0, 15);
        // Usar función reutilizable
        updateBarChart(charts.gene, geneLabels, [
            {
                label: 'Fathers',
                data: geneLabels.map(g => geneCounts[g].fathers),
                backgroundColor: colors.blue + '80',
                borderColor: colors.blue,
                borderWidth: 2
            },
            {
                label: 'Mothers',
                data: geneLabels.map(g => geneCounts[g].mothers),
                backgroundColor: colors.red + '80',
                borderColor: colors.red,
                borderWidth: 2
            },
            {
                label: 'Children',
                data: geneLabels.map(g => geneCounts[g].children),
                backgroundColor: colors.green + '80',
                borderColor: colors.green,
                borderWidth: 2
            }
        ]);
        
        // ========== Update Mutation Rate Chart (usando genetic_metrics) ==========
        const geneticMetrics = data.genetic_metrics || {};
        const mutationHistory = geneticMetrics.mutation_rate_history || [];
        
        // Crear labels numéricos simples
        const timeLabels = mutationHistory.map((_, idx) => idx);
        
        // Un solo dataset con la tasa de mutación (variantes/segundo)
        charts.mutation.data.labels = timeLabels;
        charts.mutation.data.datasets = [{
            label: 'Mutation Rate (variants/sec)',
            data: mutationHistory,
            borderColor: colors.cyan,
            backgroundColor: colors.cyan + '20',
            tension: 0.4,
            borderWidth: 3,
            fill: true,
            pointRadius: 2
        }];
        charts.mutation.update('none');  // Sin animación para actualizaciones rápidas
        
        // ========== Update Genetic Interaction Network ==========
        updateGeneticNetwork(data);
        
    } catch (error) {
        console.error('Error updating genetic charts:', error);
    }
}

// Update Genetic Interaction Network - Versión mejorada y funcional
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

// Family Filter Functions removed - now using Timeline visualization

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initializeCharts();
    updateDashboard();
    loadFamilies('fathers');

    // Auto-refresh every 3 seconds
    setInterval(updateDashboard, 3000);
});

// ==================== NUEVAS FUNCIONES DE ANÁLISIS ====================

// Análisis de Heredabilidad Genética
function updateHeritability(data) {
    const colors = getThemeColors();
    
    // Obtener datos de padres e hijos
    const fathers = data.genetic_data?.fathers || [];
    const mothers = data.genetic_data?.mothers || [];
    const children = data.genetic_data?.children || [];
    
    if (fathers.length === 0 || mothers.length === 0 || children.length === 0) {
        updateBarChart(charts.heritability, [], []);
        return;
    }
    
    // Crear mapa de genes por familia y posición cromosómica
    const parentGenes = {};  // family_id -> {chr:pos -> [genes]}
    const childGenes = {};   // family_id -> {chr:pos -> [genes]}
    
    // Procesar padres
    [...fathers, ...mothers].forEach(snp => {
        if (!snp || !snp.gene || snp.gene === 'Unknown' || !snp.chromosome || !snp.position) return;
        const familyId = snp.family_id || 'unknown';
        const key = `${snp.chromosome}:${snp.position}`;
        
        if (!parentGenes[familyId]) parentGenes[familyId] = {};
        if (!parentGenes[familyId][key]) parentGenes[familyId][key] = [];
        parentGenes[familyId][key].push(snp.gene);
    });
    
    // Procesar hijos
    children.forEach(snp => {
        if (!snp || !snp.gene || snp.gene === 'Unknown' || !snp.chromosome || !snp.position) return;
        const familyId = snp.family_id || 'unknown';
        const key = `${snp.chromosome}:${snp.position}`;
        
        if (!childGenes[familyId]) childGenes[familyId] = {};
        if (!childGenes[familyId][key]) childGenes[familyId][key] = [];
        childGenes[familyId][key].push(snp.gene);
    });
    
    // Calcular heredabilidad por gen
    const geneHeritability = {};
    
    Object.keys(parentGenes).forEach(familyId => {
        if (!childGenes[familyId]) return;
        
        Object.keys(parentGenes[familyId]).forEach(pos => {
            const parentGenesAtPos = parentGenes[familyId][pos];
            const childGenesAtPos = childGenes[familyId][pos] || [];
            
            parentGenesAtPos.forEach(gene => {
                if (!geneHeritability[gene]) {
                    geneHeritability[gene] = { inherited: 0, total: 0 };
                }
                geneHeritability[gene].total++;
                
                if (childGenesAtPos.includes(gene)) {
                    geneHeritability[gene].inherited++;
                }
            });
        });
    });
    
    // Calcular porcentajes y obtener top 15
    const heritabilityArray = Object.entries(geneHeritability)
        .map(([gene, counts]) => ({
            gene,
            percentage: (counts.inherited / counts.total * 100).toFixed(1),
            inherited: counts.inherited,
            total: counts.total
        }))
        .sort((a, b) => b.percentage - a.percentage)
        .slice(0, 15);
    
    if (heritabilityArray.length === 0) {
        updateBarChart(charts.heritability, [], []);
        return;
    }
    
    const labels = heritabilityArray.map(h => h.gene);
    const percentages = heritabilityArray.map(h => parseFloat(h.percentage));
    
    updateBarChart(charts.heritability, labels, [{
        label: '% Heredado',
        data: percentages,
        backgroundColor: percentages.map(p => {
            if (p > 80) return colors.green + 'CC';
            if (p > 50) return colors.amber + 'CC';
            return colors.red + 'CC';
        }),
        borderColor: colors.textColor,
        borderWidth: 1
    }]);
}

// Análisis de Variantes Raras
function updateRareVariants(data) {
    const colors = getThemeColors();
    
    const allData = [
        ...(data.genetic_data?.fathers || []),
        ...(data.genetic_data?.mothers || []),
        ...(data.genetic_data?.children || [])
    ];
    
    if (allData.length === 0) {
        charts.rareVariants.data.labels = ['Sin datos'];
        charts.rareVariants.data.datasets[0].data = [1];
        charts.rareVariants.data.datasets[0].backgroundColor = [colors.gridColor];
        charts.rareVariants.update('none');
        return;
    }
    
    // Contar frecuencia de cada genotipo
    const genotypeFrequency = {};
    allData.forEach(snp => {
        if (!snp || !snp.genotype) return;
        genotypeFrequency[snp.genotype] = (genotypeFrequency[snp.genotype] || 0) + 1;
    });
    
    const total = allData.length;
    const threshold = total * 0.01;  // 1% threshold
    
    // Clasificar variantes
    let rareVariants = 0;
    let uncommonVariants = 0;
    let commonVariants = 0;
    let veryRareVariants = 0;
    
    Object.values(genotypeFrequency).forEach(count => {
        if (count === 1) {
            veryRareVariants++;
        } else if (count < threshold) {
            rareVariants++;
        } else if (count < total * 0.05) {
            uncommonVariants++;
        } else {
            commonVariants++;
        }
    });
    
    charts.rareVariants.data.labels = [
        `Únicas (${veryRareVariants})`,
        `Raras <1% (${rareVariants})`, 
        `Poco comunes <5% (${uncommonVariants})`,
        `Comunes >5% (${commonVariants})`
    ];
    charts.rareVariants.data.datasets[0].data = [
        veryRareVariants, 
        rareVariants, 
        uncommonVariants, 
        commonVariants
    ];
    charts.rareVariants.data.datasets[0].backgroundColor = [
        colors.purple,
        colors.pink,
        colors.amber,
        colors.green
    ];
    charts.rareVariants.update('none');
}

// ==================== NUEVOS ANÁLISIS ====================

// Score de Riesgo Genético por Cohorte
function updateRiskScore(data) {
    const colors = getThemeColors();
    const variantTypes = data.genetic_metrics?.variant_types || {};
    
    // Calcular score de riesgo (ejemplo: deletions y frameshift son más riesgosos)
    const riskWeights = {
        'deletion': 5,
        'frameshift': 4,
        'nonsense': 4,
        'insertion': 3,
        'missense': 2,
        'synonymous': 1,
        'unknown': 1
    };
    
    let totalRisk = 0;
    let totalVariants = 0;
    
    Object.entries(variantTypes).forEach(([type, count]) => {
        const weight = riskWeights[type.toLowerCase()] || 1;
        totalRisk += weight * count;
        totalVariants += count;
    });
    
    const avgRisk = totalVariants > 0 ? (totalRisk / totalVariants) : 0;
    
    // Clasificar riesgo
    let riskLevel = 'Bajo';
    let riskColor = colors.green;
    if (avgRisk >= 3.5) {
        riskLevel = 'Alto';
        riskColor = colors.red;
    } else if (avgRisk >= 2.5) {
        riskLevel = 'Medio-Alto';
        riskColor = colors.amber;
    } else if (avgRisk >= 1.5) {
        riskLevel = 'Medio';
        riskColor = colors.cyan;
    }
    
    // Distribución por nivel de riesgo
    const riskCategories = {
        'Bajo (1-2)': 0,
        'Medio (2-3)': 0,
        'Alto (3-5)': 0
    };
    
    Object.entries(variantTypes).forEach(([type, count]) => {
        const weight = riskWeights[type.toLowerCase()] || 1;
        if (weight <= 2) riskCategories['Bajo (1-2)'] += count;
        else if (weight <= 3) riskCategories['Medio (2-3)'] += count;
        else riskCategories['Alto (3-5)'] += count;
    });
    
    // Calcular riesgo por cohorte desde genetic_data
    const cohortRisks = { fathers: 0, mothers: 0, children: 0 };
    const cohortCounts = { fathers: 0, mothers: 0, children: 0 };
    
    ['fathers', 'mothers', 'children'].forEach(cohort => {
        const variants = data.genetic_data?.[cohort] || [];
        variants.forEach(snp => {
            if (!snp || !snp.variant_type) return;
            const weight = riskWeights[snp.variant_type.toLowerCase()] || 1;
            cohortRisks[cohort] += weight;
            cohortCounts[cohort]++;
        });
    });
    
    // Calcular score promedio (0-5)
    const avgRisks = {
        fathers: cohortCounts.fathers > 0 ? (cohortRisks.fathers / cohortCounts.fathers) : 0,
        mothers: cohortCounts.mothers > 0 ? (cohortRisks.mothers / cohortCounts.mothers) : 0,
        children: cohortCounts.children > 0 ? (cohortRisks.children / cohortCounts.children) : 0
    };
    
    charts.riskScore.data.labels = ['Fathers', 'Mothers', 'Children'];
    charts.riskScore.data.datasets = [{
        label: 'Score de Riesgo (1-5)',
        data: [avgRisks.fathers, avgRisks.mothers, avgRisks.children],
        backgroundColor: [
            colors.blue + 'CC',
            colors.red + 'CC',
            colors.green + 'CC'
        ],
        borderColor: colors.textColor,
        borderWidth: 2
    }];
    
    charts.riskScore.update('none');
}

// Comparación de Cohortes MEJORADA
function updateCohortComparison(data) {
    const colors = getThemeColors();
    
    // Métricas más detalladas para comparar
    const metrics = {
        'Total Variantes': {},
        'Genes Únicos': {},
        'Cromosomas Afectados': {},
        'Heterocigosidad (%)': {},
        'Variantes Alto Riesgo': {},
        'Diversidad Genotípica': {}
    };
    
    ['fathers', 'mothers', 'children'].forEach(cohort => {
        const variants = data.genetic_data?.[cohort] || [];
        
        // 1. Total variants
        metrics['Total Variantes'][cohort] = variants.length;
        
        // 2. Unique genes
        const uniqueGenes = new Set(variants.map(v => v?.gene).filter(g => g && g !== 'Unknown'));
        metrics['Genes Únicos'][cohort] = uniqueGenes.size;
        
        // 3. Cromosomas afectados
        const chromosomes = new Set(variants.map(v => v?.chromosome).filter(Boolean));
        metrics['Cromosomas Afectados'][cohort] = chromosomes.size;
        
        // 4. Heterocigosidad promedio
        const heteroData = data.heterozygosity_population?.[cohort] || {};
        const avgHetero = Object.keys(heteroData).length > 0 ? 
            Object.values(heteroData).reduce((a, b) => a + b, 0) / Object.keys(heteroData).length : 0;
        metrics['Heterocigosidad (%)'][cohort] = avgHetero * 100;
        
        // 5. Variantes de alto riesgo (deletion, frameshift, nonsense)
        const highRisk = variants.filter(v => 
            v?.variant_type && ['deletion', 'frameshift', 'nonsense'].includes(v.variant_type.toLowerCase())
        ).length;
        metrics['Variantes Alto Riesgo'][cohort] = highRisk;
        
        // 6. Diversidad genotípica (diferentes genotipos)
        const genotypes = new Set(variants.map(v => v?.genotype).filter(Boolean));
        metrics['Diversidad Genotípica'][cohort] = genotypes.size;
    });
    
    // Normalizar valores para radar chart (0-100)
    const normalizedMetrics = {};
    Object.entries(metrics).forEach(([metric, cohortValues]) => {
        const maxVal = Math.max(...Object.values(cohortValues), 1);
        normalizedMetrics[metric] = {};
        Object.entries(cohortValues).forEach(([cohort, val]) => {
            normalizedMetrics[metric][cohort] = (val / maxVal) * 100;
        });
    });
    
    const labels = Object.keys(metrics);
    
    charts.cohortComparison.data.labels = labels;
    charts.cohortComparison.data.datasets = [
        {
            label: 'Fathers',
            data: labels.map(m => normalizedMetrics[m].fathers || 0),
            borderColor: colors.blue,
            backgroundColor: colors.blue + '30',
            borderWidth: 2,
            pointRadius: 4,
            pointBackgroundColor: colors.blue,
            pointBorderColor: colors.textColor
        },
        {
            label: 'Mothers',
            data: labels.map(m => normalizedMetrics[m].mothers || 0),
            borderColor: colors.red,
            backgroundColor: colors.red + '30',
            borderWidth: 2,
            pointRadius: 4,
            pointBackgroundColor: colors.red,
            pointBorderColor: colors.textColor
        },
        {
            label: 'Children',
            data: labels.map(m => normalizedMetrics[m].children || 0),
            borderColor: colors.green,
            backgroundColor: colors.green + '30',
            borderWidth: 2,
            pointRadius: 4,
            pointBackgroundColor: colors.green,
            pointBorderColor: colors.textColor
        }
    ];
    
    charts.cohortComparison.update('none');
}

// Red Genética - Usa datos REALES del backend (gene_frequency)
function updateGeneticNetwork(data) {
    const colors = getThemeColors();
    
    // DEBUG: Ver qué datos llegan
    console.log('🔍 Network Debug - gene_frequency:', data.genetic_metrics?.gene_frequency);
    console.log('🔍 Network Debug - genetic_data:', {
        fathers: (data.genetic_data?.fathers || []).length,
        mothers: (data.genetic_data?.mothers || []).length,
        children: (data.genetic_data?.children || []).length
    });
    
    // Obtener todos los datos genéticos
    const allData = [
        ...(data.genetic_data?.fathers || []),
        ...(data.genetic_data?.mothers || []),
        ...(data.genetic_data?.children || [])
    ];
    
    if (allData.length === 0) {
        console.warn('⚠️ No genetic data for network');
        charts.network.data.datasets = [];
        charts.network.update('none');
        return;
    }
    
    // Calcular frecuencia de genes Y cromosomas desde los datos
    const geneFrequency = {};
    const geneChromosomes = {};
    
    allData.forEach(snp => {
        if (!snp || !snp.gene || snp.gene === 'Unknown' || snp.gene.startsWith('Chr')) return;
        if (!snp.chromosome) return;
        
        // Contar frecuencia
        geneFrequency[snp.gene] = (geneFrequency[snp.gene] || 0) + 1;
        
        // Guardar cromosomas
        if (!geneChromosomes[snp.gene]) {
            geneChromosomes[snp.gene] = new Set();
        }
        geneChromosomes[snp.gene].add(snp.chromosome);
    });
    
    console.log('✅ Found', Object.keys(geneFrequency).length, 'unique genes');
    
    if (Object.keys(geneFrequency).length === 0) {
        console.warn('⚠️ No valid genes found');
        charts.network.data.datasets = [];
        charts.network.update('none');
        return;
    }
    
    // Top 15 genes más frecuentes
    const topGenes = Object.entries(geneFrequency)
        .filter(([gene, count]) => gene && gene !== 'Unknown')
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15)
        .map(([gene, count]) => ({ gene, count }));
    
    console.log('✅ Top 15:', topGenes.map(g => `${g.gene}(${g.count})`).join(', '));
    
    if (topGenes.length === 0) {
        console.warn('⚠️ No top genes after filtering');
        charts.network.data.datasets = [];
        charts.network.update('none');
        return;
    }
    
    // Posicionar genes en círculo
    const angleStep = (2 * Math.PI) / topGenes.length;
    const radius = 100;
    const genePositions = {};
    
    topGenes.forEach((geneObj, idx) => {
        const angle = idx * angleStep;
        genePositions[geneObj.gene] = {
            x: radius * Math.cos(angle),
            y: radius * Math.sin(angle),
            count: geneObj.count
        };
    });
    
    // Crear dataset de nodos
    const nodeData = topGenes.map(geneObj => ({
        x: genePositions[geneObj.gene].x,
        y: genePositions[geneObj.gene].y,
        gene: geneObj.gene,
        count: geneObj.count
    }));
    
    // Crear dataset de conexiones (genes que comparten cromosomas)
    const connectionData = [];
    const topGeneNames = topGenes.map(g => g.gene);
    
    for (let i = 0; i < topGeneNames.length; i++) {
        for (let j = i + 1; j < topGeneNames.length; j++) {
            const gene1 = topGeneNames[i];
            const gene2 = topGeneNames[j];
            
            const chr1 = geneChromosomes[gene1] || new Set();
            const chr2 = geneChromosomes[gene2] || new Set();
            
            // Si comparten al menos un cromosoma, crear conexión
            const sharedChromosomes = [...chr1].filter(c => chr2.has(c));
            
            if (sharedChromosomes.length > 0) {
                // Agregar línea de conexión
                connectionData.push({
                    x: genePositions[gene1].x,
                    y: genePositions[gene1].y
                });
                connectionData.push({
                    x: genePositions[gene2].x,
                    y: genePositions[gene2].y
                });
                // Punto nulo para separar líneas
                connectionData.push({ x: null, y: null });
            }
        }
    }
    
    // Dataset de líneas de conexión
    const connectionDataset = {
        label: 'Interacciones (mismo cromosoma)',
        data: connectionData,
        showLine: true,
        borderColor: colors.cyan + '30',
        borderWidth: 1,
        pointRadius: 0,
        fill: false,
        spanGaps: false
    };
    
    // Dataset de nodos
    const nodeDataset = {
        label: 'Genes',
        data: nodeData,
        backgroundColor: nodeData.map(() => colors.purple),
        borderColor: colors.textColor,
        borderWidth: 2,
        pointRadius: nodeData.map(n => Math.min(Math.sqrt(n.count) * 2 + 8, 18)),
        pointHoverRadius: nodeData.map(n => Math.min(Math.sqrt(n.count) * 2 + 12, 24)),
        showLine: false
    };
    
    charts.network.data.datasets = [connectionDataset, nodeDataset];
    
    // Actualizar escalas
    charts.network.options.scales.x.min = -radius * 1.3;
    charts.network.options.scales.x.max = radius * 1.3;
    charts.network.options.scales.y.min = -radius * 1.3;
    charts.network.options.scales.y.max = radius * 1.3;
    
    charts.network.update('none');
}
