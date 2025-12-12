// Global chart instances
let charts = {};
let currentGenderType = 'fathers';
let currentChromType = 'fathers';
let currentGenoType = 'fathers';
let currentSnpRangeType = 'fathers';
let currentFamilySizeType = 'fathers';

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

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    initializeCharts();
    updateDashboard();
    loadFamilies('fathers');

    // Auto-refresh every 3 seconds
    setInterval(updateDashboard, 3000);
});
