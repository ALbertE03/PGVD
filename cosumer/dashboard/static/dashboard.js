// Global chart instances
let charts = {};
let currentGenderType = 'fathers';
let currentChromType = 'fathers';
let currentGenoType = 'fathers';
let currentSnpRangeType = 'fathers';
let currentFamilySizeType = 'fathers';

// Initialize all charts
function initializeCharts() {
    // Processing Rate - Line Chart
    charts.processing = new Chart(document.getElementById('processingChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [
                {
                    label: 'Fathers',
                    data: [],
                    borderColor: '#3498db',
                    backgroundColor: 'rgba(52, 152, 219, 0.1)',
                    tension: 0.4
                },
                {
                    label: 'Mothers',
                    data: [],
                    borderColor: '#e74c3c',
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    tension: 0.4
                },
                {
                    label: 'Children',
                    data: [],
                    borderColor: '#2ecc71',
                    backgroundColor: 'rgba(46, 204, 113, 0.1)',
                    tension: 0.4
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            scales: {
                y: { beginAtZero: true }
            },
            plugins: {
                legend: { position: 'bottom' }
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
                borderColor: '#e67e22',
                backgroundColor: 'rgba(230, 126, 34, 0.1)',
                tension: 0.3,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true, max: 100 } },
            plugins: { legend: { display: false } },
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
                borderColor: '#9b59b6',
                backgroundColor: 'rgba(155, 89, 182, 0.1)',
                tension: 0.3,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true, max: 100 } },
            plugins: { legend: { display: false } },
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
                backgroundColor: ['#e74c3c', '#2ecc71'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '60%',
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: {
                        font: { size: 11 },
                        padding: 10
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

    // Current values text
    if (data.current.cpu) document.getElementById('cpu-val').textContent = data.current.cpu.value.toFixed(1);
    if (data.current.ram) document.getElementById('ram-val').textContent = data.current.ram.value.toFixed(1);
    if (data.current.disk) document.getElementById('disk-val').textContent = data.current.disk.value.toFixed(1);

    // Update CPU Graph
    if (data.cpu.length > 0) {
        charts.cpu.data.labels = data.cpu.map(d => new Date(d.timestamp).toLocaleTimeString());
        charts.cpu.data.datasets[0].data = data.cpu.map(d => d.value);
        charts.cpu.update();
    }

    // Update RAM Graph
    if (data.ram.length > 0) {
        charts.ram.data.labels = data.ram.map(d => new Date(d.timestamp).toLocaleTimeString());
        charts.ram.data.datasets[0].data = data.ram.map(d => d.value);
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
            const statusColor = isAlive ? '#2ecc71' : (m.status === 'STANDBY' ? '#f39c12' : '#e74c3c');
            const icon = isAlive ? '⚡' : (m.status === 'STANDBY' ? '💤' : '❌');

            mastersHtml += `
                <div style="background:#f8f9fa; border-top: 4px solid ${statusColor}; border-radius:8px; padding:15px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                    <div style="font-weight:bold; font-size:1em; margin-bottom:5px;">${icon} ${m.name}</div>
                    <div style="font-size:0.8em; color:#666; margin-bottom:5px;">${m.url || 'No URL'}</div>
                    <div style="font-size:0.9em; font-weight:bold; color:${statusColor};">${m.status}</div>
                </div>
            `;
        });
        mastersContainer.innerHTML = mastersHtml;
    } else {
        mastersContainer.innerHTML = '<div style="color:#666;">No masters found</div>';
    }

    // --- Render Workers ---
    if (!sparkData.workers || sparkData.workers.length === 0) {
        workersContainer.innerHTML = '<div style="grid-column: 1/-1; text-align: center; padding: 20px; color: #666;">No Workers Registered (Check Active Master)</div>';
        return;
    }

    let html = '';

    // Workers
    sparkData.workers.sort((a, b) => a.id.localeCompare(b.id)).forEach(w => {
        const coreUsage = ((w.cores - w.coresfree) / w.cores) * 100;
        const memUsage = ((w.memory - w.memoryfree) / w.memory) * 100;
        const statusColor = w.state === 'ALIVE' ? '#2ecc71' : '#e74c3c';

        html += `
            <div style="background:#f8f9fa; border-left: 4px solid ${statusColor}; border-radius:8px; padding:15px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                <div style="font-weight:bold; font-size:0.9em; margin-bottom:5px; word-break:break-all;">${w.id}</div>
                <div style="font-size:0.8em; color:#666; margin-bottom:10px;">${w.host}:${w.port}</div>
                
                <div style="margin-bottom:8px;">
                    <div style="display:flex; justify-content:space-between; font-size:0.75em; margin-bottom:2px;">
                        <span>Cores</span>
                        <span>${w.cores - w.coresfree}/${w.cores}</span>
                    </div>
                    <div style="height:6px; background:#e0e0e0; border-radius:3px; overflow:hidden;">
                        <div style="height:100%; width:${coreUsage}%; background:#3498db;"></div>
                    </div>
                </div>
                
                <div>
                    <div style="display:flex; justify-content:space-between; font-size:0.75em; margin-bottom:2px;">
                        <span>Memory (MB)</span>
                        <span>${w.memory - w.memoryfree}/${w.memory}</span>
                    </div>
                    <div style="height:6px; background:#e0e0e0; border-radius:3px; overflow:hidden;">
                        <div style="height:100%; width:${memUsage}%; background:#9b59b6;"></div>
                    </div>
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
        underReplicatedEl.style.color = (hdfs.under_replicated || 0) > 0 ? '#f39c12' : '#2ecc71';
    }

    const corruptEl = document.getElementById('hdfs-corrupt');
    if (corruptEl) {
        corruptEl.textContent = hdfs.corrupt_blocks || 0;
        corruptEl.style.color = (hdfs.corrupt_blocks || 0) > 0 ? '#e74c3c' : '#2ecc71';
    }

    const missingEl = document.getElementById('hdfs-missing');
    if (missingEl) {
        missingEl.textContent = hdfs.missing_blocks || 0;
        missingEl.style.color = (hdfs.missing_blocks || 0) > 0 ? '#e74c3c' : '#2ecc71';
    }

    // Update safemode
    const safemodeEl = document.getElementById('hdfs-safemode');
    if (safemodeEl) {
        safemodeEl.textContent = hdfs.safemode ? 'ON' : 'OFF';
        safemodeEl.style.color = hdfs.safemode ? '#f39c12' : '#2ecc71';
    }

    // Update DataNodes
    const datanodesContainer = document.getElementById('hdfs-datanodes');
    if (datanodesContainer && hdfs.datanodes && hdfs.datanodes.length > 0) {
        let html = '';
        hdfs.datanodes.forEach(node => {
            const usagePercent = node.capacity > 0 ? (node.used / node.capacity * 100) : 0;
            const statusColor = node.state === 'In Service' ? '#2ecc71' : '#f39c12';
            
            html += `
                <div style="background:#f8f9fa; border-left: 4px solid ${statusColor}; border-radius:8px; padding:15px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
                    <div style="font-weight:bold; font-size:0.9em; margin-bottom:5px; word-break:break-all;">📦 ${node.name}</div>
                    <div style="font-size:0.8em; color:#666; margin-bottom:10px;">State: ${node.state}</div>
                    
                    <div style="margin-bottom:8px;">
                        <div style="display:flex; justify-content:space-between; font-size:0.75em; margin-bottom:2px;">
                            <span>Storage</span>
                            <span>${formatBytes(node.used)} / ${formatBytes(node.capacity)}</span>
                        </div>
                        <div style="height:6px; background:#e0e0e0; border-radius:3px; overflow:hidden;">
                            <div style="height:100%; width:${usagePercent}%; background:#3498db;"></div>
                        </div>
                    </div>
                    
                    <div style="font-size:0.75em; color:#666;">
                        Remaining: ${formatBytes(node.remaining)}
                    </div>
                </div>
            `;
        });
        datanodesContainer.innerHTML = html;
    } else if (datanodesContainer) {
        datanodesContainer.innerHTML = '<div style="color:#666;">No DataNodes found</div>';
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
