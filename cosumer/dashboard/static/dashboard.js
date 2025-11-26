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
        
    } catch (error) {
        console.error('Error updating dashboard:', error);
    }
}

function updateProcessingChart(history) {
    const fathersData = history.filter(h => h.member_type === 'fathers').map(h => h.records);
    const mothersData = history.filter(h => h.member_type === 'mothers').map(h => h.records);
    const childrenData = history.filter(h => h.member_type === 'children').map(h => h.records);
    const labels = history.filter(h => h.member_type === 'fathers').map((h, i) => `B${i+1}`);
    
    charts.processing.data.labels = labels;
    charts.processing.data.datasets[0].data = fathersData;
    charts.processing.data.datasets[1].data = mothersData;
    charts.processing.data.datasets[2].data = childrenData;
    charts.processing.update();
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
