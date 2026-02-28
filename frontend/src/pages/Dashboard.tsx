import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { getMappings, getRuns, deleteRun } from '../api/client';

export default function Dashboard() {
    const navigate = useNavigate();
    const [stats, setStats] = useState({ mappings: 0, runs: 0, passed: 0, failed: 0 });
    const [recentRuns, setRecentRuns] = useState<any[]>([]);

    const loadData = () => {
        Promise.all([getMappings(), getRuns(0, 10)]).then(([mRes, rRes]) => {
            const mappings = mRes.data;
            const { runs, total } = rRes.data;
            const totalPassed = runs.reduce((s: number, r: any) => s + (r.passed_cases || 0), 0);
            const totalFailed = runs.reduce((s: number, r: any) => s + (r.failed_cases || 0), 0);
            setStats({ mappings: mappings.length, runs: total, passed: totalPassed, failed: totalFailed });
            setRecentRuns(runs);
        }).catch(() => { });
    };

    useEffect(() => {
        loadData();
    }, []);

    const handleDelete = async (id: number) => {
        if (!confirm('Are you sure you want to delete this test run?')) return;
        try {
            await deleteRun(id);
            loadData();
        } catch (err) {
            alert('Failed to delete test run');
        }
    };

    return (
        <div>
            <div className="page-header">
                <h1>Dashboard</h1>
                <p>Overview of your ETL testing activity</p>
            </div>

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="label">Mapping Documents</div>
                    <div className="value accent">{stats.mappings}</div>
                </div>
                <div className="stat-card">
                    <div className="label">Test Runs</div>
                    <div className="value accent">{stats.runs}</div>
                </div>
                <div className="stat-card">
                    <div className="label">Tests Passed</div>
                    <div className="value success">{stats.passed}</div>
                </div>
                <div className="stat-card">
                    <div className="label">Tests Failed</div>
                    <div className="value danger">{stats.failed}</div>
                </div>
            </div>

            <div className="flex justify-between items-center mb-4">
                <h2>Recent Test Runs</h2>
                <button className="btn btn-primary" onClick={() => navigate('/upload')}>
                    📄 Upload New Mapping
                </button>
            </div>

            {recentRuns.length === 0 ? (
                <div className="empty-state">
                    <div className="icon">🚀</div>
                    <p>No test runs yet. Upload a mapping document to get started.</p>
                </div>
            ) : (
                <>
                    <div className="table-wrapper">
                        <table>
                            <thead>
                                <tr>
                                    <th>Run #</th>
                                    <th>Status</th>
                                    <th>Total</th>
                                    <th>Passed</th>
                                    <th>Failed</th>
                                    <th>Date</th>
                                    <th></th>
                                </tr>
                            </thead>
                            <tbody>
                                {recentRuns.map((run: any) => (
                                    <tr key={run.id}>
                                        <td>#{run.id}</td>
                                        <td>
                                            <span className={`badge ${run.status === 'completed' ? 'badge-success' : 'badge-warning'}`}>
                                                {run.status}
                                            </span>
                                        </td>
                                        <td>{run.total_cases}</td>
                                        <td style={{ color: 'var(--success)' }}>{run.passed_cases}</td>
                                        <td style={{ color: 'var(--danger)' }}>{run.failed_cases}</td>
                                        <td className="text-muted text-sm">{new Date(run.started_at).toLocaleString()}</td>
                                        <td className="flex gap-2">
                                            <button className="btn btn-secondary btn-sm" onClick={() => navigate(`/results/${run.id}`)}>
                                                View
                                            </button>
                                            <button className="btn btn-danger btn-sm" onClick={() => handleDelete(run.id)}>
                                                🗑️
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                    <div className="flex justify-center mt-6">
                        <button className="btn btn-secondary" onClick={() => navigate('/results')}>
                            View Full History →
                        </button>
                    </div>
                </>
            )}
        </div>
    );
}
