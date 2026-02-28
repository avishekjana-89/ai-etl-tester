import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { getRuns, getRun, deleteRun } from '../api/client';

export default function Results() {
    const { id } = useParams();
    const navigate = useNavigate();
    const [runs, setRuns] = useState<any[]>([]);
    const [run, setRun] = useState<any>(null);
    const [selectedResult, setSelectedResult] = useState<any>(null);
    const [currentPage, setCurrentPage] = useState(1);
    const [totalRuns, setTotalRuns] = useState(0);
    const limit = 20;

    const loadData = () => {
        if (id) {
            getRun(Number(id)).then(r => setRun(r.data)).catch(() => { });
        } else {
            getRuns((currentPage - 1) * limit, limit).then(r => {
                setRuns(r.data.runs);
                setTotalRuns(r.data.total);
            }).catch(() => { });
        }
    };

    useEffect(() => {
        loadData();
    }, [id, currentPage]);

    const handleDelete = async (deleteId: number) => {
        if (!confirm('Are you sure you want to delete this test run?')) return;
        try {
            await deleteRun(deleteId);
            loadData();
        } catch (err) {
            alert('Failed to delete test run');
        }
    };

    // List view
    if (!id) {
        return (
            <div>
                <div className="page-header">
                    <h1>Test Results</h1>
                    <p>View execution results and mismatch details</p>
                </div>
                {runs.length === 0 ? (
                    <div className="empty-state">
                        <div className="icon">📋</div>
                        <p>No test runs yet.</p>
                    </div>
                ) : (
                    <div className="table-wrapper">
                        <table>
                            <thead><tr><th>Run #</th><th>Status</th><th>Total</th><th>Passed</th><th>Failed</th><th>Date</th><th></th></tr></thead>
                            <tbody>
                                {runs.map((r: any) => (
                                    <tr key={r.id}>
                                        <td>#{r.id}</td>
                                        <td><span className={`badge ${r.status === 'completed' ? 'badge-success' : r.status === 'failed' ? 'badge-danger' : 'badge-warning'}`}>{r.status}</span></td>
                                        <td>{r.total_cases}</td>
                                        <td style={{ color: 'var(--success)' }}>{r.passed_cases}</td>
                                        <td style={{ color: 'var(--danger)' }}>{r.failed_cases}</td>
                                        <td>{new Date(r.started_at).toLocaleString()}</td>
                                        <td className="flex gap-2">
                                            <button className="btn btn-secondary btn-sm" onClick={() => navigate(`/results/${r.id}`)}>
                                                Details
                                            </button>
                                            <button className="btn btn-danger btn-sm" onClick={() => handleDelete(r.id)}>
                                                🗑️
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}

                {totalRuns > limit && (
                    <div className="flex justify-center items-center gap-4 mt-6">
                        <button
                            className="btn btn-secondary btn-sm"
                            disabled={currentPage === 1}
                            onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                        >
                            ← Previous
                        </button>
                        <span className="text-sm text-muted">Page {currentPage} of {Math.ceil(totalRuns / limit)}</span>
                        <button
                            className="btn btn-secondary btn-sm"
                            disabled={currentPage >= Math.ceil(totalRuns / limit)}
                            onClick={() => setCurrentPage(prev => prev + 1)}
                        >
                            Next →
                        </button>
                    </div>
                )}
            </div>
        );
    }

    // Detail view
    if (!run) return <div className="loading">Loading...</div>;

    const passRate = run.total_cases > 0 ? Math.round((run.passed_cases / run.total_cases) * 100) : 0;

    const downloadCSV = () => {
        if (!run || !run.results) return;

        const headers = ["Test Case ID", "Test Name", "Type", "Status", "Source Value", "Target Value", "Mismatches", "Error Message"];
        const rows = run.results.map((r: any) => [
            r.test_case_id,
            `"${(r.test_case_name || '').replace(/"/g, '""')}"`,
            r.test_case_type,
            r.passed ? 'PASS' : 'FAIL',
            `"${(r.source_value || '').replace(/"/g, '""')}"`,
            `"${(r.target_value || '').replace(/"/g, '""')}"`,
            r.mismatch_count,
            `"${(r.error_message || '').replace(/"/g, '""')}"`
        ]);

        const csvContent = [
            `Run ID:,${run.id}`,
            `Mapping Document:,${run.mapping_name || 'Unknown'}`,
            `Status:,${run.status}`,
            `Date:,${new Date(run.started_at).toLocaleString()}`,
            `Total:,${run.total_cases}`,
            `Passed:,${run.passed_cases}`,
            `Failed:,${run.failed_cases}`,
            "",
            headers.join(","),
            ...rows.map((row: any[]) => row.join(","))
        ].join("\n");

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.setAttribute("download", `test_run_${run.id}_report.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    return (
        <div>
            <div className="page-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div>
                    <h1>Test Run #{run.id}</h1>
                    <div style={{ fontSize: '1.1rem', color: 'var(--accent)', fontWeight: 600, marginBottom: '8px' }}>
                        📋 {run.mapping_name}
                    </div>
                    <p>
                        <span className={`badge ${run.status === 'completed' ? 'badge-success' : 'badge-danger'}`}>{run.status}</span>
                        {' '}&mdash; {new Date(run.started_at).toLocaleString()}
                    </p>
                </div>
                <button className="btn btn-primary" onClick={downloadCSV}>
                    Download CSV Report ⬇️
                </button>
            </div>

            <div className="stat-grid">
                <div className="stat-card">
                    <div className="label">Total Tests</div>
                    <div className="value accent">{run.total_cases}</div>
                </div>
                <div className="stat-card">
                    <div className="label">Passed</div>
                    <div className="value success">{run.passed_cases}</div>
                </div>
                <div className="stat-card">
                    <div className="label">Failed</div>
                    <div className="value danger">{run.failed_cases}</div>
                </div>
                <div className="stat-card">
                    <div className="label">Pass Rate</div>
                    <div className={`value ${passRate >= 80 ? 'success' : passRate >= 50 ? 'warning' : 'danger'}`}>{passRate}%</div>
                </div>
            </div>

            <h2 className="mb-4">Test Results</h2>
            <div className="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Test Case</th>
                            <th>Source Value</th>
                            <th>Target Value</th>
                            <th>Mismatches</th>
                            <th></th>
                        </tr>
                    </thead>
                    <tbody>
                        {run.results?.map((r: any) => (
                            <tr key={r.id}>
                                <td>{r.passed ? <span className="badge badge-success">✅ Pass</span> : <span className="badge badge-danger">❌ Fail</span>}</td>
                                <td>
                                    <div style={{ fontWeight: 600, marginBottom: '4px' }}>{r.test_case_name || `Test #${r.test_case_id}`}</div>
                                    <div className="text-muted text-sm">
                                        <span className="badge" style={{ background: 'var(--bg-card)', border: '1px solid var(--border)' }}>{r.test_case_type}</span>
                                        <span style={{ marginLeft: '8px' }}>#{r.test_case_id}</span>
                                    </div>
                                </td>
                                <td className="font-mono">{r.source_value || '—'}</td>
                                <td className="font-mono">{r.target_value || '—'}</td>
                                <td>{r.mismatch_count > 0 ? <span style={{ color: 'var(--danger)' }}>{r.mismatch_count}</span> : '—'}</td>
                                <td>
                                    {(r.mismatch_sample || r.error_message) && (
                                        <button className="btn btn-secondary btn-sm" onClick={() => setSelectedResult(r)}>
                                            Details
                                        </button>
                                    )}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {/* Mismatch Detail Modal */}
            {selectedResult && (
                <div className="modal-overlay" onClick={() => setSelectedResult(null)}>
                    <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '800px', maxHeight: '80vh', overflow: 'auto' }}>
                        <h2>Mismatch Details</h2>
                        {selectedResult.error_message && (
                            <div className="card mb-4" style={{ background: 'var(--danger-bg)' }}>
                                <p style={{ color: 'var(--danger)' }}>{selectedResult.error_message}</p>
                            </div>
                        )}
                        {selectedResult.mismatch_sample && (
                            <div className="table-wrapper">
                                <table>
                                    <thead>
                                        <tr>
                                            {Object.keys(JSON.parse(selectedResult.mismatch_sample)[0] || {}).map(k => (
                                                <th key={k}>{k}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {JSON.parse(selectedResult.mismatch_sample).slice(0, 50).map((row: any, i: number) => (
                                            <tr key={i}>
                                                {Object.values(row).map((v: any, j: number) => (
                                                    <td key={j} className="font-mono">{String(v)}</td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}
                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={() => setSelectedResult(null)}>Close</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
