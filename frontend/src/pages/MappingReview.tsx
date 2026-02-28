import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { getMappings, getMapping, deleteMapping } from '../api/client';

export default function MappingReview() {
    const { id } = useParams();
    const navigate = useNavigate();
    const [mappingList, setMappingList] = useState<any[]>([]);
    const [mapping, setMapping] = useState<any>(null);


    useEffect(() => {
        if (id) {
            getMapping(Number(id)).then(r => setMapping(r.data)).catch(() => { });
        } else {
            getMappings().then(r => setMappingList(r.data)).catch(() => { });
        }
    }, [id]);



    const handleDelete = async (e: React.MouseEvent, id: number, name: string) => {
        e.stopPropagation();
        if (window.confirm(`Are you sure you want to completely delete "${name}"? This will also delete all associated test cases and results.`)) {
            try {
                await deleteMapping(id);
                setMappingList(prev => prev.filter(m => m.id !== id));
            } catch (err: any) {
                alert(`Error deleting document: ${err.message}`);
            }
        }
    };

    // List view
    if (!id) {
        return (
            <div>
                <div className="page-header">
                    <h1>Mapping Documents</h1>
                    <p>Review AI-parsed field mappings</p>
                </div>
                {mappingList.length === 0 ? (
                    <div className="empty-state">
                        <div className="icon">🔗</div>
                        <p>No mappings yet. Upload a mapping document first.</p>
                    </div>
                ) : (
                    <div className="table-wrapper">
                        <table>
                            <thead><tr><th>Name & Date</th><th>Status</th><th>Actions</th></tr></thead>
                            <tbody>
                                {mappingList.map((m: any) => (
                                    <tr key={m.id}>
                                        <td>
                                            <div style={{ display: 'flex', flexDirection: 'column' }}>
                                                <span style={{ fontWeight: 500 }}>{m.name}</span>
                                                <span className="text-muted text-sm">{new Date(m.created_at).toLocaleString()}</span>
                                            </div>
                                        </td>
                                        <td><span className={`badge ${m.status === 'parsed' ? 'badge-success' : m.status === 'error' ? 'badge-danger' : 'badge-info'}`}>{m.status}</span></td>
                                        <td>
                                            <div className="flex gap-2">
                                                <button className="btn btn-secondary btn-sm" onClick={() => navigate(`/mappings/${m.id}`)}>Review</button>
                                                <button className="btn btn-danger btn-sm" onClick={(e) => handleDelete(e, m.id, m.name)}>🗑️ Delete</button>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>
        );
    }

    // Detail view
    if (!mapping) return <div className="loading">Loading...</div>;

    return (
        <div>
            <div className="page-header flex justify-between items-center">
                <div>
                    <h1>{mapping.name}</h1>
                    <p>
                        <span className={`badge ${mapping.status === 'parsed' ? 'badge-success' : 'badge-warning'}`}>{mapping.status}</span>
                        {' '}&mdash; {mapping.field_mappings?.length || 0} field mappings
                    </p>
                </div>
                {mapping.status === 'parsed' && (
                    <button className="btn btn-primary" onClick={() => navigate('/testcases', { state: { mappingId: mapping.id } })}>
                        🧪 View Test Cases
                    </button>
                )}
            </div>

            {mapping.error_message && (
                <div className="card mb-4" style={{ background: 'var(--danger-bg)', borderColor: 'var(--danger)' }}>
                    <p style={{ color: 'var(--danger)' }}>Error: {mapping.error_message}</p>
                </div>
            )}

            {mapping.field_mappings?.length > 0 && (
                <div className="table-wrapper">
                    <table>
                        <thead>
                            <tr>
                                <th>Source Table</th>
                                <th>Source Column</th>
                                <th>→</th>
                                <th>Target Table</th>
                                <th>Target Column</th>
                                <th>Transformation</th>
                                <th>Key</th>
                            </tr>
                        </thead>
                        <tbody>
                            {mapping.field_mappings.map((fm: any) => (
                                <tr key={fm.id}>
                                    <td><span className="font-mono">{fm.source_table}</span></td>
                                    <td><span className="font-mono">{fm.source_column}</span></td>
                                    <td style={{ color: 'var(--accent)' }}>→</td>
                                    <td><span className="font-mono">{fm.target_table}</span></td>
                                    <td><span className="font-mono">{fm.target_column}</span></td>
                                    <td><span className="badge badge-info">{fm.transformation}</span></td>
                                    <td>{fm.is_key ? '🔑' : ''}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
