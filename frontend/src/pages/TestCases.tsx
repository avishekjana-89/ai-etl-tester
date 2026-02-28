import { useEffect, useState, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { getMappings, getTestCases, createRun } from '../api/client';

export default function TestCases() {
    const navigate = useNavigate();
    const location = useLocation();
    const [mappings, setMappings] = useState<any[]>([]);
    const [selectedMapping, setSelectedMapping] = useState<number | null>(null);
    const [testCases, setTestCases] = useState<any[]>([]);
    const [running, setRunning] = useState(false);
    const [connectors, setConnectors] = useState<any[]>([]);
    const [sourceOverride, setSourceOverride] = useState<number | null>(null);
    const [targetOverride, setTargetOverride] = useState<number | null>(null);
    const [runtimeParams, setRuntimeParams] = useState<{ key: string; value: string }[]>([]);
    const [showSettings, setShowSettings] = useState(false);
    const [editingTC, setEditingTC] = useState<any>(null); // if null, closed. if { id: 'new' }, creating. else, editing.
    const [editName, setEditName] = useState('');
    const [editType, setEditType] = useState('custom');
    const [editSourceSQL, setEditSourceSQL] = useState('');
    const [editTargetSQL, setEditTargetSQL] = useState('');
    const [editStatus, setEditStatus] = useState('manual');
    const [expandedId, setExpandedId] = useState<number | null>(null);

    useEffect(() => {
        getMappings().then(r => {
            const parsed = r.data.filter((m: any) => m.status === 'parsed');
            setMappings(parsed);

            // Priority: 1. Passed state from navigation, 2. First parsed mapping
            const passedId = location.state?.mappingId;
            if (passedId && parsed.some((m: any) => m.id === passedId)) {
                setSelectedMapping(passedId);
            } else if (parsed.length > 0) {
                setSelectedMapping(parsed[0].id);
            }
        }).catch(() => { });

        import('../api/client').then(m => m.getConnectors().then(r => setConnectors(r.data)));
    }, [location]);

    useEffect(() => {
        if (selectedMapping) {
            getTestCases(selectedMapping).then(r => setTestCases(r.data)).catch(() => setTestCases([]));
        }
    }, [selectedMapping]);

    // Derive which connectors are missing for the selected mapping
    const missingConnectors = useMemo(() => {
        const mapping = mappings.find((m: any) => m.id === selectedMapping);
        if (!mapping) return [];
        const missing: string[] = [];
        if (mapping.source_connector_id === null && !sourceOverride) missing.push('source');
        if (mapping.target_connector_id === null && !targetOverride) missing.push('target');
        return missing;
    }, [mappings, selectedMapping, sourceOverride, targetOverride]);

    const handleRun = async () => {
        if (!selectedMapping) return;
        setRunning(true);

        const paramsObj: Record<string, string> = {};
        runtimeParams.forEach(p => {
            if (p.key.trim()) paramsObj[p.key.trim()] = p.value;
        });

        try {
            const res = await createRun({
                mapping_document_id: selectedMapping,
                source_connector_override: sourceOverride || undefined,
                target_connector_override: targetOverride || undefined,
                parameters: Object.keys(paramsObj).length > 0 ? paramsObj : undefined,
            });
            navigate(`/results/${res.data.run_id}`);
        } catch (err: any) {
            alert(`❌ Error: ${err.response?.data?.detail || err.message}`);
        }
        setRunning(false);
    };

    const addParam = () => setRuntimeParams([...runtimeParams, { key: '', value: '' }]);
    const removeParam = (index: number) => setRuntimeParams(runtimeParams.filter((_, i) => i !== index));
    const updateParam = (index: number, field: 'key' | 'value', val: string) => {
        const next = [...runtimeParams];
        next[index][field] = val;
        setRuntimeParams(next);
    };

    const openEdit = (tc: any) => {
        setEditingTC(tc);
        setEditName(tc.name || '');
        setEditType(tc.type || 'custom');
        setEditSourceSQL(tc.source_sql || '');
        setEditTargetSQL(tc.target_sql || '');
        setEditStatus(tc.validation_status || 'manual');
    };

    const openCreate = () => {
        setEditingTC({ id: 'new' });
        setEditName('');
        setEditType('custom');
        setEditSourceSQL('');
        setEditTargetSQL('');
        setEditStatus('manual');
    };

    const handleDelete = async (e: React.MouseEvent, id: number, name: string) => {
        e.stopPropagation();
        if (window.confirm(`Are you sure you want to delete test case "${name}"?`)) {
            try {
                // @ts-ignore - Need to import deleteTestCase above
                import('../api/client').then(m => m.deleteTestCase(id).then(() => {
                    setTestCases(prev => prev.filter(tc => tc.id !== id));
                }));
            } catch (err: any) {
                alert(`❌ Error deleting test case: ${err.message}`);
            }
        }
    };

    const saveEdit = async () => {
        if (!editingTC || !selectedMapping) return;
        try {
            if (editingTC.id === 'new') {
                // Create
                // @ts-ignore
                const res = await import('../api/client').then(m => m.createTestCase({
                    mapping_document_id: selectedMapping,
                    name: editName || 'Manual Test Case',
                    type: editType,
                    source_sql: editSourceSQL || null,
                    target_sql: editTargetSQL,
                }));
                // Refresh list
                import('../api/client').then(m => m.getTestCases(selectedMapping).then(r => setTestCases(r.data)));
            } else {
                // Update
                await import('../api/client').then(m => m.updateTestCase(editingTC.id, {
                    source_sql: editSourceSQL || null,
                    target_sql: editTargetSQL,
                    validation_status: editStatus,
                }));
                setTestCases(prev => prev.map(tc =>
                    tc.id === editingTC.id
                        ? { ...tc, source_sql: editSourceSQL || null, target_sql: editTargetSQL, validation_status: editStatus }
                        : tc
                ));
            }
            setEditingTC(null);
        } catch (err: any) {
            alert(`❌ Error: ${err.response?.data?.detail || err.message}`);
        }
    };

    return (
        <div className="test-cases-page">
            <div className="page-header mb-6">
                <div className="flex justify-between items-start">
                    <div>
                        <h1>Test Cases</h1>
                        <p className="text-muted">Auto-generated tests from your mappings — click a row to expand, ✏️ to edit</p>
                    </div>
                </div>
            </div>

            <div className="selection-bar mb-6" style={{
                background: 'var(--bg-card)',
                padding: '12px 20px',
                borderRadius: 'var(--radius-md)',
                border: '1px solid var(--border)',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: '20px',
                flexWrap: 'wrap'
            }}>
                <div className="flex items-center gap-3" style={{ flex: '1', minWidth: '300px' }}>
                    <label style={{ fontSize: '0.8rem', color: 'var(--text-muted)', fontWeight: 600, whiteSpace: 'nowrap' }}>Active Mapping:</label>
                    {mappings.length > 0 && (
                        <select
                            value={selectedMapping || ''}
                            onChange={e => setSelectedMapping(Number(e.target.value))}
                            style={{
                                padding: '8px 12px',
                                background: 'var(--bg-input)',
                                border: '1px solid var(--border)',
                                borderRadius: 'var(--radius-sm)',
                                color: 'var(--text-primary)',
                                maxWidth: '400px',
                                textOverflow: 'ellipsis',
                                flex: 1
                            }}
                        >
                            {mappings.map(m => <option key={m.id} value={m.id}>{m.name} - {new Date(m.created_at).toLocaleString()}</option>)}
                        </select>
                    )}
                </div>

                <div className="flex gap-2 items-center">
                    <button className="btn btn-secondary btn-sm" onClick={() => setShowSettings(!showSettings)}>
                        {showSettings ? '⚙️ Hide Settings' : '⚙️ Run Settings'}
                    </button>
                    <div style={{ width: '1px', height: '20px', background: 'var(--border)', margin: '0 8px' }}></div>
                    <button className="btn btn-secondary btn-sm" onClick={openCreate} disabled={!selectedMapping}>
                        ➕ Add Test Case
                    </button>
                    <button className="btn btn-primary btn-sm" onClick={handleRun} disabled={running || testCases.length === 0}>
                        {running ? '⏳ Executing...' : '▶️ Execute All'}
                    </button>
                </div>
            </div>

            {/* Missing connector warning banner */}
            {missingConnectors.length > 0 && (
                <div style={{
                    background: 'rgba(245, 158, 11, 0.12)',
                    border: '1px solid rgba(245, 158, 11, 0.5)',
                    borderRadius: 'var(--radius-md)',
                    padding: '12px 16px',
                    marginBottom: '16px',
                    display: 'flex',
                    alignItems: 'flex-start',
                    gap: '12px',
                }}>
                    <span style={{ fontSize: '1.2rem', lineHeight: 1 }}>⚠️</span>
                    <div>
                        <p style={{ margin: 0, fontWeight: 600, color: '#f59e0b', fontSize: '0.9rem' }}>
                            Missing {missingConnectors.join(' & ')} connector{missingConnectors.length > 1 ? 's' : ''}
                        </p>
                        <p style={{ margin: '4px 0 0', fontSize: '0.825rem', color: 'var(--text-muted)' }}>
                            The <strong>{missingConnectors.join(' and ')}</strong> connector{missingConnectors.length > 1 ? 's were' : ' was'} removed from this mapping (likely deleted).
                            Open <strong>⚙️ Run Settings</strong> and select {missingConnectors.length > 1 ? 'replacement connectors' : 'a replacement connector'} before executing — otherwise test cases will fail.
                        </p>
                    </div>
                </div>
            )}

            {showSettings && (
                <div style={{ background: 'var(--bg-card)', padding: '20px', borderRadius: 'var(--radius-md)', border: '1px solid var(--border)', marginBottom: '24px', animation: 'fadeIn 0.2s ease-out' }}>
                    <h3 style={{ marginTop: 0, fontSize: '1rem', marginBottom: '16px' }}>Execution Environment Overrides</h3>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>
                        <div className="form-group">
                            <label style={{ fontSize: '0.8rem', color: missingConnectors.includes('source') ? '#f59e0b' : 'var(--text-muted)' }}>
                                Source Connector Override {missingConnectors.includes('source') && <span style={{ fontWeight: 700 }}>— Required (default was removed)</span>}
                            </label>
                            <select
                                value={sourceOverride || ''}
                                onChange={e => setSourceOverride(e.target.value ? Number(e.target.value) : null)}
                                style={{ width: '100%', padding: '10px', background: 'var(--bg-input)', border: `1px solid ${missingConnectors.includes('source') ? '#f59e0b' : 'var(--border)'}`, borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)' }}
                            >
                                <option value="">(Use default from mapping)</option>
                                {connectors.map(c => <option key={c.id} value={c.id}>{c.name} ({c.type})</option>)}
                            </select>
                        </div>
                        <div className="form-group">
                            <label style={{ fontSize: '0.8rem', color: missingConnectors.includes('target') ? '#f59e0b' : 'var(--text-muted)' }}>
                                Target Connector Override {missingConnectors.includes('target') && <span style={{ fontWeight: 700 }}>— Required (default was removed)</span>}
                            </label>
                            <select
                                value={targetOverride || ''}
                                onChange={e => setTargetOverride(e.target.value ? Number(e.target.value) : null)}
                                style={{ width: '100%', padding: '10px', background: 'var(--bg-input)', border: `1px solid ${missingConnectors.includes('target') ? '#f59e0b' : 'var(--border)'}`, borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)' }}
                            >
                                <option value="">(Use default from mapping)</option>
                                {connectors.map(c => <option key={c.id} value={c.id}>{c.name} ({c.type})</option>)}
                            </select>
                        </div>
                    </div>

                    <div style={{ marginTop: '20px' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                            <label style={{ fontSize: '0.8rem', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>Runtime SQL Parameters</label>
                            <button className="btn btn-secondary btn-sm" onClick={addParam}>+ Add Parameter</button>
                        </div>

                        {runtimeParams.length === 0 ? (
                            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', textAlign: 'center', padding: '10px', border: '1px dashed var(--border)', borderRadius: 'var(--radius-sm)' }}>
                                No parameters defined. Use placeholders like <code>:batch_id</code> in your SQL.
                            </p>
                        ) : (
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                                {runtimeParams.map((p, i) => (
                                    <div key={i} style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
                                        <input
                                            placeholder="Key (e.g. batch_id)"
                                            value={p.key}
                                            onChange={e => updateParam(i, 'key', e.target.value)}
                                            style={{ flex: 1, padding: '8px', background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)', fontSize: '0.85rem' }}
                                        />
                                        <input
                                            placeholder="Value"
                                            value={p.value}
                                            onChange={e => updateParam(i, 'value', e.target.value)}
                                            style={{ flex: 1, padding: '8px', background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)', fontSize: '0.85rem' }}
                                        />
                                        <button className="btn btn-danger btn-sm" onClick={() => removeParam(i)}>🗑️</button>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '16px', fontStyle: 'italic' }}>
                        💡 Tip: Placeholders in your SQL (e.g. <code>WHERE id = :batch_id</code>) will be replaced by these values at runtime.
                    </p>
                </div>
            )}

            {testCases.length === 0 ? (
                <div className="empty-state">
                    <div className="icon">🧪</div>
                    <p>No test cases yet. Generate them from a parsed mapping document.</p>
                </div>
            ) : (
                <div className="table-wrapper">
                    <table>
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Name</th>
                                <th>Type</th>
                                <th>Source SQL</th>
                                <th>Target SQL</th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody>
                            {testCases.map((tc: any) => (
                                <>
                                    <tr key={tc.id} onClick={() => setExpandedId(expandedId === tc.id ? null : tc.id)}
                                        style={{ cursor: 'pointer' }}>
                                        <td>#{tc.id}</td>
                                        <td>
                                            <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                                                <span>{tc.name}</span>
                                                <span className={`badge ${tc.validation_status === 'valid' ? 'badge-success' : tc.validation_status === 'invalid' ? 'badge-danger' : 'badge-warning'}`} style={{ alignSelf: 'flex-start', fontSize: '0.65rem', padding: '2px 6px' }}>
                                                    {tc.validation_status === 'valid' ? '✓ SQL Valid' : tc.validation_status === 'invalid' ? '⚠ SQL Invalid' : 'Manual / Pending'}
                                                </span>
                                            </div>
                                        </td>
                                        <td><span className="badge badge-info">{tc.type}</span></td>
                                        <td>
                                            {tc.source_sql ? (
                                                <code className="font-mono text-sm">
                                                    {tc.source_sql.length > 50 ? tc.source_sql.slice(0, 50) + '…' : tc.source_sql}
                                                </code>
                                            ) : (
                                                <span className="badge badge-warning" style={{ fontSize: '0.7rem' }}>Target-only test</span>
                                            )}
                                        </td>
                                        <td>
                                            <code className="font-mono text-sm">
                                                {tc.target_sql?.length > 50 ? tc.target_sql.slice(0, 50) + '…' : tc.target_sql}
                                            </code>
                                        </td>
                                        <td>
                                            <div className="flex gap-2">
                                                <button className="btn btn-secondary btn-sm" onClick={(e) => { e.stopPropagation(); openEdit(tc); }}>
                                                    ✏️
                                                </button>
                                                <button className="btn btn-danger btn-sm" onClick={(e) => handleDelete(e, tc.id, tc.name)} title="Delete Test Case">
                                                    🗑️
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                    {expandedId === tc.id && (
                                        <tr key={`${tc.id}-expanded`}>
                                            <td colSpan={6} style={{ background: 'var(--bg-secondary)', padding: '16px 24px' }}>
                                                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
                                                    <div>
                                                        <p style={{ fontWeight: 600, marginBottom: '8px', color: 'var(--text-muted)', fontSize: '0.8rem', textTransform: 'uppercase' }}>
                                                            Source SQL {!tc.source_sql && '(not applicable)'}
                                                        </p>
                                                        <pre style={{
                                                            background: 'var(--bg-input)', padding: '12px', borderRadius: 'var(--radius-sm)',
                                                            border: '1px solid var(--border)', fontSize: '0.85rem', whiteSpace: 'pre-wrap',
                                                            wordBreak: 'break-all', color: 'var(--text-primary)', fontFamily: "'JetBrains Mono', monospace"
                                                        }}>
                                                            {tc.source_sql || 'N/A — this test only queries the target database'}
                                                        </pre>
                                                    </div>
                                                    <div>
                                                        <p style={{ fontWeight: 600, marginBottom: '8px', color: 'var(--text-muted)', fontSize: '0.8rem', textTransform: 'uppercase' }}>
                                                            Target SQL
                                                        </p>
                                                        <pre style={{
                                                            background: 'var(--bg-input)', padding: '12px', borderRadius: 'var(--radius-sm)',
                                                            border: '1px solid var(--border)', fontSize: '0.85rem', whiteSpace: 'pre-wrap',
                                                            wordBreak: 'break-all', color: 'var(--text-primary)', fontFamily: "'JetBrains Mono', monospace"
                                                        }}>
                                                            {tc.target_sql}
                                                        </pre>
                                                    </div>
                                                </div>
                                            </td>
                                        </tr>
                                    )}
                                </>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}

            {/* Edit/Create Modal */}
            {editingTC && (
                <div className="modal-overlay" onClick={() => setEditingTC(null)}>
                    <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '700px' }}>
                        <h2>{editingTC.id === 'new' ? 'Add Test Case' : `Edit Test Case: ${editingTC.name}`}</h2>
                        {editingTC.id === 'new' && (
                            <>
                                <div className="form-group">
                                    <label>Test Case Name</label>
                                    <input
                                        type="text"
                                        value={editName}
                                        onChange={e => setEditName(e.target.value)}
                                        className="form-control"
                                        placeholder="e.g. Manual test for region logic"
                                        style={{ width: '100%', padding: '10px 14px', background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)' }}
                                    />
                                </div>
                                <div className="form-group">
                                    <label>Test Type</label>
                                    <select
                                        value={editType}
                                        onChange={e => setEditType(e.target.value)}
                                        style={{ width: '100%', padding: '10px 14px', background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)' }}
                                    >
                                        <option value="row_count">row_count</option>
                                        <option value="value_match">value_match</option>
                                        <option value="null_check">null_check</option>
                                        <option value="duplicate_check">duplicate_check</option>
                                        <option value="dq_check">dq_check</option>
                                        <option value="custom">custom</option>
                                    </select>
                                </div>
                            </>
                        )}
                        {editingTC.id !== 'new' && (
                            <div className="form-group">
                                <label>Validation Status</label>
                                <select
                                    value={editStatus}
                                    onChange={e => setEditStatus(e.target.value)}
                                    style={{ width: '100%', padding: '10px 14px', background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)' }}
                                >
                                    <option value="valid">✓ SQL Valid</option>
                                    <option value="invalid">⚠ SQL Invalid</option>
                                    <option value="manual">Manual / Pending</option>
                                </select>
                            </div>
                        )}
                        <div className="form-group">
                            <label>Source SQL {editingTC.type === 'null_check' || editingTC.type === 'duplicate' ? '(optional for target-only tests)' : ''}</label>
                            <textarea
                                value={editSourceSQL}
                                onChange={e => setEditSourceSQL(e.target.value)}
                                rows={5}
                                style={{
                                    width: '100%', padding: '10px 14px', background: 'var(--bg-input)',
                                    border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)',
                                    color: 'var(--text-primary)', fontFamily: "'JetBrains Mono', monospace",
                                    fontSize: '0.85rem', resize: 'vertical',
                                }}
                                placeholder="Leave empty for target-only tests"
                            />
                        </div>
                        <div className="form-group">
                            <label>Target SQL</label>
                            <textarea
                                value={editTargetSQL}
                                onChange={e => setEditTargetSQL(e.target.value)}
                                rows={5}
                                style={{
                                    width: '100%', padding: '10px 14px', background: 'var(--bg-input)',
                                    border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)',
                                    color: 'var(--text-primary)', fontFamily: "'JetBrains Mono', monospace",
                                    fontSize: '0.85rem', resize: 'vertical',
                                }}
                            />
                        </div>
                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={() => setEditingTC(null)}>Cancel</button>
                            <button className="btn btn-primary" onClick={saveEdit}>💾 Save</button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
