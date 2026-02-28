import { useEffect, useState } from 'react';
import { getConnectors, createConnector, testConnector, deleteConnector, getSchema, uploadConnectorFile, updateConnector, getConnectorUsage } from '../api/client';

export default function Connectors() {
    const [connectors, setConnectors] = useState<any[]>([]);
    const [showModal, setShowModal] = useState(false);
    const [form, setForm] = useState({ name: '', type: 'postgresql', host: '', port: '5432', user: '', password: '', database: '', file_path: '' });
    const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
    const [testResult, setTestResult] = useState<any>(null);
    const [schemaView, setSchemaView] = useState<{ id: number; tables: any } | null>(null);
    const [editingId, setEditingId] = useState<number | null>(null);
    const [deleteWarning, setDeleteWarning] = useState<{ id: number; name: string; linked: any[] } | null>(null);

    const load = () => getConnectors().then(r => setConnectors(r.data)).catch(() => { });
    useEffect(() => { load(); }, []);

    const isFileType = ['csv', 'excel', 'parquet', 'json'].includes(form.type);

    const handleCreate = async () => {
        if (isFileType && selectedFiles.length > 0) {
            const formData = new FormData();
            formData.append('name', form.name);
            formData.append('type', form.type);
            selectedFiles.forEach(file => {
                formData.append('files', file);
            });
            await uploadConnectorFile(formData);
        } else {
            const config = isFileType
                ? { file_path: form.file_path, file_type: form.type }
                : { host: form.host, port: form.port, user: form.user, password: form.password, database: form.database };

            if (editingId) {
                await updateConnector(editingId, { name: form.name, config: config as any });
            } else {
                await createConnector({ name: form.name, type: form.type, config: config as any });
            }
        }
        setShowModal(false);
        setEditingId(null);
        setForm({ name: '', type: 'postgresql', host: '', port: '5432', user: '', password: '', database: '', file_path: '' });
        setSelectedFiles([]);
        load();
    };

    const handleEdit = (c: any) => {
        setEditingId(c.id);
        const config = c.config || {};
        setForm({
            name: c.name,
            type: c.type,
            host: config.host || '',
            port: config.port || '5432',
            user: config.user || '',
            password: config.password || '',
            database: config.database || '',
            file_path: config.file_path || ''
        });
        setShowModal(true);
    };

    const handleCloseModal = () => {
        setShowModal(false);
        setEditingId(null);
        setForm({ name: '', type: 'postgresql', host: '', port: '5432', user: '', password: '', database: '', file_path: '' });
        setSelectedFiles([]);
    };

    const handleTest = async (id: number) => {
        const res = await testConnector(id);
        setTestResult({ id, ...res.data });
        setTimeout(() => setTestResult(null), 3000);
    };

    const handleSchema = async (id: number) => {
        const res = await getSchema(id);
        setSchemaView({ id, tables: res.data.tables });
    };

    const handleDeleteClick = async (id: number, name: string) => {
        const res = await getConnectorUsage(id);
        const linked = res.data.linked_mappings || [];
        if (linked.length > 0) {
            setDeleteWarning({ id, name, linked });
        } else {
            if (window.confirm(`Delete connector "${name}"?`)) {
                await deleteConnector(id);
                load();
            }
        }
    };

    const confirmDelete = async () => {
        if (!deleteWarning) return;
        await deleteConnector(deleteWarning.id);
        setDeleteWarning(null);
        load();
    };

    return (
        <div>
            <div className="page-header flex justify-between items-center">
                <div>
                    <h1>Connectors</h1>
                    <p>Manage your database and file connections</p>
                </div>
                <button className="btn btn-primary" onClick={() => setShowModal(true)}>+ Add Connector</button>
            </div>

            {connectors.length === 0 ? (
                <div className="empty-state">
                    <div className="icon">🔌</div>
                    <p>No connectors yet. Add a database or file connection.</p>
                </div>
            ) : (
                <div className="card-grid">
                    {connectors.map((c: any) => (
                        <div className="card" key={c.id}>
                            <div className="flex justify-between items-center">
                                <h3>{c.name}</h3>
                                <span className="badge badge-info">{c.type}</span>
                            </div>
                            <p className="text-muted text-sm mt-2">Created: {new Date(c.created_at).toLocaleDateString()}</p>
                            {testResult?.id === c.id && (
                                <p className={`mt-2 text-sm ${testResult.success ? 'badge-success' : 'badge-danger'}`}
                                    style={{ color: testResult.success ? 'var(--success)' : 'var(--danger)' }}>
                                    {testResult.message}
                                </p>
                            )}
                            <div className="flex gap-2 mt-4">
                                <button className="btn btn-secondary btn-sm" onClick={() => handleTest(c.id)}>Test</button>
                                <button className="btn btn-secondary btn-sm" onClick={() => handleSchema(c.id)}>Schema</button>
                                <button className="btn btn-secondary btn-sm" onClick={() => handleEdit(c)}>Edit</button>
                                <button className="btn btn-danger btn-sm" onClick={() => handleDeleteClick(c.id, c.name)}>Delete</button>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Delete Warning Modal */}
            {deleteWarning && (
                <div className="modal-overlay" onClick={() => setDeleteWarning(null)}>
                    <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '520px' }}>
                        <h2 style={{ color: 'var(--warning, #f59e0b)' }}>⚠️ Connector In Use</h2>
                        <p style={{ marginBottom: '12px' }}>
                            <strong>"{deleteWarning.name}"</strong> is linked to the following mapping document{deleteWarning.linked.length > 1 ? 's' : ''}:
                        </p>
                        <ul style={{ listStyle: 'none', padding: 0, margin: '0 0 16px' }}>
                            {deleteWarning.linked.map((m: any) => (
                                <li key={m.id} style={{
                                    padding: '8px 12px', marginBottom: '6px',
                                    background: 'var(--bg-secondary)', borderRadius: 'var(--radius-sm)',
                                    border: '1px solid var(--border)', fontSize: '0.9rem'
                                }}>
                                    📄 <strong>{m.name}</strong>
                                    <span className="badge badge-warning" style={{ marginLeft: '8px', fontSize: '0.7rem' }}>{m.role}</span>
                                </li>
                            ))}
                        </ul>
                        <p style={{ color: 'var(--text-muted)', fontSize: '0.875rem', marginBottom: '20px' }}>
                            Deleting this connector will set it to <em>null</em> in those mappings. Any test cases that rely on it will <strong>fail</strong> unless you select a new connector override before running.
                        </p>
                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={() => setDeleteWarning(null)}>Cancel</button>
                            <button className="btn btn-danger" onClick={confirmDelete}>Delete Anyway</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Schema Viewer */}
            {schemaView && (
                <div className="modal-overlay" onClick={() => setSchemaView(null)}>
                    <div className="modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '700px', maxHeight: '80vh', overflow: 'auto' }}>
                        <h2>Schema</h2>
                        {Object.entries(schemaView.tables).map(([table, cols]: [string, any]) => (
                            <div key={table} className="mb-4">
                                <h4 style={{ color: 'var(--accent)' }}>📋 {table}</h4>
                                <div className="table-wrapper mt-2">
                                    <table>
                                        <thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead>
                                        <tbody>
                                            {cols.map((col: any) => (
                                                <tr key={col.name}><td>{col.name}</td><td className="font-mono">{col.type}</td><td>{col.nullable ? '✅' : '❌'}</td></tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        ))}
                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={() => setSchemaView(null)}>Close</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Add/Edit Connector Modal */}
            {showModal && (
                <div className="modal-overlay" onClick={handleCloseModal}>
                    <div className="modal" onClick={e => e.stopPropagation()}>
                        <h2>{editingId ? 'Edit Connector' : 'Add Connector'}</h2>
                        <div className="form-group">
                            <label>Name</label>
                            <input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} placeholder="My Database" />
                        </div>
                        <div className="form-group">
                            <label>Type</label>
                            <select
                                value={form.type}
                                onChange={e => setForm({ ...form, type: e.target.value })}
                                disabled={!!editingId} // Type cannot be changed after creation
                            >
                                <option value="postgresql">PostgreSQL</option>
                                <option value="mysql">MySQL</option>
                                <option value="csv">CSV File</option>
                                <option value="excel">Excel File</option>
                                <option value="parquet">Parquet File</option>
                                <option value="json">JSON File</option>
                            </select>
                        </div>
                        {isFileType ? (
                            <div className="form-group">
                                <label>Upload Files (Multiple Supported)</label>
                                <input
                                    type="file"
                                    multiple
                                    onChange={e => setSelectedFiles(Array.from(e.target.files || []))}
                                    style={{ width: '100%', padding: '10px', background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)', color: 'var(--text-primary)' }}
                                    disabled={!!editingId && !!form.file_path} // Only allow uploads if no manual path set
                                />
                                {selectedFiles.length > 0 && (
                                    <div className="mt-2 text-sm text-muted">
                                        Selected: {selectedFiles.map(f => f.name).join(', ')}
                                    </div>
                                )}
                                <p className="text-muted text-xs mt-1">Or provide a direct server path below:</p>
                                <input value={form.file_path} onChange={e => setForm({ ...form, file_path: e.target.value })} placeholder="/path/to/file.csv" />
                            </div>
                        ) : (
                            <>
                                <div className="form-group"><label>Host</label><input value={form.host} onChange={e => setForm({ ...form, host: e.target.value })} placeholder="localhost" /></div>
                                <div className="form-group"><label>Port</label><input value={form.port} onChange={e => setForm({ ...form, port: e.target.value })} /></div>
                                <div className="form-group"><label>User</label><input value={form.user} onChange={e => setForm({ ...form, user: e.target.value })} /></div>
                                <div className="form-group"><label>Password</label><input type="password" value={form.password} onChange={e => setForm({ ...form, password: e.target.value })} /></div>
                                <div className="form-group"><label>Database</label><input value={form.database} onChange={e => setForm({ ...form, database: e.target.value })} /></div>
                            </>
                        )}
                        <div className="modal-actions">
                            <button className="btn btn-secondary" onClick={handleCloseModal}>Cancel</button>
                            <button className="btn btn-primary" onClick={handleCreate} disabled={!form.name}>
                                {editingId ? 'Save Changes' : 'Create'}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
