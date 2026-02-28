import { useEffect, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { getConnectors, uploadMapping, parseMapping } from '../api/client';

export default function Upload() {
    const navigate = useNavigate();
    const fileInputRef = useRef<HTMLInputElement>(null);
    const [connectors, setConnectors] = useState<any[]>([]);
    const [file, setFile] = useState<File | null>(null);
    const [sourceId, setSourceId] = useState('');
    const [targetId, setTargetId] = useState('');
    const [status, setStatus] = useState<'idle' | 'uploading' | 'parsing' | 'done' | 'error'>('idle');
    const [progress, setProgress] = useState('');
    const [mappingId, setMappingId] = useState<number | null>(null);

    useEffect(() => {
        getConnectors().then(r => setConnectors(r.data)).catch(() => { });
    }, []);

    const handleUpload = async () => {
        if (!file || !sourceId || !targetId) return;
        setStatus('uploading');
        setProgress('Uploading document...');

        try {
            const formData = new FormData();
            formData.append('file', file);
            formData.append('source_connector_id', sourceId);
            formData.append('target_connector_id', targetId);

            const uploadRes = await uploadMapping(formData);
            const docId = uploadRes.data.id;
            setMappingId(docId);

            setStatus('parsing');
            setProgress('AI is parsing your mapping document...');

            const parseRes = await parseMapping(docId);
            setProgress(`✅ Parsed ${parseRes.data.field_mappings_count} field mappings`);
            setStatus('done');
        } catch (err: any) {
            setStatus('error');
            setProgress(`❌ Error: ${err.response?.data?.detail || err.message}`);
        }
    };

    return (
        <div>
            <div className="page-header">
                <h1>Upload Mapping Document</h1>
                <p>Upload your ETL mapping document and let AI parse it</p>
            </div>

            <div className="card" style={{ maxWidth: '700px' }}>
                {/* File Upload */}
                <div
                    className="upload-zone"
                    onClick={() => fileInputRef.current?.click()}
                    onDragOver={e => e.preventDefault()}
                    onDrop={e => { e.preventDefault(); setFile(e.dataTransfer.files[0]); }}
                >
                    <input ref={fileInputRef} type="file" accept=".csv,.xlsx,.xls,.txt,.tsv,.md" style={{ display: 'none' }}
                        onChange={e => setFile(e.target.files?.[0] || null)} />
                    {file ? (
                        <>
                            <div className="icon">📄</div>
                            <p style={{ color: 'var(--text-primary)', fontWeight: 600 }}>{file.name}</p>
                            <p className="text-sm text-muted mt-2">{(file.size / 1024).toFixed(1)} KB</p>
                        </>
                    ) : (
                        <>
                            <div className="icon">📁</div>
                            <p>Drop your mapping document here or click to browse</p>
                            <p className="text-sm text-muted mt-2">Supports CSV, Excel, TSV, TXT, Markdown</p>
                        </>
                    )}
                </div>

                {/* Connector Selection */}
                <div className="flex gap-4 mt-6">
                    <div className="form-group" style={{ flex: 1 }}>
                        <label>Source Connector</label>
                        <select value={sourceId} onChange={e => setSourceId(e.target.value)}>
                            <option value="">Select source...</option>
                            {connectors.map(c => <option key={c.id} value={c.id}>{c.name} ({c.type})</option>)}
                        </select>
                    </div>
                    <div className="form-group" style={{ flex: 1 }}>
                        <label>Target Connector</label>
                        <select value={targetId} onChange={e => setTargetId(e.target.value)}>
                            <option value="">Select target...</option>
                            {connectors.map(c => <option key={c.id} value={c.id}>{c.name} ({c.type})</option>)}
                        </select>
                    </div>
                </div>

                {/* Progress */}
                {progress && (
                    <div className={`card mt-4 ${status === 'error' ? '' : ''}`}
                        style={{ background: status === 'error' ? 'var(--danger-bg)' : status === 'done' ? 'var(--success-bg)' : 'var(--accent-glow)' }}>
                        <p style={{ fontWeight: 500 }}>
                            {status === 'parsing' && '⏳ '}
                            {progress}
                        </p>
                    </div>
                )}

                {/* Actions */}
                <div className="flex gap-3 mt-6">
                    {status !== 'done' ? (
                        <button className="btn btn-primary" onClick={handleUpload}
                            disabled={!file || !sourceId || !targetId || status === 'uploading' || status === 'parsing'}>
                            {status === 'uploading' || status === 'parsing' ? '⏳ Processing...' : '🚀 Upload & Parse'}
                        </button>
                    ) : (
                        <>
                            <button className="btn btn-primary" onClick={() => navigate(`/mappings/${mappingId}`)}>
                                🔗 Review Mappings
                            </button>
                            <button className="btn btn-secondary" onClick={() => { setFile(null); setStatus('idle'); setProgress(''); }}>
                                Upload Another
                            </button>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}
