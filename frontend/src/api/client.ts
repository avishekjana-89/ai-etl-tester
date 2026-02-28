import axios from 'axios';

const api = axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
    timeout: 300000, // 5 minutes
});

// Connectors
export const getConnectorTypes = () => api.get('/connectors/types');
export const getConnectors = () => api.get('/connectors/');
export const createConnector = (data: { name: string; type: string; config: Record<string, string> }) =>
    api.post('/connectors/', data);
export const uploadConnectorFile = (formData: FormData) =>
    api.post('/connectors/upload', formData, { headers: { 'Content-Type': 'multipart/form-data' } });
export const testConnector = (id: number) => api.post(`/connectors/${id}/test`);
export const getSchema = (id: number) => api.get(`/connectors/${id}/schema`);
export const getConnectorUsage = (id: number) => api.get(`/connectors/${id}/usage`);
export const updateConnector = (id: number, data: { name?: string; config?: Record<string, any> }) =>
    api.put(`/connectors/${id}`, data);
export const deleteConnector = (id: number) => api.delete(`/connectors/${id}`);

// Mappings
export const getMappings = () => api.get('/mappings/');
export const uploadMapping = (formData: FormData) =>
    api.post('/mappings/upload', formData, { headers: { 'Content-Type': 'multipart/form-data' } });
export const parseMapping = (id: number) => api.post(`/mappings/${id}/parse`);
export const getMapping = (id: number) => api.get(`/mappings/${id}`);
export const updateFieldMapping = (mappingId: number, fieldId: number, data: Record<string, unknown>) =>
    api.put(`/mappings/${mappingId}/fields/${fieldId}`, data);
export const deleteMapping = (id: number) => api.delete(`/mappings/${id}`);

// Test Cases
export const updateTestCase = (testCaseId: number, data: { source_sql?: string | null; target_sql?: string; validation_status?: string }) =>
    api.put(`/testcases/${testCaseId}`, data);
export const getTestCases = (mappingDocumentId: number) => api.get(`/testcases/${mappingDocumentId}`);
export const createTestCase = (data: { mapping_document_id: number; name: string; type?: string; source_sql?: string | null; target_sql?: string | null; description?: string }) =>
    api.post('/testcases/', data);
export const deleteTestCase = (id: number) => api.delete(`/testcases/${id}`);

// Test Runs
export const createRun = (data: {
    mapping_document_id: number;
    source_connector_override?: number;
    target_connector_override?: number;
    parameters?: Record<string, string>;
}) =>
    api.post('/runs/', data);
export const getRun = (runId: number) => api.get(`/runs/${runId}`);
export const getRuns = (skip: number = 0, limit: number = 20) => api.get(`/runs/?skip=${skip}&limit=${limit}`);
export const deleteRun = (id: number) => api.delete(`/runs/${id}`);

export default api;
