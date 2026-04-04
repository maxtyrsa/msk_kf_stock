import { useState } from 'react';
import { Upload, X, FileJson, CheckCircle, AlertCircle } from 'lucide-react';
import { importApi } from '../services/api';

/**
 * OrderImportModal Component
 * UI for triggering JSON import from Python PDF parser output
 */
export default function OrderImportModal({ isOpen, onClose, onImportSuccess }) {
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  if (!isOpen) return null;

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      handleFile(e.target.files[0]);
    }
  };

  const handleFile = (selectedFile) => {
    if (!selectedFile.name.endsWith('.json')) {
      setError('Please select a JSON file');
      return;
    }

    setFile(selectedFile);
    setError(null);
    setResult(null);

    // Read and preview file content
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const json = JSON.parse(e.target.result);
        setPreview(json);
      } catch (err) {
        setError('Invalid JSON file');
        setPreview(null);
      }
    };
    reader.readAsText(selectedFile);
  };

  const handleImport = async () => {
    if (!preview) return;

    setLoading(true);
    setError(null);

    try {
      const response = await importApi.importOrders(preview);
      setResult(response.data);
      onImportSuccess?.(response.data);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to import orders');
    } finally {
      setLoading(false);
    }
  };

  const resetForm = () => {
    setFile(null);
    setPreview(null);
    setResult(null);
    setError(null);
  };

  const handleClose = () => {
    resetForm();
    onClose();
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl shadow-2xl max-w-3xl w-full max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b">
          <h2 className="text-xl font-semibold">Import Orders from JSON</h2>
          <button onClick={handleClose} className="p-2 hover:bg-gray-100 rounded-lg">
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6 space-y-6">
          {/* File Upload Area */}
          {!result && (
            <div
              className={`border-2 border-dashed rounded-xl p-8 text-center transition-colors ${
                dragActive 
                  ? 'border-primary-500 bg-primary-50' 
                  : 'border-gray-300 hover:border-primary-400'
              }`}
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
            >
              <FileJson className="h-12 w-12 mx-auto text-gray-400 mb-4" />
              <p className="text-lg font-medium text-gray-700 mb-2">
                Drag & drop your JSON file here
              </p>
              <p className="text-sm text-gray-500 mb-4">
                or click to browse (orders.json from Python parser)
              </p>
              <label className="btn-primary cursor-pointer inline-block">
                Select File
                <input
                  type="file"
                  accept=".json"
                  onChange={handleChange}
                  className="hidden"
                />
              </label>
            </div>
          )}

          {/* Selected File Info */}
          {file && !result && (
            <div className="card bg-gray-50">
              <div className="flex items-center gap-3">
                <FileJson className="h-8 w-8 text-primary-600" />
                <div className="flex-1">
                  <p className="font-medium">{file.name}</p>
                  <p className="text-sm text-gray-500">
                    {(file.size / 1024).toFixed(2)} KB
                  </p>
                </div>
                <button onClick={resetForm} className="p-2 hover:bg-gray-200 rounded-lg">
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>
          )}

          {/* Preview Summary */}
          {preview && !result && (
            <div className="card">
              <h3 className="font-semibold mb-3">Preview ({preview.length} orders)</h3>
              <div className="max-h-48 overflow-y-auto space-y-2">
                {preview.slice(0, 5).map((order, idx) => (
                  <div key={idx} className="text-sm p-2 bg-gray-50 rounded">
                    <span className="font-mono text-primary-600">{order.order_number}</span>
                    <span className="text-gray-500 ml-2">- {order.items.length} items</span>
                  </div>
                ))}
                {preview.length > 5 && (
                  <p className="text-sm text-gray-500 text-center">
                    ... and {preview.length - 5} more orders
                  </p>
                )}
              </div>
            </div>
          )}

          {/* Error Message */}
          {error && (
            <div className="p-4 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2 text-red-700">
              <AlertCircle className="h-5 w-5" />
              <span>{error}</span>
            </div>
          )}

          {/* Import Result */}
          {result && (
            <div className="space-y-4">
              <div className="p-4 bg-green-50 border border-green-200 rounded-lg flex items-center gap-2 text-green-700">
                <CheckCircle className="h-5 w-5" />
                <span className="font-medium">{result.message}</span>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <StatBox label="Total Orders" value={result.data.totalOrders} />
                <StatBox label="Processed" value={result.data.processedOrders} color="text-green-600" />
                <StatBox label="Failed" value={result.data.failedOrders} color="text-red-600" />
                <StatBox label="Total Items" value={result.data.totalItems} />
              </div>

              {result.data.errors?.length > 0 && (
                <div className="card bg-orange-50">
                  <h4 className="font-medium text-orange-800 mb-2">Failed Orders:</h4>
                  <ul className="text-sm text-orange-700 space-y-1">
                    {result.data.errors.map((err, idx) => (
                      <li key={idx}>
                        {err.orderNumber}: {err.error}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer Actions */}
        <div className="flex justify-end gap-3 p-6 border-t">
          {result ? (
            <>
              <button onClick={handleClose} className="btn-secondary">
                Close
              </button>
              <button onClick={resetForm} className="btn-primary">
                Import Another File
              </button>
            </>
          ) : (
            <>
              <button onClick={handleClose} className="btn-secondary">
                Cancel
              </button>
              <button
                onClick={handleImport}
                disabled={!preview || loading}
                className="btn-primary disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? 'Importing...' : 'Import Orders'}
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function StatBox({ label, value, color = 'text-gray-900' }) {
  return (
    <div className="bg-white p-4 rounded-lg border text-center">
      <p className="text-sm text-gray-500">{label}</p>
      <p className={`text-2xl font-bold ${color}`}>{value}</p>
    </div>
  );
}
