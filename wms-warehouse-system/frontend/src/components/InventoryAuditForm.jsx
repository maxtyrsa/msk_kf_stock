import { useState } from 'react';
import { Package, Plus, Minus, ClipboardCheck } from 'lucide-react';
import { productsApi } from '../services/api';

/**
 * InventoryAuditForm Component
 * Form for manual stock entry, stock-ins, and physical inventory audits
 */
export default function InventoryAuditForm({ productId, onSuccess, onCancel }) {
  const [product, setProduct] = useState(null);
  const [adjustmentType, setAdjustmentType] = useState('stock_in');
  const [quantity, setQuantity] = useState('');
  const [notes, setNotes] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  // Load product details if productId is provided
  useState(() => {
    if (productId) {
      loadProduct(productId);
    }
  });

  const loadProduct = async (id) => {
    try {
      const response = await productsApi.getById(id);
      setProduct(response.data.data);
    } catch (err) {
      setError('Failed to load product details');
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!quantity || parseInt(quantity) <= 0) {
      setError('Please enter a valid quantity');
      return;
    }

    if (!product && !document.getElementById('product-select')?.value) {
      setError('Please select a product');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const selectedProductId = product?.id || parseInt(document.getElementById('product-select').value);
      const quantityChange = adjustmentType === 'stock_out' ? -parseInt(quantity) : parseInt(quantity);

      // Note: In a real implementation, you'd have an API endpoint for stock adjustments
      // For now, we'll simulate the behavior
      await productsApi.update(selectedProductId, {
        quantity: (product?.quantity || 0) + quantityChange
      });

      setSuccess(`Successfully adjusted stock by ${quantityChange} units`);
      
      setTimeout(() => {
        onSuccess?.();
      }, 1500);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to adjust stock');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="card max-w-2xl">
      <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
        <ClipboardCheck className="h-5 w-5 text-primary-600" />
        Inventory Adjustment
      </h3>

      {success && (
        <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded-lg text-green-700">
          {success}
        </div>
      )}

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700">
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        {/* Product Selection */}
        {!product && (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Select Product
            </label>
            <select
              id="product-select"
              className="input-field"
              required
            >
              <option value="">Choose a product...</option>
              {/* In real app, populate from API */}
            </select>
          </div>
        )}

        {/* Current Stock Display */}
        {product && (
          <div className="p-4 bg-gray-50 rounded-lg">
            <div className="flex justify-between items-center">
              <div>
                <p className="font-medium">{product.name}</p>
                <p className="text-sm text-gray-500">Article: {product.article}</p>
              </div>
              <div className="text-right">
                <p className="text-sm text-gray-500">Current Stock</p>
                <p className="text-2xl font-bold">{product.quantity}</p>
              </div>
            </div>
          </div>
        )}

        {/* Adjustment Type */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Adjustment Type
          </label>
          <div className="grid grid-cols-2 gap-3">
            <button
              type="button"
              onClick={() => setAdjustmentType('stock_in')}
              className={`p-3 rounded-lg border-2 flex items-center justify-center gap-2 transition-colors ${
                adjustmentType === 'stock_in'
                  ? 'border-green-500 bg-green-50 text-green-700'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
            >
              <Plus className="h-5 w-5" />
              Stock In
            </button>
            <button
              type="button"
              onClick={() => setAdjustmentType('stock_out')}
              className={`p-3 rounded-lg border-2 flex items-center justify-center gap-2 transition-colors ${
                adjustmentType === 'stock_out'
                  ? 'border-orange-500 bg-orange-50 text-orange-700'
                  : 'border-gray-200 hover:border-gray-300'
              }`}
            >
              <Minus className="h-5 w-5" />
              Stock Out
            </button>
          </div>
        </div>

        {/* Quantity Input */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Quantity
          </label>
          <input
            type="number"
            min="1"
            value={quantity}
            onChange={(e) => setQuantity(e.target.value)}
            className="input-field"
            placeholder="Enter quantity"
            required
          />
        </div>

        {/* Notes */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Notes (optional)
          </label>
          <textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            className="input-field"
            rows="3"
            placeholder="Reason for adjustment..."
          />
        </div>

        {/* Preview */}
        {quantity && product && (
          <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <p className="text-sm text-blue-800">
              <span className="font-medium">Preview:</span> Stock will change from{' '}
              <span className="font-mono">{product.quantity}</span> to{' '}
              <span className="font-mono font-bold">
                {adjustmentType === 'stock_out' 
                  ? Math.max(0, product.quantity - parseInt(quantity))
                  : product.quantity + parseInt(quantity)
                }
              </span>
            </p>
          </div>
        )}

        {/* Actions */}
        <div className="flex justify-end gap-3 pt-4 border-t">
          {onCancel && (
            <button type="button" onClick={onCancel} className="btn-secondary">
              Cancel
            </button>
          )}
          <button
            type="submit"
            disabled={loading}
            className="btn-primary disabled:opacity-50"
          >
            {loading ? 'Processing...' : 'Submit Adjustment'}
          </button>
        </div>
      </form>
    </div>
  );
}
