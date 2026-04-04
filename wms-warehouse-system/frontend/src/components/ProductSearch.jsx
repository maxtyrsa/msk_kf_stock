import { useState, useEffect, useCallback } from 'react';
import { Search, Package, AlertCircle, CheckCircle } from 'lucide-react';
import { useDebounce } from '../hooks/useDebounce';
import { productsApi } from '../services/api';

/**
 * ProductSearch Component
 * Live search bar that filters products in real-time as user types
 * Uses pg_trgm-powered ILIKE search on the backend
 */
export default function ProductSearch({ onSelectProduct, showActions = true }) {
  const [searchQuery, setSearchQuery] = useState('');
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [pagination, setPagination] = useState({ page: 1, total: 0, totalPages: 0 });
  
  // Debounce search query to avoid excessive API calls
  const debouncedQuery = useDebounce(searchQuery, 300);

  // Search products when debounced query changes
  const searchProducts = useCallback(async (query, page = 1) => {
    if (!query || query.trim().length === 0) {
      setProducts([]);
      setPagination({ page: 1, total: 0, totalPages: 0 });
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const response = await productsApi.search(query, page, 20);
      const { products: results, pagination: pag } = response.data.data;
      
      setProducts(results);
      setPagination(pag);
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to search products');
      console.error('Search error:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  // Effect to trigger search when debounced query changes
  useEffect(() => {
    searchProducts(debouncedQuery, 1);
  }, [debouncedQuery, searchProducts]);

  const handlePageChange = (newPage) => {
    searchProducts(debouncedQuery, newPage);
  };

  const getStockStatus = (quantity) => {
    if (quantity === 0) return { label: 'Out of Stock', color: 'text-red-600', bg: 'bg-red-100' };
    if (quantity <= 5) return { label: 'Low Stock', color: 'text-orange-600', bg: 'bg-orange-100' };
    return { label: 'In Stock', color: 'text-green-600', bg: 'bg-green-100' };
  };

  return (
    <div className="w-full">
      {/* Search Input */}
      <div className="relative">
        <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
          <Search className="h-5 w-5 text-gray-400" />
        </div>
        <input
          type="text"
          className="input-field pl-10"
          placeholder="Search by name, model, or article..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          aria-label="Search products"
        />
        {loading && (
          <div className="absolute inset-y-0 right-0 pr-3 flex items-center">
            <div className="animate-spin h-5 w-5 border-2 border-primary-600 border-t-transparent rounded-full"></div>
          </div>
        )}
      </div>

      {/* Search Results */}
      {error && (
        <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-lg flex items-center gap-2 text-red-700">
          <AlertCircle className="h-5 w-5" />
          <span>{error}</span>
        </div>
      )}

      {!error && !loading && products.length === 0 && debouncedQuery && (
        <div className="mt-4 p-8 text-center text-gray-500">
          <Package className="h-12 w-12 mx-auto mb-2 opacity-50" />
          <p>No products found for "{debouncedQuery}"</p>
        </div>
      )}

      {!error && products.length > 0 && (
        <div className="mt-4 space-y-3">
          <div className="flex justify-between items-center text-sm text-gray-500">
            <span>Found {pagination.total} products</span>
          </div>

          <div className="space-y-2">
            {products.map((product) => {
              const stockStatus = getStockStatus(product.quantity);
              
              return (
                <div
                  key={product.id}
                  className="card hover:shadow-lg transition-shadow duration-200 cursor-pointer"
                  onClick={() => onSelectProduct?.(product)}
                >
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-900">{product.name}</h3>
                      <div className="mt-1 flex flex-wrap gap-2 text-sm">
                        {product.model && (
                          <span className="text-gray-500">Model: {product.model}</span>
                        )}
                        <span className="font-mono text-primary-600 bg-primary-50 px-2 py-0.5 rounded">
                          {product.article}
                        </span>
                      </div>
                    </div>
                    
                    <div className="flex flex-col items-end gap-2">
                      <span className={`px-3 py-1 rounded-full text-xs font-medium ${stockStatus.bg} ${stockStatus.color}`}>
                        {stockStatus.label}
                      </span>
                      <span className="text-2xl font-bold text-gray-900">{product.quantity}</span>
                      <span className="text-xs text-gray-500">units</span>
                    </div>
                  </div>
                  
                  {showActions && (
                    <div className="mt-3 pt-3 border-t border-gray-100 flex gap-2">
                      <button
                        className="btn-primary text-sm py-1 px-3"
                        onClick={(e) => {
                          e.stopPropagation();
                          onSelectProduct?.(product);
                        }}
                      >
                        View Details
                      </button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          {/* Pagination */}
          {pagination.totalPages > 1 && (
            <div className="flex justify-center gap-2 mt-4">
              <button
                className="btn-secondary disabled:opacity-50"
                onClick={() => handlePageChange(pagination.page - 1)}
                disabled={pagination.page === 1}
              >
                Previous
              </button>
              <span className="flex items-center px-4 text-gray-600">
                Page {pagination.page} of {pagination.totalPages}
              </span>
              <button
                className="btn-secondary disabled:opacity-50"
                onClick={() => handlePageChange(pagination.page + 1)}
                disabled={pagination.page === pagination.totalPages}
              >
                Next
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
