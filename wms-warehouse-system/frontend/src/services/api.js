import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:3001/api';

// Create axios instance with default config
const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  },
  timeout: 30000
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// Response interceptor - handle 4xx/5xx errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response) {
      // Server responded with error status
      const { status, data } = error.response;
      
      switch (status) {
        case 401:
          console.error('Unauthorized - please login');
          localStorage.removeItem('token');
          window.location.href = '/login';
          break;
        case 403:
          console.error('Forbidden - insufficient permissions');
          break;
        case 404:
          console.error('Resource not found');
          break;
        case 500:
          console.error('Server error occurred');
          break;
        default:
          console.error(`API Error: ${status}`, data);
      }
    } else if (error.request) {
      // Request was made but no response
      console.error('No response received from server');
    } else {
      // Something else happened
      console.error('Error:', error.message);
    }
    
    return Promise.reject(error);
  }
);

// API service methods
export const productsApi = {
  search: (query, page = 1, limit = 20) => 
    api.get('/products/search', { params: { q: query, page, limit } }),
  getAll: (params) => api.get('/products', { params }),
  getById: (id) => api.get(`/products/${id}`),
  getByArticle: (article) => api.get(`/products/article/${article}`),
  create: (data) => api.post('/products', data),
  update: (id, data) => api.put(`/products/${id}`, data),
  delete: (id) => api.delete(`/products/${id}`),
  getLowStock: (threshold) => api.get('/products/low-stock', { params: { threshold } })
};

export const ordersApi = {
  getAll: (params) => api.get('/orders', { params }),
  getById: (id) => api.get(`/orders/${id}`),
  updateStatus: (id, status) => api.put(`/orders/${id}/status`, { status }),
  delete: (id, restoreStock) => api.delete(`/orders/${id}`, { params: { restoreStock } }),
  getStats: () => api.get('/orders/stats/summary')
};

export const importApi = {
  importOrders: (orders) => api.post('/import/orders', orders),
  getStatus: () => api.get('/import/status'),
  getErrors: (limit) => api.get('/import/errors', { params: { limit } })
};

export const analyticsApi = {
  getDashboard: () => api.get('/analytics/dashboard'),
  getStockTrends: (days) => api.get('/analytics/stock-trends', { params: { days } }),
  getOrderTrends: (days, granularity) => api.get('/analytics/order-trends', { params: { days, granularity } }),
  getStockDistribution: () => api.get('/analytics/stock-by-category'),
  getTopProducts: (limit, metric) => api.get('/analytics/top-products', { params: { limit, metric } }),
  getTransactionTypes: (days) => api.get('/analytics/transaction-types', { params: { days } }),
  getDailySummary: (days) => api.get('/analytics/daily-summary', { params: { days } })
};

export default api;
