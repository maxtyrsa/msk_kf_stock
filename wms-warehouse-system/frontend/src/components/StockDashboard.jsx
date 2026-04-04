import { useState, useEffect } from 'react';
import { 
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, 
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  AreaChart, Area
} from 'recharts';
import { Package, TrendingUp, AlertTriangle, ShoppingCart } from 'lucide-react';
import { analyticsApi } from '../services/api';

const COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'];

/**
 * StockDashboard Component
 * Dashboard showing stock levels and order trends using Recharts
 */
export default function StockDashboard() {
  const [dashboardData, setDashboardData] = useState(null);
  const [stockDistribution, setStockDistribution] = useState([]);
  const [orderTrends, setOrderTrends] = useState([]);
  const [stockTrends, setStockTrends] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadDashboardData();
  }, []);

  const loadDashboardData = async () => {
    try {
      const [dashboard, distribution, orders, stocks] = await Promise.all([
        analyticsApi.getDashboard(),
        analyticsApi.getStockDistribution(),
        analyticsApi.getOrderTrends(30),
        analyticsApi.getStockTrends(30)
      ]);

      setDashboardData(dashboard.data.data);
      setStockDistribution(distribution.data.data);
      setOrderTrends(orders.data.data);
      setStockTrends(stocks.data.data);
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin h-8 w-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <SummaryCard
          title="Total Products"
          value={dashboardData?.stockSummary?.total_products || 0}
          icon={<Package className="h-6 w-6 text-primary-600" />}
          color="bg-primary-50"
        />
        <SummaryCard
          title="Total Stock"
          value={dashboardData?.stockSummary?.total_stock || 0}
          icon={<TrendingUp className="h-6 w-6 text-green-600" />}
          color="bg-green-50"
        />
        <SummaryCard
          title="Low Stock Items"
          value={dashboardData?.stockSummary?.low_stock_count || 0}
          icon={<AlertTriangle className="h-6 w-6 text-orange-600" />}
          color="bg-orange-50"
        />
        <SummaryCard
          title="Total Orders"
          value={dashboardData?.orderSummary?.total_orders || 0}
          icon={<ShoppingCart className="h-6 w-6 text-purple-600" />}
          color="bg-purple-50"
        />
      </div>

      {/* Charts Row 1 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Stock Distribution Pie Chart */}
        <div className="card">
          <h3 className="text-lg font-semibold mb-4">Stock Level Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={stockDistribution}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ stock_level, percent }) => `${stock_level}: ${(percent * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="product_count"
              >
                {stockDistribution.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Order Trends Line Chart */}
        <div className="card">
          <h3 className="text-lg font-semibold mb-4">Order Trends (Last 30 Days)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={orderTrends}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="period" tick={{ fontSize: 12 }} />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="order_count" stroke="#3b82f6" name="Orders" strokeWidth={2} />
              <Line type="monotone" dataKey="items_count" stroke="#10b981" name="Items" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Charts Row 2 */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Stock Movement Area Chart */}
        <div className="card">
          <h3 className="text-lg font-semibold mb-4">Stock Movement (Last 30 Days)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={stockTrends}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" tick={{ fontSize: 12 }} />
              <YAxis />
              <Tooltip />
              <Legend />
              <Area type="monotone" dataKey="stock_in" stackId="1" stroke="#10b981" fill="#10b981" name="Stock In" />
              <Area type="monotone" dataKey="stock_out" stackId="2" stroke="#ef4444" fill="#ef4444" name="Stock Out" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Low Stock Alert Table */}
        <div className="card">
          <h3 className="text-lg font-semibold mb-4">Stock Alerts</h3>
          <div className="space-y-3">
            {dashboardData?.stockSummary?.out_of_stock_count > 0 && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
                <span className="text-red-700 font-medium">
                  {dashboardData.stockSummary.out_of_stock_count} products out of stock
                </span>
              </div>
            )}
            {dashboardData?.stockSummary?.low_stock_count > 0 && (
              <div className="p-3 bg-orange-50 border border-orange-200 rounded-lg">
                <span className="text-orange-700 font-medium">
                  {dashboardData.stockSummary.low_stock_count} products with low stock (≤5 units)
                </span>
              </div>
            )}
            <div className="p-3 bg-gray-50 rounded-lg text-sm text-gray-600">
              <p>Average stock level: {dashboardData?.stockSummary?.avg_stock || 0} units</p>
              <p>Min stock: {dashboardData?.stockSummary?.min_stock || 0} | Max stock: {dashboardData?.stockSummary?.max_stock || 0}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function SummaryCard({ title, value, icon, color }) {
  return (
    <div className={`${color} p-4 rounded-xl`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-gray-600">{title}</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">{value}</p>
        </div>
        <div className="p-3 bg-white rounded-lg shadow-sm">
          {icon}
        </div>
      </div>
    </div>
  );
}
