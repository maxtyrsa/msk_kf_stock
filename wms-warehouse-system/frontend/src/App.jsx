import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import { LayoutDashboard, Package, ShoppingCart, Settings } from 'lucide-react';
import DashboardPage from './pages/DashboardPage';
import InventoryPage from './pages/InventoryPage';
import OrdersPage from './pages/OrdersPage';

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-50">
        {/* Sidebar Navigation */}
        <aside className="fixed inset-y-0 left-0 w-64 bg-white border-r shadow-sm">
          <div className="p-6">
            <h1 className="text-xl font-bold text-primary-600">WMS System</h1>
            <p className="text-xs text-gray-500 mt-1">Warehouse Management</p>
          </div>
          
          <nav className="mt-6 px-3 space-y-1">
            <NavLink to="/" icon={<LayoutDashboard className="h-5 w-5" />}>
              Dashboard
            </NavLink>
            <NavLink to="/inventory" icon={<Package className="h-5 w-5" />}>
              Inventory
            </NavLink>
            <NavLink to="/orders" icon={<ShoppingCart className="h-5 w-5" />}>
              Orders
            </NavLink>
            <NavLink to="/settings" icon={<Settings className="h-5 w-5" />}>
              Settings
            </NavLink>
          </nav>
        </aside>

        {/* Main Content */}
        <main className="ml-64 p-8">
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/inventory" element={<InventoryPage />} />
            <Route path="/orders" element={<OrdersPage />} />
            <Route path="/settings" element={<div className="card">Settings Page - Coming Soon</div>} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}

function NavLink({ to, icon, children }) {
  return (
    <Link
      to={to}
      className="flex items-center gap-3 px-3 py-2 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors"
    >
      {icon}
      <span className="font-medium">{children}</span>
    </Link>
  );
}

export default App;
