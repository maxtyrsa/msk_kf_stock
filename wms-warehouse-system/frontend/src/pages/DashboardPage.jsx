import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const data = [
  { name: 'Понедельник', received: 45, shipped: 32 },
  { name: 'Вторник', received: 52, shipped: 41 },
  { name: 'Среда', received: 38, shipped: 45 },
  { name: 'Четверг', received: 65, shipped: 52 },
  { name: 'Пятница', received: 48, shipped: 39 },
  { name: 'Суббота', received: 32, shipped: 28 },
  { name: 'Воскресенье', received: 25, shipped: 18 },
];

function DashboardPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-gray-500 mt-1">Обзор склада и статистика</p>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Всего товаров</div>
          <div className="mt-2 text-3xl font-bold text-gray-900">1,234</div>
          <div className="mt-1 text-sm text-green-600">+12% за неделю</div>
        </div>
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Заказов сегодня</div>
          <div className="mt-2 text-3xl font-bold text-gray-900">47</div>
          <div className="mt-1 text-sm text-green-600">+8% вчера</div>
        </div>
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Отгружено</div>
          <div className="mt-2 text-3xl font-bold text-gray-900">32</div>
          <div className="mt-1 text-sm text-gray-500">Ожидается: 15</div>
        </div>
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Низкий запас</div>
          <div className="mt-2 text-3xl font-bold text-gray-900">8</div>
          <div className="mt-1 text-sm text-red-600">Требует внимания</div>
        </div>
      </div>

      {/* Chart */}
      <div className="card">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Активность склада</h2>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="received" fill="#3b82f6" name="Получено" />
            <Bar dataKey="shipped" fill="#22c55e" name="Отгружено" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default DashboardPage;
