import { ShoppingCart, CheckCircle, Clock } from 'lucide-react';

const ordersData = [
  { id: 'ORD-001', customer: 'ООО "ТехноСервис"', items: 5, total: 45000, status: 'completed', date: '2024-01-15' },
  { id: 'ORD-002', customer: 'ИП Петров А.В.', items: 2, total: 12500, status: 'processing', date: '2024-01-15' },
  { id: 'ORD-003', customer: 'АО "МегаСтрой"', items: 8, total: 89000, status: 'pending', date: '2024-01-14' },
  { id: 'ORD-004', customer: 'ООО "ОфисЦентр"', items: 3, total: 23400, status: 'completed', date: '2024-01-14' },
  { id: 'ORD-005', customer: 'ИП Сидорова Е.К.', items: 1, total: 8900, status: 'shipped', date: '2024-01-13' },
];

function OrdersPage() {
  const getStatusBadge = (status) => {
    switch (status) {
      case 'pending':
        return <span className="px-2 py-1 text-xs font-medium bg-yellow-100 text-yellow-800 rounded-full flex items-center gap-1 w-fit"><Clock className="h-3 w-3" /> Ожидает</span>;
      case 'processing':
        return <span className="px-2 py-1 text-xs font-medium bg-blue-100 text-blue-800 rounded-full flex items-center gap-1 w-fit">В обработке</span>;
      case 'shipped':
        return <span className="px-2 py-1 text-xs font-medium bg-purple-100 text-purple-800 rounded-full flex items-center gap-1 w-fit">Отгружен</span>;
      case 'completed':
        return <span className="px-2 py-1 text-xs font-medium bg-green-100 text-green-800 rounded-full flex items-center gap-1 w-fit"><CheckCircle className="h-3 w-3" /> Завершен</span>;
      default:
        return null;
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Orders</h1>
          <p className="text-gray-500 mt-1">Управление заказами клиентов</p>
        </div>
        <button className="btn-primary">
          <ShoppingCart className="h-4 w-4 mr-2" />
          Новый заказ
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Всего заказов</div>
          <div className="mt-2 text-3xl font-bold text-gray-900">156</div>
        </div>
        <div className="card">
          <div className="text-sm font-medium text-gray-500">В обработке</div>
          <div className="mt-2 text-3xl font-bold text-blue-600">12</div>
        </div>
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Отгружено сегодня</div>
          <div className="mt-2 text-3xl font-bold text-purple-600">8</div>
        </div>
        <div className="card">
          <div className="text-sm font-medium text-gray-500">Завершено</div>
          <div className="mt-2 text-3xl font-bold text-green-600">136</div>
        </div>
      </div>

      {/* Orders Table */}
      <div className="card">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b">
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">№ Заказа</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Клиент</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Товаров</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Сумма</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Дата</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Статус</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Действия</th>
              </tr>
            </thead>
            <tbody>
              {ordersData.map((order) => (
                <tr key={order.id} className="border-b hover:bg-gray-50">
                  <td className="py-3 px-4 text-sm font-medium text-gray-900">{order.id}</td>
                  <td className="py-3 px-4 text-sm text-gray-900">{order.customer}</td>
                  <td className="py-3 px-4 text-sm text-gray-500">{order.items}</td>
                  <td className="py-3 px-4 text-sm text-gray-900 font-medium">{order.total.toLocaleString()} ₽</td>
                  <td className="py-3 px-4 text-sm text-gray-500">{order.date}</td>
                  <td className="py-3 px-4 text-sm">{getStatusBadge(order.status)}</td>
                  <td className="py-3 px-4 text-sm">
                    <button className="text-blue-600 hover:text-blue-800 mr-3">Просмотр</button>
                    <button className="text-gray-600 hover:text-gray-800">Редактировать</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

export default OrdersPage;
