import { Package, AlertTriangle } from 'lucide-react';

const inventoryData = [
  { id: 1, name: 'Ноутбук Dell XPS 15', sku: 'DELL-XPS-15', quantity: 45, location: 'A-01-01', status: 'in_stock' },
  { id: 2, name: 'Мышь Logitech MX Master', sku: 'LOG-MX-MASTER', quantity: 120, location: 'B-02-03', status: 'in_stock' },
  { id: 3, name: 'Клавиатура Keychron K2', sku: 'KEY-K2-RGB', quantity: 8, location: 'A-03-02', status: 'low_stock' },
  { id: 4, name: 'Монитор LG UltraFine', sku: 'LG-UF-27', quantity: 23, location: 'C-01-01', status: 'in_stock' },
  { id: 5, name: 'USB-C Хаб Anker', sku: 'ANK-USBC-7', quantity: 5, location: 'B-01-02', status: 'low_stock' },
  { id: 6, name: 'Webcam Logitech C920', sku: 'LOG-C920-HD', quantity: 0, location: 'A-02-01', status: 'out_of_stock' },
];

function InventoryPage() {
  const getStatusBadge = (status) => {
    switch (status) {
      case 'in_stock':
        return <span className="px-2 py-1 text-xs font-medium bg-green-100 text-green-800 rounded-full">В наличии</span>;
      case 'low_stock':
        return <span className="px-2 py-1 text-xs font-medium bg-yellow-100 text-yellow-800 rounded-full">Мало</span>;
      case 'out_of_stock':
        return <span className="px-2 py-1 text-xs font-medium bg-red-100 text-red-800 rounded-full">Нет в наличии</span>;
      default:
        return null;
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Inventory</h1>
          <p className="text-gray-500 mt-1">Управление запасами склада</p>
        </div>
        <button className="btn-primary">
          <Package className="h-4 w-4 mr-2" />
          Добавить товар
        </button>
      </div>

      {/* Alerts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="card border-yellow-200 bg-yellow-50">
          <div className="flex items-center gap-3">
            <AlertTriangle className="h-5 w-5 text-yellow-600" />
            <div>
              <div className="font-semibold text-yellow-800">Товары с низким запасом</div>
              <div className="text-sm text-yellow-700">2 товара требуют пополнения</div>
            </div>
          </div>
        </div>
        <div className="card border-red-200 bg-red-50">
          <div className="flex items-center gap-3">
            <AlertTriangle className="h-5 w-5 text-red-600" />
            <div>
              <div className="font-semibold text-red-800">Отсутствующие товары</div>
              <div className="text-sm text-red-700">1 товар отсутствует на складе</div>
            </div>
          </div>
        </div>
      </div>

      {/* Inventory Table */}
      <div className="card">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b">
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Название</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">SKU</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Количество</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Локация</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Статус</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-gray-500">Действия</th>
              </tr>
            </thead>
            <tbody>
              {inventoryData.map((item) => (
                <tr key={item.id} className="border-b hover:bg-gray-50">
                  <td className="py-3 px-4 text-sm text-gray-900">{item.name}</td>
                  <td className="py-3 px-4 text-sm text-gray-500 font-mono">{item.sku}</td>
                  <td className="py-3 px-4 text-sm text-gray-900">{item.quantity}</td>
                  <td className="py-3 px-4 text-sm text-gray-500">{item.location}</td>
                  <td className="py-3 px-4 text-sm">{getStatusBadge(item.status)}</td>
                  <td className="py-3 px-4 text-sm">
                    <button className="text-blue-600 hover:text-blue-800 mr-3">Редактировать</button>
                    <button className="text-red-600 hover:text-red-800">Удалить</button>
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

export default InventoryPage;
