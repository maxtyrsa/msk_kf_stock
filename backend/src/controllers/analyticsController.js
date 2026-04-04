// src/controllers/analyticsController.js
const pool = require('../utils/db');

exports.getDashboardStats = async (req, res) => {
  try {
    const [stock, ordersTrend, topProducts, recentMovements] = await Promise.all([
      // 1. Общая статистика по складу
      pool.query(`
        SELECT 
          COUNT(*) as total_products,
          SUM(stock_quantity) as total_units,
          COUNT(CASE WHEN stock_quantity <= 10 THEN 1 END) as low_stock_alerts
        FROM products
      `),
      // 2. Тренды заказов за последние 30 дней (для LineChart)
      pool.query(`
        SELECT 
          DATE_TRUNC('day', created_at)::date as date, 
          COUNT(*) as orders, 
          SUM(total_items) as items
        FROM orders 
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 1 ASC
      `),
      // 3. Топ-10 товаров по объёму продаж (для BarChart)
      pool.query(`
        SELECT p.name, p.sku, SUM(t.quantity) as sold
        FROM transactions t
        JOIN products p ON t.product_id = p.id
        WHERE t.type = 'OUT' AND t.created_at >= NOW() - INTERVAL '7 days'
        GROUP BY p.id, p.name, p.sku
        ORDER BY sold DESC LIMIT 10
      `),
      // 4. Последние 10 операций (для таблицы логов)
      pool.query(`
        SELECT t.type, t.quantity, p.name, p.sku, t.created_at
        FROM transactions t
        JOIN products p ON t.product_id = p.id
        ORDER BY t.created_at DESC LIMIT 10
      `)
    ]);

    res.json({
      stock: stock.rows[0],
      ordersTrend: ordersTrend.rows.map(r => ({ ...r, date: r.date.toISOString().split('T')[0] })),
      topProducts: topProducts.rows,
      recentMovements: recentMovements.rows
    });
  } catch (err) {
    console.error('Analytics error:', err);
    res.status(500).json({ error: 'Failed to generate dashboard data' });
  }
};
