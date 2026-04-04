// src/controllers/importController.js
const pool = require('../utils/db');

exports.importOrders = async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const payload = req.body;

    if (!Array.isArray(payload)) {
      throw new Error('Expected JSON array of orders');
    }

    const processedOrders = [];

    for (const order of payload) {
      if (!order.order_number || !Array.isArray(order.items)) continue;

      // 1. Создаём запись заказа
      const { rows: orderRows } = await client.query(
        `INSERT INTO orders (order_number, status, total_items) 
         VALUES ($1, 'completed', $2) RETURNING id`,
        [order.order_number, order.items.length]
      );
      const orderId = orderRows[0].id;

      for (const item of order.items) {
        // Пропускаем доставку и некорректные записи
        if (item.article === 'delivery' || !item.article || !item.quantity) continue;

        const sku = String(item.article).trim();
        const model = String(item.model || item.article).trim();
        const name = String(item.product || item.model || model).trim();
        const qty = parseInt(item.quantity, 10);

        // 2. Upsert продукта (авто-создание при первом импорте)
        await client.query(
          `INSERT INTO products (sku, model, name, stock_quantity) 
           VALUES ($1, $2, $3, 0)
           ON CONFLICT (sku) DO UPDATE SET model = EXCLUDED.model, name = EXCLUDED.name`,
          [sku, model, name]
        );

        // 3. Блокировка строки для предотвращения race condition
        const { rows: [product] } = await client.query(
          `SELECT id, stock_quantity FROM products WHERE sku = $1 FOR UPDATE`,
          [sku]
        );

        if (!product || product.stock_quantity < qty) {
          throw new Error(`Insufficient stock for SKU: ${sku}. Available: ${product?.stock_quantity || 0}, Requested: ${qty}`);
        }

        // 4. Атомарное списание
        await client.query(
          `UPDATE products SET stock_quantity = stock_quantity - $1, updated_at = NOW() WHERE id = $2`,
          [qty, product.id]
        );

        // 5. Лог транзакции
        await client.query(
          `INSERT INTO transactions (order_id, product_id, type, quantity) 
           VALUES ($1, $2, 'OUT', $3)`,
          [orderId, product.id, qty]
        );
      }

      processedOrders.push(order.order_number);
    }

    await client.query('COMMIT');
    res.status(201).json({ 
      success: true, 
      processed: processedOrders.length, 
      order_numbers: processedOrders 
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('🚨 Import failed:', err.message);
    res.status(400).json({ success: false, error: err.message });
  } finally {
    client.release();
  }
};
