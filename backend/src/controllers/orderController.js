// src/controllers/orderController.js
const pool = require('../utils/db');

exports.getOrders = async (req, res) => {
  try {
    const { status, page = 1, limit = 20 } = req.query;
    const offset = (page - 1) * limit;
    const conditions = status ? `WHERE status = $1` : '';
    const params = status ? [status] : [];
    const countParams = status ? [status, parseInt(limit), offset] : [parseInt(limit), offset];

    const query = `
      SELECT id, order_number, status, total_items, created_at 
      FROM orders ${conditions} 
      ORDER BY created_at DESC LIMIT $${status ? 2 : 1} OFFSET $${status ? 3 : 2}
    `;
    const countQuery = `SELECT COUNT(*) FROM orders ${conditions}`;

    const { rows } = await pool.query(query, countParams);
    const { rows: [{ count }] } = await pool.query(countQuery, params);

    res.json({ data: rows, meta: { page: parseInt(page), limit: parseInt(limit), total: parseInt(count) } });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch orders' });
  }
};

exports.getOrderDetails = async (req, res) => {
  try {
    const { id } = req.params;
    
    // Получаем заказ
    const { rows: [order] } = await pool.query(`SELECT * FROM orders WHERE id = $1`, [id]);
    if (!order) return res.status(404).json({ error: 'Order not found' });

    // Получаем товары через транзакции
    const { rows: items } = await pool.query(
      `SELECT t.quantity, p.name, p.model, p.sku 
       FROM transactions t 
       JOIN products p ON t.product_id = p.id 
       WHERE t.order_id = $1 AND t.type = 'OUT'`,
      [id]
    );

    res.json({ ...order, items });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch order details' });
  }
};

exports.updateOrderStatus = async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    if (!['pending', 'completed', 'cancelled'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    const { rows } = await pool.query(
      `UPDATE orders SET status = $1 WHERE id = $2 RETURNING id, status, updated_at`,
      [status, id]
    );
    
    if (!rows.length) return res.status(404).json({ error: 'Order not found' });
    res.json(rows[0]);
  } catch (err) {
    res.status(500).json({ error: 'Failed to update order status' });
  }
};
