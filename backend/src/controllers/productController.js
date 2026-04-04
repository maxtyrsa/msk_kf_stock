// src/controllers/productController.js
const pool = require('../utils/db');

exports.getProducts = async (req, res) => {
  try {
    const { search = '', page = 1, limit = 20, sort = 'updated_at', order = 'DESC' } = req.query;
    const offset = (page - 1) * limit;
    const validSort = ['name', 'model', 'sku', 'stock_quantity', 'updated_at'].includes(sort) ? sort : 'updated_at';
    const validOrder = ['ASC', 'DESC'].includes(order.toUpperCase()) ? order.toUpperCase() : 'DESC';
    const searchTerm = `%${search}%`;

    const query = `
      SELECT id, name, model, sku, stock_quantity, updated_at 
      FROM products 
      WHERE name ILIKE $1 OR model ILIKE $1 OR sku ILIKE $1 
      ORDER BY ${validSort} ${validOrder} 
      LIMIT $2 OFFSET $3
    `;
    const countQuery = `
      SELECT COUNT(*) FROM products 
      WHERE name ILIKE $1 OR model ILIKE $1 OR sku ILIKE $1
    `;

    const { rows } = await pool.query(query, [searchTerm, parseInt(limit), offset]);
    const { rows: [{ count }] } = await pool.query(countQuery, [searchTerm]);

    res.json({ 
      data: rows, 
      meta: { 
        page: parseInt(page), 
        limit: parseInt(limit), 
        total: parseInt(count), 
        totalPages: Math.ceil(count / limit) 
      } 
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch products' });
  }
};

exports.createProduct = async (req, res) => {
  try {
    const { name, model, sku, stock_quantity = 0 } = req.body;
    if (!sku) return res.status(400).json({ error: 'SKU is required' });

    const { rows } = await pool.query(
      `INSERT INTO products (name, model, sku, stock_quantity) 
       VALUES ($1, $2, $3, $4) RETURNING id, name, model, sku, stock_quantity, created_at`,
      [name || model, model, sku, parseInt(stock_quantity)]
    );

    res.status(201).json(rows[0]);
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'SKU already exists' });
    res.status(500).json({ error: 'Failed to create product' });
  }
};

exports.updateStock = async (req, res) => {
  try {
    const { id } = req.params;
    const { action, quantity } = req.body; // action: 'IN' | 'OUT' | 'AUDIT'
    const qty = parseInt(quantity, 10);

    await pool.query('BEGIN');
    
    if (action === 'AUDIT') {
      await pool.query('UPDATE products SET stock_quantity = $1, updated_at = NOW() WHERE id = $2', [qty, id]);
    } else {
      const operator = action === 'IN' ? '+' : '-';
      await pool.query(`UPDATE products SET stock_quantity = stock_quantity ${operator} $1, updated_at = NOW() WHERE id = $2`, [qty, id]);
    }

    await pool.query(
      `INSERT INTO transactions (product_id, type, quantity) VALUES ($1, $2, $3)`,
      [id, action, qty]
    );

    await pool.query('COMMIT');
    res.json({ success: true, message: `Stock updated via ${action}` });
  } catch (err) {
    await pool.query('ROLLBACK');
    res.status(500).json({ error: 'Stock update failed' });
  }
};
