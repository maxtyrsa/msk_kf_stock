import { asyncHandler } from '../middleware/errorHandler.js';
import pool from '../utils/db.js';
import inventoryService from '../services/inventoryService.js';

/**
 * Order Controller - Order management and status tracking
 */

/**
 * GET /api/orders
 * Get all orders with pagination and filtering
 */
export const getOrders = asyncHandler(async (req, res) => {
  const { 
    page = 1, 
    limit = 20, 
    status,
    source,
    dateFrom,
    dateTo,
    orderNumber
  } = req.query;

  const offset = (page - 1) * limit;
  
  let whereClause = 'WHERE 1=1';
  const params = [];
  let paramIndex = 1;

  if (status) {
    whereClause += ` AND status = $${paramIndex}`;
    params.push(status);
    paramIndex++;
  }

  if (source) {
    whereClause += ` AND source = $${paramIndex}`;
    params.push(source);
    paramIndex++;
  }

  if (dateFrom) {
    whereClause += ` AND imported_at >= $${paramIndex}`;
    params.push(dateFrom);
    paramIndex++;
  }

  if (dateTo) {
    whereClause += ` AND imported_at <= $${paramIndex}`;
    params.push(dateTo);
    paramIndex++;
  }

  if (orderNumber) {
    whereClause += ` AND order_number ILIKE $${paramIndex}`;
    params.push(`%${orderNumber}%`);
    paramIndex++;
  }

  const query = `
    SELECT id, order_number, source, status, total_items, imported_at, processed_at, metadata
    FROM orders
    ${whereClause}
    ORDER BY imported_at DESC
    LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
  `;

  params.push(parseInt(limit), parseInt(offset));

  const countQuery = `
    SELECT COUNT(*) as total
    FROM orders
    ${whereClause}
  `;

  const [results, countResult] = await Promise.all([
    pool.query(query, params),
    pool.query(countQuery, params.slice(0, -2))
  ]);

  // Fetch items for each order
  const ordersWithItems = await Promise.all(
    results.rows.map(async (order) => {
      const itemsResult = await pool.query(
        `SELECT id, product_name, product_model, product_article, quantity
         FROM order_items
         WHERE order_id = $1`,
        [order.id]
      );
      return {
        ...order,
        items: itemsResult.rows
      };
    })
  );

  res.json({
    success: true,
    data: {
      orders: ordersWithItems,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: parseInt(countResult.rows[0].total),
        totalPages: Math.ceil(parseInt(countResult.rows[0].total) / limit)
      }
    }
  });
});

/**
 * GET /api/orders/:id
 * Get single order with items
 */
export const getOrderById = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const orderResult = await pool.query(
    'SELECT * FROM orders WHERE id = $1',
    [id]
  );

  if (orderResult.rows.length === 0) {
    return res.status(404).json({
      success: false,
      message: 'Order not found'
    });
  }

  const itemsResult = await pool.query(
    `SELECT oi.*, p.name as product_full_name, p.quantity as current_stock
     FROM order_items oi
     LEFT JOIN products p ON oi.product_id = p.id
     WHERE oi.order_id = $1`,
    [id]
  );

  res.json({
    success: true,
    data: {
      ...orderResult.rows[0],
      items: itemsResult.rows
    }
  });
});

/**
 * PUT /api/orders/:id/status
 * Update order status
 */
export const updateOrderStatus = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { status } = req.body;

  const validStatuses = ['pending', 'processing', 'completed', 'cancelled'];
  if (!validStatuses.includes(status)) {
    return res.status(400).json({
      success: false,
      message: `Invalid status. Must be one of: ${validStatuses.join(', ')}`
    });
  }

  const result = await pool.query(
    `UPDATE orders 
     SET status = $1, processed_at = CASE WHEN $1 = 'completed' THEN NOW() ELSE processed_at END
     WHERE id = $2
     RETURNING *`,
    [status, id]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({
      success: false,
      message: 'Order not found'
    });
  }

  res.json({
    success: true,
    data: result.rows[0],
    message: 'Order status updated successfully'
  });
});

/**
 * DELETE /api/orders/:id
 * Cancel/delete order (with optional stock restoration)
 */
export const deleteOrder = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { restoreStock } = req.query;

  const order = await pool.query('SELECT * FROM orders WHERE id = $1', [id]);
  
  if (order.rows.length === 0) {
    return res.status(404).json({
      success: false,
      message: 'Order not found'
    });
  }

  if (restoreStock === 'true') {
    // Restore stock for each item
    const items = await pool.query(
      'SELECT product_id, quantity FROM order_items WHERE order_id = $1',
      [id]
    );

    for (const item of items.rows) {
      if (item.product_id) {
        await inventoryService.adjustStock(
          item.product_id,
          item.quantity,
          'adjustment',
          `Stock restored from cancelled order ${id}`
        );
      }
    }
  }

  await pool.query('DELETE FROM orders WHERE id = $1', [id]);

  res.json({
    success: true,
    message: 'Order deleted successfully'
  });
});

/**
 * GET /api/orders/stats/summary
 * Get order statistics summary
 */
export const getOrderStats = asyncHandler(async (req, res) => {
  const stats = await pool.query(`
    SELECT 
      COUNT(*) as total_orders,
      COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders,
      COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_orders,
      COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
      COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_orders,
      SUM(total_items) as total_items,
      AVG(total_items)::numeric(10,2) as avg_items_per_order
    FROM orders
  `);

  res.json({
    success: true,
    data: stats.rows[0]
  });
});
