import { asyncHandler } from '../middleware/errorHandler.js';
import pool from '../utils/db.js';

/**
 * Analytics Controller - Dashboard data and reporting for Recharts
 */

/**
 * GET /api/analytics/dashboard
 * Get comprehensive dashboard data for the main overview
 */
export const getDashboardData = asyncHandler(async (req, res) => {
  // Get stock summary
  const stockSummary = await pool.query(`
    SELECT 
      COUNT(*) as total_products,
      SUM(quantity) as total_stock,
      AVG(quantity)::numeric(10,2) as avg_stock,
      COUNT(CASE WHEN quantity <= 5 THEN 1 END) as low_stock_count,
      COUNT(CASE WHEN quantity = 0 THEN 1 END) as out_of_stock_count,
      MIN(quantity) as min_stock,
      MAX(quantity) as max_stock
    FROM products
  `);

  // Get order summary
  const orderSummary = await pool.query(`
    SELECT 
      COUNT(*) as total_orders,
      SUM(total_items) as total_items,
      COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
      COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders
    FROM orders
  `);

  // Get recent transactions count
  const recentTransactions = await pool.query(`
    SELECT COUNT(*) as count
    FROM transactions
    WHERE created_at >= NOW() - INTERVAL '24 hours'
  `);

  res.json({
    success: true,
    data: {
      stockSummary: stockSummary.rows[0],
      orderSummary: orderSummary.rows[0],
      recentTransactions: recentTransactions.rows[0].count
    }
  });
});

/**
 * GET /api/analytics/stock-trends
 * Get stock level trends over time (for area/line chart)
 */
export const getStockTrends = asyncHandler(async (req, res) => {
  const { days = 30 } = req.query;

  const trends = await pool.query(`
    SELECT 
      DATE(created_at) as date,
      SUM(CASE WHEN transaction_type = 'stock_in' THEN quantity_change ELSE 0 END) as stock_in,
      SUM(CASE WHEN transaction_type = 'stock_out' THEN quantity_change ELSE 0 END) as stock_out,
      SUM(CASE WHEN transaction_type IN ('stock_in', 'adjustment') THEN quantity_change 
               WHEN transaction_type = 'stock_out' THEN -quantity_change ELSE 0 END) as net_change
    FROM transactions
    WHERE created_at >= NOW() - INTERVAL '${parseInt(days)} days'
    GROUP BY DATE(created_at)
    ORDER BY date ASC
  `);

  res.json({
    success: true,
    data: trends.rows
  });
});

/**
 * GET /api/analytics/order-trends
 * Get order trends over time (for line/bar chart)
 */
export const getOrderTrends = asyncHandler(async (req, res) => {
  const { days = 30, granularity = 'day' } = req.query;

  let dateFormat;
  switch (granularity) {
    case 'hour':
      dateFormat = 'YYYY-MM-DD HH24';
      break;
    case 'week':
      dateFormat = 'IYYY-IW';
      break;
    case 'month':
      dateFormat = 'YYYY-MM';
      break;
    default:
      dateFormat = 'YYYY-MM-DD';
  }

  const trends = await pool.query(`
    SELECT 
      TO_CHAR(imported_at, '${dateFormat}') as period,
      COUNT(*) as order_count,
      SUM(total_items) as items_count,
      COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count,
      COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count
    FROM orders
    WHERE imported_at >= NOW() - INTERVAL '${parseInt(days)} days'
    GROUP BY TO_CHAR(imported_at, '${dateFormat}')
    ORDER BY period ASC
  `);

  res.json({
    success: true,
    data: trends.rows
  });
});

/**
 * GET /api/analytics/stock-by-category
 * Get stock distribution (for pie/donut chart)
 * Groups products by stock level ranges
 */
export const getStockDistribution = asyncHandler(async (req, res) => {
  const distribution = await pool.query(`
    SELECT 
      CASE 
        WHEN quantity = 0 THEN 'Out of Stock'
        WHEN quantity <= 5 THEN 'Low (1-5)'
        WHEN quantity <= 20 THEN 'Medium (6-20)'
        WHEN quantity <= 50 THEN 'Good (21-50)'
        ELSE 'High (50+)'
      END as stock_level,
      COUNT(*) as product_count,
      SUM(quantity) as total_quantity
    FROM products
    GROUP BY 
      CASE 
        WHEN quantity = 0 THEN 'Out of Stock'
        WHEN quantity <= 5 THEN 'Low (1-5)'
        WHEN quantity <= 20 THEN 'Medium (6-20)'
        WHEN quantity <= 50 THEN 'Good (21-50)'
        ELSE 'High (50+)'
      END
    ORDER BY 
      CASE 
        WHEN quantity = 0 THEN 1
        WHEN quantity <= 5 THEN 2
        WHEN quantity <= 20 THEN 3
        WHEN quantity <= 50 THEN 4
        ELSE 5
      END
  `);

  res.json({
    success: true,
    data: distribution.rows
  });
});

/**
 * GET /api/analytics/top-products
 * Get top products by various metrics (for bar chart)
 */
export const getTopProducts = asyncHandler(async (req, res) => {
  const { limit = 10, metric = 'quantity' } = req.query;

  const validMetrics = ['quantity', 'transactions', 'orders'];
  const selectedMetric = validMetrics.includes(metric) ? metric : 'quantity';

  let query;
  if (selectedMetric === 'transactions') {
    query = `
      SELECT 
        p.id,
        p.name,
        p.article,
        p.quantity,
        COUNT(t.id) as transaction_count
      FROM products p
      LEFT JOIN transactions t ON p.id = t.product_id
      GROUP BY p.id, p.name, p.article, p.quantity
      ORDER BY transaction_count DESC
      LIMIT $1
    `;
  } else if (selectedMetric === 'orders') {
    query = `
      SELECT 
        p.id,
        p.name,
        p.article,
        p.quantity,
        COUNT(oi.id) as order_count
      FROM products p
      LEFT JOIN order_items oi ON p.id = oi.product_id
      GROUP BY p.id, p.name, p.article, p.quantity
      ORDER BY order_count DESC
      LIMIT $1
    `;
  } else {
    query = `
      SELECT 
        id,
        name,
        article,
        quantity
      FROM products
      ORDER BY quantity DESC
      LIMIT $1
    `;
  }

  const result = await pool.query(query, [parseInt(limit)]);

  res.json({
    success: true,
    data: {
      metric: selectedMetric,
      products: result.rows
    }
  });
});

/**
 * GET /api/analytics/transaction-types
 * Get transaction breakdown by type (for pie chart)
 */
export const getTransactionTypes = asyncHandler(async (req, res) => {
  const { days = 30 } = req.query;

  const types = await pool.query(`
    SELECT 
      transaction_type,
      COUNT(*) as count,
      SUM(quantity_change) as total_quantity,
      AVG(quantity_change)::numeric(10,2) as avg_quantity
    FROM transactions
    WHERE created_at >= NOW() - INTERVAL '${parseInt(days)} days'
    GROUP BY transaction_type
    ORDER BY count DESC
  `);

  res.json({
    success: true,
    data: types.rows
  });
});

/**
 * GET /api/analytics/daily-summary
 * Get daily order summary (for calendar heatmap or bar chart)
 */
export const getDailySummary = asyncHandler(async (req, res) => {
  const { days = 90 } = req.query;

  const summary = await pool.query(`
    SELECT 
      DATE(imported_at) as date,
      COUNT(*) as orders,
      SUM(total_items) as items,
      COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
      COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled
    FROM orders
    WHERE imported_at >= NOW() - INTERVAL '${parseInt(days)} days'
    GROUP BY DATE(imported_at)
    ORDER BY date DESC
  `);

  res.json({
    success: true,
    data: summary.rows
  });
});
