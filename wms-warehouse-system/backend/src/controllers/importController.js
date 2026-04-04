import { asyncHandler } from '../middleware/errorHandler.js';
import { validateImportJson } from '../middleware/validateJson.js';
import inventoryService from '../services/inventoryService.js';
import { importLogger } from '../utils/logger.js';

/**
 * Import Controller - Handles JSON import from Python PDF parser
 */

/**
 * POST /api/import/orders
 * Import orders from Python PDF parser JSON output
 * 
 * Expected JSON format:
 * [
 *   {
 *     "order_number": "12345",
 *     "items": [
 *       {
 *         "product": "Product Name (may span multiple lines)",
 *         "article": "ART-001",
 *         "model": "Model X",
 *         "quantity": 2
 *       }
 *     ]
 *   }
 * ]
 */
export const importOrders = asyncHandler(async (req, res) => {
  const orders = req.body;

  // Validate input
  const { error } = validateImportJson(orders);
  if (error) {
    return res.status(400).json({
      success: false,
      message: 'Invalid JSON format',
      errors: error.details.map(d => d.message)
    });
  }

  importLogger.info({ orderCount: orders.length }, 'Starting order import');

  // Process orders with atomic stock deduction
  const results = await inventoryService.processOrderImport(orders);

  importLogger.info(results, 'Order import completed');

  res.json({
    success: true,
    message: `Successfully imported ${results.processedOrders} of ${results.totalOrders} orders`,
    data: results
  });
});

/**
 * GET /api/import/status
 * Get import statistics and recent activity
 */
export const getImportStatus = asyncHandler(async (req, res) => {
  const { pool } = await import('../utils/db.js');
  
  // Get recent imports count
  const recentImports = await pool.query(
    `SELECT COUNT(*) as count, DATE(imported_at) as date 
     FROM orders 
     WHERE imported_at >= NOW() - INTERVAL '7 days'
     GROUP BY DATE(imported_at)
     ORDER BY date DESC`
  );

  // Get total statistics
  const stats = await pool.query(`
    SELECT 
      (SELECT COUNT(*) FROM orders) as total_orders,
      (SELECT COUNT(*) FROM products) as total_products,
      (SELECT SUM(quantity) FROM products) as total_stock,
      (SELECT COUNT(*) FROM transactions WHERE created_at >= NOW() - INTERVAL '24 hours') as transactions_24h
  `);

  res.json({
    success: true,
    data: {
      statistics: stats.rows[0],
      recentImports: recentImports.rows
    }
  });
});

/**
 * GET /api/import/errors
 * Get recent import errors
 */
export const getImportErrors = asyncHandler(async (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const { pool } = await import('../utils/db.js');

  const errors = await pool.query(
    `SELECT t.*
     FROM transactions t
     JOIN products p ON t.product_id = p.id
     WHERE t.new_quantity < 0
     ORDER BY t.created_at DESC
     LIMIT $1`,
    [limit]
  );

  res.json({
    success: true,
    data: errors.rows
  });
});
