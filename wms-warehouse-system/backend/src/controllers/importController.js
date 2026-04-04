import { asyncHandler } from '../middleware/errorHandler.js';
import { validateImportJson } from '../middleware/validateJson.js';
import jsonDb from '../utils/jsonDb.js';
import { importLogger } from '../utils/logger.js';

/**
 * Import Controller - Handles JSON import from Python PDF parser
 * Refactored from PostgreSQL to file-based JSON database
 */

/**
 * POST /api/import/orders
 * Import orders from Python PDF parser JSON output
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
  const results = await processOrderImport(orders);

  importLogger.info(results, 'Order import completed');

  res.json({
    success: true,
    message: `Successfully imported ${results.processedOrders} of ${results.totalOrders} orders`,
    data: results
  });
});

/**
 * Process order import with atomic stock deduction
 * Uses JSON database transactions for data consistency
 */
async function processOrderImport(orders) {
  const results = {
    totalOrders: orders.length,
    processedOrders: 0,
    failedOrders: 0,
    totalItems: 0,
    newProducts: 0,
    updatedProducts: 0,
    errors: []
  };

  for (const order of orders) {
    try {
      const orderResult = jsonDb.createOrder({
        order_number: order.order_number,
        source: 'PDF',
        status: 'completed',
        items: order.items
      });

      if (orderResult && orderResult.success) {
        results.processedOrders++;
        results.totalItems += orderResult.itemCount;
        results.newProducts += orderResult.newProducts;
        results.updatedProducts += orderResult.updatedProducts;
      } else {
        results.failedOrders++;
        results.errors.push({
          orderNumber: order.order_number,
          error: orderResult?.error || 'Unknown error'
        });
      }
    } catch (error) {
      results.failedOrders++;
      results.errors.push({
        orderNumber: order.order_number,
        error: error.message
      });
      importLogger.error({ order, error }, 'Failed to process order');
    }
  }

  return results;
}

/**
 * GET /api/import/status
 * Get import statistics and recent activity
 */
export const getImportStatus = asyncHandler(async (req, res) => {
  const db = jsonDb.read();
  
  // Calculate recent imports (last 7 days)
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  const recentImportsMap = {};
  
  db.orders.forEach(order => {
    const date = order.imported_at.split('T')[0];
    if (date >= sevenDaysAgo) {
      recentImportsMap[date] = (recentImportsMap[date] || 0) + 1;
    }
  });

  const recentImports = Object.entries(recentImportsMap)
    .map(([date, count]) => ({ date, count }))
    .sort((a, b) => b.date.localeCompare(a.date));

  // Get total statistics
  const statistics = {
    total_orders: db.orders.length,
    total_products: db.products.length,
    total_stock: db.products.reduce((sum, p) => sum + (p.quantity || 0), 0),
    transactions_24h: db.transactions.filter(t => {
      const txDate = new Date(t.created_at);
      const dayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
      return txDate >= dayAgo;
    }).length
  };

  res.json({
    success: true,
    data: {
      statistics,
      recentImports
    }
  });
});

/**
 * GET /api/import/errors
 * Get recent import errors (negative stock transactions)
 */
export const getImportErrors = asyncHandler(async (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const db = jsonDb.read();

  const errors = db.transactions
    .filter(t => t.new_quantity < 0)
    .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
    .slice(0, limit)
    .map(t => {
      const product = db.products.find(p => p.id === t.product_id);
      return {
        ...t,
        product_name: product?.name,
        product_article: product?.article
      };
    });

  res.json({
    success: true,
    data: errors
  });
});
