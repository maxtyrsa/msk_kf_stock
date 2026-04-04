import { asyncHandler } from '../middleware/errorHandler.js';
import jsonDb from '../utils/jsonDb.js';

/**
 * Analytics Controller - Dashboard data and reporting for Recharts
 * Refactored from PostgreSQL to file-based JSON database
 */

/**
 * GET /api/analytics/dashboard
 * Get comprehensive dashboard data for the main overview
 */
export const getDashboardData = asyncHandler(async (req, res) => {
  const data = jsonDb.getDashboardData();

  res.json({
    success: true,
    data
  });
});

/**
 * GET /api/analytics/stock-trends
 * Get stock level trends over time (for area/line chart)
 */
export const getStockTrends = asyncHandler(async (req, res) => {
  const { days = 30 } = req.query;

  const trends = jsonDb.getStockTrends(parseInt(days));

  res.json({
    success: true,
    data: trends
  });
});

/**
 * GET /api/analytics/order-trends
 * Get order trends over time (for line/bar chart)
 */
export const getOrderTrends = asyncHandler(async (req, res) => {
  const { days = 30, granularity = 'day' } = req.query;

  const trends = jsonDb.getOrderTrends(parseInt(days), granularity);

  res.json({
    success: true,
    data: trends
  });
});

/**
 * GET /api/analytics/stock-by-category
 * Get stock distribution (for pie/donut chart)
 * Groups products by stock level ranges
 */
export const getStockDistribution = asyncHandler(async (req, res) => {
  const distribution = jsonDb.getStockDistribution();

  res.json({
    success: true,
    data: distribution
  });
});

/**
 * GET /api/analytics/top-products
 * Get top products by various metrics (for bar chart)
 */
export const getTopProducts = asyncHandler(async (req, res) => {
  const { limit = 10, metric = 'quantity' } = req.query;

  const products = jsonDb.getTopProducts(parseInt(limit), metric);

  res.json({
    success: true,
    data: {
      metric,
      products
    }
  });
});

/**
 * GET /api/analytics/transaction-types
 * Get transaction breakdown by type (for pie chart)
 */
export const getTransactionTypes = asyncHandler(async (req, res) => {
  const { days = 30 } = req.query;

  const types = jsonDb.getTransactionTypes(parseInt(days));

  res.json({
    success: true,
    data: types
  });
});

/**
 * GET /api/analytics/daily-summary
 * Get daily order summary (for calendar heatmap or bar chart)
 */
export const getDailySummary = asyncHandler(async (req, res) => {
  const { days = 90 } = req.query;

  const summary = jsonDb.getDailySummary(parseInt(days));

  res.json({
    success: true,
    data: summary
  });
});
