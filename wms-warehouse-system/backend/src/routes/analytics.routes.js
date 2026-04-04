import { Router } from 'express';
import * as analyticsController from '../controllers/analyticsController.js';

const router = Router();

/**
 * @route   GET /api/analytics/dashboard
 * @desc    Get comprehensive dashboard data
 * @access  Public
 */
router.get('/dashboard', analyticsController.getDashboardData);

/**
 * @route   GET /api/analytics/stock-trends
 * @desc    Get stock level trends over time
 * @access  Public
 */
router.get('/stock-trends', analyticsController.getStockTrends);

/**
 * @route   GET /api/analytics/order-trends
 * @desc    Get order trends over time
 * @access  Public
 */
router.get('/order-trends', analyticsController.getOrderTrends);

/**
 * @route   GET /api/analytics/stock-by-category
 * @desc    Get stock distribution by level ranges
 * @access  Public
 */
router.get('/stock-by-category', analyticsController.getStockDistribution);

/**
 * @route   GET /api/analytics/top-products
 * @desc    Get top products by various metrics
 * @access  Public
 */
router.get('/top-products', analyticsController.getTopProducts);

/**
 * @route   GET /api/analytics/transaction-types
 * @desc    Get transaction breakdown by type
 * @access  Public
 */
router.get('/transaction-types', analyticsController.getTransactionTypes);

/**
 * @route   GET /api/analytics/daily-summary
 * @desc    Get daily order summary
 * @access  Public
 */
router.get('/daily-summary', analyticsController.getDailySummary);

export default router;
