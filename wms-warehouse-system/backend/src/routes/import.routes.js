import { Router } from 'express';
import * as importController from '../controllers/importController.js';

const router = Router();

/**
 * @route   POST /api/import/orders
 * @desc    Import orders from Python PDF parser JSON
 * @access  Public (add auth middleware in production)
 */
router.post('/orders', importController.importOrders);

/**
 * @route   GET /api/import/status
 * @desc    Get import statistics and recent activity
 * @access  Public
 */
router.get('/status', importController.getImportStatus);

/**
 * @route   GET /api/import/errors
 * @desc    Get recent import errors (negative stock alerts)
 * @access  Public
 */
router.get('/errors', importController.getImportErrors);

export default router;
