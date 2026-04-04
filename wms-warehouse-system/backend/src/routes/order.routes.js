import { Router } from 'express';
import * as orderController from '../controllers/orderController.js';

const router = Router();

/**
 * @route   GET /api/orders/stats/summary
 * @desc    Get order statistics summary
 * @access  Public
 */
router.get('/stats/summary', orderController.getOrderStats);

/**
 * @route   GET /api/orders
 * @desc    Get all orders with pagination and filtering
 * @access  Public
 */
router.get('/', orderController.getOrders);

/**
 * @route   GET /api/orders/:id
 * @desc    Get single order with items
 * @access  Public
 */
router.get('/:id', orderController.getOrderById);

/**
 * @route   PUT /api/orders/:id/status
 * @desc    Update order status
 * @access  Public
 */
router.put('/:id/status', orderController.updateOrderStatus);

/**
 * @route   DELETE /api/orders/:id
 * @desc    Cancel/delete order
 * @access  Public
 */
router.delete('/:id', orderController.deleteOrder);

export default router;
