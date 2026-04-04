import { asyncHandler } from '../middleware/errorHandler.js';
import jsonDb from '../utils/jsonDb.js';
import inventoryService from '../services/inventoryService.js';

/**
 * Order Controller - Order management and status tracking
 * Refactored from PostgreSQL to file-based JSON database
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
  
  const options = {
    limit: parseInt(limit),
    offset: parseInt(offset),
    status,
    source,
    dateFrom,
    dateTo,
    orderNumber
  };

  const result = jsonDb.getOrders(options);

  res.json({
    success: true,
    data: result
  });
});

/**
 * GET /api/orders/:id
 * Get single order with items
 */
export const getOrderById = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const order = jsonDb.findOrderById(id);

  if (!order) {
    return res.status(404).json({
      success: false,
      message: 'Order not found'
    });
  }

  res.json({
    success: true,
    data: order
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

  const updatedOrder = jsonDb.updateOrderStatus(id, status);

  if (!updatedOrder) {
    return res.status(404).json({
      success: false,
      message: 'Order not found'
    });
  }

  res.json({
    success: true,
    data: updatedOrder,
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

  const order = jsonDb.findOrderById(id);
  
  if (!order) {
    return res.status(404).json({
      success: false,
      message: 'Order not found'
    });
  }

  jsonDb.deleteOrder(id, restoreStock === 'true');

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
  const db = jsonDb.read();
  
  const stats = {
    total_orders: db.orders.length,
    pending_orders: db.orders.filter(o => o.status === 'pending').length,
    processing_orders: db.orders.filter(o => o.status === 'processing').length,
    completed_orders: db.orders.filter(o => o.status === 'completed').length,
    cancelled_orders: db.orders.filter(o => o.status === 'cancelled').length,
    total_items: db.orders.reduce((sum, o) => sum + (o.total_items || 0), 0),
    avg_items_per_order: db.orders.length > 0 
      ? (db.orders.reduce((sum, o) => sum + (o.total_items || 0), 0) / db.orders.length).toFixed(2)
      : 0
  };

  res.json({
    success: true,
    data: stats
  });
});
