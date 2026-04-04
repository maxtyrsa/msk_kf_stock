import { Router } from 'express';
import importRoutes from './import.routes.js';
import productRoutes from './product.routes.js';
import orderRoutes from './order.routes.js';
import analyticsRoutes from './analytics.routes.js';

const router = Router();

/**
 * API Routes Mapping
 * All routes are prefixed with /api
 */

// Import routes - /api/import/*
router.use('/import', importRoutes);

// Product routes - /api/products/*
router.use('/products', productRoutes);

// Order routes - /api/orders/*
router.use('/orders', orderRoutes);

// Analytics routes - /api/analytics/*
router.use('/analytics', analyticsRoutes);

// Health check endpoint
router.get('/health', (req, res) => {
  res.json({
    success: true,
    message: 'WMS API is running',
    timestamp: new Date().toISOString()
  });
});

export default router;
