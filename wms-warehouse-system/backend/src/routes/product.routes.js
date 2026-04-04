import { Router } from 'express';
import * as productController from '../controllers/productController.js';

const router = Router();

/**
 * @route   GET /api/products/search?q=query
 * @desc    High-performance search with ILIKE across Name, Model, Article
 * @access  Public
 */
router.get('/search', productController.searchProducts);

/**
 * @route   GET /api/products/low-stock
 * @desc    Get products with low stock levels
 * @access  Public
 */
router.get('/low-stock', productController.getLowStockProducts);

/**
 * @route   GET /api/products
 * @desc    Get all products with pagination and filtering
 * @access  Public
 */
router.get('/', productController.getProducts);

/**
 * @route   GET /api/products/article/:article
 * @desc    Get product by article number (exact match)
 * @access  Public
 */
router.get('/article/:article', productController.getProductByArticle);

/**
 * @route   GET /api/products/:id
 * @desc    Get single product by ID
 * @access  Public
 */
router.get('/:id', productController.getProductById);

/**
 * @route   POST /api/products
 * @desc    Create new product
 * @access  Public
 */
router.post('/', productController.createProduct);

/**
 * @route   PUT /api/products/:id
 * @desc    Update existing product
 * @access  Public
 */
router.put('/:id', productController.updateProduct);

/**
 * @route   DELETE /api/products/:id
 * @desc    Delete product
 * @access  Public
 */
router.delete('/:id', productController.deleteProduct);

export default router;
