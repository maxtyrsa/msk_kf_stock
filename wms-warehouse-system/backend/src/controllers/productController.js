import { asyncHandler } from '../middleware/errorHandler.js';
import jsonDb from '../utils/jsonDb.js';
import { productLogger } from '../utils/logger.js';

/**
 * Product Controller - CRUD operations and high-performance search
 * Refactored from PostgreSQL to file-based JSON database
 */

/**
 * GET /api/products/search?q=query&page=1&limit=20
 * High-performance search with case-insensitive partial matching
 * Uses linear filtering with includes() for JSON database
 */
export const searchProducts = asyncHandler(async (req, res) => {
  const { q, page = 1, limit = 20 } = req.query;

  if (!q || q.trim().length === 0) {
    return res.status(400).json({
      success: false,
      message: 'Search query is required'
    });
  }

  const offset = (page - 1) * limit;
  const searchTerm = q.trim();

  // Search using JSON database with case-insensitive partial matching
  const result = jsonDb.searchProducts(searchTerm, parseInt(limit), parseInt(offset));

  productLogger.info({ query: searchTerm, results: result.products.length, total: result.total }, 'Product search executed');

  res.json({
    success: true,
    data: {
      products: result.products,
      pagination: {
        page: result.page,
        limit: result.limit,
        total: result.total,
        totalPages: result.totalPages,
        hasMore: result.hasMore
      }
    }
  });
});

/**
 * GET /api/products
 * Get all products with optional filtering
 */
export const getProducts = asyncHandler(async (req, res) => {
  const { 
    page = 1, 
    limit = 20, 
    lowStock, 
    category,
    sortBy = 'name',
    sortOrder = 'ASC'
  } = req.query;

  const offset = (page - 1) * limit;
  
  const options = {
    limit: parseInt(limit),
    offset: parseInt(offset),
    lowStock,
    category,
    sortBy,
    sortOrder
  };

  const result = jsonDb.getProducts(options);

  res.json({
    success: true,
    data: result
  });
});

/**
 * GET /api/products/:id
 * Get single product by ID
 */
export const getProductById = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const product = jsonDb.findProductById(id);

  if (!product) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  res.json({
    success: true,
    data: product
  });
});

/**
 * GET /api/products/article/:article
 * Get product by article number (exact match)
 */
export const getProductByArticle = asyncHandler(async (req, res) => {
  const { article } = req.params;

  const product = jsonDb.findProductByArticle(article);

  if (!product) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  res.json({
    success: true,
    data: product
  });
});

/**
 * POST /api/products
 * Create new product
 */
export const createProduct = asyncHandler(async (req, res) => {
  const { name, model, article, quantity = 0 } = req.body;

  const newProduct = jsonDb.createProduct({ name, model, article, quantity });

  productLogger.info({ productId: newProduct.id, article }, 'Product created');

  res.status(201).json({
    success: true,
    data: newProduct,
    message: 'Product created successfully'
  });
});

/**
 * PUT /api/products/:id
 * Update existing product
 */
export const updateProduct = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { name, model, article, quantity } = req.body;

  const updates = {};
  if (name !== undefined) updates.name = name;
  if (model !== undefined) updates.model = model;
  if (article !== undefined) updates.article = article;
  if (quantity !== undefined) updates.quantity = quantity;

  const updatedProduct = jsonDb.updateProduct(id, updates);

  if (!updatedProduct) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  productLogger.info({ productId: id }, 'Product updated');

  res.json({
    success: true,
    data: updatedProduct,
    message: 'Product updated successfully'
  });
});

/**
 * DELETE /api/products/:id
 * Delete product (soft delete recommended in production)
 */
export const deleteProduct = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const deletedProduct = jsonDb.deleteProduct(id);

  if (!deletedProduct) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  productLogger.info({ productId: id }, 'Product deleted');

  res.json({
    success: true,
    message: 'Product deleted successfully'
  });
});

/**
 * GET /api/products/low-stock
 * Get products with low stock levels
 */
export const getLowStockProducts = asyncHandler(async (req, res) => {
  const threshold = parseInt(req.query.threshold) || 5;

  const products = jsonDb.getLowStockProducts(threshold);

  res.json({
    success: true,
    data: {
      products,
      threshold,
      count: products.length
    }
  });
});
