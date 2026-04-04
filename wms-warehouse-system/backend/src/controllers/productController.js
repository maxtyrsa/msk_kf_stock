import { asyncHandler } from '../middleware/errorHandler.js';
import pool from '../utils/db.js';
import { productLogger } from '../utils/logger.js';

/**
 * Product Controller - CRUD operations and high-performance search
 */

/**
 * GET /api/products/search?q=query&page=1&limit=20
 * High-performance search with ILIKE across Name, Model, and SKU (Article)
 * Uses pg_trgm GIN indexes for fast partial matching
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
  const searchTerm = `%${q.trim()}%`;

  // High-performance search using ILIKE with pg_trgm indexes
  const query = `
    SELECT 
      id, name, model, article, quantity, created_at, updated_at,
      similarity(name, $1) + similarity(model, $1) + similarity(article, $1) as relevance
    FROM products
    WHERE 
      name ILIKE $1 OR 
      model ILIKE $1 OR 
      article ILIKE $1
    ORDER BY 
      relevance DESC,
      quantity DESC,
      name ASC
    LIMIT $2 OFFSET $3
  `;

  const countQuery = `
    SELECT COUNT(*) as total
    FROM products
    WHERE 
      name ILIKE $1 OR 
      model ILIKE $1 OR 
      article ILIKE $1
  `;

  const [results, countResult] = await Promise.all([
    pool.query(query, [searchTerm, parseInt(limit), parseInt(offset)]),
    pool.query(countQuery, [searchTerm])
  ]);

  const total = parseInt(countResult.rows[0].total);

  productLogger.info({ query: q, results: results.rows.length, total }, 'Product search executed');

  res.json({
    success: true,
    data: {
      products: results.rows,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total,
        totalPages: Math.ceil(total / limit),
        hasMore: offset + results.rows.length < total
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
  
  let whereClause = 'WHERE 1=1';
  const params = [];
  let paramIndex = 1;

  if (lowStock === 'true') {
    whereClause += ` AND quantity <= $${paramIndex}`;
    params.push(5);
    paramIndex++;
  }

  const validSortColumns = ['name', 'model', 'article', 'quantity', 'created_at', 'updated_at'];
  const sortColumn = validSortColumns.includes(sortBy) ? sortBy : 'name';
  const order = sortOrder.toUpperCase() === 'DESC' ? 'DESC' : 'ASC';

  const query = `
    SELECT id, name, model, article, quantity, created_at, updated_at
    FROM products
    ${whereClause}
    ORDER BY ${sortColumn} ${order}
    LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
  `;

  params.push(parseInt(limit), parseInt(offset));

  const countQuery = `
    SELECT COUNT(*) as total
    FROM products
    ${whereClause}
  `;

  const [results, countResult] = await Promise.all([
    pool.query(query, params),
    pool.query(countQuery, params.slice(0, -2))
  ]);

  res.json({
    success: true,
    data: {
      products: results.rows,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: parseInt(countResult.rows[0].total),
        totalPages: Math.ceil(parseInt(countResult.rows[0].total) / limit)
      }
    }
  });
});

/**
 * GET /api/products/:id
 * Get single product by ID
 */
export const getProductById = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const result = await pool.query(
    'SELECT * FROM products WHERE id = $1',
    [id]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  res.json({
    success: true,
    data: result.rows[0]
  });
});

/**
 * GET /api/products/article/:article
 * Get product by article number (exact match)
 */
export const getProductByArticle = asyncHandler(async (req, res) => {
  const { article } = req.params;

  const result = await pool.query(
    'SELECT * FROM products WHERE article = $1',
    [article]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  res.json({
    success: true,
    data: result.rows[0]
  });
});

/**
 * POST /api/products
 * Create new product
 */
export const createProduct = asyncHandler(async (req, res) => {
  const { name, model, article, quantity = 0 } = req.body;

  const result = await pool.query(
    `INSERT INTO products (name, model, article, quantity)
     VALUES ($1, $2, $3, $4)
     RETURNING *`,
    [name, model || null, article, parseInt(quantity)]
  );

  productLogger.info({ productId: result.rows[0].id, article }, 'Product created');

  res.status(201).json({
    success: true,
    data: result.rows[0],
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

  const result = await pool.query(
    `UPDATE products 
     SET name = COALESCE($1, name),
         model = COALESCE($2, model),
         article = COALESCE($3, article),
         quantity = COALESCE($4, quantity)
     WHERE id = $5
     RETURNING *`,
    [name, model, article, quantity !== undefined ? parseInt(quantity) : undefined, id]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({
      success: false,
      message: 'Product not found'
    });
  }

  productLogger.info({ productId: id }, 'Product updated');

  res.json({
    success: true,
    data: result.rows[0],
    message: 'Product updated successfully'
  });
});

/**
 * DELETE /api/products/:id
 * Delete product (soft delete recommended in production)
 */
export const deleteProduct = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const result = await pool.query(
    'DELETE FROM products WHERE id = $1 RETURNING *',
    [id]
  );

  if (result.rows.length === 0) {
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

  const result = await pool.query(
    `SELECT id, name, model, article, quantity
     FROM products
     WHERE quantity <= $1
     ORDER BY quantity ASC`,
    [threshold]
  );

  res.json({
    success: true,
    data: {
      products: result.rows,
      threshold,
      count: result.rows.length
    }
  });
});
