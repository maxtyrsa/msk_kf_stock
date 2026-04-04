import pool, { getClient } from '../utils/db.js';
import { importLogger } from '../utils/logger.js';

/**
 * Inventory Service - Core business logic for stock management
 * Handles atomic stock deductions, validations, and transaction logging
 */

class InventoryService {
  /**
   * Process order import with atomic stock deduction
   * Uses database transactions to ensure data consistency
   * 
   * @param {Array} orders - Array of orders from Python PDF parser
   * @returns {Object} - Import results with statistics
   */
  async processOrderImport(orders) {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');
      
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
          const orderResult = await this._processSingleOrder(client, order);
          
          if (orderResult.success) {
            results.processedOrders++;
            results.totalItems += orderResult.itemCount;
            results.newProducts += orderResult.newProducts;
            results.updatedProducts += orderResult.updatedProducts;
          } else {
            results.failedOrders++;
            results.errors.push({
              orderNumber: order.order_number,
              error: orderResult.error
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

      // Commit transaction if at least one order succeeded
      if (results.processedOrders > 0) {
        await client.query('COMMIT');
      } else {
        await client.query('ROLLBACK');
      }

      return results;
    } catch (error) {
      await client.query('ROLLBACK');
      importLogger.error({ error }, 'Transaction failed');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Process a single order within a transaction
   */
  async _processSingleOrder(client, order) {
    const { order_number, items } = order;
    
    try {
      // Check if order already exists
      const existingOrder = await client.query(
        'SELECT id FROM orders WHERE order_number = $1',
        [order_number]
      );

      if (existingOrder.rows.length > 0) {
        return {
          success: false,
          error: 'Order already exists'
        };
      }

      // Insert order
      const orderResult = await client.query(
        `INSERT INTO orders (order_number, source, status, total_items, metadata)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id`,
        [order_number, 'PDF', 'completed', items.length, JSON.stringify(order)]
      );

      const orderId = orderResult.rows[0].id;
      let newProducts = 0;
      let updatedProducts = 0;

      // Process each item in the order
      for (const item of items) {
        const { product, article, model, quantity } = item;
        
        // Normalize article - handle spaces and special characters
        const normalizedArticle = this._normalizeArticle(article);
        
        // Find or create product
        let productId = await this._findOrCreateProduct(
          client,
          product || `${model || ''} ${normalizedArticle || ''}`.trim(),
          model,
          normalizedArticle,
          quantity
        );

        if (productId.isNew) {
          newProducts++;
        } else {
          updatedProducts++;
        }

        // Deduct stock atomically
        await this._deductStock(
          client,
          productId.id,
          quantity,
          orderId,
          order_number
        );

        // Insert order item
        await client.query(
          `INSERT INTO order_items 
           (order_id, product_name, product_model, product_article, quantity, product_id)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [orderId, product, model, normalizedArticle, quantity, productId.id]
        );
      }

      return {
        success: true,
        itemCount: items.length,
        newProducts,
        updatedProducts
      };
    } catch (error) {
      importLogger.error({ order, error }, 'Error processing single order');
      throw error;
    }
  }

  /**
   * Normalize article number - handle spaces and special characters
   */
  _normalizeArticle(article) {
    if (!article) return null;
    
    // Trim whitespace and normalize internal spaces
    return article.trim().replace(/\s+/g, ' ');
  }

  /**
   * Find existing product or create new one
   */
  async _findOrCreateProduct(client, name, model, article, initialQuantity = 0) {
    // Try to find by article first (most reliable identifier)
    if (article) {
      const existing = await client.query(
        'SELECT id, name, model, article, quantity FROM products WHERE article = $1',
        [article]
      );

      if (existing.rows.length > 0) {
        return { id: existing.rows[0].id, isNew: false };
      }
    }

    // Create new product
    const result = await client.query(
      `INSERT INTO products (name, model, article, quantity)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (article) DO UPDATE SET
         name = COALESCE(EXCLUDED.name, products.name),
         model = COALESCE(EXCLUDED.model, products.model)
       RETURNING id`,
      [name, model || null, article || null, 0]
    );

    return { id: result.rows[0].id, isNew: true };
  }

  /**
   * Deduct stock with transaction logging
   */
  async _deductStock(client, productId, quantity, referenceId, orderNumber) {
    // Get current stock
    const currentStock = await client.query(
      'SELECT quantity FROM products WHERE id = $1',
      [productId]
    );

    if (currentStock.rows.length === 0) {
      throw new Error(`Product ${productId} not found`);
    }

    const previousQuantity = currentStock.rows[0].quantity;
    const newQuantity = previousQuantity - quantity;

    // Update product stock
    await client.query(
      'UPDATE products SET quantity = $1 WHERE id = $2',
      [newQuantity, productId]
    );

    // Log transaction
    await client.query(
      `INSERT INTO transactions 
       (product_id, transaction_type, quantity_change, previous_quantity, new_quantity, reference_type, reference_id, notes)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [productId, 'stock_out', quantity, previousQuantity, newQuantity, 'order', referenceId, `Order: ${orderNumber}`]
    );

    // Check for negative stock
    if (newQuantity < 0) {
      importLogger.warn({
        productId,
        orderNumber,
        previousQuantity,
        newQuantity
      }, 'Negative stock detected after order import');
    }
  }

  /**
   * Manual stock adjustment (for inventory audits)
   */
  async adjustStock(productId, quantityChange, transactionType, notes = null, createdBy = 'system') {
    const client = await getClient();
    
    try {
      await client.query('BEGIN');

      // Get current stock
      const currentStock = await client.query(
        'SELECT quantity FROM products WHERE id = $1',
        [productId]
      );

      if (currentStock.rows.length === 0) {
        throw new Error(`Product ${productId} not found`);
      }

      const previousQuantity = currentStock.rows[0].quantity;
      const newQuantity = previousQuantity + quantityChange;

      // Update product stock
      await client.query(
        'UPDATE products SET quantity = $1 WHERE id = $2',
        [newQuantity, productId]
      );

      // Log transaction
      await client.query(
        `INSERT INTO transactions 
         (product_id, transaction_type, quantity_change, previous_quantity, new_quantity, reference_type, notes, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [productId, transactionType, quantityChange, previousQuantity, newQuantity, 'manual', notes, createdBy]
      );

      await client.query('COMMIT');

      return {
        productId,
        previousQuantity,
        newQuantity,
        quantityChange
      };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Get low stock products
   */
  async getLowStockProducts(threshold = 5) {
    const result = await pool.query(
      `SELECT id, name, model, article, quantity 
       FROM products 
       WHERE quantity <= $1 
       ORDER BY quantity ASC`,
      [threshold]
    );

    return result.rows;
  }

  /**
   * Get stock movement history for a product
   */
  async getStockHistory(productId, limit = 50) {
    const result = await pool.query(
      `SELECT t.*, p.name as product_name, p.article as product_article
       FROM transactions t
       JOIN products p ON t.product_id = p.id
       WHERE t.product_id = $1
       ORDER BY t.created_at DESC
       LIMIT $2`,
      [productId, limit]
    );

    return result.rows;
  }
}

export default new InventoryService();
