import jsonDb from '../utils/jsonDb.js';
import { importLogger } from '../utils/logger.js';

/**
 * Inventory Service - Core business logic for stock management
 * Refactored from PostgreSQL to file-based JSON database
 * Handles atomic stock deductions, validations, and transaction logging
 */

class InventoryService {
  /**
   * Adjust stock manually (for inventory audits)
   */
  async adjustStock(productId, quantityChange, transactionType, notes = null, createdBy = 'system') {
    const result = jsonDb.adjustStock(productId, quantityChange, transactionType, notes, createdBy);
    
    return {
      productId,
      previousQuantity: result.previousQuantity,
      newQuantity: result.newQuantity,
      quantityChange
    };
  }

  /**
   * Get low stock products
   */
  async getLowStockProducts(threshold = 5) {
    return jsonDb.getLowStockProducts(threshold);
  }

  /**
   * Get stock movement history for a product
   */
  async getStockHistory(productId, limit = 50) {
    return jsonDb.getProductTransactions(productId, limit);
  }
}

export default new InventoryService();
