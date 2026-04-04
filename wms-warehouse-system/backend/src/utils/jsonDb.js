import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = path.join(__dirname, '../../data');
const DB_FILE = path.join(DATA_DIR, 'wms_db.json');
const LOCK_FILE = path.join(DATA_DIR, 'wms_db.lock');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

/**
 * File-based JSON Database Manager
 * Implements atomic writes with fsync + rename pattern
 * Uses simple mutex locking for concurrent access protection
 * Note: flock() not available in Node.js standard fs module,
 * using write lock flag instead for single-process scenarios
 */

class JsonDatabase {
  constructor() {
    this.cache = null;
    this.cacheTimestamp = 0;
    this.cacheTTL = 100; // ms - cache validity window
    this.writeLock = false;
    this.readQueue = [];
  }

  /**
   * Initialize database with default structure if not exists
   */
  init() {
    if (!fs.existsSync(DB_FILE)) {
      const initialData = {
        products: [],
        orders: [],
        transactions: [],
        order_items: [],
        meta: {
          nextProductId: 1,
          nextOrderId: 1,
          nextTxId: 1,
          nextOrderItemId: 1
        }
      };
      this._atomicWrite(initialData);
      console.log('Initialized new JSON database at', DB_FILE);
    }
    return this;
  }

  /**
   * Read database with caching
   * Note: File locking (flock) not available in Node.js standard fs,
   * using in-memory write lock for single-process scenarios
   */
  read() {
    if (this.cache && (Date.now() - this.cacheTimestamp) < this.cacheTTL) {
      return JSON.parse(JSON.stringify(this.cache)); // Deep clone
    }

    try {
      const content = fs.readFileSync(DB_FILE, 'utf8');
      this.cache = JSON.parse(content);
      this.cacheTimestamp = Date.now();
      
      return JSON.parse(JSON.stringify(this.cache));
    } catch (error) {
      console.error('Error reading database:', error.message);
      throw new Error(`Failed to read database: ${error.message}`);
    }
  }

  /**
   * Write database atomically with exclusive locking
   * Pattern: write to temp → fsync → rename
   */
  write(data) {
    if (this.writeLock) {
      throw new Error('Write operation already in progress');
    }

    this.writeLock = true;
    try {
      this._atomicWrite(data);
      this.cache = JSON.parse(JSON.stringify(data));
      this.cacheTimestamp = Date.now();
    } finally {
      this.writeLock = false;
    }
  }

  /**
   * Atomic write implementation
   */
  _atomicWrite(data) {
    const tempFile = `${DB_FILE}.tmp.${process.pid}.${Date.now()}`;

    try {
      // Write data to temp file
      const content = JSON.stringify(data, null, 2);
      fs.writeFileSync(tempFile, content, 'utf8');

      // Force sync to disk
      const fd = fs.openSync(tempFile, 'r+');
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }

      // Atomic rename
      fs.renameSync(tempFile, DB_FILE);

    } catch (error) {
      // Clean up temp file if it exists
      try {
        if (fs.existsSync(tempFile)) {
          fs.unlinkSync(tempFile);
        }
      } catch (e) {
        // Ignore cleanup errors
      }
      throw error;
    }
  }

  /**
   * Execute operation within a transaction-like context
   * Provides optimistic concurrency control
   */
  async transaction(operation) {
    const maxRetries = 3;
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const db = this.read();
        const result = await operation(db);
        
        if (result !== false) {
          this.write(db);
          return result;
        }
        return null;
      } catch (error) {
        lastError = error;
        if (attempt < maxRetries) {
          // Brief delay before retry
          await new Promise(resolve => setTimeout(resolve, 10 * attempt));
        }
      }
    }

    throw lastError;
  }

  /**
   * Get next ID for a collection
   */
  getNextId(collection) {
    const db = this.read();
    const key = `next${collection.charAt(0).toUpperCase()}${collection.slice(1)}Id`;
    const id = db.meta[key] || 1;
    db.meta[key] = id + 1;
    this.write(db);
    return id;
  }

  /**
   * Find product by article (exact match)
   */
  findProductByArticle(article) {
    const db = this.read();
    return db.products.find(p => p.article === article) || null;
  }

  /**
   * Find product by ID
   */
  findProductById(id) {
    const db = this.read();
    return db.products.find(p => p.id === parseInt(id)) || null;
  }

  /**
   * Search products with case-insensitive partial matching
   */
  searchProducts(query, limit = 100, offset = 0) {
    const db = this.read();
    const searchTerm = query.toLowerCase();
    
    const matches = db.products.filter(p => 
      (p.name && p.name.toLowerCase().includes(searchTerm)) ||
      (p.model && p.model.toLowerCase().includes(searchTerm)) ||
      (p.article && p.article.toLowerCase().includes(searchTerm))
    );

    // Simple relevance scoring
    const scored = matches.map(p => ({
      ...p,
      relevance: this._calculateRelevance(p, searchTerm)
    })).sort((a, b) => {
      if (b.relevance !== a.relevance) return b.relevance - a.relevance;
      if (b.quantity !== a.quantity) return b.quantity - a.quantity;
      return (a.name || '').localeCompare(b.name || '');
    });

    const total = scored.length;
    const paginated = scored.slice(offset, offset + limit);

    return {
      products: paginated,
      total,
      page: Math.floor(offset / limit) + 1,
      limit,
      totalPages: Math.ceil(total / limit),
      hasMore: offset + limit < total
    };
  }

  /**
   * Calculate simple relevance score
   */
  _calculateRelevance(product, searchTerm) {
    let score = 0;
    const name = (product.name || '').toLowerCase();
    const model = (product.model || '').toLowerCase();
    const article = (product.article || '').toLowerCase();

    // Exact startsWith gets highest score
    if (name.startsWith(searchTerm)) score += 10;
    if (model.startsWith(searchTerm)) score += 10;
    if (article.startsWith(searchTerm)) score += 10;

    // Contains gets medium score
    if (name.includes(searchTerm)) score += 5;
    if (model.includes(searchTerm)) score += 5;
    if (article.includes(searchTerm)) score += 5;

    return score;
  }

  /**
   * Get all products with optional filtering
   */
  getProducts(options = {}) {
    const db = this.read();
    let results = [...db.products];

    if (options.lowStock === 'true') {
      results = results.filter(p => p.quantity <= 5);
    }

    if (options.category) {
      // Category filtering would require category field
      // For now, skip
    }

    // Sorting
    const sortBy = options.sortBy || 'name';
    const sortOrder = (options.sortOrder || 'ASC').toUpperCase();
    
    results.sort((a, b) => {
      let aVal = a[sortBy];
      let bVal = b[sortBy];
      
      if (aVal === undefined || aVal === null) aVal = '';
      if (bVal === undefined || bVal === null) bVal = '';

      const comparison = typeof aVal === 'number' 
        ? aVal - bVal 
        : String(aVal).localeCompare(String(bVal));

      return sortOrder === 'DESC' ? -comparison : comparison;
    });

    const total = results.length;
    const limit = parseInt(options.limit) || 20;
    const offset = parseInt(options.offset) || 0;
    const paginated = results.slice(offset, offset + limit);

    return {
      products: paginated,
      pagination: {
        page: Math.floor(offset / limit) + 1,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      }
    };
  }

  /**
   * Create a new product
   */
  createProduct(productData) {
    return this.transaction((db) => {
      const id = db.meta.nextProductId++;
      const now = new Date().toISOString();
      
      const newProduct = {
        id,
        name: productData.name,
        model: productData.model || null,
        article: productData.article,
        quantity: parseInt(productData.quantity) || 0,
        created_at: now,
        updated_at: now
      };

      db.products.push(newProduct);
      return newProduct;
    });
  }

  /**
   * Update an existing product
   */
  updateProduct(id, updates) {
    return this.transaction((db) => {
      const index = db.products.findIndex(p => p.id === parseInt(id));
      if (index === -1) return null;

      const product = db.products[index];
      const now = new Date().toISOString();

      if (updates.name !== undefined) product.name = updates.name;
      if (updates.model !== undefined) product.model = updates.model;
      if (updates.article !== undefined) product.article = updates.article;
      if (updates.quantity !== undefined) product.quantity = parseInt(updates.quantity);
      
      product.updated_at = now;
      db.products[index] = product;

      return product;
    });
  }

  /**
   * Delete a product
   */
  deleteProduct(id) {
    return this.transaction((db) => {
      const index = db.products.findIndex(p => p.id === parseInt(id));
      if (index === -1) return null;

      const deleted = db.products.splice(index, 1)[0];
      return deleted;
    });
  }

  /**
   * Adjust product stock
   */
  adjustStock(productId, quantityChange, transactionType, notes = null, createdBy = 'system') {
    return this.transaction((db) => {
      const productIndex = db.products.findIndex(p => p.id === parseInt(productId));
      if (productIndex === -1) {
        throw new Error(`Product ${productId} not found`);
      }

      const product = db.products[productIndex];
      const previousQuantity = product.quantity;
      const newQuantity = previousQuantity + quantityChange;

      product.quantity = newQuantity;
      product.updated_at = new Date().toISOString();
      db.products[productIndex] = product;

      // Create transaction record
      const txId = db.meta.nextTxId++;
      const now = new Date().toISOString();
      
      const transaction = {
        id: txId,
        product_id: productId,
        transaction_type: transactionType,
        quantity_change: quantityChange,
        previous_quantity: previousQuantity,
        new_quantity: newQuantity,
        reference_type: 'manual',
        reference_id: null,
        notes,
        created_by: createdBy,
        created_at: now
      };

      db.transactions.push(transaction);

      return {
        productId,
        previousQuantity,
        newQuantity,
        quantityChange,
        transaction
      };
    });
  }

  /**
   * Find order by order number
   */
  findOrderByNumber(orderNumber) {
    const db = this.read();
    return db.orders.find(o => o.order_number === orderNumber) || null;
  }

  /**
   * Find order by ID
   */
  findOrderById(id) {
    const db = this.read();
    return db.orders.find(o => o.id === parseInt(id)) || null;
  }

  /**
   * Get all orders with filtering
   */
  getOrders(options = {}) {
    const db = this.read();
    let results = [...db.orders];

    if (options.status) {
      results = results.filter(o => o.status === options.status);
    }

    if (options.source) {
      results = results.filter(o => o.source === options.source);
    }

    if (options.dateFrom) {
      results = results.filter(o => o.imported_at >= options.dateFrom);
    }

    if (options.dateTo) {
      results = results.filter(o => o.imported_at <= options.dateTo);
    }

    if (options.orderNumber) {
      const term = options.orderNumber.toLowerCase();
      results = results.filter(o => o.order_number.toLowerCase().includes(term));
    }

    // Sort by imported_at DESC
    results.sort((a, b) => new Date(b.imported_at) - new Date(a.imported_at));

    const total = results.length;
    const limit = parseInt(options.limit) || 20;
    const offset = parseInt(options.offset) || 0;
    const paginated = results.slice(offset, offset + limit);

    // Fetch items for each order
    const ordersWithItems = paginated.map(order => {
      const items = db.order_items.filter(oi => oi.order_id === order.id);
      return { ...order, items };
    });

    return {
      orders: ordersWithItems,
      pagination: {
        page: Math.floor(offset / limit) + 1,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      }
    };
  }

  /**
   * Create order and process items
   */
  createOrder(orderData) {
    return this.transaction((db) => {
      // Check for duplicate
      if (db.orders.some(o => o.order_number === orderData.order_number)) {
        return { success: false, error: 'Order already exists' };
      }

      const orderId = db.meta.nextOrderId++;
      const now = new Date().toISOString();

      const order = {
        id: orderId,
        order_number: orderData.order_number,
        source: orderData.source || 'PDF',
        status: orderData.status || 'completed',
        total_items: orderData.items?.length || 0,
        imported_at: now,
        processed_at: orderData.status === 'completed' ? now : null,
        metadata: orderData
      };

      db.orders.push(order);

      let newProducts = 0;
      let updatedProducts = 0;

      // Process items
      for (const item of (orderData.items || [])) {
        const normalizedArticle = this._normalizeArticle(item.article);
        
        // Find or create product
        let product = db.products.find(p => p.article === normalizedArticle);
        let isNewProduct = false;

        if (!product) {
          const productId = db.meta.nextProductId++;
          product = {
            id: productId,
            name: item.product || `${item.model || ''} ${normalizedArticle || ''}`.trim(),
            model: item.model || null,
            article: normalizedArticle,
            quantity: 0,
            created_at: now,
            updated_at: now
          };
          db.products.push(product);
          isNewProduct = true;
          newProducts++;
        } else {
          updatedProducts++;
        }

        // Deduct stock
        const previousQuantity = product.quantity;
        product.quantity -= item.quantity;
        product.updated_at = now;

        // Create transaction
        const txId = db.meta.nextTxId++;
        db.transactions.push({
          id: txId,
          product_id: product.id,
          transaction_type: 'stock_out',
          quantity_change: item.quantity,
          previous_quantity: previousQuantity,
          new_quantity: product.quantity,
          reference_type: 'order',
          reference_id: orderId,
          notes: `Order: ${orderData.order_number}`,
          created_at: now
        });

        // Create order item
        const orderItemId = db.meta.nextOrderItemId++;
        db.order_items.push({
          id: orderItemId,
          order_id: orderId,
          product_name: item.product,
          product_model: item.model,
          product_article: normalizedArticle,
          quantity: item.quantity,
          product_id: product.id,
          created_at: now
        });
      }

      return {
        success: true,
        orderId,
        itemCount: orderData.items?.length || 0,
        newProducts,
        updatedProducts
      };
    });
  }

  /**
   * Normalize article number
   */
  _normalizeArticle(article) {
    if (!article) return null;
    return article.trim().replace(/\s+/g, ' ');
  }

  /**
   * Update order status
   */
  updateOrderStatus(id, status) {
    return this.transaction((db) => {
      const index = db.orders.findIndex(o => o.id === parseInt(id));
      if (index === -1) return null;

      const order = db.orders[index];
      order.status = status;
      if (status === 'completed' && !order.processed_at) {
        order.processed_at = new Date().toISOString();
      }

      db.orders[index] = order;
      return order;
    });
  }

  /**
   * Delete order
   */
  deleteOrder(id, restoreStock = false) {
    return this.transaction((db) => {
      const index = db.orders.findIndex(o => o.id === parseInt(id));
      if (index === -1) return null;

      const order = db.orders[index];

      if (restoreStock) {
        // Restore stock for each item
        const items = db.order_items.filter(oi => oi.order_id === order.id);
        for (const item of items) {
          if (item.product_id) {
            const productIndex = db.products.findIndex(p => p.id === item.product_id);
            if (productIndex !== -1) {
              const product = db.products[productIndex];
              const previousQuantity = product.quantity;
              product.quantity += item.quantity;
              product.updated_at = new Date().toISOString();

              // Create adjustment transaction
              const txId = db.meta.nextTxId++;
              db.transactions.push({
                id: txId,
                product_id: product.id,
                transaction_type: 'adjustment',
                quantity_change: item.quantity,
                previous_quantity: previousQuantity,
                new_quantity: product.quantity,
                reference_type: 'manual',
                reference_id: null,
                notes: `Stock restored from cancelled order ${id}`,
                created_at: new Date().toISOString()
              });
            }
          }
        }
      }

      // Remove order items
      db.order_items = db.order_items.filter(oi => oi.order_id !== order.id);

      // Remove order
      db.orders.splice(index, 1);

      return order;
    });
  }

  /**
   * Get transactions for a product
   */
  getProductTransactions(productId, limit = 50) {
    const db = this.read();
    const transactions = db.transactions
      .filter(t => t.product_id === parseInt(productId))
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
      .slice(0, limit);

    // Enrich with product info
    return transactions.map(t => {
      const product = db.products.find(p => p.id === t.product_id);
      return {
        ...t,
        product_name: product?.name,
        product_article: product?.article
      };
    });
  }

  /**
   * Get dashboard analytics data
   */
  getDashboardData() {
    const db = this.read();
    const now = new Date();

    // Stock summary
    const totalProducts = db.products.length;
    const totalStock = db.products.reduce((sum, p) => sum + (p.quantity || 0), 0);
    const avgStock = totalProducts > 0 ? (totalStock / totalProducts).toFixed(2) : 0;
    const lowStockCount = db.products.filter(p => p.quantity <= 5).length;
    const outOfStockCount = db.products.filter(p => p.quantity === 0).length;
    const minStock = db.products.length > 0 ? Math.min(...db.products.map(p => p.quantity)) : 0;
    const maxStock = db.products.length > 0 ? Math.max(...db.products.map(p => p.quantity)) : 0;

    // Order summary
    const totalOrders = db.orders.length;
    const totalItems = db.orders.reduce((sum, o) => sum + (o.total_items || 0), 0);
    const completedOrders = db.orders.filter(o => o.status === 'completed').length;
    const pendingOrders = db.orders.filter(o => o.status === 'pending').length;

    // Recent transactions (last 24h)
    const dayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
    const recentTransactions = db.transactions.filter(t => t.created_at >= dayAgo).length;

    return {
      stockSummary: {
        total_products: totalProducts,
        total_stock: totalStock,
        avg_stock: parseFloat(avgStock),
        low_stock_count: lowStockCount,
        out_of_stock_count: outOfStockCount,
        min_stock: minStock,
        max_stock: maxStock
      },
      orderSummary: {
        total_orders: totalOrders,
        total_items: totalItems,
        completed_orders: completedOrders,
        pending_orders: pendingOrders
      },
      recentTransactions
    };
  }

  /**
   * Get stock trends
   */
  getStockTrends(days = 30) {
    const db = this.read();
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const trends = {};
    
    db.transactions
      .filter(t => t.created_at >= cutoff)
      .forEach(t => {
        const date = t.created_at.split('T')[0];
        if (!trends[date]) {
          trends[date] = { date, stock_in: 0, stock_out: 0, net_change: 0 };
        }

        if (t.transaction_type === 'stock_in') {
          trends[date].stock_in += t.quantity_change;
          trends[date].net_change += t.quantity_change;
        } else if (t.transaction_type === 'stock_out') {
          trends[date].stock_out += t.quantity_change;
          trends[date].net_change -= t.quantity_change;
        } else if (['adjustment', 'audit'].includes(t.transaction_type)) {
          trends[date].net_change += t.quantity_change;
        }
      });

    return Object.values(trends).sort((a, b) => a.date.localeCompare(b.date));
  }

  /**
   * Get order trends
   */
  getOrderTrends(days = 30, granularity = 'day') {
    const db = this.read();
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const trends = {};

    db.orders
      .filter(o => o.imported_at >= cutoff)
      .forEach(o => {
        let period;
        const date = new Date(o.imported_at);

        if (granularity === 'hour') {
          period = `${o.imported_at.split('T')[0]} ${String(date.getHours()).padStart(2, '0')}`;
        } else if (granularity === 'week') {
          // Simplified week grouping
          const weekStart = new Date(date);
          weekStart.setDate(date.getDate() - date.getDay());
          period = weekStart.toISOString().split('T')[0];
        } else if (granularity === 'month') {
          period = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
        } else {
          period = o.imported_at.split('T')[0];
        }

        if (!trends[period]) {
          trends[period] = { 
            period, 
            order_count: 0, 
            items_count: 0, 
            completed_count: 0, 
            pending_count: 0 
          };
        }

        trends[period].order_count++;
        trends[period].items_count += o.total_items || 0;
        if (o.status === 'completed') trends[period].completed_count++;
        if (o.status === 'pending') trends[period].pending_count++;
      });

    return Object.values(trends).sort((a, b) => a.period.localeCompare(b.period));
  }

  /**
   * Get stock distribution
   */
  getStockDistribution() {
    const db = this.read();

    const distribution = {
      'Out of Stock': { stock_level: 'Out of Stock', product_count: 0, total_quantity: 0 },
      'Low (1-5)': { stock_level: 'Low (1-5)', product_count: 0, total_quantity: 0 },
      'Medium (6-20)': { stock_level: 'Medium (6-20)', product_count: 0, total_quantity: 0 },
      'Good (21-50)': { stock_level: 'Good (21-50)', product_count: 0, total_quantity: 0 },
      'High (50+)': { stock_level: 'High (50+)', product_count: 0, total_quantity: 0 }
    };

    db.products.forEach(p => {
      const qty = p.quantity || 0;
      let category;

      if (qty === 0) category = 'Out of Stock';
      else if (qty <= 5) category = 'Low (1-5)';
      else if (qty <= 20) category = 'Medium (6-20)';
      else if (qty <= 50) category = 'Good (21-50)';
      else category = 'High (50+)';

      distribution[category].product_count++;
      distribution[category].total_quantity += qty;
    });

    return Object.values(distribution);
  }

  /**
   * Get top products
   */
  getTopProducts(limit = 10, metric = 'quantity') {
    const db = this.read();
    let products;

    if (metric === 'transactions') {
      const txCount = {};
      db.transactions.forEach(t => {
        txCount[t.product_id] = (txCount[t.product_id] || 0) + 1;
      });

      products = db.products.map(p => ({
        ...p,
        transaction_count: txCount[p.id] || 0
      })).sort((a, b) => b.transaction_count - a.transaction_count);
    } else if (metric === 'orders') {
      const orderCount = {};
      db.order_items.forEach(oi => {
        orderCount[oi.product_id] = (orderCount[oi.product_id] || 0) + 1;
      });

      products = db.products.map(p => ({
        ...p,
        order_count: orderCount[p.id] || 0
      })).sort((a, b) => b.order_count - a.order_count);
    } else {
      products = [...db.products].sort((a, b) => b.quantity - a.quantity);
    }

    return products.slice(0, parseInt(limit));
  }

  /**
   * Get transaction types breakdown
   */
  getTransactionTypes(days = 30) {
    const db = this.read();
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const types = {};

    db.transactions
      .filter(t => t.created_at >= cutoff)
      .forEach(t => {
        if (!types[t.transaction_type]) {
          types[t.transaction_type] = {
            transaction_type: t.transaction_type,
            count: 0,
            total_quantity: 0,
            quantities: []
          };
        }
        types[t.transaction_type].count++;
        types[t.transaction_type].total_quantity += t.quantity_change;
        types[t.transaction_type].quantities.push(t.quantity_change);
      });

    return Object.values(types).map(t => ({
      ...t,
      avg_quantity: t.count > 0 ? (t.total_quantity / t.count).toFixed(2) : 0
    })).sort((a, b) => b.count - a.count);
  }

  /**
   * Get daily order summary
   */
  getDailySummary(days = 90) {
    const db = this.read();
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const summary = {};

    db.orders
      .filter(o => o.imported_at >= cutoff)
      .forEach(o => {
        const date = o.imported_at.split('T')[0];
        if (!summary[date]) {
          summary[date] = { date, orders: 0, items: 0, completed: 0, cancelled: 0 };
        }
        summary[date].orders++;
        summary[date].items += o.total_items || 0;
        if (o.status === 'completed') summary[date].completed++;
        if (o.status === 'cancelled') summary[date].cancelled++;
      });

    return Object.values(summary).sort((a, b) => b.date.localeCompare(a.date));
  }

  /**
   * Get low stock products
   */
  getLowStockProducts(threshold = 5) {
    const db = this.read();
    return db.products
      .filter(p => p.quantity <= threshold)
      .sort((a, b) => a.quantity - b.quantity);
  }

  /**
   * Backup database
   */
  backup() {
    const db = this.read();
    const backupFile = `${DB_FILE}.backup.${Date.now()}`;
    fs.writeFileSync(backupFile, JSON.stringify(db, null, 2));
    return backupFile;
  }

  /**
   * Restore from backup
   */
  restore(backupFile) {
    const content = fs.readFileSync(backupFile, 'utf8');
    const data = JSON.parse(content);
    this._atomicWrite(data);
    return true;
  }
}

// Export singleton instance
export default new JsonDatabase();
