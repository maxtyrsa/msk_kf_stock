-- Additional optimization indexes for high-performance search

-- ============================================
-- FULL TEXT SEARCH INDEXES (Alternative to pg_trgm)
-- ============================================

-- Add a searchable text column for full-text search
ALTER TABLE products ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Create function to update search vector
CREATE OR REPLACE FUNCTION update_product_search_vector()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector := 
        setweight(to_tsvector('russian', COALESCE(NEW.name, '')), 'A') ||
        setweight(to_tsvector('simple', COALESCE(NEW.model, '')), 'B') ||
        setweight(to_tsvector('simple', COALESCE(NEW.article, '')), 'C');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to maintain search vector
DROP TRIGGER IF EXISTS update_product_search_vector ON products;
CREATE TRIGGER update_product_search_vector
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_product_search_vector();

-- GIN index for full-text search
CREATE INDEX IF NOT EXISTS idx_products_search_vector ON products USING gin (search_vector);

-- ============================================
-- COMPOSITE INDEXES FOR COMMON QUERIES
-- ============================================

-- Index for stock level queries
CREATE INDEX IF NOT EXISTS idx_products_quantity ON products (quantity);

-- Index for low stock alerts
CREATE INDEX IF NOT EXISTS idx_products_low_stock ON products (quantity) WHERE quantity <= 5;

-- Composite index for article + quantity lookups
CREATE INDEX IF NOT EXISTS idx_products_article_quantity ON products (article, quantity);

-- ============================================
-- ORDER PERFORMANCE INDEXES
-- ============================================

-- Partial index for pending orders
CREATE INDEX IF NOT EXISTS idx_orders_pending ON orders (imported_at) WHERE status = 'pending';

-- Composite index for order date range queries
CREATE INDEX IF NOT EXISTS idx_orders_date_status ON orders (imported_at, status);

-- ============================================
-- TRANSACTION ANALYTICS INDEXES
-- ============================================

-- Index for product transaction history
CREATE INDEX IF NOT EXISTS idx_transactions_product_date ON transactions (product_id, created_at DESC);

-- Index for transaction type filtering
CREATE INDEX IF NOT EXISTS idx_transactions_type_date ON transactions (transaction_type, created_at DESC);

-- ============================================
-- STATISTICS UPDATE
-- ============================================

-- Analyze tables for query planner optimization
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;
ANALYZE transactions;
