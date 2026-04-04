-- WMS Database Schema
-- PostgreSQL 15+ with pg_trgm extension for high-performance search

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- PRODUCTS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    model VARCHAR(200),
    article VARCHAR(200) NOT NULL,
    quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT unique_article UNIQUE (article)
);

-- Index for fast ILIKE searches on name, model, and article
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_products_model_trgm ON products USING gin (model gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_products_article_trgm ON products USING gin (article gin_trgm_ops);

-- Composite index for combined searches
CREATE INDEX IF NOT EXISTS idx_products_search_combined ON products USING gin (
    (name || ' ' || COALESCE(model, '') || ' ' || article) gin_trgm_ops
);

-- Index for exact article lookups
CREATE INDEX IF NOT EXISTS idx_products_article_exact ON products (article);

-- ============================================
-- ORDERS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(100) NOT NULL,
    source VARCHAR(50) DEFAULT 'PDF', -- PDF, OZON, WB, MANUAL
    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, cancelled
    total_items INTEGER DEFAULT 0,
    imported_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITH TIME ZONE,
    metadata JSONB,
    
    CONSTRAINT unique_order_number UNIQUE (order_number)
);

CREATE INDEX IF NOT EXISTS idx_orders_order_number ON orders (order_number);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_imported_at ON orders (imported_at);

-- ============================================
-- ORDER ITEMS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    product_name VARCHAR(500),
    product_model VARCHAR(200),
    product_article VARCHAR(200),
    quantity INTEGER NOT NULL DEFAULT 1,
    product_id INTEGER REFERENCES products(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_article ON order_items (product_article);

-- ============================================
-- TRANSACTIONS TABLE (Stock Movement Log)
-- ============================================
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    transaction_type VARCHAR(50) NOT NULL, -- stock_in, stock_out, adjustment, audit
    quantity_change INTEGER NOT NULL,
    previous_quantity INTEGER,
    new_quantity INTEGER,
    reference_type VARCHAR(50), -- order, manual, audit
    reference_id INTEGER,
    notes TEXT,
    created_by VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_product_id ON transactions (product_id);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions (transaction_type);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_reference ON transactions (reference_type, reference_id);

-- ============================================
-- AUDIT LOG TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    record_id INTEGER,
    action VARCHAR(50), -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_log_table_record ON audit_log (table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log (changed_at);

-- ============================================
-- FUNCTIONS AND TRIGGERS
-- ============================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for products table
DROP TRIGGER IF EXISTS update_products_updated_at ON products;
CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Function to create audit log entries
CREATE OR REPLACE FUNCTION create_audit_log_entry()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, record_id, action, new_values, changed_at)
        VALUES (TG_TABLE_NAME, NEW.id, 'INSERT', to_jsonb(NEW), CURRENT_TIMESTAMP);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, new_values, changed_at)
        VALUES (TG_TABLE_NAME, NEW.id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW), CURRENT_TIMESTAMP);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, record_id, action, old_values, changed_at)
        VALUES (TG_TABLE_NAME, OLD.id, 'DELETE', to_jsonb(OLD), CURRENT_TIMESTAMP);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ language 'plpgsql';

-- Apply audit triggers (uncomment if needed)
-- CREATE TRIGGER products_audit_trigger
--     AFTER INSERT OR UPDATE OR DELETE ON products
--     FOR EACH ROW EXECUTE FUNCTION create_audit_log_entry();

-- ============================================
-- VIEWS FOR ANALYTICS
-- ============================================

-- Low stock products view
CREATE OR REPLACE VIEW low_stock_products AS
SELECT * FROM products
WHERE quantity <= 5
ORDER BY quantity ASC;

-- Daily order summary view
CREATE OR REPLACE VIEW daily_order_summary AS
SELECT 
    DATE(imported_at) as order_date,
    COUNT(*) as total_orders,
    SUM(total_items) as total_items,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders
FROM orders
GROUP BY DATE(imported_at)
ORDER BY order_date DESC;

-- Stock movement summary view
CREATE OR REPLACE VIEW stock_movement_summary AS
SELECT 
    p.id as product_id,
    p.name as product_name,
    p.article as product_article,
    p.quantity as current_stock,
    COALESCE(SUM(CASE WHEN t.transaction_type = 'stock_in' THEN t.quantity_change ELSE 0 END), 0) as total_stock_in,
    COALESCE(SUM(CASE WHEN t.transaction_type = 'stock_out' THEN t.quantity_change ELSE 0 END), 0) as total_stock_out,
    COUNT(t.id) as transaction_count
FROM products p
LEFT JOIN transactions t ON p.id = t.product_id
GROUP BY p.id, p.name, p.article, p.quantity
ORDER BY p.name;

-- ============================================
-- INITIAL DATA (Optional Seed)
-- ============================================

-- Sample products for testing
INSERT INTO products (name, model, article, quantity) VALUES
('Смартфон Apple iPhone 15 Pro Max', 'iPhone 15 Pro Max', 'APL-IP15PM-256', 50),
('Ноутбук MacBook Pro 16"', 'MacBook Pro 16 M3', 'APL-MBP16-M3', 25),
('Наушники Sony WH-1000XM5', 'WH-1000XM5', 'SNY-WH1000XM5-BLK', 100),
('Планшет Samsung Galaxy Tab S9', 'Galaxy Tab S9', 'SMS-TABS9-128', 75),
('Умные часы Apple Watch Series 9', 'Watch Series 9', 'APL-AWS9-45MM', 60)
ON CONFLICT (article) DO NOTHING;
