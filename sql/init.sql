-- Create dimension table for stocks
CREATE TABLE IF NOT EXISTS dim_stock (
    symbol VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create fact table for stock prices
CREATE TABLE IF NOT EXISTS fact_stock_prices (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL REFERENCES dim_stock(symbol),
    price DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    day_high DECIMAL(10,2),
    day_low DECIMAL(10,2),
    change DECIMAL(10,2),
    change_percent DECIMAL(5,2),
    event_time TIMESTAMP NOT NULL,
    processing_time TIMESTAMP NOT NULL,
    load_time TIMESTAMP NOT NULL
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_symbol ON fact_stock_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_fact_event_time ON fact_stock_prices(event_time);
CREATE INDEX IF NOT EXISTS idx_fact_load_time ON fact_stock_prices(load_time);

-- Create composite index for common queries
CREATE INDEX IF NOT EXISTS idx_fact_symbol_time ON fact_stock_prices(symbol, event_time DESC);

-- Create materialized view for daily aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_stock_summary AS
SELECT 
    symbol,
    DATE(event_time) as trade_date,
    COUNT(*) as num_transactions,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price,
    SUM(volume) as total_volume,
    MAX(day_high) as highest_day_high,
    MIN(day_low) as lowest_day_low
FROM fact_stock_prices
GROUP BY symbol, DATE(event_time)
WITH DATA;

-- Create index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_symbol_date 
ON mv_daily_stock_summary(symbol, trade_date);

-- Create function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_daily_summary()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_stock_summary;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to refresh materialized view
DROP TRIGGER IF EXISTS refresh_daily_summary_trigger ON fact_stock_prices;
CREATE TRIGGER refresh_daily_summary_trigger
    AFTER INSERT ON fact_stock_prices
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_daily_summary();

-- Create view for Power BI real-time dashboard
CREATE OR REPLACE VIEW vw_realtime_stock_dashboard AS
SELECT 
    f.*,
    s.company_name,
    s.sector
FROM fact_stock_prices f
JOIN dim_stock s ON f.symbol = s.symbol
WHERE f.event_time >= NOW() - INTERVAL '1 hour'
ORDER BY f.event_time DESC;