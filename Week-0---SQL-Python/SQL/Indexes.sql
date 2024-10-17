-----------------------------
-- Create and Drop Indexes --
-----------------------------

-- Create Index on multi columns
DROP INDEX IF EXISTS stock.idx_stocktrade;
CREATE INDEX idx_stocktrade ON stock.stocktrade (transaction_id, tickerSymbol);

-- Create Index on a column
DROP INDEX IF EXISTS stock.idx_stocktrade;
CREATE INDEX idx_stocktrade ON stock.stocktrade (transaction_id);


-- Check and monitor Index with Query

-- SELECT * FROM stock.stocktrade WHERE transaction_id = 17 and tickerSymbol = 'NTL';
-- SELECT * FROM stock.stocktrade WHERE transaction_id = 17;

EXPLAIN ANALYZE SELECT * FROM stock.stocktrade 
                WHERE transaction_id = 17 
                AND tickerSymbol = 'NTL';

EXPLAIN ANALYZE SELECT * FROM stock.stocktrade WHERE transaction_id = 17;