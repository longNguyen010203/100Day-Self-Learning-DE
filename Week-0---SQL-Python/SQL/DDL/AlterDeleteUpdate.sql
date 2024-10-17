-- Truncate Table
TRUNCATE stock.stockinfo CASCADE;
TRUNCATE stock.stocktrade CASCADE;


-- Alter Table
-- Adding a Column
ALTER TABLE stock.stocktrade
ADD COLUMN des