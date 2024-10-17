------------------------------
-- Create and Drop Database --
------------------------------
DROP DATABASE IF EXISTS warehouse;
CREATE DATABASE warehouse;


----------------------------
-- Create and Drop Schema --
----------------------------
DROP SCHEMA IF EXISTS stock;
CREATE SCHEMA IF NOT EXISTS stock;


---------------------------
-- Create and Drop Table --
---------------------------
DROP TABLE IF EXISTS stock.stocktrade;
CREATE TABLE IF NOT EXISTS stock.stocktrade (
    transaction_id SERIAL UNIQUE NOT NULL,
    tickerSymbol VARCHAR(5) NOT NULL,
    tradeType VARCHAR(4) NOT NULL,
    price REAL CHECK (price > 0) NOT NULL,
    quantity INTEGER CHECK (quantity > 0) NOT NULL,
    trading_platform VARCHAR(3) DEFAULT 'XTB' NOT NULL,
    CHECK (price > quantity),
    PRIMARY KEY (transaction_id),
    FOREIGN KEY (tickerSymbol) REFERENCES stock.stockinfo (tickerSymbol)
);

DROP TABLE IF EXISTS stock.stockinfo;
CREATE TABLE IF NOT EXISTS stock.stockinfo (
    tickerSymbol VARCHAR(5) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (tickerSymbol)
);


---------------------
-- Adding a Column --
---------------------
ALTER TABLE stock.stocktrade
ADD COLUMN description TEXT CHECK(description <> '');

-----------------------
-- removing a Column --
-----------------------
ALTER TABLE stock.stocktrade
DROP COLUMN description CASCADE;

-------------------------
-- Adding a Constraint --
-------------------------
ALTER TABLE stock.stocktrade 
ADD CHECK (LENGTH(tickerSymbol) = 3),
ADD CHECK (tradeType IN ('BUY', 'SELL'));

---------------------------
-- Removing a Constraint --
---------------------------
ALTER TABLE stock.stocktrade 
DROP CONSTRAINT stocktrade_tradetype_check;