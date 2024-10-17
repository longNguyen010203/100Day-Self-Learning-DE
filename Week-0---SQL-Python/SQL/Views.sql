------------------
-- Create Views --
------------------

-- Simple Views
DROP VIEW IF EXISTS stock.pricePerShare;
CREATE OR REPLACE VIEW stock.pricePerShare AS
SELECT 
    transaction_id, 
    tickerSymbol,
    tradeType,
    price,
    quantity,
    ROUND((price / quantity)::numeric, 2) as pricePerShare_1
FROM stock.stocktrade;


-- Complex Views
DROP VIEW IF EXISTS stock.stockHighestPurchases;
CREATE OR REPLACE VIEW stock.stockHighestPurchases AS
SELECT 
    tickerSymbol,
    tradeType,
    SUM(quantity) as totalQuantity
FROM stock.stocktrade
WHERE tradeType = 'BUY'
GROUP BY tickerSymbol, tradeType
HAVING SUM(quantity) >= ALL (
    SELECT SUM(quantity)
    FROM stock.stocktrade
    WHERE tradeType = 'BUY'
    GROUP BY tickerSymbol, tradeType
);


-- Materialized Views
DROP VIEW IF EXISTS stock.stockHighestPurchasesMaterialized;
CREATE MATERIALIZED VIEW stock.stockHighestPurchasesMaterialized AS
SELECT 
    tickerSymbol,
    tradeType,
    SUM(quantity) as totalQuantity
FROM stock.stocktrade
WHERE tradeType = 'BUY'
GROUP BY tickerSymbol, tradeType
HAVING SUM(quantity) >= ALL (
    SELECT SUM(quantity)
    FROM stock.stocktrade
    WHERE tradeType = 'BUY'
    GROUP BY tickerSymbol, tradeType
);

-- Refresh Materialized Views
REFRESH MATERIALIZED VIEW stock.stockHighestPurchasesMaterialized;