--- Insert into data to TABLE
INSERT INTO stock.stocktrade (
    -- transaction_id, 
    tickerSymbol, 
    tradeType, 
    price, 
    quantity
    -- trading_platform
)
VALUES ('NGK', 'BUY', '12933.6', 345),
       ('NTT', 'SELL', '1232.6', 345),
       ('LTO', 'SELL', '6343.6', 654),
       ('NVN', 'BUY', '923743.6', 453),
       ('NTT', 'SELL', '348343.6', 824),
       ('NNQ', 'BUY', '127243.6', 367),
       ('NNQ', 'SELL', '64343.6', 357),
       ('VTD', 'BUY', '12343.6', 572),
       ('NTT', 'SELL', '12343.6', 7367),
       ('NVN', 'BUY', '12343.6', 345),
       ('NTL', 'SELL', '32443.6', 566),
       ('VTD', 'SELL', '12343.6', 954),
       ('NTT', 'BUY', '123422.6', 264),
       ('NGK', 'SELL', '12343.6', 734),
       ('NVN', 'BUY', '136343.6', 264),
       ('NNQ', 'BUY', '12633.6', 345),
       ('NTL', 'SELL', '14543.6', 724),
       ('NNQ', 'BUY', '3453.6', 634);


INSERT INTO stock.stockinfo (
    tickerSymbol, 
    company_name)
VALUES ('NNQ', 'Nguyen Nhu Quynh'),
       ('NTL', 'Nguyen Thanh Long'),
       ('NTT', 'Nguyen Tuan Thanh'),
       ('NGK', 'Nguyen Gia Khanh'),
       ('NVN', 'Nguyen Van Nui'),
       ('VTD', 'Vu Thi Dien'),
       ('LTO', 'Le Tu Oanh'),
       ('LTU', 'Le Tu Uyen');


-- Query data with questions 
SELECT * FROM stock.stockinfo;
SELECT * FROM stock.stocktrade;

SELECT * FROM stock.stocktrade a 
INNER JOIN stock.stockinfo b 
    ON a.tickerSymbol = b.tickerSymbol;

-- The value of each share per buy and sell
SELECT 
    transaction_id, 
    m.tickerSymbol,
    tradeType,
    price,
    quantity,
    ROUND((price / quantity)::numeric, 2) as pricePerShare,
    n.company_name
FROM stock.stocktrade m
INNER JOIN stock.stockinfo n
    ON m.tickerSymbol = n.tickerSymbol;

-- Total number of shares purchased and total number of shares sold
SELECT
    tradeType,
    SUM(quantity) as tradeSum,
    SUM(price) as priceSum
FROM stock.stocktrade m
INNER JOIN stock.stockinfo n
    ON m.tickerSymbol = n.tickerSymbol
GROUP BY tradeType;

-- most bought stocks and most sold stocks.
SELECT
    m.tickerSymbol,
    tradeType,
    COUNT(m.tickerSymbol) as stockCount
FROM stock.stocktrade m
INNER JOIN stock.stockinfo n
    ON m.tickerSymbol = n.tickerSymbol
WHERE tradeType = 'BUY'
GROUP BY m.tickerSymbol, tradeType
HAVING COUNT(m.tickerSymbol) >= 3
ORDER BY stockCount DESC;

-- Get the stock code with the highest number of purchases
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

SELECT 
    m.tickersymbol,
    CASE
        WHEN price < 2000 THEN 'Low'
        WHEN price < 50000 THEN 'Medium'
        ELSE 'High'
    END AS priceCategory,
    NULLIF(price, 12933.6) AS priceValue,
    COALESCE(price, 34548) as bonus_value

FROM stock.stocktrade m INNER JOIN stock.stockinfo n
    ON m.tickerSymbol = n.tickerSymbol;



-------------------------------------------
---      DELETE and UPDATE in Table     ---
---  Returning Data from Modified Rows  ---
-------------------------------------------
UPDATE stock.stocktrade
SET price = price * 2
WHERE price < 10000;
Correlated Subqueries
UPDATE stock.stockinfo
SET company_name = company_name || ' (Fresher Data Engineer)'
WHERE tickersymbol LIKE 'N%';

DELETE FROM stock.stocktrade
WHERE quantity > 5000;

UPDATE stock.stocktrade 
SET price = price * 1.10
WHERE price <= 20000
RETURNING tickersymbol, tradetype, price AS new_price;

UPDATE stock.stockinfo 
SET company_name = company_name
WHERE company_name = company_name
RETURNING *;

UPDATE stock.stocktrade
SET quantity = 500
WHERE tickerSymbol = 'NGK' and tradeType = 'BUY';

UPDATE stock.stocktrade
SET tradetype = 'BUY'
WHERE transaction_id = 7 and tickersymbol = 'NNQ';