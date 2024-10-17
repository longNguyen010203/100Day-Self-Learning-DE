-------------------------
-- Union and Union All --
-------------------------

-- Union
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'BUY'
UNION
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'SELL';


SELECT * FROM stock.stocktrade 
WHERE tradeType = 'BUY'
UNION
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'SELL'
UNION
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'B';


SELECT * FROM stock.stocktrade 
WHERE tradeType = 'BUY'
UNION
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'SELL'
UNION
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'B'
UNION
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'SELL';


-- Union All
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'BUY'
UNION ALL
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'SELL'
UNION ALL
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'B'
UNION ALL
SELECT * FROM stock.stocktrade 
WHERE tradeType = 'SELL';