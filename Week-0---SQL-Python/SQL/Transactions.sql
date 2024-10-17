------------------
-- Transactions --
------------------

DO $$
BEGIN
    BEGIN
    UPDATE stock.stocktrade
    SET price = price - 4343
    WHERE transaction_id = 7;

    UPDATE stock.stocktrade
    SET price = price + 4343
    WHERE transaction_id = 2;

    IF (SELECT price FROM stock.stocktrade WHERE transaction_id = 7) < 0 THEN
        ROLLBACK;
        RAISE EXCEPTION 'price is less than 0';
    ELSE 
        COMMIT;
    END IF;
	END;
END
$$ LANGUAGE plpgsql;


DO $$
BEGIN
    BEGIN
    UPDATE stock.stocktrade
    SET price = price - 15000
    WHERE transaction_id = 17;

    UPDATE stock.stocktrade
    SET price = price + 4343
    WHERE transaction_id = 13;

    IF (SELECT price FROM stock.stocktrade WHERE transaction_id = 17) < 15000 THEN
        ROLLBACK;
        RAISE EXCEPTION 'price is less than 0';
    ELSE 
        COMMIT;
    END IF;
	END;
END
$$ LANGUAGE plpgsql;


DO $$
BEGIN
    BEGIN
    IF (SELECT price FROM stock.stocktrade WHERE transaction_id = 17) < 15000 THEN
        RAISE EXCEPTION 'price is less than 15000';
    ELSE 
        UPDATE stock.stocktrade
        SET price = price - 15000
        WHERE transaction_id = 17;

        UPDATE stock.stocktrade
        SET price = price + 4343
        WHERE transaction_id = 13;
    END IF;
    
    COMMIT;
	END;
END
$$ LANGUAGE plpgsql;


DO $$
BEGIN
    -- Kiểm tra điều kiện
    IF (SELECT price FROM stock.stocktrade WHERE transaction_id = 17) < 15000 THEN
        -- Nếu giá nhỏ hơn 15000, ném ra ngoại lệ
        RAISE EXCEPTION 'price is less than 15000';
    ELSE
        -- Nếu đủ điều kiện, thực hiện các cập nhật
        UPDATE stock.stocktrade
        SET price = price - 15000
        WHERE transaction_id = 17;

        UPDATE stock.stocktrade
        SET price = price + 4343
        WHERE transaction_id = 13;
    END IF;

    -- Hoàn tất transaction
    COMMIT;

END
$$ LANGUAGE plpgsql;


DO $$
DECLARE
    quantity_num INTEGER;
BEGIN
    -- Tạo SAVEPOINT đầu tiên
    SAVEPOINT before_update;

    -- Cập nhật tài khoản A: Trừ 5000 từ tài khoản A
    UPDATE stock.stocktrade
    SET quantity = quantity - 5000
    WHERE transaction_id = 1;

    -- Kiểm tra nếu số dư tài khoản A < 0 thì rollback về savepoint
    IF (SELECT quantity FROM stock.stocktrade WHERE transaction_id = 1) < 5000 THEN
        ROLLBACK TO SAVEPOINT before_update;
        RAISE EXCEPTION 'Không đủ số lượng trong giao dịch A';
    END IF;

    -- Tạo SAVEPOINT thứ hai
    SAVEPOINT after_update_A;

    -- Cập nhật tài khoản B: Cộng 5000 vào tài khoản B
    UPDATE stock.stocktrade
    SET quantity = quantity + 5000
    WHERE transaction_id = 2;

    -- Nếu xảy ra lỗi trong bước này, quay lại SAVEPOINT after_update_A
    SELECT quantity INTO quantity_num FROM stock.stocktrade WHERE transaction_id = 2;
    IF quantity_num < 5000 OR quantity_num IS NULL THEN
        ROLLBACK TO SAVEPOINT after_update_A;
        RAISE EXCEPTION 'Cập nhật Giao dịch B gặp lỗi';
    END IF;

    -- Nếu tất cả các bước trên thành công, hoàn tất transaction
    COMMIT;
END
$$ LANGUAGE plpgsql;

