----------------------
-- Stored Procedure --
----------------------

-- Drop Stored Procedure
DROP PROCEDURE IF EXISTS transfer_quantity;

-- Create Stored Procedure
CREATE OR REPLACE PROCEDURE transfer_quantity(
    IN from_account INT, 
    IN to_account INT, 
    IN amount NUMERIC)
LANGUAGE plpgsql AS $$
BEGIN
    -- Trừ tiền từ tài khoản nguồn
    UPDATE stock.stocktrade 
    SET quantity = quantity - amount 
    WHERE transaction_id = from_account;

    -- Cộng tiền vào tài khoản đích
    UPDATE stock.stocktrade 
    SET quantity = quantity + amount 
    WHERE transaction_id = to_account;

    -- Kiểm tra số dư
    IF (SELECT quantity 
        FROM stock.stocktrade 
        WHERE transaction_id = from_account) < amount THEN
        RAISE EXCEPTION 'Số lượng chứng khoán không đủ';
    END IF;
END;
$$;

CALL transfer_quantity(1, 2, 100);



CREATE OR REPLACE PROCEDURE transfer_with_transaction(
    IN from_account INT, 
    IN to_account INT, 
    IN amount NUMERIC)
LANGUAGE plpgsql AS $$
BEGIN
    -- Thực hiện các cập nhật
    UPDATE stock.stocktrade SET price = price - amount WHERE transaction_id = from_account;
    UPDATE stock.stocktrade SET price = price + amount WHERE transaction_id = to_account;

    -- Kiểm tra và hủy bỏ transaction nếu cần
    IF (SELECT price FROM stock.stocktrade WHERE transaction_id = from_account) < amount THEN
        ROLLBACK;
        RAISE EXCEPTION 'Số dư tài khoản không đủ';
    END IF;

    -- Kết thúc transaction nếu không có lỗi
    COMMIT;
END;
$$;

CALL transfer_with_transaction(1, 2, 5933);

