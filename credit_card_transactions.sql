DROP TABLE IF EXISTS public.credit_card_transactions;

CREATE TABLE IF NOT EXISTS public.credit_card_transactions
(
    user_id INTEGER,
    "timestamp" TIMESTAMP with time zone,
	event_type TEXT,
	product_id INTEGER,
	price FLOAT
);

COPY credit_card_transactions(user_id, timestamp, event_type, product_id, price)
FROM '/Users/LeonardoGiovanini/GCP Portfolio/user_transactions_optimized_with_products.csv'
DELIMITER ','
CSV HEADER;

SELECT COUNT(*) FROM credit_card_transactions;