CREATE TABLE total_quantity (
    total_quantity BIGINT,
    dummy_id INT PRIMARY KEY
);
CREATE TABLE product_revenue (
    product_id INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    product_revenue FLOAT,
    PRIMARY KEY (product_id, window_start)
);
CREATE TABLE store_revenue (
    store_id INT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    store_revenue FLOAT,
    PRIMARY KEY (store_id, window_start)
);

CREATE TABLE total_revenue (
    total_revenue FLOAT,
    dummy_id INT PRIMARY KEY
);

-- Validate Total Revenue
SELECT * FROM total_revenue;

-- Validate Total Quantity
SELECT * FROM total_quantity;

-- Validate Revenue per Product
SELECT * FROM product_revenue ORDER BY window_start DESC;

-- Validate Revenue per Store
SELECT * FROM store_revenue ORDER BY window_start DESC;

-- Sum of product revenues
SELECT SUM(product_revenue) AS sum_product_revenue
FROM product_revenue;

-- Total revenue
SELECT total_revenue FROM total_revenue;

-- Sum of store revenues
SELECT SUM(store_revenue) AS sum_store_revenue
FROM store_revenue;

-- Total revenue
SELECT total_revenue FROM total_revenue;