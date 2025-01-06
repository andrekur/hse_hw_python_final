CREATE TABLE IF NOT EXISTS "Users" (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(60),
    phone VARCHAR(14),
    registration_date TIMESTAMP,
    loyalty_status VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS "ProductCategories" (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(120) NOT NULL,
    parent_category_id INT
);

CREATE TABLE IF NOT EXISTS "Products" (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT NOT NULL,
    category_id INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INT NOT NULL,
    creation_date TIMESTAMP NOT NULL,
    FOREIGN KEY (category_id) REFERENCES "ProductCategories" (category_id)
);

CREATE TABLE IF NOT EXISTS "Orders" (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    order_date TIMESTAMP NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    delivery_date TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES "Users" (user_id)
);

CREATE TABLE IF NOT EXISTS "OrderDetails" (
    order_detail_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price_per_unit DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES "Orders" (order_id),
    FOREIGN KEY (product_id) REFERENCES "Products" (product_id)
);