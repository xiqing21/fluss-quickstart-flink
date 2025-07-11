-- 创建目标数据库的表结构
CREATE SCHEMA IF NOT EXISTS result;

-- 创建最终的订单宽表（包含用户信息）
CREATE TABLE result.orders_with_user_info (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    username VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(20),
    city VARCHAR(50),
    product_name VARCHAR(200) NOT NULL,
    product_category VARCHAR(100),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    order_status VARCHAR(20),
    order_time TIMESTAMP,
    user_register_time TIMESTAMP,
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建更新触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为表添加更新时间触发器
CREATE TRIGGER update_orders_with_user_info_updated_at 
    BEFORE UPDATE ON result.orders_with_user_info
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 创建索引以提高查询性能
CREATE INDEX idx_orders_user_id ON result.orders_with_user_info(user_id);
CREATE INDEX idx_orders_status ON result.orders_with_user_info(order_status);
CREATE INDEX idx_orders_category ON result.orders_with_user_info(product_category);
CREATE INDEX idx_orders_time ON result.orders_with_user_info(order_time);
CREATE INDEX idx_orders_city ON result.orders_with_user_info(city); 