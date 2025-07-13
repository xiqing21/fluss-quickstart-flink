-- 创建源数据库的表结构
CREATE SCHEMA IF NOT EXISTS business;

-- 创建用户维表
CREATE TABLE business.users (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    phone VARCHAR(20),
    city VARCHAR(50),
    register_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建订单事实表
CREATE TABLE business.orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    product_category VARCHAR(100),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    order_status VARCHAR(20) DEFAULT 'PENDING',
    order_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试用户数据
INSERT INTO business.users (user_id, username, email, phone, city) VALUES
(1001, '张三', 'zhangsan@example.com', '13812345678', '北京'),
(1002, '李四', 'lisi@example.com', '13987654321', '上海'),
(1003, '王五', 'wangwu@example.com', '13611112222', '广州'),
(1004, '赵六', 'zhaoliu@example.com', '13733334444', '深圳'),
(1005, '钱七', 'qianqi@example.com', '13855556666', '杭州');

-- 插入测试订单数据
INSERT INTO business.orders (order_id, user_id, product_name, product_category, quantity, unit_price, total_amount, order_status) VALUES
(2001, 1001, 'iPhone 15', '电子产品', 1, 5999.00, 5999.00, 'COMPLETED'),
(2002, 1002, 'MacBook Pro', '电子产品', 1, 12999.00, 12999.00, 'PENDING'),
(2003, 1003, '运动鞋', '服装鞋帽', 2, 299.00, 598.00, 'SHIPPED'),
(2004, 1001, '咖啡杯', '家居用品', 3, 89.00, 267.00, 'COMPLETED'),
(2005, 1004, '笔记本', '办公用品', 5, 15.00, 75.00, 'PENDING');

-- 创建更新触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 为表添加更新时间触发器
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON business.users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON business.orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 设置表的副本身份为 FULL（支持 CDC）
ALTER TABLE business.users REPLICA IDENTITY FULL;
ALTER TABLE business.orders REPLICA IDENTITY FULL; 