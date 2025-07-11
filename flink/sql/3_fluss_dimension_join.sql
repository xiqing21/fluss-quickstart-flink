-- ========================================
-- 第三步：使用 Fluss 进行维表双流 Join
-- ========================================

-- 读取 DWD 层订单事实流
CREATE TABLE dwd_orders_source (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING,
    product_category STRING,
    product_category_normalized STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    total_amount_yuan STRING,
    order_status STRING,
    order_status_desc STRING,
    order_time TIMESTAMP(3),
    order_date STRING,
    order_hour INT,
    updated_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd_orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'fluss_join_orders_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 读取 ODS 层用户数据并写入 Fluss 作为维表
CREATE TABLE users_fluss_dim (
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    register_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'table-name' = 'users_dimension',
    'value.format' = 'json'
);

-- 先创建一个简单的测试数据插入到 Fluss 维表
INSERT INTO users_fluss_dim
VALUES 
(1001, '张三', 'zhangsan@example.com', '13812345678', '北京', TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-07-11 17:00:00'),
(1002, '李四', 'lisi@example.com', '13987654321', '上海', TIMESTAMP '2024-01-02 11:00:00', TIMESTAMP '2024-07-11 17:00:00'),
(1003, '王五', 'wangwu@example.com', '13611112222', '广州', TIMESTAMP '2024-01-03 12:00:00', TIMESTAMP '2024-07-11 17:00:00'),
(1004, '赵六', 'zhaoliu@example.com', '13733334444', '深圳', TIMESTAMP '2024-01-04 13:00:00', TIMESTAMP '2024-07-11 17:00:00'),
(1005, '钱七', 'qianqi@example.com', '13855556666', '杭州', TIMESTAMP '2024-01-05 14:00:00', TIMESTAMP '2024-07-11 17:00:00');

-- 读取 Fluss 中的用户维表用于 Join
CREATE TABLE users_dim_lookup (
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    register_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'table-name' = 'users_dimension',
    'value.format' = 'json'
);

-- 创建最终的宽表结果 Kafka Topic
CREATE TABLE result_orders_with_user_info (
    order_id BIGINT,
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    product_name STRING,
    product_category STRING,
    product_category_normalized STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    total_amount_yuan STRING,
    order_status STRING,
    order_status_desc STRING,
    order_time TIMESTAMP(3),
    order_date STRING,
    order_hour INT,
    user_register_time TIMESTAMP(3),
    user_city_tier STRING,
    join_time TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'result_orders_with_user_info',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'result_producer_group',
    'format' = 'json'
);

-- 执行维表 Join 并写入结果
INSERT INTO result_orders_with_user_info
SELECT 
    o.order_id,
    o.user_id,
    COALESCE(u.username, 'Unknown User') as username,
    COALESCE(u.email, 'unknown@example.com') as email,
    COALESCE(u.phone, '00000000000') as phone,
    COALESCE(u.city, 'Unknown City') as city,
    o.product_name,
    o.product_category,
    o.product_category_normalized,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.total_amount_yuan,
    o.order_status,
    o.order_status_desc,
    o.order_time,
    o.order_date,
    o.order_hour,
    u.register_time as user_register_time,
    CASE 
        WHEN u.city IN ('北京', '上海', '广州', '深圳') THEN '一线城市'
        WHEN u.city IN ('杭州', '南京', '武汉', '成都', '重庆') THEN '新一线城市'
        WHEN u.city IS NOT NULL THEN '其他城市'
        ELSE '未知'
    END as user_city_tier,
    CURRENT_TIMESTAMP as join_time
FROM dwd_orders_source o
LEFT JOIN users_dim_lookup FOR SYSTEM_TIME AS OF o.etl_time AS u
ON o.user_id = u.user_id; 