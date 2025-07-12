-- 简化版第三步：不使用 Fluss 的维表 Join
-- 直接使用 Kafka 中的用户数据进行 Join

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
    etl_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd_orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'join_orders_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 读取 ODS 层用户数据作为维表
CREATE TABLE ods_users_dim (
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    register_time TIMESTAMP(3),
    updated_at TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_users',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'join_users_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
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
    'connector' = 'upsert-kafka',
    'topic' = 'result_orders_with_user_info',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 执行维表 Join 并写入结果（使用 INTERVAL JOIN）
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
LEFT JOIN ods_users_dim u ON o.user_id = u.user_id; 