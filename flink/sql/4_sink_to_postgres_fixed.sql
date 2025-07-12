-- ========================================
-- 第四步：将最终结果写入 PostgreSQL 目标数据库（修复版）
-- ========================================

-- 读取最终结果 Kafka Topic（移除主键约束）
CREATE TABLE result_source (
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
    join_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'result_orders_with_user_info',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'postgres_sink_consumer_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 创建 PostgreSQL Sink 表
CREATE TABLE orders_with_user_info_sink (
    order_id BIGINT,
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    product_name STRING,
    product_category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status STRING,
    order_time TIMESTAMP(3),
    user_register_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-sink:5432/sink_db',
    'table-name' = 'result.orders_with_user_info',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3',
    'sink.parallelism' = '1'
);

-- 将最终结果写入 PostgreSQL
INSERT INTO orders_with_user_info_sink
SELECT 
    order_id,
    user_id,
    username,
    email,
    phone,
    city,
    product_name,
    product_category,
    quantity,
    unit_price,
    total_amount,
    order_status,
    order_time,
    user_register_time,
    join_time as processed_time,
    CURRENT_TIMESTAMP as updated_at
FROM result_source; 