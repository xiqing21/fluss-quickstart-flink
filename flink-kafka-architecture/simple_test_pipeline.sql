-- 简化的流处理测试
-- 只测试 PostgreSQL CDC -> Kafka 的基本流转

-- 创建 CDC 源表：订单表
CREATE TABLE orders_source (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING,
    product_category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status STRING,
    order_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-source',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'source_db',
    'schema-name' = 'business',
    'table-name' = 'orders',
    'slot.name' = 'orders_slot_simple',
    'decoding.plugin.name' = 'pgoutput'
);

-- 创建 Kafka Sink 表：测试输出（使用 upsert-kafka 支持 CDC 变更）
CREATE TABLE simple_orders_output (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING,
    product_category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status STRING,
    order_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'simple_test_orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 将订单数据写入 Kafka
INSERT INTO simple_orders_output 
SELECT 
    order_id,
    user_id,
    product_name,
    product_category,
    quantity,
    unit_price,
    total_amount,
    order_status,
    order_time,
    updated_at
FROM orders_source; 