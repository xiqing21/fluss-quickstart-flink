-- ========================================
-- 第一步：从 PostgreSQL 通过 CDC 读取数据并写入 Kafka
-- ========================================

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
    'slot.name' = 'orders_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 创建 CDC 源表：用户表
CREATE TABLE users_source (
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    register_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-source',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'source_db',
    'schema-name' = 'business',
    'table-name' = 'users',
    'slot.name' = 'users_slot',
    'decoding.plugin.name' = 'pgoutput'
);

-- 创建 Kafka Sink 表：ODS 层订单数据
CREATE TABLE ods_orders_topic (
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
    'connector' = 'kafka',
    'topic' = 'ods_orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'ods_orders_group',
    'format' = 'json'
);

-- 创建 Kafka Sink 表：ODS 层用户数据
CREATE TABLE ods_users_topic (
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    register_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ods_users',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'ods_users_group',
    'format' = 'json'
);

-- 将订单数据写入 Kafka
INSERT INTO ods_orders_topic 
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