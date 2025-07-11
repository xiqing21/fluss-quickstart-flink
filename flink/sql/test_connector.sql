-- 测试 postgres-cdc 连接器是否可用
CREATE TABLE test_orders (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres-source',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'source_db',
    'schema-name' = 'business',
    'table-name' = 'orders'
);

-- 测试 kafka 连接器
CREATE TABLE test_kafka (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'test_topic',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- 显示创建的表
SHOW TABLES; 