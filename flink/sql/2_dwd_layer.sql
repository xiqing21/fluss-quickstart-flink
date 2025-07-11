-- ========================================
-- 第二步：DWD 层数据处理和转换
-- ========================================

-- 读取 ODS 层订单数据
CREATE TABLE ods_orders_source (
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
    'properties.group.id' = 'dwd_orders_consumer_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 读取 ODS 层用户数据
CREATE TABLE ods_users_source (
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
    'properties.group.id' = 'dwd_users_consumer_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 创建 DWD 层订单表（数据清洗和转换后）
CREATE TABLE dwd_orders_topic (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING,
    product_category STRING,
    product_category_normalized STRING,  -- 标准化的商品分类
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    total_amount_yuan STRING,           -- 添加人民币格式
    order_status STRING,
    order_status_desc STRING,          -- 状态描述
    order_time TIMESTAMP(3),
    order_date STRING,                 -- 订单日期（用于分区）
    order_hour INT,                    -- 订单小时（用于时间分析）
    updated_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),             -- ETL 处理时间
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd_orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'dwd_orders_producer_group',
    'format' = 'json'
);

-- 创建 DWD 层用户表（数据清洗和转换后）
CREATE TABLE dwd_users_topic (
    user_id BIGINT,
    username STRING,
    email STRING,
    email_domain STRING,              -- 邮箱域名
    phone STRING,
    phone_area_code STRING,           -- 手机区号
    city STRING,
    city_tier STRING,                 -- 城市级别
    register_time TIMESTAMP(3),
    register_date STRING,             -- 注册日期
    user_age_days INT,                -- 用户注册天数
    updated_at TIMESTAMP(3),
    etl_time TIMESTAMP(3),            -- ETL 处理时间
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd_users',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'dwd_users_producer_group',
    'format' = 'json'
);

-- DWD 层订单数据处理和写入
INSERT INTO dwd_orders_topic
SELECT 
    order_id,
    user_id,
    TRIM(product_name) as product_name,
    product_category,
    CASE 
        WHEN product_category = '电子产品' THEN 'ELECTRONICS'
        WHEN product_category = '服装鞋帽' THEN 'CLOTHING'
        WHEN product_category = '家居用品' THEN 'HOME'
        WHEN product_category = '办公用品' THEN 'OFFICE'
        ELSE 'OTHER'
    END as product_category_normalized,
    quantity,
    unit_price,
    total_amount,
    CONCAT('¥', CAST(total_amount AS STRING)) as total_amount_yuan,
    order_status,
    CASE 
        WHEN order_status = 'PENDING' THEN '待处理'
        WHEN order_status = 'COMPLETED' THEN '已完成'
        WHEN order_status = 'SHIPPED' THEN '已发货'
        WHEN order_status = 'CANCELLED' THEN '已取消'
        ELSE '未知状态'
    END as order_status_desc,
    order_time,
    DATE_FORMAT(order_time, 'yyyy-MM-dd') as order_date,
    HOUR(order_time) as order_hour,
    updated_at,
    CURRENT_TIMESTAMP as etl_time
FROM ods_orders_source
WHERE order_id IS NOT NULL 
    AND user_id IS NOT NULL 
    AND total_amount > 0; 