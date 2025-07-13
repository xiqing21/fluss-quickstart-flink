-- ========================================
-- 传统架构：基于Kafka的分层数据处理
-- ODS → DWD → DWS → ADS 四层架构
-- ========================================

-- ========================================
-- ODS层（原始数据层）- Kafka Topic
-- ========================================

-- 1. CDC原始数据写入Kafka ODS层
CREATE TABLE ods_orders_kafka (
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
    'format' = 'json'
);

CREATE TABLE ods_users_kafka (
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
    'format' = 'json'
);

-- ========================================
-- DWD层（明细数据层）- Kafka Topic
-- ========================================

-- 2. 从ODS读取，清洗转换后写入DWD层
CREATE TABLE dwd_orders_kafka (
    order_id BIGINT,
    user_id BIGINT,
    product_name STRING,
    product_category STRING,
    product_category_code STRING,        -- 新增：分类编码
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status STRING,
    order_status_code INT,               -- 新增：状态编码
    order_time TIMESTAMP(3),
    order_date STRING,                   -- 新增：日期分区
    order_hour INT,                      -- 新增：小时分区
    is_weekend BOOLEAN,                  -- 新增：是否周末
    updated_at TIMESTAMP(3),
    dwd_insert_time TIMESTAMP(3),        -- 新增：DWD处理时间
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd_orders',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

CREATE TABLE dwd_users_kafka (
    user_id BIGINT,
    username STRING,
    email STRING,
    email_domain STRING,                 -- 新增：邮箱域名
    phone STRING,
    phone_carrier STRING,               -- 新增：运营商
    city STRING,
    city_level STRING,                  -- 新增：城市级别
    province STRING,                    -- 新增：省份
    register_time TIMESTAMP(3),
    register_date STRING,               -- 新增：注册日期
    user_age_days INT,                  -- 新增：用户年龄（天）
    updated_at TIMESTAMP(3),
    dwd_insert_time TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dwd_users',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- DWD层处理任务1：订单清洗
INSERT INTO dwd_orders_kafka
SELECT 
    order_id,
    user_id,
    TRIM(product_name) as product_name,
    product_category,
    -- 业务逻辑：分类编码
    CASE 
        WHEN product_category = '电子产品' THEN 'ELEC'
        WHEN product_category = '服装鞋帽' THEN 'CLOTH'
        WHEN product_category = '家居用品' THEN 'HOME'
        ELSE 'OTHER'
    END as product_category_code,
    quantity,
    unit_price,
    total_amount,
    order_status,
    -- 业务逻辑：状态编码
    CASE 
        WHEN order_status = 'PENDING' THEN 1
        WHEN order_status = 'COMPLETED' THEN 2
        WHEN order_status = 'SHIPPED' THEN 3
        WHEN order_status = 'CANCELLED' THEN 4
        ELSE 0
    END as order_status_code,
    order_time,
    DATE_FORMAT(order_time, 'yyyy-MM-dd') as order_date,
    HOUR(order_time) as order_hour,
    -- 业务逻辑：是否周末
    CASE WHEN DAYOFWEEK(order_time) IN (1, 7) THEN true ELSE false END as is_weekend,
    updated_at,
    CURRENT_TIMESTAMP as dwd_insert_time
FROM ods_orders_kafka;

-- DWD层处理任务2：用户清洗
INSERT INTO dwd_users_kafka
SELECT 
    user_id,
    username,
    email,
    SUBSTRING(email, POSITION('@' IN email) + 1) as email_domain,
    phone,
    -- 业务逻辑：运营商识别
    CASE 
        WHEN phone LIKE '138%' OR phone LIKE '139%' THEN '中国移动'
        WHEN phone LIKE '188%' OR phone LIKE '189%' THEN '中国联通'
        WHEN phone LIKE '133%' OR phone LIKE '153%' THEN '中国电信'
        ELSE '其他'
    END as phone_carrier,
    city,
    -- 业务逻辑：城市级别
    CASE 
        WHEN city IN ('北京', '上海', '广州', '深圳') THEN '一线'
        WHEN city IN ('杭州', '南京', '武汉', '成都') THEN '新一线'
        ELSE '其他'
    END as city_level,
    -- 业务逻辑：省份映射
    CASE 
        WHEN city IN ('北京') THEN '北京市'
        WHEN city IN ('上海') THEN '上海市'
        WHEN city IN ('广州', '深圳') THEN '广东省'
        WHEN city IN ('杭州') THEN '浙江省'
        ELSE '其他省份'
    END as province,
    register_time,
    DATE_FORMAT(register_time, 'yyyy-MM-dd') as register_date,
    DATEDIFF('DAY', register_time, CURRENT_TIMESTAMP) as user_age_days,
    updated_at,
    CURRENT_TIMESTAMP as dwd_insert_time
FROM ods_users_kafka;

-- ========================================
-- DWS层（汇总数据层）- Kafka Topic
-- ========================================

-- 3. 从DWD读取，进行聚合计算后写入DWS层
CREATE TABLE dws_order_summary_kafka (
    stat_date STRING,                   -- 统计日期
    city_level STRING,                  -- 城市级别
    product_category_code STRING,       -- 商品分类
    order_count BIGINT,                 -- 订单数量
    total_amount DECIMAL(20,2),         -- 总金额
    avg_amount DECIMAL(10,2),           -- 平均金额
    unique_users BIGINT,                -- 独立用户数
    weekend_orders BIGINT,              -- 周末订单数
    stat_time TIMESTAMP(3),             -- 统计时间
    PRIMARY KEY (stat_date, city_level, product_category_code) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dws_order_summary',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- DWS层聚合任务：订单汇总
INSERT INTO dws_order_summary_kafka
SELECT 
    o.order_date as stat_date,
    u.city_level,
    o.product_category_code,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_amount,
    AVG(o.total_amount) as avg_amount,
    COUNT(DISTINCT o.user_id) as unique_users,
    SUM(CASE WHEN o.is_weekend THEN 1 ELSE 0 END) as weekend_orders,
    CURRENT_TIMESTAMP as stat_time
FROM dwd_orders_kafka o
LEFT JOIN dwd_users_kafka u ON o.user_id = u.user_id
GROUP BY o.order_date, u.city_level, o.product_category_code;

-- ========================================
-- ADS层（应用数据层）- Kafka Topic
-- ========================================

-- 4. 从DWS读取，生成最终的业务指标写入ADS层
CREATE TABLE ads_business_metrics_kafka (
    metric_date STRING,
    city_level STRING,
    total_orders BIGINT,
    total_revenue DECIMAL(20,2),
    avg_order_value DECIMAL(10,2),
    top_category STRING,                -- 热销分类
    weekend_ratio DECIMAL(5,4),         -- 周末订单占比
    growth_rate DECIMAL(5,4),           -- 环比增长率
    user_penetration DECIMAL(5,4),      -- 用户渗透率
    create_time TIMESTAMP(3),
    PRIMARY KEY (metric_date, city_level) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'ads_business_metrics',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);

-- ADS层业务指标计算
INSERT INTO ads_business_metrics_kafka
SELECT 
    stat_date as metric_date,
    city_level,
    SUM(order_count) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(avg_amount) as avg_order_value,
    -- 找出热销分类（订单数最多的）
    FIRST_VALUE(product_category_code) OVER (
        PARTITION BY stat_date, city_level 
        ORDER BY order_count DESC
    ) as top_category,
    -- 周末订单占比
    SUM(weekend_orders) * 1.0 / SUM(order_count) as weekend_ratio,
    -- 这里简化，实际需要和昨天对比
    0.0 as growth_rate,
    -- 用户渗透率（简化计算）
    SUM(unique_users) * 1.0 / SUM(order_count) as user_penetration,
    CURRENT_TIMESTAMP as create_time
FROM dws_order_summary_kafka
GROUP BY stat_date, city_level;

-- ========================================
-- 传统架构的问题总结
-- ========================================

/*
🔴 传统Kafka分层架构的问题：

1. 📊 存储冗余：
   - ODS、DWD、DWS、ADS四层都要在Kafka中存储
   - 同样的数据被复制多次，存储成本高
   - 每层都需要设置分区、副本等

2. ⏱️ 处理延迟：
   - 数据要经过4个层次的串行处理
   - 每层都有网络传输和序列化开销
   - 端到端延迟累积，难以做到亚秒级

3. 🔧 运维复杂：
   - 需要管理多个Kafka Topic
   - 每层的数据格式、Schema可能不同
   - 数据血缘关系复杂，故障排查困难

4. 💰 资源消耗：
   - 大量的Kafka Broker资源
   - 每层都需要独立的Flink任务
   - 网络带宽消耗大

5. 🔄 状态管理：
   - 每个Flink任务都要维护状态
   - 状态恢复和迁移复杂
   - 难以保证跨层的数据一致性
*/ 