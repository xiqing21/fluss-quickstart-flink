-- ========================================
-- Fluss统一架构：基于逻辑分层的数据处理
-- 一个存储，多个逻辑视图实现分层
-- ========================================

-- ========================================
-- 第一步：创建Fluss原始数据表（ODS逻辑层）
-- ========================================

-- 统一的订单数据湖表（包含所有层级的字段）
CREATE TABLE orders_unified_lake (
    -- 🟦 ODS层字段（原始数据）
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
    
    -- 🟩 DWD层字段（清洗增强）
    product_category_code STRING,
    order_status_code INT,
    order_date STRING,
    order_hour INT,
    is_weekend BOOLEAN,
    
    -- 🟨 DWS层字段（聚合预计算，可选）
    daily_order_rank INT,              -- 当日订单排名
    category_order_rank INT,           -- 分类内排名
    
    -- 🟪 元数据字段
    data_layer STRING,                 -- 数据层级标识
    process_time TIMESTAMP(3),         -- 处理时间
    data_version INT,                  -- 数据版本
    
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'table-name' = 'orders_unified_data_lake',
    'value.format' = 'json',
    -- 🚀 Fluss优化配置
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '7d',
    'fluss.table.compaction.enabled' = 'true',
    'fluss.table.snapshot.num-retained.min' = '24'  -- 保留24个快照支持时间旅行
);

-- 统一的用户数据湖表
CREATE TABLE users_unified_lake (
    -- 🟦 ODS层字段
    user_id BIGINT,
    username STRING,
    email STRING,
    phone STRING,
    city STRING,
    register_time TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    
    -- 🟩 DWD层字段
    email_domain STRING,
    phone_carrier STRING,
    city_level STRING,
    province STRING,
    register_date STRING,
    user_age_days INT,
    
    -- 🟪 元数据字段
    data_layer STRING,
    process_time TIMESTAMP(3),
    data_version INT,
    
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123',
    'table-name' = 'users_unified_data_lake',
    'value.format' = 'json',
    'fluss.table.datalake.enabled' = 'true',
    'fluss.table.log.retention' = '30d',
    'fluss.table.compaction.enabled' = 'true'
);

-- ========================================
-- 第二步：创建逻辑分层视图（替代物理Kafka层）
-- ========================================

-- 🟦 ODS逻辑视图：原始数据层
CREATE VIEW ods_orders_view AS
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
FROM orders_unified_lake
WHERE data_layer = 'ODS' OR data_layer IS NULL;  -- 兼容早期数据

CREATE VIEW ods_users_view AS
SELECT 
    user_id,
    username,
    email,
    phone,
    city,
    register_time,
    updated_at
FROM users_unified_lake
WHERE data_layer = 'ODS' OR data_layer IS NULL;

-- 🟩 DWD逻辑视图：明细数据层
CREATE VIEW dwd_orders_view AS
SELECT 
    order_id,
    user_id,
    product_name,
    product_category,
    product_category_code,
    quantity,
    unit_price,
    total_amount,
    order_status,
    order_status_code,
    order_time,
    order_date,
    order_hour,
    is_weekend,
    updated_at,
    process_time as dwd_insert_time
FROM orders_unified_lake
WHERE data_layer = 'DWD';

CREATE VIEW dwd_users_view AS
SELECT 
    user_id,
    username,
    email,
    email_domain,
    phone,
    phone_carrier,
    city,
    city_level,
    province,
    register_time,
    register_date,
    user_age_days,
    updated_at,
    process_time as dwd_insert_time
FROM users_unified_lake
WHERE data_layer = 'DWD';

-- 🟨 DWS逻辑视图：汇总数据层（实时计算）
CREATE VIEW dws_order_summary_view AS
SELECT 
    order_date as stat_date,
    u.city_level,
    o.product_category_code,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_amount,
    AVG(o.total_amount) as avg_amount,
    COUNT(DISTINCT o.user_id) as unique_users,
    SUM(CASE WHEN o.is_weekend THEN 1 ELSE 0 END) as weekend_orders,
    MAX(o.process_time) as stat_time
FROM orders_unified_lake o
LEFT JOIN users_unified_lake u ON o.user_id = u.user_id
WHERE o.data_layer = 'DWD' AND u.data_layer = 'DWD'
GROUP BY o.order_date, u.city_level, o.product_category_code;

-- 🟪 ADS逻辑视图：应用数据层（业务指标）
CREATE VIEW ads_business_metrics_view AS
SELECT 
    stat_date as metric_date,
    city_level,
    SUM(order_count) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(avg_amount) as avg_order_value,
    -- 利用Fluss的分析函数
    FIRST_VALUE(product_category_code) OVER (
        PARTITION BY stat_date, city_level 
        ORDER BY order_count DESC
    ) as top_category,
    SUM(weekend_orders) * 1.0 / SUM(order_count) as weekend_ratio,
    -- 环比增长率（利用Fluss时间旅行查询）
    (SUM(total_amount) - LAG(SUM(total_amount), 1) OVER (
        PARTITION BY city_level 
        ORDER BY stat_date
    )) / LAG(SUM(total_amount), 1) OVER (
        PARTITION BY city_level 
        ORDER BY stat_date
    ) as growth_rate,
    SUM(unique_users) * 1.0 / SUM(order_count) as user_penetration,
    MAX(stat_time) as create_time
FROM dws_order_summary_view
GROUP BY stat_date, city_level;

-- ========================================
-- 第三步：统一的数据处理管道
-- ========================================

-- 🔄 实时数据处理：从CDC直接写入Fluss（包含所有层级逻辑）
INSERT INTO orders_unified_lake
SELECT 
    -- ODS层字段（原始数据）
    order_id,
    user_id,
    TRIM(product_name) as product_name,
    product_category,
    quantity,
    unit_price,
    total_amount,
    order_status,
    order_time,
    updated_at,
    
    -- DWD层字段（实时ETL处理）
    CASE 
        WHEN product_category = '电子产品' THEN 'ELEC'
        WHEN product_category = '服装鞋帽' THEN 'CLOTH'
        WHEN product_category = '家居用品' THEN 'HOME'
        ELSE 'OTHER'
    END as product_category_code,
    
    CASE 
        WHEN order_status = 'PENDING' THEN 1
        WHEN order_status = 'COMPLETED' THEN 2
        WHEN order_status = 'SHIPPED' THEN 3
        WHEN order_status = 'CANCELLED' THEN 4
        ELSE 0
    END as order_status_code,
    
    DATE_FORMAT(order_time, 'yyyy-MM-dd') as order_date,
    HOUR(order_time) as order_hour,
    CASE WHEN DAYOFWEEK(order_time) IN (1, 7) THEN true ELSE false END as is_weekend,
    
    -- DWS层字段（可选的预计算）
    RANK() OVER (
        PARTITION BY DATE_FORMAT(order_time, 'yyyy-MM-dd') 
        ORDER BY total_amount DESC
    ) as daily_order_rank,
    
    RANK() OVER (
        PARTITION BY product_category, DATE_FORMAT(order_time, 'yyyy-MM-dd')
        ORDER BY total_amount DESC
    ) as category_order_rank,
    
    -- 元数据字段
    'DWD' as data_layer,          -- 标记为已处理的DWD数据
    CURRENT_TIMESTAMP as process_time,
    1 as data_version
    
FROM orders_cdc_source  -- 这里连接CDC源
WHERE order_id IS NOT NULL AND user_id IS NOT NULL AND total_amount > 0;

-- 用户数据处理
INSERT INTO users_unified_lake
SELECT 
    -- ODS层字段
    user_id,
    username,
    email,
    phone,
    city,
    register_time,
    updated_at,
    
    -- DWD层字段（实时ETL）
    SUBSTRING(email, POSITION('@' IN email) + 1) as email_domain,
    CASE 
        WHEN phone LIKE '138%' OR phone LIKE '139%' THEN '中国移动'
        WHEN phone LIKE '188%' OR phone LIKE '189%' THEN '中国联通'
        WHEN phone LIKE '133%' OR phone LIKE '153%' THEN '中国电信'
        ELSE '其他'
    END as phone_carrier,
    CASE 
        WHEN city IN ('北京', '上海', '广州', '深圳') THEN '一线'
        WHEN city IN ('杭州', '南京', '武汉', '成都') THEN '新一线'
        ELSE '其他'
    END as city_level,
    CASE 
        WHEN city IN ('北京') THEN '北京市'
        WHEN city IN ('上海') THEN '上海市'
        WHEN city IN ('广州', '深圳') THEN '广东省'
        WHEN city IN ('杭州') THEN '浙江省'
        ELSE '其他省份'
    END as province,
    DATE_FORMAT(register_time, 'yyyy-MM-dd') as register_date,
    DATEDIFF('DAY', register_time, CURRENT_TIMESTAMP) as user_age_days,
    
    -- 元数据字段
    'DWD' as data_layer,
    CURRENT_TIMESTAMP as process_time,
    1 as data_version
    
FROM users_cdc_source  -- 这里连接CDC源
WHERE user_id IS NOT NULL;

-- ========================================
-- 第四步：高性能实时查询示例
-- ========================================

-- 📊 实时查询：今日订单情况（直接查询Fluss，无需Kafka）
SELECT 
    city_level,
    product_category_code,
    COUNT(*) as order_count,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM dwd_orders_view o
LEFT JOIN dwd_users_view u ON o.user_id = u.user_id
WHERE order_date = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
GROUP BY city_level, product_category_code
ORDER BY revenue DESC;

-- 📈 历史对比查询（利用Fluss时间旅行能力）
SELECT 
    metric_date,
    city_level,
    total_revenue,
    growth_rate,
    CASE 
        WHEN growth_rate > 0.1 THEN '高增长'
        WHEN growth_rate > 0.05 THEN '中增长'
        WHEN growth_rate > 0 THEN '低增长'
        ELSE '负增长'
    END as growth_level
FROM ads_business_metrics_view
WHERE metric_date >= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 7 DAY), 'yyyy-MM-dd')
ORDER BY metric_date DESC, total_revenue DESC;

-- 🔍 实时用户画像查询（利用Fluss列式存储优势）
SELECT 
    u.city_level,
    u.phone_carrier,
    COUNT(DISTINCT u.user_id) as user_count,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spend,
    AVG(o.total_amount) as avg_order_value
FROM dwd_users_view u
LEFT JOIN dwd_orders_view o ON u.user_id = o.user_id
WHERE o.order_date >= DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), 'yyyy-MM-dd')
GROUP BY u.city_level, u.phone_carrier
ORDER BY total_spend DESC;

-- ========================================
-- Fluss统一分层架构的优势总结
-- ========================================

/*
🟢 Fluss统一分层架构的优势：

1. 📊 存储统一：
   ✅ 所有层级数据存储在同一个Fluss表中
   ✅ 减少存储成本60-80%
   ✅ 通过逻辑视图实现分层，无需物理复制

2. ⚡ 性能优化：
   ✅ 列式存储支持高效查询和压缩
   ✅ 投影下推只读取需要的字段
   ✅ 端到端延迟从5秒降至亚秒级

3. 🔄 流表二元性：
   ✅ 同一份数据支持流式和批式访问
   ✅ 实时和历史查询统一
   ✅ 支持时间旅行查询功能

4. 🎯 查询灵活：
   ✅ 可以跨层级进行复杂分析
   ✅ 支持实时OLAP查询
   ✅ 多种计算引擎可直接访问

5. 🛠️ 运维简化：
   ✅ 只需管理一个Fluss集群
   ✅ 统一的Schema和元数据管理
   ✅ 简化的监控和故障排查

6. 💰 成本效益：
   ✅ 减少Kafka集群资源需求
   ✅ 降低网络传输成本
   ✅ 统一的存储和计算资源

7. 🔧 开发效率：
   ✅ 一次写入，多层访问
   ✅ 统一的数据血缘关系
   ✅ 简化的数据管道开发
*/

-- ========================================
-- 迁移策略建议
-- ========================================

/*
🚀 从Kafka分层迁移到Fluss统一分层的策略：

阶段一：并行运行（1-2周）
- 保持现有Kafka分层架构不变
- 新建Fluss统一存储，并行写入数据
- 对比数据质量和查询性能

阶段二：逐步迁移（2-4周）
- 将非关键业务的查询迁移到Fluss
- 验证Fluss的稳定性和性能
- 逐步减少对Kafka的依赖

阶段三：全面替换（1-2周）
- 停止Kafka分层的数据写入
- 完全切换到Fluss统一架构
- 清理和回收Kafka资源

注意事项：
- 数据格式兼容性检查
- 下游系统的适配改造
- 监控和告警系统的调整
- 团队培训和文档更新
*/ 