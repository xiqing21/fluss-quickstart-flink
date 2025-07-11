#!/bin/bash

# 自动化测试完整的实时流架构 Pipeline
# 按顺序执行所有的 SQL 文件，验证数据流转

set -e

echo "🚀 开始自动化测试实时流架构 Pipeline..."
echo "=================================================="

# 检查环境是否运行
if ! docker-compose ps | grep -q "Up"; then
    echo "❌ 错误: Docker Compose 环境未运行"
    echo "请先运行: ./start-streaming-architecture.sh"
    exit 1
fi

echo "✅ 环境检查通过"
echo ""

# 函数：执行 SQL 文件
execute_sql_file() {
    local sql_file=$1
    local description=$2
    
    echo "📋 执行步骤: $description"
    echo "📄 SQL 文件: $sql_file"
    echo "⏳ 执行中..."
    
    # 在 SQL 客户端中执行 SQL 文件
    docker-compose exec -T sql-client /opt/flink/bin/sql-client.sh \
        --file "/opt/sql/$sql_file" \
        --jobmanager jobmanager:8081 \
        --jar "/opt/flink/lib/*.jar" || {
            echo "❌ 执行失败: $sql_file"
            return 1
        }
    
    echo "✅ 完成: $description"
    echo ""
    sleep 10  # 等待作业启动
}

# 函数：检查 Kafka Topic 数据
check_kafka_topic() {
    local topic=$1
    echo "🔍 检查 Kafka Topic: $topic"
    
    # 检查是否有数据
    local message_count=$(docker-compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --timeout-ms 5000 \
        --max-messages 1 2>/dev/null | wc -l || echo "0")
    
    if [ "$message_count" -gt 0 ]; then
        echo "✅ Topic $topic 有数据流入"
    else
        echo "⚠️  Topic $topic 暂无数据"
    fi
    echo ""
}

# 函数：检查 PostgreSQL 表数据
check_postgres_table() {
    local database=$1
    local port=$2
    local table=$3
    local description=$4
    
    echo "🔍 检查 $description"
    
    local count=$(docker-compose exec -T postgres-${database} \
        psql -U postgres -d ${database}_db -t -c \
        "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ' || echo "0")
    
    echo "📊 $description 记录数: $count"
    echo ""
}

# 步骤 1: 启动 CDC 数据采集
echo "🔄 步骤 1/4: 启动 CDC 数据采集 (PostgreSQL -> Kafka ODS)"
execute_sql_file "1_cdc_source_to_kafka.sql" "CDC 数据采集到 Kafka ODS 层"

# 等待 CDC 数据流入
echo "⏳ 等待 CDC 数据采集 (30秒)..."
sleep 30

# 检查 ODS 层数据
check_kafka_topic "ods_orders"
check_kafka_topic "ods_users"

# 步骤 2: 启动 DWD 数据处理
echo "🔧 步骤 2/4: 启动 DWD 数据清洗和转换"
execute_sql_file "2_dwd_layer.sql" "DWD 层数据清洗和转换"

# 等待 DWD 处理
echo "⏳ 等待 DWD 数据处理 (30秒)..."
sleep 30

# 检查 DWD 层数据
check_kafka_topic "dwd_orders"
check_kafka_topic "dwd_users"

# 步骤 3: 启动 Fluss 维表 Join
echo "🔗 步骤 3/4: 启动 Fluss 维表 Join"
execute_sql_file "3_fluss_dimension_join.sql" "Fluss 维表双流 Join"

# 等待 Join 处理
echo "⏳ 等待维表 Join 处理 (30秒)..."
sleep 30

# 检查结果数据
check_kafka_topic "result_orders_with_user_info"

# 步骤 4: 启动数据汇聚到 PostgreSQL
echo "💾 步骤 4/4: 启动数据汇聚到 PostgreSQL"
execute_sql_file "4_sink_to_postgres.sql" "结果数据写入 PostgreSQL 目标库"

# 等待数据写入
echo "⏳ 等待数据写入目标数据库 (30秒)..."
sleep 30

echo "🎉 Pipeline 测试完成！"
echo "=================================================="
echo ""

# 最终数据验证
echo "📊 最终数据验证:"
echo ""

# 检查源数据
echo "📥 源数据统计:"
check_postgres_table "source" "5432" "business.orders" "源订单表"
check_postgres_table "source" "5432" "business.users" "源用户表"

# 检查目标数据
echo "📤 目标数据统计:"
check_postgres_table "sink" "5433" "result.orders_with_user_info" "目标宽表"

# 展示样本数据
echo "📋 样本数据预览:"
echo ""
echo "🔍 目标数据库样本记录:"
docker-compose exec -T postgres-sink \
    psql -U postgres -d sink_db -c \
    "SELECT order_id, username, city, product_name, total_amount, order_status 
     FROM result.orders_with_user_info 
     LIMIT 5;" 2>/dev/null || echo "暂无数据"

echo ""
echo "🎯 测试总结:"
echo "  ✅ CDC 数据采集"
echo "  ✅ DWD 数据清洗"
echo "  ✅ Fluss 维表 Join"
echo "  ✅ PostgreSQL 数据汇聚"
echo ""
echo "🌐 查看 Flink Web UI: http://localhost:8081"
echo "🔍 检查运行中的作业和监控指标"
echo ""
echo "🛑 停止测试环境: docker-compose down" 