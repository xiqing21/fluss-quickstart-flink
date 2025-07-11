#!/bin/bash

# 启动完整的实时流架构测试环境
# 包含 PostgreSQL 源/目标数据库、Kafka、Flink、Fluss 等组件

set -e

echo "🚀 正在启动完整的实时流架构测试环境..."
echo "=================================================="
echo ""
echo "📋 环境组件:"
echo "  🗄️  PostgreSQL 源数据库:    localhost:5432"
echo "  🗄️  PostgreSQL 目标数据库:   localhost:5433"
echo "  🔄 Kafka Broker:           localhost:9092"
echo "  🗂️  Zookeeper:              localhost:2181"
echo "  📊 Flink Web UI:           localhost:8081"
echo "  🏪 Fluss Coordinator:       localhost:9123"
echo "  📱 Fluss Tablet Server:     localhost:9124"
echo ""

# 检查 Docker 环境
if ! command -v docker &> /dev/null; then
    echo "❌ 错误: Docker 未安装"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ 错误: Docker Compose 未安装"
    exit 1
fi

# 创建必要的目录
echo "📁 创建必要的目录..."
mkdir -p /tmp/flink-checkpoints-directory
mkdir -p /tmp/flink-savepoints-directory

# 检查端口占用
echo "🔍 检查端口占用情况..."
ports=(5432 5433 8081 9092 2181 9123 9124)
for port in "${ports[@]}"; do
    if lsof -i :$port &> /dev/null 2>&1; then
        echo "⚠️  警告: 端口 $port 已被占用"
    fi
done

# 启动所有服务
echo "🐳 启动 Docker Compose 服务..."
docker-compose up -d

echo "⏳ 等待服务启动..."
sleep 10

# 检查服务状态
echo "📊 检查容器状态..."
docker-compose ps

echo ""
echo "⏳ 等待所有服务就绪 (约2分钟)..."
sleep 120

echo ""
echo "🎉 实时流架构测试环境启动完成！"
echo "=================================================="
echo ""
echo "🌐 Web 界面:"
echo "  Flink Web UI:     http://localhost:8081"
echo ""
echo "🔗 数据库连接:"
echo "  源数据库:         postgresql://postgres:postgres@localhost:5432/source_db"
echo "  目标数据库:       postgresql://postgres:postgres@localhost:5433/sink_db"
echo ""
echo "🛠️ 测试命令:"
echo "  连接 SQL 客户端:   docker-compose exec sql-client /opt/flink/bin/sql-client.sh"
echo "  查看日志:         docker-compose logs -f [service-name]"
echo "  停止环境:         docker-compose down"
echo ""
echo "📋 数据流架构测试步骤:"
echo "  1. 连接 SQL 客户端"
echo "  2. 执行 /opt/sql/1_cdc_source_to_kafka.sql    (CDC 数据采集)"
echo "  3. 执行 /opt/sql/2_dwd_layer.sql             (数据清洗层)"
echo "  4. 执行 /opt/sql/3_fluss_dimension_join.sql   (维表 Join)"
echo "  5. 执行 /opt/sql/4_sink_to_postgres.sql      (结果输出)"
echo ""
echo "📊 验证数据:"
echo "  源数据:           psql -h localhost -p 5432 -U postgres -d source_db"
echo "  结果数据:         psql -h localhost -p 5433 -U postgres -d sink_db"
echo "  Kafka Topics:     docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092"
echo ""
echo "🛑 停止环境: docker-compose down" 