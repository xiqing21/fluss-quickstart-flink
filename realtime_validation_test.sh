#!/bin/bash

# ========================================
# Apache Flink 实时业务验证测试脚本
# ========================================

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# 检查环境
check_environment() {
    print_step "检查环境状态..."
    
    # 检查Docker Compose服务
    if ! docker-compose ps | grep -q "Up"; then
        print_error "Docker Compose服务未运行，请先执行: docker-compose up -d"
        exit 1
    fi
    
    # 检查Flink JobManager
    if ! curl -s http://localhost:8081/overview > /dev/null; then
        print_error "Flink JobManager未就绪，请等待服务启动"
        exit 1
    fi
    
    print_success "环境检查通过"
}

# 验证初始数据
verify_initial_data() {
    print_step "验证初始数据状态..."
    
    echo -e "\n${BLUE}=== 源数据库初始订单 ===${NC}"
    docker exec postgres-source psql -U postgres -d source_db -c "
    SELECT order_id, user_id, product_name, order_status, order_time 
    FROM business.orders 
    ORDER BY order_id;
    " 2>/dev/null | grep -E "(order_id|---|[0-9]+)"
    
    echo -e "\n${BLUE}=== 检查Flink作业状态 ===${NC}"
    local job_count=$(docker exec jobmanager flink list 2>/dev/null | grep -c "RUNNING" || echo "0")
    print_info "运行中的Flink作业数量: $job_count"
    
    if [ "$job_count" -lt 4 ]; then
        print_warning "Flink作业数量不足，请确保已执行所有SQL脚本"
        print_info "执行命令："
        echo "  docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/1_cdc_source_to_kafka.sql"
        echo "  docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/2_dwd_layer.sql"
        echo "  docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/3_dimension_join.sql"
        echo "  docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/4_sink_to_postgres.sql"
        read -p "是否继续测试？ (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# 执行实时业务操作
execute_realtime_operations() {
    print_step "执行实时业务操作..."
    
    # 生成唯一的订单ID
    ORDER_ID=$((2000 + RANDOM % 1000))
    START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
    
    print_info "测试订单ID: $ORDER_ID"
    print_info "开始时间: $START_TIME"
    
    # 1. 插入新订单
    print_info "1. 插入新订单 (PENDING状态)"
    docker exec postgres-source psql -U postgres -d source_db -c "
    INSERT INTO business.orders (order_id, user_id, product_name, product_category, quantity, unit_price, total_amount, order_status, order_time, updated_at)
    VALUES ($ORDER_ID, 1001, 'AirPods Pro 测试', '电子产品', 1, 1999.00, 1999.00, 'PENDING', NOW(), NOW());
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "订单插入成功"
    else
        print_error "订单插入失败"
        return 1
    fi
    
    sleep 3
    
    # 2. 更新订单状态为SHIPPED
    print_info "2. 更新订单状态 (PENDING → SHIPPED)"
    docker exec postgres-source psql -U postgres -d source_db -c "
    UPDATE business.orders 
    SET order_status = 'SHIPPED', updated_at = NOW() 
    WHERE order_id = $ORDER_ID;
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "订单状态更新为SHIPPED"
    else
        print_error "订单状态更新失败"
        return 1
    fi
    
    sleep 3
    
    # 3. 完成订单
    print_info "3. 完成订单 (SHIPPED → COMPLETED)"
    docker exec postgres-source psql -U postgres -d source_db -c "
    UPDATE business.orders 
    SET order_status = 'COMPLETED', updated_at = NOW() 
    WHERE order_id = $ORDER_ID;
    " >/dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        print_success "订单完成"
    else
        print_error "订单完成失败"
        return 1
    fi
    
    # 存储订单ID供后续验证使用
    echo $ORDER_ID > /tmp/test_order_id.txt
}

# 验证数据流转
verify_data_flow() {
    print_step "验证数据流转..."
    
    ORDER_ID=$(cat /tmp/test_order_id.txt 2>/dev/null || echo "2006")
    
    # 验证CDC层
    print_info "验证ODS层CDC数据捕获..."
    local ods_count=$(timeout 10s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic ods_orders \
        --from-beginning \
        --timeout-ms 8000 2>/dev/null | grep -c "$ORDER_ID" 2>/dev/null || echo "0")
    ods_count=$(echo "$ods_count" | tr -d '\n\r' | grep -o '[0-9]*' | head -1)
    [ -z "$ods_count" ] && ods_count=0
    
    if [ "$ods_count" -gt 0 ]; then
        print_success "ODS层数据验证通过 (发现 $ods_count 条记录)"
    else
        print_warning "ODS层未发现测试数据，可能需要更多时间"
    fi
    
    # 验证DWD层
    print_info "验证DWD层数据清洗..."
    local dwd_count=$(timeout 10s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic dwd_orders \
        --from-beginning \
        --timeout-ms 8000 2>/dev/null | grep -c "$ORDER_ID" 2>/dev/null || echo "0")
    dwd_count=$(echo "$dwd_count" | tr -d '\n\r' | grep -o '[0-9]*' | head -1)
    [ -z "$dwd_count" ] && dwd_count=0
    
    if [ "$dwd_count" -gt 0 ]; then
        print_success "DWD层数据验证通过 (发现 $dwd_count 条记录)"
    else
        print_warning "DWD层未发现测试数据"
    fi
    
    # 验证维度关联
    print_info "验证维度关联结果..."
    local result_count=$(timeout 10s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic result_orders_with_user_info \
        --from-beginning \
        --timeout-ms 8000 2>/dev/null | grep -c "$ORDER_ID" 2>/dev/null || echo "0")
    result_count=$(echo "$result_count" | tr -d '\n\r' | grep -o '[0-9]*' | head -1)
    [ -z "$result_count" ] && result_count=0
    
    if [ "$result_count" -gt 0 ]; then
        print_success "维度关联验证通过 (发现 $result_count 条记录)"
    else
        print_warning "维度关联层未发现测试数据"
    fi
}

# 验证最终结果和延迟
verify_final_results() {
    print_step "验证最终结果和延迟..."
    
    ORDER_ID=$(cat /tmp/test_order_id.txt 2>/dev/null || echo "2006")
    
    # 等待数据写入
    print_info "等待数据写入最终数据库..."
    sleep 10
    
    # 检查最终结果
    echo -e "\n${BLUE}=== 最终结果验证 ===${NC}"
    local final_result=$(docker exec postgres-sink psql -U postgres -d sink_db -c "
    SELECT 
        order_id,
        username,
        city,
        product_name,
        total_amount,
        order_status,
        TO_CHAR(processed_time, 'YYYY-MM-DD HH24:MI:SS') as processed_time
    FROM result.orders_with_user_info 
    WHERE order_id = $ORDER_ID
    ORDER BY processed_time DESC
    LIMIT 3;
    " 2>/dev/null)
    
    if echo "$final_result" | grep -q "$ORDER_ID"; then
        print_success "最终结果验证通过"
        echo "$final_result" | grep -E "(order_id|---|$ORDER_ID)"
        
        # 延迟分析
        echo -e "\n${BLUE}=== 端到端延迟分析 ===${NC}"
        docker exec postgres-sink psql -U postgres -d sink_db -c "
        SELECT 
            order_id,
            order_status,
            ROUND(CAST(DATE_PART('epoch', processed_time - order_time) AS NUMERIC), 2) as latency_seconds,
            TO_CHAR(order_time, 'HH24:MI:SS') as source_time,
            TO_CHAR(processed_time, 'HH24:MI:SS') as result_time
        FROM result.orders_with_user_info 
        WHERE order_id = $ORDER_ID
        ORDER BY processed_time DESC;
        " 2>/dev/null
        
    else
        print_error "最终结果验证失败，未在目标数据库中找到测试数据"
        
        # 尝试故障排除
        print_info "执行故障排除..."
        
        # 检查Flink作业状态
        print_info "Flink作业状态:"
        docker exec jobmanager flink list 2>/dev/null | grep -E "(Job ID|RUNNING|FAILED)"
        
        # 检查Sink作业是否运行
        local sink_jobs=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | grep -o '"state":"RUNNING"' | wc -l || echo "0")
        print_info "运行中的作业数量: $sink_jobs"
        
        return 1
    fi
}

# 性能统计
performance_summary() {
    print_step "生成性能统计报告..."
    
    echo -e "\n${BLUE}=== 性能统计报告 ===${NC}"
    
    # 总订单数量
    local total_orders=$(docker exec postgres-sink psql -U postgres -d sink_db -c "
    SELECT COUNT(*) FROM result.orders_with_user_info;
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    # 平均延迟
    local avg_latency=$(docker exec postgres-sink psql -U postgres -d sink_db -c "
    SELECT ROUND(CAST(AVG(DATE_PART('epoch', processed_time - order_time)) AS NUMERIC), 2) 
    FROM result.orders_with_user_info 
    WHERE processed_time > order_time;
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+\.[0-9]+[[:space:]]*$" | tr -d ' ')
    
    # Flink集群资源
    local cluster_info=$(curl -s http://localhost:8081/overview 2>/dev/null)
    local taskmanagers=$(echo "$cluster_info" | grep -o '"taskmanagers":[0-9]*' | grep -o '[0-9]*')
    local slots_total=$(echo "$cluster_info" | grep -o '"slots-total":[0-9]*' | grep -o '[0-9]*')
    local slots_available=$(echo "$cluster_info" | grep -o '"slots-available":[0-9]*' | grep -o '[0-9]*')
    
    echo "📊 数据处理统计:"
    echo "  • 总处理订单数: ${total_orders:-0}"
    echo "  • 平均端到端延迟: ${avg_latency:-N/A} 秒"
    echo ""
    echo "🔧 集群资源状态:"
    echo "  • TaskManager数量: ${taskmanagers:-0}"
    echo "  • 总Task Slots: ${slots_total:-0}"
    echo "  • 可用Task Slots: ${slots_available:-0}"
    echo "  • 资源利用率: $((100 - slots_available * 100 / slots_total))%"
    echo ""
    
    # 延迟评估
    if [ ! -z "$avg_latency" ] && [ "$(echo "$avg_latency < 5" | bc -l 2>/dev/null || echo "1")" = "1" ]; then
        print_success "✅ 延迟性能: 优秀 (< 5秒)"
    elif [ ! -z "$avg_latency" ] && [ "$(echo "$avg_latency < 10" | bc -l 2>/dev/null || echo "1")" = "1" ]; then
        print_warning "⚠️ 延迟性能: 良好 (< 10秒)"
    else
        print_warning "⚠️ 延迟性能: 需要优化 (≥ 10秒)"
    fi
}

# 清理临时文件
cleanup() {
    rm -f /tmp/test_order_id.txt
}

# 主函数
main() {
    print_header "Apache Flink 实时业务验证测试"
    
    # 检查环境
    check_environment
    
    # 验证初始数据
    verify_initial_data
    
    # 执行实时业务操作
    if execute_realtime_operations; then
        print_success "实时业务操作完成"
        
        # 验证数据流转
        verify_data_flow
        
        # 验证最终结果
        if verify_final_results; then
            print_success "🎉 实时验证测试通过！"
        else
            print_error "❌ 实时验证测试失败"
        fi
        
        # 性能统计
        performance_summary
        
    else
        print_error "实时业务操作失败"
        exit 1
    fi
    
    # 清理
    cleanup
    
    print_header "测试完成"
    echo -e "详细信息请查看上方输出结果"
    echo -e "如需重复测试，请再次运行此脚本"
}

# 运行主函数
main "$@" 