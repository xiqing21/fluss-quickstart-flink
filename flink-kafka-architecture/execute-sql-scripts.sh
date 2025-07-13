#!/bin/bash

# ========================================
# Apache Flink SQL脚本批量执行脚本
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
    
    # 检查Docker容器状态
    if ! docker ps | grep -q "sql-client"; then
        print_error "sql-client容器未运行，请先执行: docker-compose up -d"
        exit 1
    fi
    
    # 检查Flink集群状态
    if ! curl -s http://localhost:8081/overview > /dev/null; then
        print_error "Flink集群未就绪，请等待服务启动"
        exit 1
    fi
    
    print_success "环境检查通过"
}

# 检查现有Flink任务
check_existing_jobs() {
    print_step "检查现有Flink任务..."
    
    local running_jobs=$(docker exec jobmanager flink list 2>/dev/null | grep "RUNNING" | wc -l | tr -d ' ')
    
    if [ "$running_jobs" -gt 0 ]; then
        print_warning "发现 $running_jobs 个正在运行的Flink任务"
        echo ""
        print_info "当前运行的任务："
        docker exec jobmanager flink list 2>/dev/null | grep -E "(RUNNING|Job ID)" | head -10
        echo ""
        
        print_info "选择处理方式："
        print_info "  1. 取消所有现有任务，重新开始"
        print_info "  2. 保留现有任务，跳过执行"
        print_info "  3. 强制继续执行 (可能产生重复任务)"
        
        read -p "请选择 (1/2/3): " -n 1 -r
        echo ""
        
        case $REPLY in
            1)
                print_info "取消所有现有任务..."
                cancel_all_jobs
                ;;
            2)
                print_info "保留现有任务，跳过SQL脚本执行"
                print_success "现有任务保持运行状态"
                verify_execution
                exit 0
                ;;
            3)
                print_warning "强制继续执行，可能产生重复任务"
                ;;
            *)
                print_error "无效选择，退出"
                exit 1
                ;;
        esac
    else
        print_success "未发现运行中的任务，可以开始执行"
    fi
}

# 取消所有任务
cancel_all_jobs() {
    print_info "正在取消所有Flink任务..."
    
    # 获取所有运行中的任务ID
    local job_ids=$(docker exec jobmanager flink list 2>/dev/null | grep "RUNNING" | awk '{print $4}' | tr -d ':')
    
    if [ -z "$job_ids" ]; then
        print_info "没有任务需要取消"
        return 0
    fi
    
    # 取消每个任务
    local cancelled_count=0
    for job_id in $job_ids; do
        if [ -n "$job_id" ]; then
            print_info "取消任务: $job_id"
            if docker exec jobmanager flink cancel $job_id >/dev/null 2>&1; then
                cancelled_count=$((cancelled_count + 1))
            else
                print_warning "取消任务 $job_id 失败"
            fi
        fi
    done
    
    print_success "已取消 $cancelled_count 个任务"
    
    # 等待任务完全停止
    print_info "等待任务完全停止..."
    sleep 5
    
    # 验证任务是否都已停止
    local remaining_jobs=$(docker exec jobmanager flink list 2>/dev/null | grep "RUNNING" | wc -l | tr -d ' ')
    if [ "$remaining_jobs" -eq 0 ]; then
        print_success "所有任务已成功停止"
    else
        print_warning "仍有 $remaining_jobs 个任务在运行"
    fi
}

# 执行单个SQL文件
execute_sql_file() {
    local sql_file=$1
    local step_name=$2
    
    print_step "执行 $step_name: $sql_file"
    
    # 直接执行SQL文件，不使用后台模式
    print_info "开始执行SQL脚本..."
    
    # 检查是否有timeout命令（Linux）或gtimeout（macOS with coreutils）
    if command -v timeout >/dev/null 2>&1; then
        timeout 120s docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/$sql_file
        local exit_code=$?
    elif command -v gtimeout >/dev/null 2>&1; then
        gtimeout 120s docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/$sql_file
        local exit_code=$?
    else
        # 如果没有timeout命令，直接执行
        print_info "没有找到timeout命令，直接执行（可能需要等待较长时间）"
        docker exec sql-client /opt/flink/bin/sql-client.sh -f /opt/sql/$sql_file
        local exit_code=$?
    fi
    
    if [ $exit_code -eq 0 ]; then
        print_success "$step_name 执行完成"
        
        # 等待一段时间让作业完全启动
        sleep 8
        
        # 验证作业是否成功启动
        local job_count=$(docker exec jobmanager flink list 2>/dev/null | grep -c "RUNNING" || echo "0")
        print_info "当前运行的作业数量: $job_count"
        
        return 0
    elif [ $exit_code -eq 124 ]; then
        print_error "$step_name 执行超时"
        return 1
    else
        print_error "$step_name 执行失败 (退出代码: $exit_code)"
        return 1
    fi
}

# 批量执行所有SQL脚本
execute_all_sql_scripts() {
    print_header "开始执行所有SQL脚本"
    
    # 定义SQL文件执行顺序
    declare -a sql_files=(
        "1_cdc_source_to_kafka.sql:第一阶段-CDC数据捕获"
        "2_dwd_layer.sql:第二阶段-DWD数据清洗"
        "3_dimension_join.sql:第三阶段-维度关联"
        "4_sink_to_postgres.sql:第四阶段-数据写入"
    )
    
    local total_steps=${#sql_files[@]}
    local current_step=1
    
    # 执行每个SQL文件
    for item in "${sql_files[@]}"; do
        local sql_file="${item%%:*}"
        local step_name="${item##*:}"
        
        print_info "进度: $current_step/$total_steps"
        
        if execute_sql_file "$sql_file" "$step_name"; then
            print_success "✅ $step_name 完成"
        else
            print_error "❌ $step_name 失败"
            return 1
        fi
        
        current_step=$((current_step + 1))
        
        # 在步骤之间加入短暂延迟
        if [ $current_step -le $total_steps ]; then
            sleep 3
        fi
    done
    
    print_success "🎉 所有SQL脚本执行完成！"
}

# 验证执行结果
verify_execution() {
    print_step "验证执行结果..."
    
    # 检查Flink作业状态
    local job_count=$(docker exec jobmanager flink list 2>/dev/null | grep -c "RUNNING" || echo "0")
    print_info "运行中的Flink作业数量: $job_count"
    
    if [ "$job_count" -ge 4 ]; then
        print_success "✅ 所有作业都在运行"
    else
        print_warning "⚠️ 作业数量不足，预期至少4个作业"
    fi
    
    # 检查Kafka主题
    print_info "检查Kafka主题..."
    local topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "(ods_|dwd_|result_)" | wc -l)
    print_info "相关Kafka主题数量: $topics"
    
    # 检查数据库表
    print_info "检查目标数据库表..."
    local table_exists=$(docker exec postgres-sink psql -U postgres -d sink_db -c "
    SELECT COUNT(*) FROM information_schema.tables 
    WHERE table_schema = 'result' AND table_name = 'orders_with_user_info';
    " 2>/dev/null | grep -E "^[[:space:]]*[0-9]+[[:space:]]*$" | tr -d ' ')
    
    if [ "$table_exists" = "1" ]; then
        print_success "✅ 目标表已创建"
    else
        print_warning "⚠️ 目标表未找到"
    fi
    
    # 显示集群资源使用情况
    print_info "集群资源使用情况:"
    local cluster_info=$(curl -s http://localhost:8081/overview 2>/dev/null)
    local slots_total=$(echo "$cluster_info" | grep -o '"slots-total":[0-9]*' | grep -o '[0-9]*')
    local slots_available=$(echo "$cluster_info" | grep -o '"slots-available":[0-9]*' | grep -o '[0-9]*')
    
    echo "  • 总Task Slots: ${slots_total:-0}"
    echo "  • 可用Task Slots: ${slots_available:-0}"
    echo "  • 使用率: $((100 - slots_available * 100 / slots_total))%"
}

# 主函数
main() {
    print_header "Apache Flink SQL脚本批量执行器"
    
    # 检查环境
    check_environment
    
    # 检查现有Flink任务
    check_existing_jobs
    
    # 执行SQL脚本
    if execute_all_sql_scripts; then
        print_success "🎉 所有SQL脚本执行成功！"
        
        # 验证执行结果
        verify_execution
        
        echo ""
        print_info "接下来可以运行实时验证测试："
        print_info "  ./realtime_validation_test.sh"
        
    else
        print_error "❌ SQL脚本执行失败"
        exit 1
    fi
    
    print_header "执行完成"
}

# 运行主函数
main "$@" 