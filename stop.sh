#!/bin/bash

# 个人AI理财助手 - 停止脚本
# 支持强制杀死所有相关进程

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 项目名称（用于进程识别）
PROJECT_NAME="personal_finance_assistant"
APP_NAME="app.py"
PROCESS_PATTERNS=(
    "python3 app.py"
    "python.*app.py"
    "gunicorn.*app"
    "flask.*run"
    "apscheduler"
    "scheduler"
)

# 超时设置（秒）
GRACEFUL_TIMEOUT=10
FORCE_KILL_DELAY=2

# 解析参数
FORCE_MODE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--force)
            FORCE_MODE=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# 函数：显示标题
print_title() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# 函数：查找所有相关进程
find_processes() {
    local pids=""
    
    # 方法1: 通过PID文件
    if [ -f logs/app.pid ]; then
        PID=$(cat logs/app.pid 2>/dev/null)
        if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
            pids="$pids $PID"
        fi
    fi
    
    if [ -f logs/scheduler.pid ]; then
        PID=$(cat logs/scheduler.pid 2>/dev/null)
        if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
            pids="$pids $PID"
        fi
    fi
    
    # 方法2: 通过进程名查找
    for pattern in "${PROCESS_PATTERNS[@]}"; do
        NEW_PIDS=$(pgrep -f "$pattern" 2>/dev/null || true)
        if [ -n "$NEW_PIDS" ]; then
            pids="$pids $NEW_PIDS"
        fi
    done
    
    # 方法3: 通过项目目录查找
    PROJECT_PIDS=$(pgrep -f "$SCRIPT_DIR" 2>/dev/null | grep -v "$$" || true)
    if [ -n "$PROJECT_PIDS" ]; then
        pids="$pids $PROJECT_PIDS"
    fi
    
    # 方法4: 通过端口查找（如果指定了端口）
    if [ -n "$PORT" ]; then
        PORT_PID=$(lsof -ti:$PORT 2>/dev/null || true)
        if [ -n "$PORT_PID" ]; then
            pids="$pids $PORT_PID"
        fi
    fi
    
    # 去重并排序
    echo "$pids" | tr ' ' '\n' | sort -u | grep -v "^$" | grep -v "^$$$" | tr '\n' ' '
}

# 函数：优雅停止进程
graceful_stop() {
    local pid=$1
    local timeout=${2:-$GRACEFUL_TIMEOUT}
    
    if ! kill -0 $pid 2>/dev/null; then
        return 0
    fi
    
    echo -e "${YELLOW}优雅停止进程 (PID: $pid)...${NC}"
    
    # 发送SIGTERM信号
    kill -TERM $pid 2>/dev/null || return 1
    
    # 等待进程退出
    local elapsed=0
    while kill -0 $pid 2>/dev/null && [ $elapsed -lt $timeout ]; do
        sleep 1
        elapsed=$((elapsed + 1))
        echo -n "."
    done
    echo ""
    
    if kill -0 $pid 2>/dev/null; then
        return 1  # 进程仍在运行
    else
        return 0  # 进程已退出
    fi
}

# 函数：强制杀死进程
force_kill() {
    local pid=$1
    
    if ! kill -0 $pid 2>/dev/null; then
        return 0
    fi
    
    echo -e "${RED}强制杀死进程 (PID: $pid)...${NC}"
    kill -9 $pid 2>/dev/null || true
    sleep $FORCE_KILL_DELAY
    
    if kill -0 $pid 2>/dev/null; then
        echo -e "${RED}❌ 无法杀死进程 (PID: $pid)${NC}"
        return 1
    else
        echo -e "${GREEN}✅ 进程已杀死 (PID: $pid)${NC}"
        return 0
    fi
}

# 函数：杀死进程树（包括子进程）
kill_process_tree() {
    local pid=$1
    
    if ! kill -0 $pid 2>/dev/null; then
        return 0
    fi
    
    # 获取所有子进程
    local children=$(pgrep -P $pid 2>/dev/null || true)
    
    # 先杀死子进程
    for child in $children; do
        kill_process_tree $child
    done
    
    # 再杀死父进程
    kill -9 $pid 2>/dev/null || true
}

# 函数：强制杀死进程树
force_kill_tree() {
    local pid=$1
    
    echo -e "${RED}强制杀死进程树 (PID: $pid)...${NC}"
    
    # 获取所有子进程
    local children=$(pgrep -P $pid 2>/dev/null || true)
    
    # 先杀死子进程
    for child in $children; do
        force_kill_tree $child
    done
    
    # 再杀死父进程
    kill -9 $pid 2>/dev/null || true
}

# 开始停止
print_title "停止个人AI理财助手"

# 获取所有相关进程
PIDS=$(find_processes)

if [ -z "$PIDS" ]; then
    echo -e "${YELLOW}未找到运行中的进程${NC}"
else
    echo -e "${YELLOW}找到以下进程:${NC}"
    for PID in $PIDS; do
        PROC_INFO=$(ps -p $PID -o pid,comm,args --no-headers 2>/dev/null || echo "$PID unknown")
        echo "   - $PROC_INFO"
    done
    echo ""
fi

# 强制模式
if [ "$FORCE_MODE" = true ]; then
    echo -e "${RED}使用强制模式...${NC}"
    for PID in $PIDS; do
        force_kill_tree $PID
    done
    # 清理PID文件
    rm -f logs/app.pid logs/scheduler.pid
    echo -e "${GREEN}✅ 强制停止完成${NC}"
    exit 0
fi

# 优雅停止模式
STOPPED_COUNT=0
FAILED_COUNT=0

for PID in $PIDS; do
    if graceful_stop $PID; then
        STOPPED_COUNT=$((STOPPED_COUNT + 1))
        echo -e "${GREEN}✅ 进程已停止 (PID: $PID)${NC}"
    else
        FAILED_COUNT=$((FAILED_COUNT + 1))
        echo -e "${RED}⚠️ 进程未响应 (PID: $PID)${NC}"
    fi
done

# 处理未响应的进程
if [ $FAILED_COUNT -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}有 $FAILED_COUNT 个进程未响应，是否强制杀死？ (y/n)${NC}"
    read -t 5 -r ANSWER || ANSWER="n"
    if [[ $ANSWER =~ ^[Yy]$ ]]; then
        echo -e "${RED}强制杀死未响应进程...${NC}"
        for PID in $PIDS; do
            if kill -0 $PID 2>/dev/null; then
                force_kill_tree $PID
            fi
        done
    else
        echo -e "${YELLOW}跳过强制杀死，进程可能仍在运行${NC}"
    fi
fi

# 二次检查：确保所有进程都已停止
echo ""
echo -e "${YELLOW}检查残留进程...${NC}"
REMAINING_PIDS=$(find_processes)

if [ -n "$REMAINING_PIDS" ]; then
    echo -e "${RED}发现残留进程: $REMAINING_PIDS${NC}"
    echo -e "${YELLOW}尝试强制清理...${NC}"
    for PID in $REMAINING_PIDS; do
        force_kill_tree $PID
    done
fi

# 清理PID文件
rm -f logs/app.pid logs/scheduler.pid

# 清理可能残留的socket文件
rm -f /tmp/flask_*.sock 2>/dev/null || true
rm -f /tmp/gunicorn_*.sock 2>/dev/null || true

# 最终检查
FINAL_PIDS=$(find_processes)
if [ -z "$FINAL_PIDS" ]; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✅ 所有进程已停止${NC}"
    echo -e "${GREEN}========================================${NC}"
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}⚠️ 仍有进程残留: $FINAL_PIDS${NC}"
    echo -e "${RED}请手动检查: ps aux | grep python${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi

print_title "停止完成"