#!/bin/bash

# 个人AI理财助手 - 守护进程启动脚本
# 支持从任意目录运行，自动检测项目根目录

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 获取脚本所在目录（项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 项目名称（用于进程识别）
PROJECT_NAME="personal_finance_assistant"
APP_NAME="app.py"

# 配置文件（可选）
CONFIG_FILE="${SCRIPT_DIR}/.env"
if [ -f "$CONFIG_FILE" ]; then
    echo -e "${BLUE}加载配置文件: $CONFIG_FILE${NC}"
    source "$CONFIG_FILE"
fi

# 默认配置
PORT=${PORT:-8080}
HOST=${HOST:-0.0.0.0}
DEBUG=${DEBUG:-false}
WORKERS=${WORKERS:-1}

# 日志目录
mkdir -p logs

# 函数：显示标题
print_title() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# 函数：检查端口是否被占用
check_port() {
    local port=$1
    if lsof -i:$port > /dev/null 2>&1; then
        return 0  # 端口被占用
    else
        return 1  # 端口空闲
    fi
}

# 函数：获取占用端口的PID
get_port_pid() {
    local port=$1
    lsof -ti:$port 2>/dev/null | head -1
}

print_title "启动个人AI理财助手"

# 停止旧进程（确保干净启动）
echo -e "${YELLOW}检查并停止旧进程...${NC}"
"${SCRIPT_DIR}/stop.sh" --force

# 等待进程完全退出
sleep 2

# 检查端口是否被占用
if check_port $PORT; then
    PORT_PID=$(get_port_pid $PORT)
    echo -e "${RED}⚠️ 端口 $PORT 仍被占用 (PID: $PORT_PID)${NC}"
    echo -e "${YELLOW}尝试强制释放端口...${NC}"
    kill -9 $PORT_PID 2>/dev/null || true
    sleep 1
fi

# 清理旧的PID文件
rm -f logs/app.pid
rm -f logs/scheduler.pid

# 启动新进程
print_title "启动服务"
echo -e "${GREEN}配置信息:${NC}"
echo "   项目目录: $SCRIPT_DIR"
echo "   监听地址: $HOST:$PORT"
echo "   调试模式: $DEBUG"
echo "   工作进程: $WORKERS"
echo ""

# 设置环境变量
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH}"
export FLASK_APP="${APP_NAME}"
export FLASK_DEBUG="${DEBUG}"
export DEBUG="${DEBUG}"
export USE_RELOADER="false"

# 优先使用项目虚拟环境 Python，避免依赖缺失
PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
    echo -e "${YELLOW}⚠️ 未检测到 .venv，回退到系统 python3${NC}"
fi

# 使用nohup后台运行
nohup "$PYTHON_BIN" app.py > logs/app.log 2>&1 &

# 获取新进程ID
APP_PID=$!
echo $APP_PID > logs/app.pid

echo -e "${GREEN}✅ 主进程已启动 (PID: $APP_PID)${NC}"

# 等待服务启动
echo -n "等待服务启动"
for i in {1..10}; do
    sleep 1
    echo -n "."
    if curl -s "http://localhost:$PORT/health" > /dev/null 2>&1; then
        echo ""
        echo -e "${GREEN}✅ 服务启动成功！${NC}"
        break
    fi
    if [ $i -eq 10 ]; then
        echo ""
        echo -e "${YELLOW}⚠️ 服务可能还在启动中，请稍后检查${NC}"
    fi
done

# 显示进程信息
echo ""
print_title "运行状态"
echo -e "${GREEN}✅ 服务运行中${NC}"
echo "   PID: $APP_PID"
echo "   访问地址: http://localhost:$PORT"
echo "   健康检查: curl http://localhost:$PORT/health"
echo ""
echo -e "${BLUE}常用命令:${NC}"
echo "   查看日志: tail -f logs/app.log"
echo "   停止服务: ./stop.sh"
echo "   重启服务: ./restart.sh"
echo "   查看状态: ./status.sh"

# 可选：显示健康状态
sleep 1
if curl -s "http://localhost:$PORT/health" > /dev/null 2>&1; then
    HEALTH_RESPONSE=$(curl -s "http://localhost:$PORT/health")
    echo -e "${GREEN}✅ 健康检查通过${NC}"
    echo "   响应: $HEALTH_RESPONSE"
else
    echo -e "${YELLOW}⚠️ 健康检查失败，请检查日志: tail -f logs/app.log${NC}"
fi

print_title "启动完成"