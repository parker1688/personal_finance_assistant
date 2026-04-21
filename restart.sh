#!/bin/bash

# 个人AI理财助手 - 重启脚本
# 支持优雅重启和强制重启

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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

print_title "重启个人AI理财助手"

if [ "$FORCE_MODE" = true ]; then
    echo -e "${YELLOW}使用强制重启模式...${NC}"
else
    echo -e "${YELLOW}使用优雅重启模式...${NC}"
fi

# 步骤1: 停止服务
echo ""
echo -e "${BLUE}[1/3] 停止服务${NC}"

if [ "$FORCE_MODE" = true ]; then
    "${SCRIPT_DIR}/stop.sh" --force
else
    "${SCRIPT_DIR}/stop.sh"
fi

# 检查停止结果
if [ $? -ne 0 ]; then
    echo -e "${RED}停止服务失败，尝试强制停止...${NC}"
    "${SCRIPT_DIR}/stop.sh" --force
fi

# 步骤2: 等待服务完全停止
echo ""
echo -e "${BLUE}[2/3] 等待服务完全停止${NC}"
echo -n "等待"

# 等待所有进程退出（最多10秒）
for i in {1..10}; do
    sleep 1
    echo -n "."
    # 检查是否还有相关进程
    if ! pgrep -f "python3 app.py" > /dev/null 2>&1; then
        echo ""
        echo -e "${GREEN}✅ 所有进程已退出${NC}"
        break
    fi
    if [ $i -eq 10 ]; then
        echo ""
        echo -e "${YELLOW}⚠️ 部分进程可能未完全退出，继续启动...${NC}"
    fi
done

# 额外等待2秒确保端口释放
sleep 2

# 步骤3: 启动服务
echo ""
echo -e "${BLUE}[3/3] 启动服务${NC}"
"${SCRIPT_DIR}/start.sh"

# 检查启动结果
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✅ 重启完成${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    # 显示服务状态
    sleep 2
    if [ -f "${SCRIPT_DIR}/status.sh" ]; then
        "${SCRIPT_DIR}/status.sh"
    fi
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}❌ 重启失败${NC}"
    echo -e "${RED}请检查日志: tail -f logs/app.log${NC}"
    echo -e "${RED}========================================${NC}"
    exit 1
fi