#!/bin/bash
# 一键运行脚本
# 路径: scripts/run_all.sh

echo "=========================================="
echo "个人AI理财助手 - 数据采集与训练"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
	PYTHON_BIN="python3"
	echo "⚠️ 未检测到 .venv，回退到系统 python3"
fi

# 创建目录
mkdir -p data data/models scripts

# 1. 采集基础历史数据
echo ""
echo "[1/4] 采集基础历史数据..."
"$PYTHON_BIN" scripts/collect_historical_data.py --years 3 --export

# 2. 准备训练所需补充数据（基金 / ETF / 黄金 / 白银）
echo ""
echo "[2/4] 准备训练数据集..."
"$PYTHON_BIN" scripts/prepare_training_datasets.py

# 3. 按资产顺序训练AI模型
echo ""
echo "[3/4] 按资产顺序训练AI模型..."
"$PYTHON_BIN" scripts/train_asset_suite.py --stop-on-error

# 4. 启动Web服务
echo ""
echo "[4/4] 启动Web服务..."
"$PYTHON_BIN" app.py