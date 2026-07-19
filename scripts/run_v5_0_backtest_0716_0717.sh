#!/bin/bash
# v5.0 回测 7/16-7/17
# 用法: 在服务器上直接运行此脚本
set -euo pipefail

PROJECT_DIR="/home/investmentofficehku/.openclaw/workspace/projects/duanxianxia"
cd "$PROJECT_DIR"

echo ">>> 拉取最新代码..."
git pull origin main

echo ""
echo ">>> 运行 v5.0 回测 7/16 + 7/17..."
cd scripts
python3 duanxianxia_v5_0_backtest_dates.py --dates 2026-07-16,2026-07-17 --text

echo ""
echo ">>> 完成! 结果保存在:"
echo "    $PROJECT_DIR/reports/_audit/v5_0_premarket/2026-07-16.json"
echo "    $PROJECT_DIR/reports/_audit/v5_0_premarket/2026-07-17.json"