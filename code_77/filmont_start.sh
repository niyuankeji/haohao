#!/bin/bash

if [ -z "$1" ]; then
  echo "❌ 请输入会话编号，例如：sh s.sh 1"
  exit 1
fi

SESSION_NAME="filmont_$1"

tmux new-session -d -s "$SESSION_NAME" "source /root/myenv/bin/activate && python filmont.py $SESSION_NAME"
echo "✅ 已启动 tmux 会话: $SESSION_NAME"

