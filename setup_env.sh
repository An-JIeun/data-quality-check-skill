#!/bin/bash
# data-quality-check-skill: Python 가상환경 자동 구축 및 의존성 설치 스크립트
# 사용법: ./setup_env.sh

set -e

VENV_DIR=".venv"
REQ_FILE="requirements.txt"

# 1. 가상환경 생성
if [ ! -d "$VENV_DIR" ]; then
  echo "[INFO] Python 가상환경 생성: $VENV_DIR"
  python3 -m venv "$VENV_DIR"
else
  echo "[INFO] 가상환경이 이미 존재합니다: $VENV_DIR"
fi

# 2. 가상환경 활성화 및 requirements 설치
source "$VENV_DIR/bin/activate"

if [ -f "$REQ_FILE" ]; then
  echo "[INFO] requirements.txt 설치"
  pip install --upgrade pip
  pip install -r "$REQ_FILE"
else
  echo "[WARN] requirements.txt 파일이 없습니다."
fi

echo "[SUCCESS] 가상환경 준비 완료. (deactivate로 종료)"
