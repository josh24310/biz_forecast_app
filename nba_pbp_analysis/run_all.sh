#!/usr/bin/env bash
# 전체 파이프라인: 수집 -> 저장(MongoDB, HDFS) -> 전처리/기록지(PySpark) -> 모델링(MLlib) -> 시각화
# 사전 조건: MongoDB(27017), HDFS(9000) 기동.  PYTHON 으로 pyspark/pymongo/matplotlib 이 설치된 인터프리터 지정 가능.
set -euo pipefail
cd "$(dirname "$0")"
PY=${PYTHON:-python3}
$PY tests/test_parser.py
$PY 01_collect_to_mongo.py
$PY 02_mongo_to_hdfs.py
$PY 03_spark_boxscore.py
$PY 04_spark_mllib.py
$PY 05_visualize.py
$PY nba_report.py game 0022400001
