"""공통 설정 (환경변수로 덮어쓸 수 있음)"""
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("NBA_DATA_DIR", os.path.join(BASE_DIR, "data"))
OUT_DIR = os.path.join(BASE_DIR, "outputs")

SEASON = os.environ.get("NBA_SEASON", "2024")          # 2024 = 2024-25 시즌
SOURCE_URL = ("https://raw.githubusercontent.com/shufinskiy/nba_data/main/"
              f"datasets/cdnnba_{SEASON}.tar.xz")

PLAYERS_URL = ("https://raw.githubusercontent.com/shufinskiy/nba_data/main/"
               f"datasets/nbastats_{SEASON}.tar.xz")         # 선수 풀네임 사전용

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "nba")

HDFS_URL = os.environ.get("HDFS_URL", "hdfs://localhost:9000")
HDFS_BASE = os.environ.get("HDFS_BASE", "/user/nba")
HDFS_RAW = f"{HDFS_URL}{HDFS_BASE}/raw_pbp"            # Mongo -> HDFS 원천 중계 텍스트
HDFS_OUT = f"{HDFS_URL}{HDFS_BASE}/curated"            # Spark 결과 (parquet)
