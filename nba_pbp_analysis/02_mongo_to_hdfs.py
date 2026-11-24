"""[저장] MongoDB 의 중계 텍스트 문서를 HDFS(Pseudo 모드 Hadoop)로 내보내기

MongoDB(nba.pbp_games) --(JSON Lines)--> HDFS /user/nba/raw_pbp/
PySpark 는 이후 HDFS 경로에서 원천 데이터를 읽는다.
"""
import json
import os
import subprocess
import tempfile

from pymongo import MongoClient

import config

HDFS_BIN = os.path.join(os.environ.get("HADOOP_HOME", "/opt/hadoop-3.4.1"), "bin", "hdfs")
N_SPLITS = 8   # HDFS 파일 분할 수 (Spark 파티션 병렬성)


def main():
    col = MongoClient(config.MONGO_URI)[config.MONGO_DB].pbp_games
    tmp = tempfile.mkdtemp(prefix="nba_raw_")
    files = [open(os.path.join(tmp, f"part-{i:05d}.jsonl"), "w", encoding="utf-8") for i in range(N_SPLITS)]
    n = 0
    for i, g in enumerate(col.find({}, sort=[("_id", 1)])):
        files[i % N_SPLITS].write(json.dumps(g, ensure_ascii=False) + "\n")
        n += 1
    for f in files:
        f.close()
    dest = f"{config.HDFS_BASE}/raw_pbp"
    subprocess.run([HDFS_BIN, "dfs", "-rm", "-r", "-f", "-skipTrash", dest], check=False,
                   stderr=subprocess.DEVNULL)
    subprocess.run([HDFS_BIN, "dfs", "-mkdir", "-p", dest], check=True)
    subprocess.run([HDFS_BIN, "dfs", "-put", "-f"] + [os.path.join(tmp, x) for x in sorted(os.listdir(tmp))]
                   + [dest], check=True)
    out = subprocess.run([HDFS_BIN, "dfs", "-du", "-s", "-h", dest], capture_output=True, text=True).stdout
    print(f"[hdfs] {n} 경기 문서 -> {config.HDFS_URL}{dest}  ({out.strip()})")


if __name__ == "__main__":
    main()
