"""[수집 -> 저장] NBA 중계 텍스트 수집 후 MongoDB 적재 + CRUD(삽입/검색/수정/삭제) 시연

원천: NBA.com 공식 실시간 중계(play-by-play) JSON 을 시즌 단위로 모은 공개 데이터셋
      (github.com/shufinskiy/nba_data, cdnnba_<season>.tar.xz)
저장: MongoDB  nba.pbp_games  (경기 1개 = 문서 1개, 중계 문장 배열 내장)
"""
import csv
import io
import os
import sys
import tarfile
import time
import urllib.request
from datetime import datetime, timedelta

from pymongo import MongoClient, ASCENDING, TEXT

import config

csv.field_size_limit(10 ** 9)


def download(url=None):
    url = url or config.SOURCE_URL
    os.makedirs(config.DATA_DIR, exist_ok=True)
    path = os.path.join(config.DATA_DIR, os.path.basename(url))
    if not os.path.exists(path):
        print(f"[collect] downloading {url}")
        urllib.request.urlretrieve(url, path)
    print(f"[collect] archive: {path} ({os.path.getsize(path)/1e6:.1f} MB)")
    return path


def iter_rows(archive):
    with tarfile.open(archive, "r:xz") as tar:
        member = next(m for m in tar.getmembers() if m.name.endswith(".csv"))
        f = io.TextIOWrapper(tar.extractfile(member), encoding="utf-8", newline="")
        yield from csv.DictReader(f)


def to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def build_games(archive):
    games = {}
    for r in iter_rows(archive):
        gid = r["gameId"].zfill(10)
        g = games.get(gid)
        if g is None:
            # timeActual 은 UTC -> 미국 동부시간 기준 경기일로 변환
            utc = datetime.strptime(r["timeActual"][:19], "%Y-%m-%dT%H:%M:%S")
            g = games[gid] = {"_id": gid, "season": f"{config.SEASON}-{(int(config.SEASON)+1)%100:02d}",
                              "gameDate": (utc - timedelta(hours=5)).strftime("%Y-%m-%d"), "homeTeam": None, "awayTeam": None,
                              "_prev": (0, 0), "events": []}
        sh, sa = to_int(r["scoreHome"]) or 0, to_int(r["scoreAway"]) or 0
        team = r["teamTricode"] or None
        # 점수 변화로 홈/원정 팀 판별
        if team and (sh, sa) != g["_prev"]:
            if sh > g["_prev"][0]:
                g["homeTeam"] = team
            elif sa > g["_prev"][1]:
                g["awayTeam"] = team
        g["_prev"] = (sh, sa)
        g["events"].append({
            "no": int(r["actionNumber"]), "period": int(r["period"]), "clock": r["clock"],
            "team": team, "text": r["description"],
            # ref: 검증/라벨 용도로만 쓰는 원천 메타데이터 (기록 집계에는 사용하지 않음)
            "ref": {"actionType": r["actionType"], "personId": to_int(r["personId"]),
                    "playerNameI": r["playerNameI"] or None, "assistPersonId": to_int(r.get("assistPersonId")),
                    "scoreHome": sh, "scoreAway": sa},
        })
    for g in games.values():
        g["finalHome"], g["finalAway"] = g.pop("_prev")
        g["nEvents"] = len(g["events"])
    return list(games.values())


def build_players(archive):
    """선수 사전 (personId -> 풀네임): nbastats_<season> 의 PLAYERn_ID/NAME 에서 추출"""
    players = {}
    with tarfile.open(archive, "r:xz") as tar:
        member = next(m for m in tar.getmembers() if m.name.endswith(".csv"))
        f = io.TextIOWrapper(tar.extractfile(member), encoding="utf-8", newline="")
        for r in csv.DictReader(f):
            for i in (1, 2, 3):
                pid, name = to_int(r[f"PLAYER{i}_ID"]), r[f"PLAYER{i}_NAME"]
                if pid and name and r[f"PLAYER{i}_TEAM_ABBREVIATION"]:
                    players[pid] = {"_id": pid, "fullName": name}
    return list(players.values())


def crud_demo(col):
    print("\n[CRUD] --- MongoDB 관리 기능 시연 ---")
    test = {"_id": "TEST000001", "season": "demo", "gameDate": "2099-01-01",
            "homeTeam": "AAA", "awayTeam": "BBB", "finalHome": 0, "finalAway": 0,
            "events": [{"no": 1, "period": 1, "clock": "PT12M00.00S", "team": None,
                        "text": "Period Start", "ref": {}}], "nEvents": 1}
    col.insert_one(test)
    print("[CRUD] insert :", col.count_documents({"_id": "TEST000001"}), "건 삽입")
    col.update_one({"_id": "TEST000001"},
                   {"$push": {"events": {"no": 2, "period": 1, "clock": "PT11M40.00S", "team": "AAA",
                                         "text": "X. Test 25' 3PT (3 PTS)", "ref": {}}},
                    "$set": {"finalHome": 3}, "$inc": {"nEvents": 1}})
    print("[CRUD] update :", col.find_one({"_id": "TEST000001"}, {"nEvents": 1, "finalHome": 1}))
    n = col.count_documents({"events.text": {"$regex": "Jokić.*3PT"}})
    print(f"[CRUD] search : 'Jokić ... 3PT' 문장이 있는 경기 수 = {n}")
    q = col.aggregate([{"$match": {"$text": {"$search": "\"dunk\""}}},
                       {"$project": {"_id": 1}}, {"$count": "games"}])
    print("[CRUD] text search(전문검색 'dunk'):", next(q, {}).get("games"), "경기")
    col.delete_one({"_id": "TEST000001"})
    print("[CRUD] delete :", col.count_documents({"_id": "TEST000001"}), "건 남음")


def main():
    archive = download()
    t0 = time.time()
    games = build_games(archive)
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]
    col = db.pbp_games
    col.drop()
    col.insert_many(games, ordered=False)
    col.create_index([("gameDate", ASCENDING)])
    col.create_index([("homeTeam", ASCENDING), ("awayTeam", ASCENDING)])
    col.create_index([("events.text", TEXT)])
    n_ev = sum(g["nEvents"] for g in games)
    stats = db.command("collstats", "pbp_games")
    print(f"[collect] MongoDB {config.MONGO_DB}.pbp_games: {len(games)} 경기, {n_ev:,} 중계 문장, "
          f"{stats['size']/1e6:.1f} MB ({time.time()-t0:.1f}s)")
    plist = build_players(download(config.PLAYERS_URL))
    db.players.drop()
    db.players.insert_many(plist)
    print(f"[collect] MongoDB {config.MONGO_DB}.players: 선수 사전 {len(plist)}명")
    crud_demo(col)


if __name__ == "__main__":
    sys.exit(main())
