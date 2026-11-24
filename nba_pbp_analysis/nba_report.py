"""게임/선수 분석 조회 시스템 (MongoDB 기반 CLI)

  python nba_report.py games --team BOS [--month 2025-01]   경기 목록
  python nba_report.py game 0022400001                     경기 기록지 (쿼터별 스코어 + 선수별 박스스코어)
  python nba_report.py player "N. Jokić" [--last 10]       선수 시즌 기록 + 최근 경기 로그 + 거리별 슛
  python nba_report.py team OKC                            팀 성적 + 주요 선수
  python nba_report.py search "Curry.*3PT" [--limit 20]    중계 문장 검색 (정규식)
"""
import argparse
import re

import pandas as pd
from pymongo import MongoClient

import config
import nba_parser

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 40)
BOX_COLS = ["player", "MIN", "PTS", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
            "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF", "GmSc"]


def db():
    return MongoClient(config.MONGO_URI)[config.MONGO_DB]


def cmd_games(a):
    q = {"$or": [{"homeTeam": a.team}, {"awayTeam": a.team}]} if a.team else {}
    if a.month:
        q["gameDate"] = {"$regex": f"^{a.month}"}
    rows = list(db().pbp_games.find(q, {"events": 0}).sort("gameDate", 1))
    for g in rows:
        print(f"{g['_id']}  {g['gameDate']}  {g['awayTeam']:>3} {g['finalAway']:>3} @ "
              f"{g['homeTeam']:<3} {g['finalHome']:>3}")
    print(f"총 {len(rows)} 경기")


def quarter_scores(g):
    ev = sorted(g["events"], key=lambda e: (e["period"], -nba_parser.clock_to_sec(e["clock"]), e["no"]))
    end = {}
    for e in ev:
        end[e["period"]] = (e["ref"]["scoreAway"], e["ref"]["scoreHome"])
    prev, out = (0, 0), {}
    for p in sorted(end):
        out[p] = (end[p][0] - prev[0], end[p][1] - prev[1])
        prev = end[p]
    return out


def cmd_game(a):
    d = db()
    g = d.pbp_games.find_one({"_id": a.game_id})
    if not g:
        raise SystemExit(f"경기 없음: {a.game_id}")
    qs = quarter_scores(g)
    labels = [f"Q{p}" if p <= 4 else f"OT{p-4}" for p in qs]
    print(f"\n=== {g['awayTeam']} @ {g['homeTeam']}  {g['gameDate']}  (gameId {g['_id']}) ===")
    print(f"{'':5}" + "".join(f"{l:>5}" for l in labels) + "    T")
    print(f"{g['awayTeam']:5}" + "".join(f"{v[0]:>5}" for v in qs.values()) + f"  {g['finalAway']:>3}")
    print(f"{g['homeTeam']:5}" + "".join(f"{v[1]:>5}" for v in qs.values()) + f"  {g['finalHome']:>3}")
    box = pd.DataFrame(list(d.player_box.find({"gameId": a.game_id}, {"_id": 0})))
    for team in [g["awayTeam"], g["homeTeam"]]:
        t = box[box.team == team].sort_values(["starter", "MIN"], ascending=False).copy()
        t["player"] = t.apply(lambda r: ("*" if r.starter else " ") + r.fullName, axis=1)
        tot = t[BOX_COLS[2:-1]].sum()
        print(f"\n[{team}] 기록지  (*선발)")
        print(t[BOX_COLS].to_string(index=False))
        print(f"  TEAM: PTS {tot.PTS}  FG {tot.FGM}/{tot.FGA} ({tot.FGM/tot.FGA:.3f})  "
              f"3P {tot.FG3M}/{tot.FG3A}  FT {tot.FTM}/{tot.FTA}  REB {tot.REB}  AST {tot.AST}  TOV {tot.TOV}")
    best = box.sort_values("GmSc", ascending=False).iloc[0]
    print(f"\n경기 MVP(Game Score 최고): {best.fullName} ({best.team}) {best.PTS}점 {best.REB}리바 "
          f"{best.AST}어시 GmSc {best.GmSc}")


def find_player(d, name):
    """풀네임/약칭(예: 'Nikola Jokić', 'N. Jokić', 'jokic') -> (personId, 풀네임)"""
    import unicodedata
    norm = lambda x: unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode().lower()
    people = {(r["personId"], r["player"], r["nameI"]) for r in d.player_season.find({}, {"personId": 1, "player": 1, "nameI": 1})}
    q = norm(name)
    exact = [p for p in people if q in (norm(p[1]), norm(p[2]))]
    cand = exact or [p for p in people if q in norm(p[1]) or q in norm(p[2])]
    if len({p[0] for p in cand}) == 1:
        return cand[0][0], cand[0][1]
    raise SystemExit(f"선수 '{name}' 후보: {sorted({p[1] for p in cand})[:20] if cand else '없음'}")


def cmd_player(a):
    d = db()
    pid, name = find_player(d, a.name)
    s = pd.DataFrame(list(d.player_season.find({"personId": pid}, {"_id": 0})))
    print(f"\n=== {name} 2024-25 시즌 (팀별) ===")
    print(s[["team", "GP", "GS", "MPG", "PPG", "RPG", "APG", "SPG", "BPG", "TOPG",
             "FG_PCT", "FG3_PCT", "FT_PCT", "TS_PCT", "GmSc"]].to_string(index=False))
    log = pd.DataFrame(list(d.player_box.find({"personId": pid}, {"_id": 0}).sort("gameDate", -1)))
    log["vs"] = log.apply(lambda r: ("vs " if r.home else "@ ") + r.opp, axis=1)
    print(f"\n최근 {a.last}경기")
    print(log.head(a.last)[["gameDate", "team", "vs", "gameId"] + BOX_COLS[1:]].to_string(index=False))
    b = log.sort_values("GmSc", ascending=False).iloc[0]
    print(f"\n시즌 최고 경기: {b.gameDate} {b.vs}  {b.PTS}점 {b.REB}리바 {b.AST}어시 (GmSc {b.GmSc})")
    c = d.player_clusters.find_one({"personId": pid})
    if c:
        try:
            nm = pd.read_csv(f"{config.OUT_DIR}/tables/ml_player_cluster_names.csv", index_col=0)["name"]
            print(f"MLlib 선수 유형: {nm.get(c['cluster'], c['cluster'])}")
        except FileNotFoundError:
            print(f"MLlib 선수 유형 군집: {c['cluster']}")
    sh = pd.DataFrame(list(d.shots.find({"personId": pid, "distance": {"$ne": None}}, {"_id": 0})))
    if len(sh):
        sh["구간"] = pd.cut(sh.distance, [-1, 3, 9, 15, 22, 27, 99],
                          labels=["0-3ft(림)", "4-9ft", "10-15ft", "16-22ft", "23-27ft(3점)", "28ft+"])
        tab = sh.groupby("구간", observed=False).made.agg(시도="size", 성공="sum")
        tab["성공률"] = (tab.성공 / tab.시도).round(3)
        print("\n거리별 야투 (중계 문장 거리 표현 기반)")
        print(tab.to_string())


def cmd_team(a):
    d = db()
    t = d.team_season.find_one({"team": a.team}, {"_id": 0})
    if not t:
        raise SystemExit(f"팀 없음: {a.team}")
    rank = sorted(d.team_season.find({}, {"team": 1, "W": 1}), key=lambda r: -r["W"])
    pos = [r["team"] for r in rank].index(a.team) + 1
    print(f"\n=== {a.team}  {t['W']}-{t['L']} (리그 승수 {pos}위) ===")
    print(pd.DataFrame([t]).to_string(index=False))
    p = pd.DataFrame(list(d.player_season.find({"team": a.team, "GP": {"$gte": 10}}, {"_id": 0})))
    print("\n주요 선수 (10경기 이상, PPG 순)")
    print(p.sort_values("PPG", ascending=False)[["player", "GP", "MPG", "PPG", "RPG", "APG", "TS_PCT", "GmSc"]]
          .head(12).to_string(index=False))


def cmd_search(a):
    pipe = [{"$match": {"events.text": {"$regex": a.pattern}}}, {"$unwind": "$events"},
            {"$match": {"events.text": {"$regex": a.pattern}}},
            {"$project": {"gameDate": 1, "homeTeam": 1, "awayTeam": 1, "e": "$events"}},
            {"$sort": {"gameDate": 1}}]
    n = 0
    for r in db().pbp_games.aggregate(pipe):
        n += 1
        if n <= a.limit:
            e = r["e"]
            clk = re.sub(r"PT(\d+)M([\d.]+)S", lambda m: f"{int(m.group(1))}:{float(m.group(2)):04.1f}", e["clock"])
            print(f"{r['gameDate']} {r['awayTeam']}@{r['homeTeam']} Q{e['period']} {clk:>7} [{e['team'] or '-'}] {e['text']}")
    print(f"총 {n}건 일치 (표시 {min(n, a.limit)}건)")


def main():
    ap = argparse.ArgumentParser(description="NBA 중계 텍스트 기반 기록지/분석 조회")
    sp = ap.add_subparsers(dest="cmd", required=True)
    p = sp.add_parser("games"); p.add_argument("--team"); p.add_argument("--month"); p.set_defaults(f=cmd_games)
    p = sp.add_parser("game"); p.add_argument("game_id"); p.set_defaults(f=cmd_game)
    p = sp.add_parser("player"); p.add_argument("name"); p.add_argument("--last", type=int, default=10)
    p.set_defaults(f=cmd_player)
    p = sp.add_parser("team"); p.add_argument("team"); p.set_defaults(f=cmd_team)
    p = sp.add_parser("search"); p.add_argument("pattern"); p.add_argument("--limit", type=int, default=20)
    p.set_defaults(f=cmd_search)
    a = ap.parse_args()
    a.f(a)


if __name__ == "__main__":
    main()
