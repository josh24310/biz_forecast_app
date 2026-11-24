"""NBA 중계 텍스트(play-by-play 문장) 파서.

입력: 한 경기의 중계 이벤트 목록 (각 이벤트는 period, clock, team, text 만 사용)
출력: 선수별 기록지(box score) 행, 팀 기록 행, 슛 이벤트 행, 검증 통계

기록 집계는 오직 `text`(자연어 중계 문장)에서 정규식으로 추출한다.
문장 끝에 붙어 있는 누적 카운터 "(12 PTS)", "(3 AST)", "(Off:1 Def:4)" 등은
파서가 계산한 누적값과 비교하는 '정답지(검증용)'로만 사용한다.
"""
import re
from collections import defaultdict

STAT_KEYS = ["MIN", "PTS", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
             "OREB", "DREB", "REB", "AST", "STL", "BLK", "TOV", "PF"]

SUFFIX = r"(?:Jr\.|Sr\.|II|III|IV|V)"
RE_FALLBACK_NAME = re.compile(rf"^([A-Z][\w'’\-]*\.?(?: [A-Z]\.)? [^\s(]+(?: {SUFFIX})?)")
RE_SUB = re.compile(r"^SUB (in|out): (.+?)\s*$")
RE_REB = re.compile(r"^(.+?) REBOUND \(Off:(\d+) Def:(\d+)\)")
RE_TEAM_REB = re.compile(r"^TEAM (offensive|defensive) REBOUND")
RE_STL = re.compile(r"^(.+?) STEAL \((\d+) STL\)")
RE_BLK = re.compile(r"^(.+?) BLOCK \((\d+) BLK\)")
RE_TOV = re.compile(r"TURNOVER \((\d+) TO\)")
RE_TEAM_TOV = re.compile(r"Team TURNOVER")
RE_PF = re.compile(r"FOUL \((\d+) PF\)")
RE_TECH = re.compile(r"FOUL \((\d+) Tech\)")
RE_PTS = re.compile(r"\((\d+) PTS\)")
RE_AST = re.compile(r"\(([^()]+?) (\d+) AST\)")
RE_FT = re.compile(r"Free Throw")
RE_DIST = re.compile(r" (\d+)' ")
RE_3PT = re.compile(r"\b3PT\b")
RE_CLOCK = re.compile(r"PT(\d+)M([\d.]+)S")

SHOT_TYPES = ["Dunk", "Layup", "Hook", "Fadeaway", "Step Back", "Pullup",
              "Floating", "Jump Shot", "Tip", "Putback", "Alley Oop"]


def clock_to_sec(clock):
    """'PT11M58.00S' -> 718.0 (해당 쿼터 남은 시간, 초)"""
    m = RE_CLOCK.match(clock or "")
    return int(m.group(1)) * 60 + float(m.group(2)) if m else 0.0


def period_length(period):
    return 720.0 if period <= 4 else 300.0


def classify(text):
    """중계 문장 -> 이벤트 유형 (텍스트 규칙 기반 분류)"""
    t = text or ""
    if t.startswith("SUB "):
        return "substitution"
    if t.startswith("Jump Ball"):
        return "jumpball"
    if t in ("Period Start", "Period End"):
        return "period"
    if t == "Game End":
        return "game"
    if "Timeout" in t:
        return "timeout"
    if t.startswith("Ejection"):
        return "ejection"
    if "REBOUND" in t:
        return "rebound"
    if "STEAL (" in t:
        return "steal"
    if "BLOCK (" in t:
        return "block"
    if "TURNOVER" in t:
        return "turnover"
    if "FOUL" in t:
        return "foul"
    if "VIOLATION" in t:
        return "violation"
    if RE_FT.search(t):
        return "freethrow"
    if RE_3PT.search(t):
        return "3pt"
    if t.startswith("MISS ") or RE_PTS.search(t):
        return "2pt"
    return "unknown"


class NameResolver:
    """경기별 선수 이름 사전.
    'SUB in: X', 'X REBOUND (..)', 'X STEAL (..)', 'X BLOCK (..)'처럼 이름 경계가 분명한
    문장에서 먼저 이름 사전을 만든 뒤, 슛/파울 문장에서는 가장 긴 사전 이름과 앞부분을 매칭한다."""

    def __init__(self, texts):
        names = set()
        for t in texts:
            for rx in (RE_SUB, RE_REB, RE_STL, RE_BLK):
                m = rx.match(t)
                if m:
                    names.add(m.group(2) if rx is RE_SUB else m.group(1))
        names.discard("TEAM")
        self.names = sorted(names, key=len, reverse=True)

    def leading(self, text):
        for n in self.names:
            if text.startswith(n + " ") or text == n:
                return n
        m = RE_FALLBACK_NAME.match(text)
        return m.group(1) if m else None


def parse_game(game):
    """game: {'_id', 'gameDate', 'homeTeam', 'awayTeam', 'events': [...]}
    반환: dict(players=[...], teams=[...], shots=[...], checks={...})"""
    gid = game["_id"]
    # actionNumber 는 사후 정정으로 순서가 뒤바뀐 경우가 있어 (쿼터, 경기시계, 번호) 순으로 시간 정렬
    events = sorted(game["events"], key=lambda e: (e["period"], -clock_to_sec(e["clock"]), e["no"]))
    resolver = NameResolver([e["text"] for e in events])

    box = defaultdict(lambda: {k: 0 for k in STAT_KEYS})
    team_extra = defaultdict(lambda: {"TEAM_REB": 0, "TEAM_TOV": 0})
    # 검증: 문장 속 누적 카운터 vs 파서 누적값
    checks = defaultdict(lambda: [0, 0])  # stat -> [일치, 전체]
    shots = []
    unknown = 0

    # 출전시간 계산용
    on_court = {}            # key -> (period, 시작 남은시간)
    seen_in_period = set()   # (period, key)
    sec_played = defaultdict(float)
    starters = set()
    cur_period = 1

    def touch(key, period, clock_s):
        """해당 쿼터에서 처음 등장한 선수는 쿼터 시작부터 코트에 있었다고 간주"""
        if (period, key) not in seen_in_period:
            seen_in_period.add((period, key))
            if key not in on_court:
                on_court[key] = (period, period_length(period))
                if period == 1:
                    starters.add(key)

    def close_period(period):
        for key, (p, start) in list(on_court.items()):
            if p == period:
                sec_played[key] += start
                del on_court[key]

    names = {}               # key -> 텍스트에서 추출한 이름

    def link(e, team, name, assist=False):
        """엔티티 링킹: 텍스트에서 뽑은 약칭 이름을 이벤트에 붙은 선수 ID 와 연결.
        'J. Williams' 처럼 같은 팀에 약칭이 겹치는 선수를 구분하기 위함.
        ID 가 없으면 (팀, 이름) 을 키로 사용."""
        ref = e.get("ref") or {}
        pid = ref.get("assistPersonId") if assist else ref.get("personId")
        key = (team, pid) if pid else (team, name)
        names.setdefault(key, name)
        if not assist and ref.get("playerNameI"):
            chk("NAME_LINK", name, ref["playerNameI"])
        return key

    def chk(stat, computed, reported):
        checks[stat][1] += 1
        if computed == reported:
            checks[stat][0] += 1

    for e in events:
        t, team, period = e["text"] or "", e.get("team"), e["period"]
        cs = clock_to_sec(e["clock"])
        if period != cur_period:
            close_period(cur_period)
            cur_period = period
        kind = classify(t)
        if kind == "unknown":
            unknown += 1
            continue
        if kind == "period":
            if t == "Period End":
                close_period(period)
            continue
        if kind in ("game", "timeout", "jumpball", "ejection") or not team:
            continue

        if kind == "substitution":
            io, name = RE_SUB.match(t).groups()
            key = link(e, team, name)
            if io == "out":
                touch(key, period, cs)
                if key in on_court:
                    p, start = on_court.pop(key)
                    sec_played[key] += start - cs
            else:
                seen_in_period.add((period, key))
                on_court[key] = (period, cs)
            continue

        if kind == "rebound":
            m = RE_REB.match(t)
            if not m:
                team_extra[team]["TEAM_REB"] += 1
                continue
            name, off, de = m.group(1), int(m.group(2)), int(m.group(3))
            key = link(e, team, name)
            touch(key, period, cs)
            # 카운터 증가분으로 공격/수비 리바운드 판정 (문장에 Off/Def 누적이 같이 표기됨)
            if off > box[key]["OREB"]:
                box[key]["OREB"] += 1
            else:
                box[key]["DREB"] += 1
            box[key]["REB"] += 1
            chk("REB", box[key]["OREB"] + box[key]["DREB"], off + de)
            continue

        if kind == "steal":
            m = RE_STL.match(t)
            key = link(e, team, m.group(1))
            touch(key, period, cs)
            box[key]["STL"] += 1
            chk("STL", box[key]["STL"], int(m.group(2)))
            continue

        if kind == "block":
            m = RE_BLK.match(t)
            key = link(e, team, m.group(1))
            touch(key, period, cs)
            box[key]["BLK"] += 1
            chk("BLK", box[key]["BLK"], int(m.group(2)))
            continue

        if kind == "turnover":
            m = RE_TOV.search(t)
            if not m:
                team_extra[team]["TEAM_TOV"] += 1
                continue
            key = link(e, team, resolver.leading(t))
            touch(key, period, cs)
            box[key]["TOV"] += 1
            chk("TOV", box[key]["TOV"], int(m.group(1)))
            continue

        if kind == "foul":
            m = RE_PF.search(t)
            if not m:
                continue  # 테크니컬 파울 등은 개인 파울에 포함하지 않음
            key = link(e, team, resolver.leading(t))
            touch(key, period, cs)
            box[key]["PF"] += 1
            chk("PF", box[key]["PF"], int(m.group(1)))
            continue

        if kind == "violation":
            continue

        # ---- 슛 (2점/3점/자유투) ----
        miss = t.startswith("MISS ")
        body = t[5:] if miss else t
        name = resolver.leading(body)
        key = link(e, team, name)
        touch(key, period, cs)
        b = box[key]
        if kind == "freethrow":
            b["FTA"] += 1
            if not miss:
                b["FTM"] += 1
                b["PTS"] += 1
        else:
            is3 = kind == "3pt"
            b["FGA"] += 1
            b["FG3A"] += is3
            if not miss:
                b["FGM"] += 1
                b["FG3M"] += is3
                b["PTS"] += 3 if is3 else 2
            dm = RE_DIST.search(" " + body + " ")
            low = body.lower()
            stype = next((s for s in SHOT_TYPES if s.lower() in low), "Other")
            dist = int(dm.group(1)) if dm else None
            # 레이업/덩크/팁인은 거리 표기가 생략되는 경우가 많음 -> 림 근처(1ft)로 대체하고 표시
            imputed = dist is None and stype in ("Dunk", "Layup", "Tip", "Putback", "Alley Oop")
            if imputed:
                dist = 1
            shots.append({"gameId": gid, "team": team, "player": name,
                          "personId": key[1] if isinstance(key[1], int) else None, "period": period,
                          "clockSec": cs, "made": int(not miss), "is3": int(is3),
                          "distance": dist, "distImputed": int(imputed), "shotType": stype})
        pm = RE_PTS.search(t)
        if pm:
            chk("PTS", b["PTS"], int(pm.group(1)))
        am = RE_AST.search(t)
        if am:
            akey = link(e, team, am.group(1), assist=True)
            touch(akey, period, cs)
            box[akey]["AST"] += 1
            chk("AST", box[akey]["AST"], int(am.group(2)))

    close_period(cur_period)
    for key, sec in sec_played.items():
        box[key]["MIN"] = round(sec / 60.0, 1)

    players = []
    for (team, ident), s in box.items():
        name = names.get((team, ident))
        if name is None:
            continue
        row = {"gameId": gid, "gameDate": game.get("gameDate"), "team": team,
               "opp": game["awayTeam"] if team == game.get("homeTeam") else game.get("homeTeam"),
               "home": int(team == game.get("homeTeam")), "player": name,
               "personId": ident if isinstance(ident, int) else None,
               "starter": int((team, ident) in starters)}
        row.update(s)
        players.append(row)

    teams = []
    for team in {p["team"] for p in players} | set(team_extra):
        rows = [p for p in players if p["team"] == team]
        agg = {k: sum(r[k] for r in rows) for k in STAT_KEYS if k != "MIN"}
        agg["TEAM_REB"] = team_extra[team]["TEAM_REB"]   # 팀 리바운드는 공식 기록처럼 REB 와 별도 집계
        agg["TOV"] += team_extra[team]["TEAM_TOV"]
        teams.append({"gameId": gid, "gameDate": game.get("gameDate"), "team": team,
                      "home": int(team == game.get("homeTeam")), **agg})
    # 팀 득점 vs 최종 스코어 (중계 데이터의 스코어 필드)
    final = {game.get("homeTeam"): game.get("finalHome"), game.get("awayTeam"): game.get("finalAway")}
    for tr in teams:
        tr["finalScore"] = final.get(tr["team"])
        chk("TEAM_PTS_vs_FINAL", tr["PTS"], tr["finalScore"])
    for tr in teams:
        others = [o for o in teams if o["team"] != tr["team"]]
        tr["oppPTS"] = others[0]["PTS"] if others else None
        tr["win"] = int(tr["oppPTS"] is not None and tr["PTS"] > tr["oppPTS"])

    return {"players": players, "teams": teams, "shots": shots,
            "checks": {k: v for k, v in checks.items()},
            "unknown": unknown, "nEvents": len(events)}
