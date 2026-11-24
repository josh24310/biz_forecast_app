"""nba_parser 단위 테스트:  python -m pytest tests/ -q   (또는 python tests/test_parser.py)"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import nba_parser as P  # noqa: E402


def ev(no, text, team, clock="PT10M00.00S", period=1, pid=None, apid=None, name=None):
    return {"no": no, "period": period, "clock": clock, "team": team, "text": text,
            "ref": {"personId": pid, "assistPersonId": apid, "playerNameI": name}}


def test_classify():
    assert P.classify("MISS J. Brown 24' 3PT") == "3pt"
    assert P.classify("J. Brown driving Layup (5 PTS) (D. White 2 AST)") == "2pt"
    assert P.classify("MISS C. Capela Free Throw 1 of 2") == "freethrow"
    assert P.classify("J. Tatum BLOCK (1 BLK)") == "block"
    assert P.classify("J. Johnson traveling TURNOVER (1 TO)") == "turnover"
    assert P.classify("TEAM offensive REBOUND") == "rebound"
    assert P.classify("SUB in: D. Roddy") == "substitution"


def test_clock():
    assert P.clock_to_sec("PT11M58.00S") == 718.0
    assert P.clock_to_sec("PT00M39.30S") == 39.3


def test_box_and_name_disambiguation():
    # 같은 팀에 'J. Williams' 가 두 명 (선수 ID 로 구분되어야 함)
    events = [
        ev(1, "Period Start", None, "PT12M00.00S"),
        ev(2, "J. Williams 25' 3PT (3 PTS) (J. Williams 1 AST)", "OKC", "PT11M30.00S", pid=1, apid=2, name="J. Williams"),
        ev(3, "MISS T. Test 10' Jump Shot", "BOS", "PT11M00.00S", pid=9, name="T. Test"),
        ev(4, "J. Williams REBOUND (Off:0 Def:1)", "OKC", "PT10M58.00S", pid=2, name="J. Williams"),
        ev(5, "T. Test shooting personal FOUL (1 PF) (Williams 2 FT)", "BOS", "PT10M40.00S", pid=9, name="T. Test"),
        ev(6, "J. Williams Free Throw 1 of 2 (4 PTS)", "OKC", "PT10M40.00S", pid=1, name="J. Williams"),
        ev(7, "MISS J. Williams Free Throw 2 of 2", "OKC", "PT10M40.00S", pid=1, name="J. Williams"),
        ev(8, "TEAM offensive REBOUND", "OKC", "PT10M40.00S"),
        ev(9, "Period End", None, "PT00M00.00S"),
    ]
    game = {"_id": "T1", "gameDate": "2025-01-01", "homeTeam": "OKC", "awayTeam": "BOS",
            "finalHome": 4, "finalAway": 0, "events": events}
    r = P.parse_game(game)
    by = {(p["team"], p["personId"]): p for p in r["players"]}
    a, b = by[("OKC", 1)], by[("OKC", 2)]
    assert (a["PTS"], a["FGM"], a["FG3M"], a["FTM"], a["FTA"]) == (4, 1, 1, 1, 2)
    assert (b["AST"], b["DREB"], b["REB"]) == (1, 1, 1)
    assert by[("BOS", 9)]["PF"] == 1 and by[("BOS", 9)]["FGA"] == 1
    okc = next(t for t in r["teams"] if t["team"] == "OKC")
    assert okc["PTS"] == 4 and okc["REB"] == 1 and okc["TEAM_REB"] == 1 and okc["win"] == 1
    assert all(ok == tot for ok, tot in r["checks"].values())       # 누적 카운터 전부 일치
    assert a["MIN"] == 12.0 and a["starter"] == 1                    # 쿼터 내내 코트에 있었다고 추정


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
