"""[시각화] MongoDB / Spark 결과 -> outputs/charts/*.png (matplotlib)"""
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np
import pandas as pd
from pymongo import MongoClient

import config
import nba_parser

CH = os.path.join(config.OUT_DIR, "charts")
TB = os.path.join(config.OUT_DIR, "tables")
os.makedirs(CH, exist_ok=True)

# ---- 스타일: 한글 폰트 + 검증된 범주형 팔레트(고정 순서) ----
for fp in [os.path.join(config.BASE_DIR, "..", "fonts", "NanumGothic.ttf"),
           os.path.join(config.BASE_DIR, "fonts", "NanumGothic.ttf")]:
    if os.path.exists(fp):
        font_manager.fontManager.addfont(fp)
        plt.rcParams["font.family"] = [font_manager.FontProperties(fname=fp).get_name(), "DejaVu Sans"]
        break
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
INK, INK2, MUTED, GRID, BASE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
plt.rcParams.update({
    "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb", "savefig.facecolor": "#fcfcfb",
    "axes.edgecolor": BASE, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "axes.titleweight": "bold",
    "axes.titlesize": 13, "axes.titlecolor": INK, "axes.titlelocation": "left",
    "font.size": 10, "axes.unicode_minus": False, "legend.frameon": False,
})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(CH, name), dpi=150)
    plt.close(fig)
    print("[viz]", name)


def chart_validation():
    v = pd.read_csv(os.path.join(TB, "validation.csv"))
    names = {"PTS": "득점", "REB": "리바운드", "AST": "어시스트", "STL": "스틸", "BLK": "블록",
             "TOV": "턴오버", "PF": "개인파울", "TEAM_PTS_vs_FINAL": "팀 득점 = 최종 스코어",
             "TEAM_MINUTES(coverage)": "팀 출전시간 합계 커버리지", "NAME_LINK": "이름↔선수ID 연결", "TEXT_PARSE_COVERAGE": "문장 파싱 커버리지"}
    v["name"] = v["stat"].map(names).fillna(v["stat"])
    v = v.sort_values("accuracy")
    fig, ax = plt.subplots(figsize=(8, 4.6))
    ax.barh(v["name"], v["accuracy"] * 100, color=SERIES[0], height=0.6)
    for y, a in enumerate(v["accuracy"]):
        ax.text(a * 100 + 0.005, y, f"{a*100:.2f}%", va="center", color=INK2, fontsize=9)
    ax.set_xlim(99.5, 100.12)
    ax.set_xlabel("일치율 (%)")
    ax.grid(axis="y", visible=False)
    ax.set_title("텍스트 파싱 기록지 검증: 중계 문장 속 공식 누적 기록과의 일치율")
    save(fig, "01_validation_accuracy.png")


def chart_game(db, gid):
    g = db.pbp_games.find_one({"_id": gid})
    ev = sorted(g["events"], key=lambda e: (e["period"], -nba_parser.clock_to_sec(e["clock"]), e["no"]))
    t, margin = [], []
    for e in ev:
        p = e["period"]
        elapsed = (p - 1) * 720 if p <= 4 else 2880 + (p - 5) * 300
        elapsed += nba_parser.period_length(p) - nba_parser.clock_to_sec(e["clock"])
        t.append(elapsed / 60)
        margin.append(e["ref"]["scoreHome"] - e["ref"]["scoreAway"])
    t, margin = np.array(t), np.array(margin)
    home, away = g["homeTeam"], g["awayTeam"]
    fig, ax = plt.subplots(figsize=(9, 3.8))
    ax.step(t, margin, where="post", color=INK2, linewidth=1.2)
    ax.fill_between(t, margin, 0, where=margin >= 0, step="post", color=SERIES[0], alpha=0.35, linewidth=0)
    ax.fill_between(t, margin, 0, where=margin < 0, step="post", color=SERIES[1], alpha=0.35, linewidth=0)
    ax.axhline(0, color=BASE, linewidth=1)
    top = max(abs(margin).max(), 5) + 3
    ax.text(0.3, top - 1.5, f"{home} 리드", color=INK2, fontsize=9, va="top")
    ax.text(0.3, -top + 1.5, f"{away} 리드", color=INK2, fontsize=9, va="bottom")
    ax.set_ylim(-top, top)
    for q in range(1, int(t.max() // 12) + 1):
        ax.axvline(q * 12, color=GRID, linewidth=0.8, linestyle="--")
    ax.set_xlabel("경기 시간 (분)")
    ax.set_ylabel(f"점수차 ({home} − {away})")
    ax.set_title(f"경기 흐름: {away} {g['finalAway']} @ {home} {g['finalHome']}  ({g['gameDate']})")
    save(fig, f"02_game_flow_{gid}.png")

    # 기록지(박스스코어) 표 이미지
    box = pd.DataFrame(list(db.player_box.find({"gameId": gid}, {"_id": 0})))
    cols = ["player", "MIN", "PTS", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "OREB", "DREB", "REB",
            "AST", "STL", "BLK", "TOV", "PF", "GmSc"]
    fig, axes = plt.subplots(2, 1, figsize=(12, 7.2))
    for ax, team in zip(axes, [away, home]):
        d = box[box.team == team].sort_values(["starter", "MIN"], ascending=False)
        tot = d[cols[2:-1]].sum()
        rows = [[("* " if s else "") + p] + [f"{m:.0f}"] + [str(int(x)) for x in r] + [f"{gs:.1f}"]
                for p, s, m, r, gs in zip(d.fullName, d.starter, d.MIN, d[cols[2:-1]].values, d.GmSc)]
        rows.append(["TEAM 합계", "240" if d.MIN.sum() < 260 else f"{d.MIN.sum():.0f}"]
                    + [str(int(x)) for x in tot] + [""])
        ax.axis("off")
        tab = ax.table(cellText=rows, colLabels=["선수 (*선발)"] + cols[1:], loc="center", cellLoc="center")
        tab.auto_set_font_size(False)
        tab.set_fontsize(8.5)
        tab.scale(1, 1.25)
        for (r, c), cell in tab.get_celld().items():
            cell.set_edgecolor(GRID)
            if r == 0:
                cell.set_facecolor("#eef3fb")
                cell.set_text_props(weight="bold", color=INK)
            if r == len(rows):
                cell.set_text_props(weight="bold")
            if c == 0:
                cell.set_width(0.16)
                cell._loc = "left"
        ax.set_title(f"{team}  {int(tot.PTS)}점", loc="left", fontsize=12)
    fig.suptitle(f"선수별 기록지 (중계 텍스트에서 자동 생성) · {away} @ {home} · {g['gameDate']}",
                 x=0.01, ha="left", fontweight="bold")
    save(fig, f"03_boxscore_{gid}.png")


def chart_top_players():
    ps = pd.read_csv(os.path.join(TB, "player_season_stats.csv"))
    top = ps.sort_values("PPG", ascending=False).head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 5.6))
    ax.barh(top.player + " (" + top.team + ")", top.PPG, color=SERIES[0], height=0.62)
    for y, (p, ts) in enumerate(zip(top.PPG, top.TS_PCT)):
        ax.text(p + 0.3, y, f"{p:.1f}  · TS {ts:.3f}", va="center", fontsize=8.5, color=INK2)
    ax.set_xlim(0, top.PPG.max() * 1.25)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("경기당 득점 (PPG)")
    ax.set_title("2024-25 시즌 득점 TOP 15 (40경기 이상, 중계 텍스트 집계)")
    save(fig, "04_top_scorers.png")


def chart_shots(db):
    s = pd.DataFrame(list(db.shots.find({"distance": {"$ne": None}}, {"_id": 0, "distance": 1, "made": 1, "is3": 1})))
    s = s[s.distance <= 35]
    s["bin"] = (s.distance // 2) * 2
    g = s.groupby("bin").agg(fga=("made", "size"), fg=("made", "mean")).reset_index()
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(9, 6), sharex=True, gridspec_kw={"height_ratios": [1.3, 1]})
    a1.plot(g.bin + 1, g.fg * 100, color=SERIES[0], linewidth=2, marker="o", markersize=5)
    a1.set_ylabel("야투 성공률 (%)")
    a1.axvline(23.5, color=MUTED, linestyle="--", linewidth=1)
    a1.text(24, a1.get_ylim()[1] * 0.95, "3점 라인(약 22~23.75ft)", color=INK2, fontsize=9, va="top")
    a1.set_title("슛 거리별 성공률과 시도 수 (중계 문장의 \"26' 3PT\" 등 거리 표현에서 추출, 거리 생략 레이업/덩크는 1ft)")
    a2.bar(g.bin + 1, g.fga / 1000, width=1.7, color=SERIES[0], alpha=0.75)
    a2.set_ylabel("시도 수 (천 회)")
    a2.set_xlabel("슛 거리 (ft)")
    a2.grid(axis="x", visible=False)
    save(fig, "05_shot_distance.png")


def chart_shot_tokens():
    t = pd.read_csv(os.path.join(TB, "ml_shot_text_tokens.csv"))
    # 거리 토큰(distNN)은 05_shot_distance 차트에서 다루므로 여기서는 '동작/슛 종류' 단어만 표시
    t = t[~t.token.str.match(r"^(dist.*|3pt|num|shot|jump)$")]
    sel = pd.concat([t.head(9), t.tail(9)]).drop_duplicates().sort_values("coef")
    fig, ax = plt.subplots(figsize=(8, 6))
    colors = [SERIES[1] if c < 0 else SERIES[0] for c in sel.coef]
    ax.barh(sel.token, sel.coef, color=colors, height=0.65)
    ax.axvline(0, color=BASE, linewidth=1)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("로지스틱 회귀 계수 (TF-IDF 기준, +면 성공 확률↑)")
    ax.text(0.99, 0.02, "성공↑ 표현", transform=ax.transAxes, ha="right", color=INK2)
    ax.text(0.01, 0.97, "성공↓ 표현", transform=ax.transAxes, ha="left", va="top", color=INK2)
    ax.set_title("MLlib(A) 슛 묘사 문장 → 성공 확률: 슛 동작 단어의 영향 (거리 통제)")
    save(fig, "06_shot_text_tokens.png")


def roc(y, p):
    order = np.argsort(-p)
    y = np.asarray(y)[order]
    tpr = np.concatenate([[0], np.cumsum(y) / y.sum()])
    fpr = np.concatenate([[0], np.cumsum(1 - y) / (1 - y).sum()])
    return fpr, tpr


def chart_models():
    res = json.load(open(os.path.join(TB, "ml_results.json")))
    f = pd.read_csv(os.path.join(TB, "ml_win_model_features.csv"))
    kor = {"home": "홈 경기", "d_FG_PCT": "야투율 차", "d_FG3_PCT": "3점 성공률 차", "d_FT_PCT": "자유투율 차",
           "d_FG3A": "3점 시도 차", "d_FTA": "자유투 시도 차", "d_OREB": "공격리바 차", "d_DREB": "수비리바 차",
           "d_AST": "어시스트 차", "d_STL": "스틸 차", "d_BLK": "블록 차", "d_TOV": "턴오버 차", "d_PF": "파울 차"}
    f = f.sort_values("rf_importance")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.8))
    a1.barh(f.feature.map(kor), f.rf_importance, color=SERIES[0], height=0.62)
    a1.grid(axis="y", visible=False)
    a1.set_xlabel("RandomForest 변수 중요도")
    a1.set_title("MLlib(B) 승패를 가르는 기록 (양 팀 차이)")
    for name, file, key, c in [("경기 후 기록 기반 LR", "ml_win_model_test_scores.csv", ("win_model", "lr_auc"), SERIES[0]),
                               ("경기 전 누적기록 기반 LR", "ml_pregame_test_scores.csv", ("pregame_model", "auc"), SERIES[1])]:
        d = pd.read_csv(os.path.join(TB, file))
        fpr, tpr = roc(d.label.values, d.p.values)
        a2.plot(fpr, tpr, color=c, linewidth=2, label=f"{name} (AUC {res[key[0]][key[1]]:.3f})")
    a2.plot([0, 1], [0, 1], color=MUTED, linestyle="--", linewidth=1, label="무작위 (AUC 0.5)")
    a2.set_xlabel("위양성률 (FPR)")
    a2.set_ylabel("진양성률 (TPR)")
    a2.legend(loc="lower right", fontsize=9)
    a2.set_title("승패 예측 ROC (테스트: 2025-03-01 이후 경기)")
    save(fig, "07_win_models.png")


def name_clusters(prof):
    """군집 프로필(평균) -> 사람이 읽을 유형 이름. 규칙을 순서대로 적용해 이름이 겹치지 않게 한다."""
    names, rest = {}, list(prof.index)

    def take(c, label):
        names[c] = label
        rest.remove(c)

    take(prof.PTS_36.idxmax(), "슈퍼스타 (득점·리바·어시 올라운드)")
    take(prof.loc[rest].FG3A_RATE.idxmin(), "전통적 빅맨 (림 근처)")
    take(prof.loc[rest].FG3A_RATE.idxmax(), "3&D 윙 (스팟업 3점)")
    if rest:
        take(prof.loc[rest].PTS_36.idxmax(), "주득점 윙·볼핸들러")
    for c in list(rest):
        take(c, "인사이드 포워드·스트레치 빅" if prof.loc[c].REB_36 > prof.REB_36.median() else "롤플레이어 가드/윙")
    return names


def chart_clusters():
    d = pd.read_csv(os.path.join(TB, "ml_player_clusters.csv"))
    prof = d.groupby("cluster")[["PTS_36", "AST_36", "REB_36", "BLK_36", "FG3A_RATE"]].mean()
    names = name_clusters(prof)
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    for i, c in enumerate(sorted(d.cluster.unique())):
        s = d[d.cluster == c]
        ax.scatter(s.pc1, s.pc2, s=28, color=SERIES[i], alpha=0.85, edgecolor="#fcfcfb", linewidth=0.8,
                   label=f"{names[c]} ({len(s)}명)")
    placed = []   # 라벨 겹침 방지 (데이터 좌표 기준 간단한 greedy 배치)
    for _, r in d.sort_values("PPG", ascending=False).head(12).iterrows():
        x, y = r.pc1 + 0.06, r.pc2 + 0.08
        while any(abs(x - px) < 1.1 and abs(y - py) < 0.22 for px, py in placed):
            y -= 0.24
        placed.append((x, y))
        ax.annotate(r.player, (r.pc1, r.pc2), xytext=(x, y), fontsize=8, color=INK2,
                    arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.5) if abs(y - r.pc2) > 0.2 else None)
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=9, title="KMeans 유형", title_fontsize=9)
    ax.set_title("MLlib(C) 선수 유형 군집화 (per-36 기록 + 슛거리, KMeans → PCA 2D)")
    save(fig, "08_player_clusters.png")
    pd.Series(names).to_csv(os.path.join(TB, "ml_player_cluster_names.csv"), header=["name"])


def chart_teams():
    t = pd.read_csv(os.path.join(TB, "team_season_stats.csv"))
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.scatter(t.PTS, t.OPP_PTS, s=40 + (t.W - t.W.min()) * 3, color=SERIES[0], alpha=0.75,
               edgecolor="#fcfcfb", linewidth=1.2)
    placed = []   # 라벨 겹침 방지: 오른쪽 -> 위 -> 아래 -> 왼쪽 순으로 빈 자리 탐색
    for _, r in t.sort_values("W", ascending=False).iterrows():
        for dx, dy, ha in [(0.25, 0, "left"), (0.25, -0.45, "left"), (0.25, 0.45, "left"), (-0.25, 0, "right"),
                           (-0.25, 0.45, "right"), (-0.25, -0.45, "right")]:
            x0 = r.PTS + dx if ha == "left" else r.PTS + dx - 2.0
            if not any(abs(x0 - px) < 2.0 and abs(r.OPP_PTS + dy - py) < 0.4 for px, py in placed):
                break
        placed.append((x0, r.OPP_PTS + dy))
        ax.annotate(f"{r.team} {r.W}-{r.L}", (r.PTS, r.OPP_PTS), xytext=(r.PTS + dx, r.OPP_PTS + dy),
                    fontsize=8, color=INK2, ha=ha, va="center")
    lo, hi = min(t.PTS.min(), t.OPP_PTS.min()) - 1, max(t.PTS.max(), t.OPP_PTS.max()) + 1
    ax.plot([lo, hi], [lo, hi], color=MUTED, linestyle="--", linewidth=1)
    ax.invert_yaxis()
    ax.set_xlabel("경기당 득점 (높을수록 공격 ↑)")
    ax.set_ylabel("경기당 실점 (위로 갈수록 수비 ↑)")
    ax.set_title("팀별 공격력 vs 수비력 (점 크기 = 승수, 점선 위 = 득실 +)")
    save(fig, "09_team_offense_defense.png")


def main(gid="0022400001"):
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    chart_validation()
    chart_game(db, gid)
    chart_top_players()
    chart_shots(db)
    chart_shot_tokens()
    chart_models()
    chart_clusters()
    chart_teams()


if __name__ == "__main__":
    main(*sys.argv[1:])
