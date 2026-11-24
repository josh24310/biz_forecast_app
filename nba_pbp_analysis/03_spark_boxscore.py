"""[전처리 -> 분석] PySpark: HDFS 중계 텍스트 -> 선수별 기록지(Box Score) 생성 + 검증

1) HDFS 의 원천 JSON 텍스트를 RDD 로 읽어 경기 단위로 nba_parser.parse_game 적용 (분산 처리)
2) 선수-경기 기록지 / 팀-경기 기록 / 슛 이벤트 DataFrame 생성
3) Spark SQL 로 시즌 누적·평균, 고급지표(TS%, eFG%, Game Score) 계산
4) 검증: 파서 누적값 vs 문장 내 공식 누적 카운터, 팀 득점 vs 최종 스코어, 출전시간 합계
5) 결과 저장: HDFS(parquet) + MongoDB 컬렉션 + outputs/tables/*.csv
"""
import json
import os
import time

from pyspark.sql import functions as F, Row

import config
from spark_common import get_spark, save_mongo

TABLES = os.path.join(config.OUT_DIR, "tables")


def parse_line(line):
    import nba_parser
    g = json.loads(line)
    r = nba_parser.parse_game(g)
    max_period = max(e["period"] for e in g["events"])
    return [("players", p) for p in r["players"]] + \
           [("teams", t) for t in r["teams"]] + \
           [("shots", s) for s in r["shots"]] + \
           [("checks", {"stat": k, "ok": v[0], "total": v[1]}) for k, v in r["checks"].items()] + \
           [("games", {"gameId": g["_id"], "gameDate": g["gameDate"], "homeTeam": g["homeTeam"],
                       "awayTeam": g["awayTeam"], "finalHome": g["finalHome"], "finalAway": g["finalAway"],
                       "periods": max_period, "nEvents": r["nEvents"], "unknown": r["unknown"]})]


def main():
    t0 = time.time()
    spark = get_spark("nba-boxscore")
    os.makedirs(TABLES, exist_ok=True)

    raw = spark.sparkContext.textFile(config.HDFS_RAW)
    print(f"[spark] HDFS 원천 파일 파티션: {raw.getNumPartitions()}")
    parsed = raw.flatMap(parse_line).cache()

    def to_df(kind):
        return spark.createDataFrame(parsed.filter(lambda x: x[0] == kind).map(lambda x: Row(**x[1])))

    players, teams, shots = to_df("players"), to_df("teams"), to_df("shots")
    checks, games = to_df("checks"), to_df("games")

    # ---- 선수 사전(풀네임) 결합: MongoDB nba.players ----
    from pymongo import MongoClient
    roster = list(MongoClient(config.MONGO_URI)[config.MONGO_DB].players.find())
    roster_df = spark.createDataFrame([(int(r["_id"]), r["fullName"]) for r in roster], "personId long, fullName string")
    players = players.join(F.broadcast(roster_df), "personId", "left") \
        .withColumn("fullName", F.coalesce("fullName", "player"))

    # ---- 고급 지표 ----
    players = players.withColumn(
        "GmSc",  # Hollinger Game Score
        F.round(F.col("PTS") + 0.4 * F.col("FGM") - 0.7 * F.col("FGA") - 0.4 * (F.col("FTA") - F.col("FTM"))
                + 0.7 * F.col("OREB") + 0.3 * F.col("DREB") + F.col("STL") + 0.7 * F.col("AST")
                + 0.7 * F.col("BLK") - 0.4 * F.col("PF") - F.col("TOV"), 1))
    players.createOrReplaceTempView("player_box")
    teams.createOrReplaceTempView("team_box")
    games.createOrReplaceTempView("games")

    player_season = spark.sql("""
        SELECT personId, FIRST(fullName) AS player, FIRST(player) AS nameI, team, COUNT(*) AS GP, SUM(starter) AS GS,
               ROUND(AVG(MIN),1) AS MPG, ROUND(AVG(PTS),1) AS PPG, ROUND(AVG(REB),1) AS RPG,
               ROUND(AVG(AST),1) AS APG, ROUND(AVG(STL),1) AS SPG, ROUND(AVG(BLK),1) AS BPG,
               ROUND(AVG(TOV),1) AS TOPG, ROUND(AVG(GmSc),1) AS GmSc,
               SUM(MIN) AS MIN, SUM(PTS) AS PTS, SUM(FGM) AS FGM, SUM(FGA) AS FGA,
               SUM(FG3M) AS FG3M, SUM(FG3A) AS FG3A, SUM(FTM) AS FTM, SUM(FTA) AS FTA,
               SUM(OREB) AS OREB, SUM(DREB) AS DREB, SUM(REB) AS REB, SUM(AST) AS AST,
               SUM(STL) AS STL, SUM(BLK) AS BLK, SUM(TOV) AS TOV, SUM(PF) AS PF,
               ROUND(SUM(FGM)/NULLIF(SUM(FGA),0),3) AS FG_PCT,
               ROUND(SUM(FG3M)/NULLIF(SUM(FG3A),0),3) AS FG3_PCT,
               ROUND(SUM(FTM)/NULLIF(SUM(FTA),0),3) AS FT_PCT,
               ROUND(SUM(PTS)/NULLIF(2*(SUM(FGA)+0.44*SUM(FTA)),0),3) AS TS_PCT,
               ROUND((SUM(FGM)+0.5*SUM(FG3M))/NULLIF(SUM(FGA),0),3) AS EFG_PCT
        FROM player_box GROUP BY personId, team""")

    team_season = spark.sql("""
        SELECT t.team, COUNT(*) AS GP, SUM(t.win) AS W, COUNT(*)-SUM(t.win) AS L,
               ROUND(AVG(t.PTS),1) AS PTS, ROUND(AVG(t.oppPTS),1) AS OPP_PTS,
               ROUND(AVG(t.PTS - t.oppPTS),1) AS MARGIN,
               ROUND(SUM(t.FGM)/SUM(t.FGA),3) AS FG_PCT, ROUND(SUM(t.FG3M)/SUM(t.FG3A),3) AS FG3_PCT,
               ROUND(AVG(t.FG3A),1) AS FG3A, ROUND(AVG(t.REB),1) AS REB, ROUND(AVG(t.AST),1) AS AST,
               ROUND(AVG(t.TOV),1) AS TOV
        FROM team_box t GROUP BY t.team ORDER BY W DESC""")

    # ---- 검증 ----
    validation = checks.groupBy("stat").agg(F.sum("ok").alias("matched"), F.sum("total").alias("total")) \
        .withColumn("accuracy", F.round(F.col("matched") / F.col("total"), 5)).orderBy("stat")
    minutes = spark.sql("""
        SELECT AVG(ABS(m.team_min - (240 + 25*(g.periods-4)))) AS mae_min,
               AVG(m.team_min / (240 + 25*(g.periods-4))) AS coverage
        FROM (SELECT gameId, team, SUM(MIN) AS team_min FROM player_box GROUP BY gameId, team) m
        JOIN games g ON m.gameId = g.gameId""").first()

    val_pd = validation.toPandas()
    val_pd.loc[len(val_pd)] = ["TEAM_MINUTES(coverage)", None, None, round(minutes["coverage"], 5)]
    tot_ev = games.agg(F.sum("nEvents"), F.sum("unknown")).first()
    val_pd.loc[len(val_pd)] = ["TEXT_PARSE_COVERAGE", int(tot_ev[0] - tot_ev[1]), int(tot_ev[0]),
                               round(1 - tot_ev[1] / tot_ev[0], 5)]
    val_pd.to_csv(os.path.join(TABLES, "validation.csv"), index=False)
    print("\n[검증] 파서 누적값 vs 중계 문장 속 공식 누적 카운터")
    print(val_pd.to_string(index=False))
    print(f"[검증] 팀 출전시간 합계 평균 오차 {minutes['mae_min']:.1f}분 (커버리지 {minutes['coverage']:.1%})")

    # ---- 저장 ----
    for name, df in [("player_box", players), ("team_box", teams), ("shots", shots),
                     ("player_season", player_season), ("team_season", team_season), ("games", games)]:
        df.write.mode("overwrite").parquet(f"{config.HDFS_OUT}/{name}")
    save_mongo(players, "player_box", ["personId", "gameId"])
    save_mongo(teams, "team_box", ["gameId", "team"])
    save_mongo(player_season, "player_season", ["personId"])
    save_mongo(team_season, "team_season", ["team"])
    save_mongo(shots, "shots", ["personId"])
    save_mongo(games, "games", ["gameId"])

    player_season.orderBy(F.desc("PPG")).filter("GP >= 40").toPandas() \
        .to_csv(os.path.join(TABLES, "player_season_stats.csv"), index=False)
    team_season.toPandas().to_csv(os.path.join(TABLES, "team_season_stats.csv"), index=False)
    players.filter("gameId = '0022400001'").orderBy("team", F.desc("MIN")).toPandas() \
        .to_csv(os.path.join(TABLES, "boxscore_sample_0022400001.csv"), index=False)

    print(f"\n[spark] 선수-경기 기록 {players.count():,}행, 팀-경기 {teams.count():,}행, "
          f"슛 {shots.count():,}건 -> HDFS {config.HDFS_OUT}, MongoDB 저장 완료 ({time.time()-t0:.0f}s)")
    print("\n[분석] 시즌 평균 득점 TOP 10 (40경기 이상)")
    player_season.filter("GP >= 40").orderBy(F.desc("PPG")) \
        .select("player", "team", "GP", "MPG", "PPG", "RPG", "APG", "TS_PCT").show(10, truncate=False)
    team_season.select("team", "W", "L", "PTS", "OPP_PTS", "MARGIN").show(8)
    spark.stop()


if __name__ == "__main__":
    main()
