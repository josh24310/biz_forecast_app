"""[모델링 -> 분석] Spark MLlib 3종 모델

(A) 슛 묘사 문장 -> 슛 성공 확률 (텍스트 마이닝)
    정답 단서(MISS/득점/어시스트/이름)를 지운 문장 -> RegexTokenizer -> CountVectorizer -> IDF
    -> LogisticRegression. 어떤 슛 표현(거리·동작)이 효율적인지 계수로 해석.
(B) 경기 승패 요인 분석 (경기 후 기록지 기반, 설명 모델)
    양 팀 기록지 차이(FG%, 3P%, FT%, 리바운드, 어시스트, 턴오버 등; 득점 제외)
    -> LogisticRegression / RandomForest, 시간 순 분할(3월 이후 = 테스트)
(B2) 경기 전 승패 예측: 직전 경기까지의 누적 기록만 사용 (Window 함수)
(C) 선수 유형 군집화
    시즌 per-36 기록 + 슛 거리/3점 비중 -> StandardScaler -> KMeans(실루엣으로 k 선택) -> PCA 2D
"""
import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator, ClusteringEvaluator,
                                   MulticlassClassificationEvaluator)
from pyspark.ml.feature import (IDF, PCA, RegexTokenizer, StandardScaler, StringIndexer,
                                VectorAssembler)
from pyspark.sql import functions as F

import config
from spark_common import get_spark, save_mongo

TABLES = os.path.join(config.OUT_DIR, "tables")
CUR = config.HDFS_OUT


def shot_text_model(spark, results):
    """슛 묘사 문장(거리/슛 종류/동작)만으로 성공 확률 예측.
    'MISS', '(n PTS)', '(이름 n AST)', 'blocked', 선수 이름을 제거해 정답 단서를 없앤 뒤 학습한다."""
    from pyspark.ml.feature import CountVectorizer
    raw = spark.read.json(config.HDFS_RAW)
    ev = raw.select(F.explode("events").alias("e")) \
        .filter(F.col("e.ref.actionType").isin("2pt", "3pt")) \
        .select(F.col("e.text").alias("text"))
    ev = ev.withColumn("label", F.when(F.col("text").startswith("MISS "), 0.0).otherwise(1.0))
    c = F.regexp_replace("text", r"^MISS ", "")
    c = F.regexp_replace(c, r"\([^)]*\)", " ")                       # (n PTS), (이름 n AST)
    c = F.regexp_replace(c, r"- blocked", " ")
    c = F.regexp_replace(c, r"^[A-Z][\w'\-.]*\.? (?:(?:da|de|del|van|der|la|le|di) )*[^ ]+( (Jr\.|Sr\.|II|III|IV))?",
                         " ")  # 선수 이름 (P.J. Washington, T. da Silva 등 포함)
    dist = F.regexp_extract("text", r"(\d+)' ", 1)
    c = F.concat_ws(" ", F.regexp_replace(c, r"\d+' ", " "),
                    F.when(dist != "", F.concat(F.lit("dist"), (F.floor(dist.cast("int") / 3) * 3).cast("string")))
                    .otherwise(F.lit("distna")))
    ev = ev.withColumn("clean", F.lower(c))
    train, test = ev.randomSplit([0.8, 0.2], seed=42)
    pipe = Pipeline(stages=[
        RegexTokenizer(inputCol="clean", outputCol="tokens", pattern=r"[^\p{L}0-9]+"),
        CountVectorizer(inputCol="tokens", outputCol="tf", minDF=20),
        IDF(inputCol="tf", outputCol="features"),
        LogisticRegression(maxIter=50, regParam=0.0005),
    ])
    model = pipe.fit(train)
    pred = model.transform(test)
    auc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(pred)
    acc = MulticlassClassificationEvaluator(metricName="accuracy").evaluate(pred)
    base = test.agg(F.avg("label")).first()[0]
    import pandas as pd
    vocab = model.stages[1].vocabulary
    coef = model.stages[-1].coefficients.toArray()
    pd.DataFrame({"token": vocab, "coef": coef}).sort_values("coef") \
        .to_csv(os.path.join(TABLES, "ml_shot_text_tokens.csv"), index=False)
    ex = ev.select("text", "clean").limit(3).collect()
    print(f"[A] 슛 문장 -> 성공확률  train={train.count():,} test={test.count():,}  "
          f"AUC={auc:.4f} ACC={acc:.4f} (다수클래스 기준 {max(base, 1-base):.4f})")
    for r in ex:
        print(f"     '{r.text}'  ->  '{r.clean.strip()}'")
    results["shot_text_model"] = {"train": train.count(), "test": test.count(), "auc": auc,
                                  "accuracy": acc, "baseline_acc": max(base, 1 - base)}


FEATS = ["FG_PCT", "FG3_PCT", "FT_PCT", "FG3A", "FTA", "OREB", "DREB", "AST", "STL", "BLK", "TOV", "PF"]


def win_model(spark, results):
    tb = spark.read.parquet(f"{CUR}/team_box") \
        .withColumn("FG_PCT", F.col("FGM") / F.col("FGA")) \
        .withColumn("FG3_PCT", F.col("FG3M") / F.col("FG3A")) \
        .withColumn("FT_PCT", F.when(F.col("FTA") > 0, F.col("FTM") / F.col("FTA")).otherwise(0.75))
    a, b = tb.alias("a"), tb.alias("b")
    pair = a.join(b, (F.col("a.gameId") == F.col("b.gameId")) & (F.col("a.team") != F.col("b.team"))) \
        .select(F.col("a.gameId"), F.col("a.gameDate"), F.col("a.team"), F.col("a.home"),
                F.col("a.win").cast("double").alias("label"),
                *[(F.col(f"a.{f}") - F.col(f"b.{f}")).alias(f"d_{f}") for f in FEATS])
    cols = ["home"] + [f"d_{f}" for f in FEATS]
    assembler = VectorAssembler(inputCols=cols, outputCol="raw")
    scaler = StandardScaler(inputCol="raw", outputCol="features", withMean=True)
    train = pair.filter(F.col("gameDate") < "2025-03-01")
    test = pair.filter(F.col("gameDate") >= "2025-03-01")
    auc_ev = BinaryClassificationEvaluator(metricName="areaUnderROC")
    acc_ev = MulticlassClassificationEvaluator(metricName="accuracy")
    out = {"train_rows": train.count(), "test_rows": test.count()}

    lr = Pipeline(stages=[assembler, scaler, LogisticRegression(maxIter=100)]).fit(train)
    p_lr = lr.transform(test)
    coefs = lr.stages[-1].coefficients.toArray().tolist()
    rf = Pipeline(stages=[assembler, scaler, RandomForestClassifier(numTrees=200, maxDepth=6, seed=7)]).fit(train)
    p_rf = rf.transform(test)
    imps = rf.stages[-1].featureImportances.toArray().tolist()
    out.update({"lr_auc": auc_ev.evaluate(p_lr), "lr_acc": acc_ev.evaluate(p_lr),
                "rf_auc": auc_ev.evaluate(p_rf), "rf_acc": acc_ev.evaluate(p_rf)})
    import pandas as pd
    pd.DataFrame({"feature": cols, "lr_coef_std": coefs, "rf_importance": imps}) \
        .sort_values("rf_importance", ascending=False) \
        .to_csv(os.path.join(TABLES, "ml_win_model_features.csv"), index=False)
    # ROC 곡선용 점수 저장
    p_lr.select("label", F.udf(lambda v: float(v[1]), "double")("probability").alias("p")) \
        .toPandas().to_csv(os.path.join(TABLES, "ml_win_model_test_scores.csv"), index=False)
    print(f"[B] 승패 예측  train={out['train_rows']} test={out['test_rows']}  "
          f"LR AUC={out['lr_auc']:.4f} ACC={out['lr_acc']:.4f} | RF AUC={out['rf_auc']:.4f} ACC={out['rf_acc']:.4f}")
    results["win_model"] = out


def pregame_model(spark, results):
    """경기 전 예측: 각 팀의 '직전 경기까지' 시즌 누적 기록(윈도 함수)만 사용"""
    from pyspark.sql.window import Window
    tb = spark.read.parquet(f"{CUR}/team_box").withColumn("MARGIN", F.col("PTS") - F.col("oppPTS"))
    w = Window.partitionBy("team").orderBy("gameDate").rowsBetween(Window.unboundedPreceding, -1)
    w10 = Window.partitionBy("team").orderBy("gameDate").rowsBetween(-10, -1)
    prior = tb.select("gameId", "gameDate", "team", "home", F.col("win").cast("double").alias("label"),
                      F.count("*").over(w).alias("n_prev"),
                      F.avg("win").over(w).alias("WIN_PCT"),
                      F.avg("MARGIN").over(w).alias("MARGIN"),
                      F.avg("MARGIN").over(w10).alias("MARGIN_L10"),
                      ((F.sum("FGM").over(w) + 0.5 * F.sum("FG3M").over(w)) / F.sum("FGA").over(w)).alias("EFG"),
                      F.avg("TOV").over(w).alias("TOV"), F.avg("OREB").over(w).alias("OREB"))
    feats = ["WIN_PCT", "MARGIN", "MARGIN_L10", "EFG", "TOV", "OREB"]
    a, b = prior.alias("a"), prior.alias("b")
    pair = a.join(b, (F.col("a.gameId") == F.col("b.gameId")) & (F.col("a.team") != F.col("b.team"))) \
        .filter((F.col("a.n_prev") >= 5) & (F.col("b.n_prev") >= 5)) \
        .select(F.col("a.gameDate"), F.col("a.home"), F.col("a.label"),
                *[(F.col(f"a.{f}") - F.col(f"b.{f}")).alias(f"d_{f}") for f in feats])
    cols = ["home"] + [f"d_{f}" for f in feats]
    train = pair.filter(F.col("gameDate") < "2025-03-01")
    test = pair.filter(F.col("gameDate") >= "2025-03-01")
    m = Pipeline(stages=[VectorAssembler(inputCols=cols, outputCol="raw"),
                         StandardScaler(inputCol="raw", outputCol="features", withMean=True),
                         LogisticRegression(maxIter=100, regParam=0.01)]).fit(train)
    p = m.transform(test)
    auc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(p)
    acc = MulticlassClassificationEvaluator(metricName="accuracy").evaluate(p)
    home_acc = test.agg(F.avg(F.when(F.col("home") == F.col("label"), 1.0).otherwise(0.0))).first()[0]
    import pandas as pd
    pd.DataFrame({"feature": cols, "coef_std": m.stages[-1].coefficients.toArray()}) \
        .to_csv(os.path.join(TABLES, "ml_pregame_features.csv"), index=False)
    p.select("label", F.udf(lambda v: float(v[1]), "double")("probability").alias("p")) \
        .toPandas().to_csv(os.path.join(TABLES, "ml_pregame_test_scores.csv"), index=False)
    print(f"[B2] 경기 전 승패 예측  train={train.count()} test={test.count()}  AUC={auc:.4f} ACC={acc:.4f} "
          f"(홈팀 승 기준선 {home_acc:.4f})")
    results["pregame_model"] = {"auc": auc, "accuracy": acc, "home_baseline": home_acc}


def player_clusters(spark, results):
    ps = spark.read.parquet(f"{CUR}/player_season").filter("MIN >= 800")
    shots = spark.read.parquet(f"{CUR}/shots").groupBy("personId", "team").agg(
        F.avg("distance").alias("avg_shot_dist"),
        F.avg(F.when(F.col("distance") <= 3, 1).otherwise(0)).alias("rim_rate"))
    df = ps.join(shots, ["personId", "team"], "left").fillna(0)
    per36 = ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FG3A", "FTA", "OREB"]
    for c in per36:
        df = df.withColumn(f"{c}_36", F.col(c) / F.col("MIN") * 36)
    df = df.withColumn("FG3A_RATE", F.col("FG3A") / F.col("FGA"))
    feats = [f"{c}_36" for c in per36] + ["FG3A_RATE", "TS_PCT", "avg_shot_dist", "rim_rate"]
    vec = VectorAssembler(inputCols=feats, outputCol="raw").transform(df)
    scaled = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True) \
        .fit(vec).transform(vec)
    ev = ClusteringEvaluator()
    sil = {}
    for k in range(3, 9):
        sil[k] = ev.evaluate(KMeans(k=k, seed=11).fit(scaled).transform(scaled))
    # 해석 가능성을 위해 최소 5개 유형 이상에서 실루엣 최대값 선택
    best_k = max((k for k in sil if k >= 5), key=lambda k: sil[k])
    km = KMeans(k=best_k, seed=11).fit(scaled)
    clustered = km.transform(scaled)
    pca = PCA(k=2, inputCol="features", outputCol="pca").fit(clustered)
    out = pca.transform(clustered)
    to_list = F.udf(lambda v: v.toArray().tolist(), "array<double>")
    res = out.select("personId", "player", "team", "GP", "MPG", "PPG", "RPG", "APG", *feats,
                     F.col("prediction").alias("cluster"), to_list("pca").alias("pca"))
    pdf = res.toPandas()
    pdf["pc1"] = pdf["pca"].map(lambda v: v[0])
    pdf["pc2"] = pdf["pca"].map(lambda v: v[1])
    prof = pdf.groupby("cluster")[feats + ["PPG"]].mean().round(2)
    prof["n"] = pdf.groupby("cluster").size()
    prof["examples"] = pdf.sort_values("PPG", ascending=False).groupby("cluster")["player"] \
        .apply(lambda s: ", ".join(s.head(4)))
    prof.to_csv(os.path.join(TABLES, "ml_player_cluster_profiles.csv"))
    pdf.drop(columns=["pca"]).to_csv(os.path.join(TABLES, "ml_player_clusters.csv"), index=False)
    save_mongo(res.drop("pca"), "player_clusters", ["personId"])
    print(f"[C] 선수 군집화  선수 {len(pdf)}명, 실루엣={ {k: round(v, 3) for k, v in sil.items()} } -> k={best_k}")
    print(prof[["n", "PTS_36", "REB_36", "AST_36", "BLK_36", "FG3A_RATE", "avg_shot_dist", "examples"]]
          .to_string())
    results["player_clusters"] = {"n_players": len(pdf), "silhouette": sil, "k": best_k,
                                  "pca_explained": pca.explainedVariance.toArray().tolist()}


def main():
    spark = get_spark("nba-mllib")
    os.makedirs(TABLES, exist_ok=True)
    results = {}
    shot_text_model(spark, results)
    win_model(spark, results)
    pregame_model(spark, results)
    player_clusters(spark, results)
    with open(os.path.join(TABLES, "ml_results.json"), "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=float)
    spark.stop()


if __name__ == "__main__":
    main()
