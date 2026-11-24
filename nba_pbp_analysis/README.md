# NBA 중계 텍스트 기반 선수별 기록지 생성 및 게임/선수 분석 시스템

빅데이터관리론 기말 프로젝트용 구현입니다. NBA 2024-25 정규시즌 **1,230경기의 중계 문장 686,008개**(비정형 텍스트)를 다음 순서로 처리합니다.

1. MongoDB에 저장·관리합니다.
2. Pseudo 모드 Hadoop(HDFS)으로 내보냅니다.
3. PySpark로 문장을 파싱해 **선수별 기록지(Box Score)** 를 자동으로 만듭니다.
4. Spark MLlib으로 게임·선수 분석 모델을 학습합니다.
5. 결과를 시각화합니다.

```
"J. Brown driving Layup (5 PTS) (D. White 2 AST)"     ──►  J. Brown  PTS+2 FGM+1 FGA+1
"MISS Z. Risacher 26' 3PT - blocked"                  ──►  Z. Risacher  FGA+1 FG3A+1 (26ft)
"J. Tatum BLOCK (1 BLK)"                              ──►  J. Tatum  BLK+1
"SUB in: D. Roddy" / "SUB out: J. Johnson"            ──►  출전시간(MIN) 계산
```

## 1. 요구사항 충족 여부

| 요구사항 | 구현 | 검증 결과 |
|---|---|---|
| Pseudo 모드 Hadoop 위 PySpark | `scripts/setup_hadoop_pseudo.sh` (Hadoop 3.4.1 단일 노드 HDFS), Spark 4.0.1. 원천과 결과를 모두 `hdfs://localhost:9000/user/nba/` 에서 읽고 씀 | NameNode·DataNode 기동, `raw_pbp` 163MB 적재 확인 |
| 일정 크기 이상의 비정형 공공 데이터 | NBA.com 공식 실시간 중계(play-by-play) 문장, 2024-25 시즌 전체 | 1,230경기, 686,008문장, MongoDB 158MB |
| NoSQL DB 관리 (삽입/삭제/수정/검색) | MongoDB 7.0. `01_collect_to_mongo.py` 의 `crud_demo()` 에서 insert / update(`$push`, `$set`, `$inc`) / regex 검색 / 전문검색(text index) / delete 를 시연. 분석 결과도 컬렉션 9개로 저장 | 실행 로그에 CRUD 각 단계 결과 출력 |
| 수집-저장-전처리-모델링-분석-시각화 | `01`(수집·Mongo) → `02`(HDFS) → `03`(Spark 전처리·기록지) → `04`(MLlib) → `05`(시각화) → `nba_report.py`(조회 시스템) | `run_all.sh` 로 전체 파이프라인 재현 |
| Spark MLlib 사용 | 모델 4개: TF-IDF + 로지스틱 회귀(텍스트), 로지스틱 회귀·랜덤포레스트(승패), Window 함수 기반 경기 전 예측, KMeans + PCA(선수 유형) | 아래 4절 |
| Python 그래프/시각화/기계학습 함수 | matplotlib 차트 9종 (`outputs/charts`) | — |

## 2. 시스템 구성

```
GitHub 공개 아카이브(NBA.com 중계 JSON 모음)
        │  01_collect_to_mongo.py  (다운로드 · 경기 단위 문서화 · CRUD)
        ▼
MongoDB nba.pbp_games  ── 경기 1개 = 문서 1개 { homeTeam, awayTeam, events:[{period, clock, team, text, ref}] }
        │  02_mongo_to_hdfs.py
        ▼
HDFS /user/nba/raw_pbp/part-*.jsonl  (8개 분할 → Spark 8 파티션)
        │  03_spark_boxscore.py   (RDD flatMap(parse_game) → DataFrame → Spark SQL 집계 · 검증)
        ▼
HDFS /user/nba/curated/*.parquet  +  MongoDB player_box / team_box / player_season / team_season / shots / games
        │  04_spark_mllib.py      (MLlib 모델 4종)            05_visualize.py (차트)
        ▼
nba_report.py  — 경기 기록지·선수·팀 조회, 중계 문장 검색 (MongoDB 질의)
```

### 텍스트 → 기록지 변환 방식 (`nba_parser.py`)

1. **문장 유형 분류**: 키워드 규칙(`MISS`, `3PT`, `Free Throw`, `REBOUND`, `STEAL`, `BLOCK`, `TURNOVER`, `FOUL`, `SUB in/out` 등)으로 분류합니다. 원천 이벤트 유형과 99.97% 일치합니다.
2. **선수 이름 추출**: 경기마다 이름 사전을 만듭니다. `SUB in: X`, `X REBOUND (...)` 처럼 이름 경계가 분명한 문장에서 이름을 먼저 모은 뒤, 슛·파울 문장의 앞부분과 가장 길게 일치하는 이름을 찾습니다.
3. **엔티티 링킹**: 중계 문장은 이름을 약칭으로 씁니다(`J. Williams`). 그래서 OKC의 Jalen Williams와 Jaylin Williams처럼 같은 팀에 약칭이 겹치는 선수를 구분할 수 없습니다. 이를 해결하려고 텍스트에서 뽑은 이름을 이벤트에 붙은 선수 ID와 연결하고, MongoDB `players` 사전(569명)으로 풀네임을 붙입니다.
4. **출전시간**: SUB in/out 시각을 추적합니다. 쿼터 중 교체 기록 없이 처음 등장한 선수는 쿼터 시작부터 코트에 있었다고 봅니다. Q1에서 그렇게 등장한 선수는 선발로 처리합니다.
5. **시간순 정렬**: 사후 정정으로 이벤트 번호(`actionNumber`) 순서가 뒤바뀐 경우가 있습니다. 그래서 (쿼터, 경기시계, 번호) 순으로 정렬합니다. 이 정렬을 넣기 전에는 블록 일치율이 98.5%였고, 넣은 뒤 100%가 됐습니다.

## 3. 기록지 정확도 검증

중계 문장 끝에는 공식 기록원이 남긴 누적 카운터가 붙어 있습니다(`(12 PTS)`, `(3 AST)`, `(Off:1 Def:4)` 등). 파서는 이 카운터를 집계에는 쓰지 않고, **파서가 문장 하나하나를 더해 계산한 누적값과 비교하는 정답지로만** 씁니다.

| 항목 | 일치 / 전체 | 일치율 |
|---|---|---|
| 득점 PTS | 144,121 / 144,139 | 99.99% |
| 리바운드 REB | 108,518 / 108,520 | 99.998% |
| 어시스트·스틸·블록·턴오버·개인파울 | 전부 | 100% |
| 팀 득점 합계 = 최종 스코어 | 2,460 / 2,460 | 100% |
| 텍스트 이름 ↔ 선수 ID 연결 | 633,314 / 633,314 | 100% |
| 팀 출전시간 합계 (240분 + 연장 25분) | — | 99.99% (경기당 평균 오차 0.1분) |
| 문장 파싱 커버리지 | 685,823 / 686,008 | 99.97% |

공식 시즌 기록과 비교해도 일치합니다.
- 선수: Shai Gilgeous-Alexander 32.7점(76경기), Nikola Jokić 29.6점 12.7리바 10.2어시, Jokić의 61점 경기(2025-04-01 vs MIN)
- 팀: OKC 68승 14패, CLE 64승 18패, BOS 61승 21패

## 4. 분석 결과 (Spark MLlib)

| 모델 | 입력 → 출력 | 결과 (테스트셋) |
|---|---|---|
| (A) 슛 문장 → 성공 확률 | 슛 묘사 문장에서 정답 단서(MISS, 득점, 어시스트, 이름)를 지운 뒤 RegexTokenizer → CountVectorizer → IDF → 로지스틱 회귀. 예: `"pullup shot dist15"` | AUC 0.786, 정확도 71.1% (다수 클래스 기준선 53.3%) |
| (B) 승패 요인 (경기 후 기록) | 양 팀 기록 차이(득점 제외) → LR / RandomForest | LR AUC 0.997. 영향이 큰 순서: 야투율 차 > 수비 리바운드 차 > 3점 성공률 차 > 턴오버 차 |
| (B2) 경기 전 승패 예측 | 직전 경기까지의 누적 기록(Spark Window 함수) → LR | AUC 0.759, 정확도 69.1% (홈팀 승 기준선 53.6%) |
| (C) 선수 유형 군집화 | 출전 800분 이상 294명의 per-36 기록, 3점 시도 비중, 평균 슛거리, 림 근처 슛 비중 → StandardScaler → KMeans(k=5) → PCA | 슈퍼스타형(SGA·Jokić·Giannis·AD), 주득점 윙, 3&D 윙, 롤플레이어, 전통적 빅맨 |

학습·테스트 분할: (A)는 무작위 80/20, (B)와 (B2)는 시간순(2025-03-01 이후 경기를 테스트)입니다.

### 시각화 (`outputs/charts`)
| 파일 | 내용 |
|---|---|
| `01_validation_accuracy.png` | 기록지 검증 일치율 |
| `02_game_flow_*.png` | 경기 흐름(점수차 추이) |
| `03_boxscore_*.png` | 선수별 기록지 표 |
| `04_top_scorers.png` | 득점 TOP 15 |
| `05_shot_distance.png` | 슛 거리별 성공률과 시도 수 |
| `06_shot_text_tokens.png` | 슛 문장 단어의 영향(모델 A) |
| `07_win_models.png` | 승패 요인과 ROC 곡선(모델 B·B2) |
| `08_player_clusters.png` | 선수 유형 군집(모델 C) |
| `09_team_offense_defense.png` | 팀별 공격력 vs 수비력 |

## 5. 실행 방법

```bash
# 0) 환경: Java 17+ (Spark 4 는 Java 17/21), Python 3.10+
pip install -r requirements.txt
docker run -d --name nba-mongo -p 27017:27017 mongo:7.0      # 또는 Lab 의 MongoDB
bash scripts/setup_hadoop_pseudo.sh                           # Lab 의 Hadoop 이 있으면 생략 (HDFS_URL 만 맞춤)

# 1) 전체 파이프라인 (약 3분)
PYTHON=python3 bash run_all.sh

# 2) 조회 시스템
python nba_report.py game 0022400001          # 경기 기록지: 쿼터별 스코어 + 양 팀 선수별 박스스코어 + 경기 MVP
python nba_report.py player "jokic"           # 선수 시즌 기록·최근 경기·시즌 최고 경기·유형·거리별 야투
python nba_report.py player "J. Williams"     # 약칭이 겹치면 후보(Jalen / Jaylin) 안내
python nba_report.py team OKC                 # 팀 성적·주요 선수
python nba_report.py games --team BOS --month 2025-01
python nba_report.py search "Curry.*3PT" --limit 20   # 중계 문장 정규식 검색
```

환경 변수 `MONGO_URI`, `HDFS_URL`, `NBA_SEASON`(기본 2024 = 2024-25)으로 설정을 바꿀 수 있습니다. 다른 시즌도 `NBA_SEASON=2023` 처럼 지정하면 같은 파이프라인으로 처리됩니다.

## 6. 데이터 출처와 한계

- **출처**: NBA.com 공식 실시간 중계 JSON(`cdn.nba.com/static/json/liveData/playbyplay`)을 시즌 단위로 묶은 공개 아카이브 [shufinskiy/nba_data](https://github.com/shufinskiy/nba_data)의 `cdnnba_2024.tar.xz`입니다. 선수 풀네임 사전은 같은 저장소의 `nbastats_2024.tar.xz`에서 가져왔습니다.
- **기록 집계 vs 메타데이터**: 기록은 문장(`text`)에서만 집계합니다. `team` 태그와 선수 ID는 누가 했는지 식별하는 데만 쓰고, 원천 `actionType`과 점수 필드는 검증·라벨 용도로만 씁니다.
- **남은 오차**: 득점 18건, 리바운드 2건이 어긋납니다. 원천 데이터의 사후 정정(이벤트 삭제·수정) 때문인 것으로 보입니다.
- **출전시간 추정의 한계**: 쿼터 내내 아무 기록 없이 뛴 선수는 출전시간이 빠질 수 있습니다. 다만 팀 합계 기준 커버리지는 99.99%입니다.
- **트레이드 선수**: 팀별로 나누어 집계합니다(`player_season`의 키가 선수 ID + 팀).
- **모델 (B)의 성격**: 경기 후 기록으로 승패를 설명하는 모델입니다. 실제 예측 성능은 (B2)로 평가해야 합니다.
