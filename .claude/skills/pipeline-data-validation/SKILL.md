---
name: pipeline-data-validation
description: "실시간 뱅킹 파이프라인의 데이터 품질·현실성을 통계적으로 검증한다. 생성된 이상거래(large_amount/high_velocity) 비율이 설계 의도와 부합하는지, Flink 집계가 원본 데이터와 정합하는지 확인. \"이상거래 비율 검증\", \"데이터 현실성\", \"정합성 확인\" 요청 시 반드시 사용. 눈대중 판단이 아니라 이론값/시뮬레이션값과의 통계적 비교가 핵심."
---

# 파이프라인 데이터 검증 (실시간 뱅킹)

이 스킬은 "적당해 보인다"가 아니라 "설계된 확률과 통계적으로 부합하는가"를 검증하는 절차를 담는다. 이 프로젝트는 생성기와 탐지 룰이 모두 코드로 정의돼 있어 이론값을 계산할 수 있다는 점을 활용한다.

## 절차

1. 파이프라인이 충분한 데이터를 쌓았는지 먼저 확인한다 (표본 크기가 작으면 통계적 결론이 무의미).
2. 검증 대상별로 "이론값/시뮬레이션값"과 "실측값"을 각각 구한다.
3. 통계적 판정 기준(신뢰구간)으로 비교한다.
4. 불일치 발견 시 원인 후보를 좁혀 담당 에이전트에게 전달한다.

## 데이터 조회 명령

```bash
# ClickHouse
docker exec txn-clickhouse clickhouse-client --query "SELECT reason, count() FROM anomaly_flags GROUP BY reason"
docker exec txn-clickhouse clickhouse-client --query "SELECT count() FROM windowed_txn_stats"

# Postgres 원본
docker exec txn-postgres psql -U bankuser -d bankdb -c "SELECT count(*), sum(amount) FROM transactions;"
```

## 검증 1: large_amount 비율

generator의 `AMOUNT_RANGE = (-50000, 50000)`(정수 균등분포)과 Flink `AMOUNT_THRESHOLD`(현재 47500)로부터 이론 확률을 계산한다:

```
qualifying = (50000 - THRESHOLD) * 2   # 대칭이므로 양쪽 방향
total = 100001                          # -50000..50000 정수 개수
p_theory = qualifying / total
```

실측 비율 `p_hat = large_amount 건수 / 전체 거래 건수`을 구하고, Wilson score interval(또는 정규근사 `p_hat ± 1.96*sqrt(p_hat*(1-p_hat)/n)`)로 95% 신뢰구간을 계산한다. `p_theory`가 신뢰구간 안에 들면 "부합", 벗어나면 "불일치"로 판정한다. **표본 n이 수백 건 미만이면 신뢰구간이 넓어 판단이 어렵다는 점을 명시한다.**

## 검증 2: high_velocity 비율 (시뮬레이션 필요)

닫힌 형태 공식으로 정확히 구할 수 없다 — generator가 매 이벤트마다 5개 계좌 중 하나를 균등 랜덤 선택하고, 전체 이벤트 간격이 `Uniform(0.5, 2.0)`초이기 때문에 개별 계좌의 도착 과정은 단순 포아송이 아니다.

**반드시 generator와 동일한 랜덤 프로세스를 Python으로 재현**해 몬테카를로 시뮬레이션을 돌린다:

```python
import random

def simulate(n_events, n_accounts=5, interval_range=(0.5, 2.0),
             velocity_window_s=5.0, velocity_count=4, seed=None):
    rng = random.Random(seed)
    t = 0.0
    account_times = {i: [] for i in range(n_accounts)}
    triggers = 0
    for _ in range(n_events):
        t += rng.uniform(*interval_range)
        acc = rng.choice(range(n_accounts))
        times = [x for x in account_times[acc] if t - x <= velocity_window_s]
        times.append(t)
        account_times[acc] = times
        if len(times) >= velocity_count:
            triggers += 1
    return triggers / n_events

# 여러 시드로 반복 실행해 발생률의 분포(평균/표준편차)를 구한다
rates = [simulate(10000, seed=s) for s in range(30)]
```

실측 `high_velocity` 비율이 `rates`의 평균 ± 2*표준편차 범위 안에 있는지 비교한다. 시뮬레이션 코드는 `_workspace/`에 남겨 재사용 가능하게 한다. **generator나 Flink 룰의 파라미터(계좌 수, 간격 범위, velocity window/count)가 바뀌면 시뮬레이션 파라미터도 반드시 함께 갱신한다** — 파라미터가 어긋난 시뮬레이션과 비교하는 것은 검증이 아니라 오판의 원인이 된다.

## 검증 3: 집계/잔액 정합성

```bash
# 계좌별 원본 누적합 (Postgres)
docker exec txn-postgres psql -U bankuser -d bankdb -c \
  "SELECT account_id, sum(amount) FROM transactions GROUP BY account_id ORDER BY account_id;"

# ClickHouse 최신 잔액 (ReplacingMergeTree면 FINAL 필수)
docker exec txn-clickhouse clickhouse-client --query \
  "SELECT account_id, balance FROM balance_snapshot FINAL ORDER BY account_id"
```

두 결과가 계좌별로 정확히 일치해야 한다 (seed 잔액은 accounts.balance에 있지만 generator가 갱신하지 않으므로, 이 비교는 "거래 누적합"과 "Flink가 계산한 running balance"의 일치 여부를 보는 것이지 accounts.balance와의 비교가 아니다 — 이 점을 리포트에 명시한다). 불일치가 있으면 Flink 잡이 일부 이벤트를 누락/중복 처리했다는 신호이므로 체크포인트 재시작 이력, at-least-once sink 중복 가능성을 함께 조사한다.

윈도우 집계는 특정 1분 구간을 골라 Postgres `created_at` 기준 필터링한 합계와 `windowed_txn_stats`를 대조한다:
```bash
docker exec txn-postgres psql -U bankuser -d bankdb -c \
  "SELECT count(*), sum(amount) FROM transactions WHERE created_at >= '<window_start>' AND created_at < '<window_start + 1min>';"
```

## 판정 기준 요약

| 판정 | 조건 |
|---|---|
| 부합 | 실측값이 이론/시뮬레이션 신뢰구간 안 |
| 불일치 | 실측값이 신뢰구간 밖 → 원인 후보와 함께 담당 에이전트에 전달 |
| 판단 불가 | 표본 크기가 통계적 결론을 내리기에 부족 (수백 건 미만) |

리포트에는 항상 표본 크기, 계산식/시뮬레이션 코드, 신뢰구간, 판정을 함께 남긴다. "비율이 대략 맞다" 같은 근거 없는 결론은 쓰지 않는다.
