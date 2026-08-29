---
name: flink-stream-processing
description: "Flink DataStream 잡의 계좌별 잔액 계산(keyed state), 1분 윈도우 집계, 이상거래 탐지 룰(대금액/속도)을 설계·구현·튜닝한다. flink-job/ 자바 코드 수정, 임계치 조정, watermark/체크포인팅/exactly-once 검토가 필요할 때 사용. \"Flink 집계\", \"이상거래 룰\", \"임계치 튜닝\" 언급 시 반드시 사용."
---

# Flink 집계·이상거래 룰 설계 (실시간 뱅킹 파이프라인)

대상: `flink-job/src/main/java/com/jm/txnpipeline/flink/`. 핵심 클래스: `TransactionProcessor`(잡 조립), `BalanceAndAnomalyFunction`(keyed state 잔액 + 이상거래 side output), `WindowStatsFunction`(1분 tumbling window 집계), `DebeziumEventParser`(CDC JSON → `TransactionEvent`).

## 절차

1. **변경 전 데이터 생성기의 분포를 확인한다.** `generator/generate_transactions.py`의 `AMOUNT_RANGE`, `INTERVAL_RANGE`가 이상거래 임계치 설계의 기준값이다. 이 값을 모르고 임계치를 조정하면 의도한 이상거래 비율에서 벗어난다.
2. 로직을 수정한다.
3. `cd flink-job && ./gradlew build`로 컴파일 검증.
4. 가능하면 실제로 짧게 실행해(`./gradlew run` 또는 빌드된 jar) ClickHouse에 데이터가 들어오는지 확인한다.
5. 임계치를 바꿨다면 이론적 예상 발생률을 계산해 data-validation-agent가 검증할 수 있는 근거로 남긴다.

## 이상거래 룰 설계 원칙

### 왜 이 임계치인가를 항상 남긴다

지금 `AMOUNT_THRESHOLD = 47500`은 generator의 `Uniform(-50000, 50000)` 정수 분포에서 `|amount| > 47500`이 될 확률을 역산한 값이다:

```
qualifying values = (50000 - 47500) = 2500개 (양의 방향), 대칭이므로 음의 방향도 2500개
전체 표본공간 = 100001개 (정수 -50000..50000)
P(|amount| > 47500) = 5000 / 100001 ≈ 4.9995%
```

임계치를 바꿀 때는 이 계산식을 다시 적용해 목표 비율과 맞춘다. "느낌상 이 정도면 되겠지"로 숫자를 고르지 않는다.

### 고정 임계치 vs 통계적 임계치

현재는 모든 계좌에 동일한 절대 금액 임계치를 적용한다. 5개 계좌 데모 규모에서는 이 방식이 적절하지만, 계좌별 거래 규모가 크게 다른 시나리오(요청이 오면)에서는 `ValueState<BigDecimal>`에 이동평균/표준편차를 유지하고 z-score 기반으로 판정하는 방식을 검토한다. 요청받지 않았는데 미리 구현하지 않는다 — 지금 범위에서는 오버엔지니어링이다.

### velocity 룰과 watermark의 관계

`recentTxnTimestamps`는 이벤트 시간(`event.createdAt`) 기준으로 5초 윈도우를 유지한다. watermark 전략(`forBoundedOutOfOrderness(Duration.ofSeconds(5))`)과 velocity window(`VELOCITY_WINDOW_MS = 5_000`)가 같은 크기인 것은 의도적이다 — out-of-order 허용 범위와 velocity 판정 윈도우가 어긋나면, 순서가 뒤바뀐 이벤트가 velocity 카운트를 부정확하게 만들 수 있다. 둘 중 하나를 바꾸면 다른 쪽도 재검토한다.

### 새 룰 추가 시

`BalanceAndAnomalyFunction.processElement`에 조건을 추가하고 `ctx.output(ANOMALY_TAG, new AnomalyFlag(...))`로 side output에 내보낸다. `AnomalyFlag.reason` 문자열은 ClickHouse `anomaly_flags.reason`과 Grafana 필터/범례에 그대로 노출되므로, 변경 시 grafana-dashboard-agent에게 알린다.

## 상태·장애 내성 체크리스트

- [ ] `env.enableCheckpointing(10_000)` — 체크포인트 간격이 velocity window(5초)보다 길다. 체크포인트 사이에 장애가 나면 `recentTxnTimestamps` 상태가 마지막 체크포인트로 롤백되므로, 재시작 직후 velocity 판정이 일시적으로 부정확할 수 있음을 인지한다 (허용 가능한 수준인지 판단).
- [ ] JDBC sink는 at-least-once다. 재시작 시 같은 이상거래가 중복 삽입될 수 있다 — 멱등성이 필요하면 clickhouse-schema-agent와 dedup 키 설계를 조율한다.
- [ ] watermark은 `KafkaSource`에서 바로 걸지 않고 `DebeziumEventParser`(flatMap) **이후**에 `assignTimestampsAndWatermarks`를 붙인다 — 파싱 전 원시 문자열에는 이벤트 시간이 없기 때문. 이 순서를 바꾸지 않는다.
- [ ] `keyBy(e -> e.accountId)` 이후의 모든 상태(ValueState/ListState)는 계좌별로 격리된다 — 여러 계좌의 상태가 섞이는 버그는 대개 keyBy 이전에 로직을 넣었을 때 발생한다.

## 빌드/실행 명령

```
cd flink-job
./gradlew build          # 컴파일 + 테스트
./gradlew run             # 로컬 임베디드 클러스터로 실행 (Kafka/ClickHouse가 떠 있어야 함)
```

Kafka bootstrap(`localhost:19092`)과 ClickHouse JDBC(`localhost:8123`)는 `infra/docker-compose.yml`의 호스트 포트 매핑과 일치해야 한다 — 포트를 바꾸는 인프라 변경이 있으면 `TransactionProcessor`의 상수도 함께 바꾼다.
