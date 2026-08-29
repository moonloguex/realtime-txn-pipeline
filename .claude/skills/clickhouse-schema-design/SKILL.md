---
name: clickhouse-schema-design
description: "실시간 뱅킹 파이프라인의 ClickHouse 스키마(잔액 스냅샷/윈도우 집계/이상거래 플래그)를 설계·리뷰·마이그레이션한다. MergeTree 계열 엔진 선택, ORDER BY/PARTITION BY 설계, Flink JDBC sink와의 타입 정합성, Grafana 쿼리 최적화가 필요할 때 사용. \"ClickHouse 스키마\", \"테이블 설계\", \"DDL\", \"엔진 선택\" 언급 시 반드시 사용."
---

# ClickHouse 스키마 설계 (실시간 뱅킹 파이프라인)

이 스킬은 `infra/clickhouse-schema.sql`을 설계·수정할 때의 절차와 판단 기준을 담는다. 대상 테이블: `balance_snapshot`(계좌별 잔액), `windowed_txn_stats`(1분 윈도우 집계), `anomaly_flags`(이상거래).

## 절차

1. **현재 스키마와 sink 코드를 함께 읽는다.** `infra/clickhouse-schema.sql`과 `flink-job/src/main/java/com/jm/txnpipeline/flink/TransactionProcessor.java`의 `JdbcSink.sink(...)` INSERT 문 3개를 나란히 놓고 컬럼 순서/타입이 정확히 일치하는지 확인한다. 하나만 보고 판단하지 않는다.
2. **각 테이블의 조회 패턴을 먼저 정의한다** (표 참고). 조회 패턴이 정해져야 ORDER BY/엔진을 고를 수 있다.
3. DDL을 수정하고, `docker exec txn-clickhouse clickhouse-client --query "..."`로 실제 적용해 문법 오류를 확인한다.
4. 컬럼 타입/이름이 바뀌면 flink-stream-agent(sink 코드)와 grafana-dashboard-agent(쿼리)에게 반드시 알린다.

## 테이블별 설계 가이드

### balance_snapshot — 계좌별 최신 잔액

- **조회 패턴**: "계좌 X의 현재 잔액", "모든 계좌의 최신 잔액 목록" (Grafana 시계열 + 최신값 stat)
- **엔진**: `ReplacingMergeTree(updated_at)`가 적절 — 같은 계좌에 대해 여러 번 갱신되는 것을 "최신값만 유지"로 표현. 단, **병합(merge)은 백그라운드에서 비동기로 일어나므로 조회 시점에 중복 행이 남아있을 수 있다.** 모든 조회 쿼리는 `FINAL` 키워드 또는 `argMax(balance, updated_at) GROUP BY account_id` 패턴을 써야 한다 — 이 제약을 DDL 주석에 명시하고 grafana-dashboard-agent에게 전달한다.
- **ORDER BY**: `account_id` (단일 계좌 조회가 주 패턴이므로 이걸로 충분)
- **account_id 타입**: Postgres `accounts.id`는 `SERIAL`(4바이트 정수), Flink `TransactionEvent.accountId`도 `int`다. ClickHouse에서 `String`으로 받으면 불필요한 변환/비교 비용이 생긴다 — `Int32` 또는 `UInt32`를 기본으로 검토한다. String을 유지해야 하는 근거(예: 향후 UUID 전환 계획)가 없다면 정수형으로 바꾸는 것을 제안한다.
- **balance 타입**: `Decimal(18,2)` 유지 — Postgres `NUMERIC(18,2)` → Debezium `decimal.handling.mode: string` → Flink `BigDecimal`로 이어지는 정밀도 체인을 끊지 않는다.

### windowed_txn_stats — 1분 윈도우 집계

- **조회 패턴**: "최근 N시간 계좌별 거래량 추이" (Grafana 시계열, `$__timeFilter(window_start)` 사용)
- **엔진**: `MergeTree` — append-only, 갱신 없음. 적절.
- **ORDER BY**: `(account_id, window_start)` — 이미 적용됨. 계좌별 시계열 조회에 최적.
- **PARTITION BY**: 데이터량이 적은 학습/데모 프로젝트에서는 파티션을 세분화(일별)하면 오히려 파트 수만 늘어난다. `toYYYYMM(window_start)` 정도의 월 단위를 기본으로 검토하고, 실제 데이터량이 매우 작다면(수만 건 이하) 파티션 없이 두는 것도 합리적 선택지로 제시한다.

### anomaly_flags — 이상거래 플래그

- **조회 패턴**: "최근 이상거래 목록(전체)", "특정 계좌의 이상거래 이력" — 계좌별 필터가 자주 쓰일 가능성이 높다.
- **현재 문제**: `ORDER BY detected_at`만 있으면 계좌 필터 쿼리(`WHERE account_id = X`)가 전체 스캔이 된다. `ORDER BY (account_id, detected_at)`로 바꾸는 것을 우선 검토하되, "최신 이상거래 전체 목록(계좌 무관)"이 더 흔한 조회라면 `detected_at DESC` 유지가 맞을 수 있다 — 실제 Grafana 대시보드 요구사항(grafana-dashboard-agent와 조율)에 따라 결정한다.
- **멱등성 고려**: JDBC sink가 at-least-once이므로 체크포인트 재시작 시 중복 삽입 가능. flink-stream-agent가 dedup 키(이벤트 ID 등)를 추가하기로 하면, `ReplacingMergeTree`로 전환하고 해당 키를 정렬키에 포함하는 방안을 함께 설계한다.

## 흔한 실수 체크리스트

- [ ] `ReplacingMergeTree` 테이블 조회에 `FINAL`/`argMax`를 빠뜨림 → 중복 행 노출
- [ ] Flink sink INSERT 문의 컬럼 순서와 DDL 컬럼 순서 불일치 (타입은 같지만 순서가 달라 값이 바뀌어 들어감)
- [ ] 금액 컬럼을 `Float64`로 설계 → 정밀도 손실
- [ ] account_id를 String으로 유지하면서 Flink 쪽 int와 매번 `String.valueOf()` 변환하는데, 이게 실제로 필요한 변환인지 검토 없이 방치
- [ ] 파티션을 과도하게 세분화해 소량 데이터에도 파트 수가 많아짐

## 검증

DDL 적용 후 반드시 실행:
```
docker exec txn-clickhouse clickhouse-client --query "DESCRIBE TABLE balance_snapshot"
docker exec txn-clickhouse clickhouse-client --query "SHOW CREATE TABLE anomaly_flags"
```
스키마가 의도대로 적용됐는지 눈으로 확인한다. 파일만 고치고 끝내지 않는다.
