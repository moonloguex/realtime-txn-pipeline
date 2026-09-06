# 실시간 거래 데이터 파이프라인 — 아키텍처 및 의사결정 기록

작성일: 2026-08-29 (2026-09-06 갱신) / 현재 상태: Phase 0~3 완료 (Flink 집계/이상거래, ClickHouse 스키마, Grafana 대시보드, 데이터 검증까지 엔드투엔드 가동 중), Phase 4는 스트레치로 보류

## 1. 배경과 목적

기존 `FDS` 프로젝트에서 Kafka + Kafka Streams + Redis 기반 실시간 사기 탐지 파이프라인을 구축한 경험이 있음. 금융권 Data Engineer 포지션을 목표로, FDS와 겹치지 않는 새로운 아키텍처 패턴을 다뤄보는 개인 학습/포트폴리오 프로젝트로 시작함.

## 2. 검토했던 대안과 선택 근거

세 가지 방향을 검토했다.

| 안 | 구성 | 장점 | 단점 |
|---|---|---|---|
| A | Kafka + Flink + ClickHouse + Grafana (실시간 시세 분석) | 저지연 스트림 처리 + 실시간 OLAP은 트레이딩/핀테크 업계 표준 스택. Kafka Streams와 다른 엔진(Flink)이라 스택 폭이 넓어짐 | 시세 데이터는 은행 실무보다는 트레이딩 도메인에 가까움 |
| B | Postgres → Debezium(CDC) → Kafka → Spark → MinIO+Iceberg (뱅킹 CDC+레이크하우스) | "레거시 DB → 실시간 스트림화"는 은행/카드사 채용 공고에 자주 등장하는 실제 패턴 | 컴포넌트가 너무 많아(Postgres+Debezium+Kafka+Spark+MinIO+Iceberg) 개인 프로젝트로 셋업 부담이 큼. 실시간성 체감이 약함(마이크로배치 위주) |
| C | 기존 스택 확장 (Kafka Streams + Elasticsearch) | 셋업 부담 적음(FDS 재사용) | 새로 배우는 게 적어 포트폴리오 차별화가 약함 |

**최종 선택: A + B 하이브리드.** 은행 실무에서 거래 데이터는 EAI/CDC로 수집하는 경우가 많다는 점(B의 강점)과, Flink가 Kafka를 활용한 강력한 상태 기반(stateful) 처리를 보여주기 좋고 Grafana 대시보드가 시각적 성과물을 준다는 점(A의 강점)을 합쳤다. B의 셋업 부담이었던 MinIO+Iceberg 데이터레이크는 초기 범위에서 제외하고, 나중에 붙이기 쉽도록 CDC 원본 이벤트를 Kafka에 충분히 리텐션하는 방식으로만 설계에 반영했다 (§6 확장 지점 참고).

## 3. 최종 아키텍처

```
[Postgres: accounts/transactions]
        │ (synthetic tx generator가 주기적으로 INSERT)
        ▼
   Debezium (Kafka Connect) — CDC
        ▼
   Kafka topic: cdc.public.transactions   ← 원본 이벤트, 리텐션 길게 유지 (향후 레이크 확장 지점)
        ▼
   Flink job (Phase 2, 완료)
        - 계좌별 running balance (keyed state)
        - 1분 tumbling window 거래량/거래액 집계
        - 간단한 이상거래 플래그 (금액 임계치 / 짧은 시간 내 다건 거래)
        ▼
   ClickHouse (JDBC sink): balance_snapshot, windowed_txn_stats, anomaly_flags
        ▼
   Grafana 대시보드 (실시간 거래량, 잔액 추이, 이상거래 알림 테이블)
```

## 4. 구성 요소별 선택 근거

- **Postgres (`postgres:16`)**: CDC의 소스가 되는 "코어뱅킹" DB 역할. `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`를 command로 설정 — Debezium이 논리적 복제(logical replication)로 변경분을 읽으려면 필수 전제조건이기 때문.
- **Debezium (`debezium/connect:2.7.3.Final`)**: Postgres CDC의 사실상 표준 오픈소스 도구. Kafka Connect 플러그인 형태라 별도 설치 없이 이미지 하나로 CDC 커넥터를 구동할 수 있어 개인 프로젝트 셋업 부담이 적음.
- **Kafka (`apache/kafka:3.9.1`, KRaft 모드)**: FDS에서 이미 검증된 설정을 재사용. Zookeeper 없이 KRaft로 단일 노드 구성해 리소스를 아꼈다.
- **Kafka Connect REST API**로 커넥터를 등록하는 방식(`plugin.name: pgoutput`) — pgoutput은 Postgres 10+ 내장 플러그인이라 wal2json 등 외부 플러그인 설치가 필요 없어 가장 가벼운 선택.
- **ClickHouse (`clickhouse/clickhouse-server:24-alpine`)**: 실시간 집계 결과를 빠르게 조회하기 위한 컬럼 지향 OLAP DB. 금융권 실시간 리스크/거래 분석에서 실제로 널리 쓰이는 엔진.
- **Grafana (`grafana/grafana:11.4.0`)**: ClickHouse 데이터소스 플러그인(`grafana-clickhouse-datasource`)을 컨테이너 기동 시 자동 설치(`GF_INSTALL_PLUGINS` 환경변수)하도록 미리 준비해둠 — Phase 3에서 실제로 datasource + 7패널 대시보드를 코드 기반 provisioning(`infra/grafana/provisioning/`)으로 구성해 사용.
- **Flink (Phase 2, 완료)**: Kafka Streams(FDS에서 이미 경험)와 다른 처리 엔진을 씀으로써 포트폴리오 스택 폭을 넓히기 위해 선택. DataStream API + keyed state + 체크포인팅을 직접 다뤄 "상태 기반 스트림 처리"와 "장애 내성"을 이해하고 있음을 보여주는 것이 목적. 실제로 이 과정에서 체크포인트/오프셋 커밋, watermark idleness, JDBC sink의 at-least-once 특성 등 장애 내성 관련 실전 이슈를 여러 건 직접 겪고 고쳤다 (§7 참고).

## 5. 사용 언어와 선택 이유

- **인프라 정의**: Docker Compose (YAML) — 로컬에서 여러 서비스를 한 번에 재현 가능하게 하는 가장 가벼운 방법. Kubernetes 등은 개인 학습 목적 범위를 넘어서 제외.
- **거래 생성기 (`generator/generate_transactions.py`)**: Python. FDS의 generator는 Java였지만, 이번엔 (1) 스크립트를 빠르게 작성하고 반복 수정하기 쉽고 (2) `psycopg2-binary`로 Postgres 연결이 매우 간단하며 (3) 이미 Java 스택(FDS)을 다뤄봤으니 스택 다양성 측면에서도 유리하다고 판단해 Python을 선택. 프로젝트 전용 `.venv`(`generator/.venv`)로 격리, 의존성은 `requirements.txt`에 `psycopg2-binary`만 명시.
- **Flink job (`flink-job/`, Java)**: DataStream API + Java로 확정. Table API보다 DataStream API를 선택한 이유는 `KeyedProcessFunction`으로 keyed state와 타이머 로직을 직접 다뤄야 이상거래 탐지 로직(짧은 시간 내 다건 거래 감지)을 구현하며 배우는 게 많기 때문. Gradle(`build.gradle.kts`, `application` 플러그인)로 빌드하며 `flink-connector-kafka`, `flink-connector-jdbc`(ClickHouse HTTP JDBC)를 사용. 별도 클러스터 배포 없이 `main()`으로 뜨는 로컬 임베디드 클러스터로 운용.

## 6. 데이터레이크 확장 지점 (설계만 반영, 아직 구현 안 함)

CDC 원본 이벤트가 담기는 Kafka 토픽(`cdc.public.transactions`)의 리텐션을 길게(예: 7일 이상) 설정해두면, 나중에 별도 Flink/Kafka Connect S3 싱크 job을 하나 더 붙여 MinIO+Iceberg로 적재할 수 있다. 기존 CDC → Flink → ClickHouse 파이프라인은 수정할 필요가 없다. 실제 Iceberg 적재는 Phase 4(스트레치 목표)로 미뤘다.

## 7. 진행 중 발생한 이슈와 해결

1. **`debezium/connect:2.7` 이미지 태그 없음** — Docker Hub에 해당 태그가 없어 `docker compose up` 실패. Docker Hub API로 실제 존재하는 태그를 조회해 `2.7.3.Final`로 수정.
2. **거래 금액(`amount`)이 깨져서 나옴** — Debezium이 Postgres `NUMERIC` 컬럼을 기본적으로 정밀도 보존을 위해 바이너리(base64)로 직렬화하는데, 스키마 없는 JSON(`schemas.enable: false`)으로 보면 사람이 읽을 수 없는 문자열(`"K2+I"` 등)로 나옴. 커넥터 설정에 `decimal.handling.mode: string`을 추가해 금액이 `"-25810.00"`처럼 정확한 문자열로 나오도록 수정. 이 과정에서 기존 커넥터/replication slot/publication/Kafka 토픽/`transactions` 테이블 데이터를 모두 초기화하고 재등록함.
3. **`anomaly_flags`에 580만 건 폭증 (Phase 2)** — `TransactionProcessor`의 Kafka source가 `OffsetsInitializer.earliest()`를 쓰고 있었는데, 이 job은 세이브포인트 복원이 없는 로컬 임베디드 클러스터라 재시작(수동/자동 모두)마다 토픽 전체를 처음부터 재처리했다. `BalanceAndAnomalyFunction`이 이벤트마다 즉시 이상거래를 내보내는 구조라 재처리될 때마다 같은 이벤트에 대해 같은 플래그가 그대로 다시 쌓여 수 시간 만에 580만 건까지 누적됐다. `OffsetsInitializer.committedOffsets(EARLIEST)`로 변경해 커밋된 오프셋이 있으면 거기서 재개하도록 수정.
4. **`windowed_txn_stats`가 계속 0행** — watermark 전략(`forBoundedOutOfOrderness(5s)`)에 idleness 설정이 없어서, 낮은 트래픽(~0.8건/초)에서 파티션이 자주 유휴 상태가 되면 그 파티션의 워터마크가 전체 최솟값을 영구히 붙잡아 1분 tumbling window가 한 번도 안 닫혔다. `withIdleness(30s)`를 추가해 유휴 파티션을 워터마크 계산에서 제외하도록 수정. (참고: 이 조사 과정에서 `KAFKA_CFG_NUM_PARTITIONS=3` 같은 `KAFKA_CFG_*` 환경변수가 Bitnami 이미지 전용이라 지금 쓰는 `apache/kafka` 공식 이미지에는 적용되지 않고, 실제 토픽은 파티션 1개로 생성되어 있었음을 확인함 — §4의 Kafka 설정 관련 향후 참고사항.)
5. **`anomaly_flags`/`windowed_txn_stats` 실시간 중복 삽입** — 위 두 버그를 고친 뒤에도, ClickHouse가 `keep_alive_timeout=10s`로 유휴 HTTP 커넥션을 닫는데 `clickhouse-jdbc:0.8.6:http` 클라이언트가 그걸 감지 못 하고 재사용하려다 실패 → `JdbcExecutionOptions.withMaxRetries(3)`이 배치를 재실행 → 원 요청이 이미 서버에 도달했었다면 진짜 중복 INSERT가 발생했다. `http_keep_alive=false`로 완화를 시도했으나 라이브 검증에서 CLOSE_WAIT 소켓이 완전히는 사라지지 않아 근본 원인이 100% 해소되지는 않았음 — 대신 `windowed_txn_stats`/`anomaly_flags`를 버전 컬럼 없는 `ReplacingMergeTree()`로 전환해 스키마 레벨 dedup 백스톱을 두었고(조회 시 `FINAL` 필수), 검증 결과 이 백스톱이 완전히 커버함을 확인했다. **커넥션 재사용 자체를 원천 차단하는 근본 수정은 향후 과제로 남음.**

## 8. 포트/네트워크 설계 근거

FDS 프로젝트와 동시에 띄울 가능성을 고려해 호스트 포트를 겹치지 않게 조정함 (FDS는 Kafka 9092, Kafka UI 8080 사용).

| 서비스 | 호스트 포트 | 비고 |
|---|---|---|
| Postgres | 5433 | 로컬 기본 Postgres(5432)와 충돌 방지 |
| Kafka (external) | 19092 | FDS의 9092와 충돌 방지 |
| Kafka UI | 8081 | FDS의 8080과 충돌 방지 |
| Kafka Connect REST | 8083 | 기본값 유지 (FDS엔 없음) |
| ClickHouse | 8123 (HTTP), 9000 (native) | 기본값 유지 |
| Grafana | 3000 | 기본값 유지 |

## 9. 현재 파일 구조

```
realtime-txn-pipeline/
├── ARCHITECTURE_DECISIONS.md   (본 문서)
├── infra/
│   ├── docker-compose.yml      (postgres, kafka, kafka-ui, kafka-connect, clickhouse, grafana)
│   ├── schema.sql               (accounts, transactions 테이블 + 시드 데이터 5개 계좌)
│   ├── connector-config.json    (Debezium Postgres 커넥터 설정)
│   ├── clickhouse-schema.sql    (balance_snapshot / windowed_txn_stats / anomaly_flags DDL)
│   └── grafana/provisioning/
│       ├── datasources/clickhouse.yml
│       └── dashboards/{dashboards.yml, json/txn-pipeline-overview.json}
├── flink-job/                   (Java, Gradle, DataStream API)
│   ├── build.gradle.kts
│   └── src/main/java/com/jm/txnpipeline/flink/
│       (TransactionProcessor, BalanceAndAnomalyFunction, WindowStatsFunction,
│        DebeziumEventParser, TransactionEvent/BalanceUpdate/WindowedStats/AnomalyFlag)
└── generator/
    ├── requirements.txt         (psycopg2-binary)
    ├── generate_transactions.py (합성 거래 생성기)
    └── .venv/                   (프로젝트 전용 가상환경)
```

## 10. 완료된 검증

1. `docker compose up -d`로 6개 서비스(postgres, kafka, kafka-ui, kafka-connect, clickhouse, grafana) 정상 기동 확인 (Phase 0).
2. `schema.sql` 적용 → `accounts` 5건, `transactions` 테이블 생성 확인.
3. Debezium 커넥터 등록 → `status: RUNNING` 확인.
4. 생성기 실행 → `kafka-console-consumer`로 `cdc.public.transactions` 토픽에 CDC 이벤트가 올바른 금액 형식으로 도착하는 것 확인 (Phase 1).
5. Flink job이 재시작 없이 수 시간 연속 가동, 체크포인트 정상 완료 지속 확인 (Phase 2).
6. `windowed_txn_stats` 1분 윈도우 집계가 Postgres 원본과 건수/금액 정확히 일치함을 대조 확인, `balance_snapshot` 계좌별 잔액이 Postgres 누적합과 정확히 일치함을 확인 (Phase 2).
7. 이상거래 비율 실측치(large_amount 4.05%, high_velocity 2.34%)가 이론값(각각 ≈5.0%, ≈2.06%, 후자는 실제 생성기 분포 몬테카를로 시뮬레이션으로 산출)과 Wilson 95% 신뢰구간 내에서 통계적으로 부합함을 확인 (Phase 2/3).
8. `anomaly_flags`/`windowed_txn_stats`에 `FINAL` 적용 시 중복 0건임을 확인 — §7-5 dedup 백스톱이 실제로 작동함을 검증 (Phase 2).
9. Grafana 대시보드(datasource + 7패널) provisioning 후 API로 정상 로드/쿼리 실행 확인 (Phase 3).

## 11. 다음 단계

- **Phase 2, 3 완료**: Flink job(잔액/윈도우 집계/이상거래), ClickHouse 스키마, Grafana 대시보드, 데이터 검증까지 엔드투엔드로 가동 및 검증 완료 (2026-09-06).
- **남은 과제**: §7-5의 ClickHouse HTTP 커넥션 재사용 레이스 근본 원인이 완전히는 안 잡혔음 — 현재는 스키마 dedup 백스톱으로 기능상 문제 없으나, 성능/정리 관점에서 추가 조사 여지가 있음.
- **Phase 4 (스트레치)**: MinIO+Iceberg 데이터레이크 싱크 추가
