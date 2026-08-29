---
name: txn-pipeline-orchestrator
description: "실시간 뱅킹 거래 데이터 파이프라인(Kafka+Debezium CDC → Flink 집계/이상거래탐지 → ClickHouse → Grafana) 작업을 4인 전문 에이전트 팀(ClickHouse 스키마, Flink 집계/이상거래 룰, Grafana 대시보드, 데이터 검증)으로 조율한다. \"파이프라인 구성/점검/개선해줘\", \"Flink 룰 다시 설계해줘\", \"대시보드 만들어줘\", \"이상거래 비율 검증해줘\" 등 이 프로젝트의 데이터 파이프라인 관련 요청 시 반드시 사용. 초기 구축뿐 아니라 재실행/부분 수정/업데이트/보완 요청(\"스키마만 다시\", \"임계치 다시 튜닝\", \"검증 결과 반영해서 개선\")에도 사용."
---

# 실시간 뱅킹 파이프라인 오케스트레이터

Kafka+Debezium CDC → Flink(잔액/윈도우집계/이상거래탐지) → ClickHouse → Grafana 파이프라인을 4인 전문 에이전트 팀으로 조율하는 스킬. 각 레이어가 서로의 산출물에 의존하고(스키마 변경 → sink/쿼리 영향, 룰 변경 → 검증 대상 변경) 검증 결과가 다시 설계로 피드백되므로 **에이전트 팀 모드**를 사용한다.

## 실행 모드: 에이전트 팀

## 에이전트 구성

| 팀원 | 에이전트 타입 | 역할 | 스킬 | 주 출력 |
|------|-------------|------|------|--------|
| clickhouse-schema-agent | 커스텀 (`.claude/agents/clickhouse-schema-agent.md`) | ClickHouse DDL 설계/리뷰 | `clickhouse-schema-design` | `infra/clickhouse-schema.sql` |
| flink-stream-agent | 커스텀 (`.claude/agents/flink-stream-agent.md`) | Flink 집계·이상거래 룰 | `flink-stream-processing` | `flink-job/src/main/java/.../*.java` |
| grafana-dashboard-agent | 커스텀 (`.claude/agents/grafana-dashboard-agent.md`) | Grafana provisioning/대시보드 | `grafana-dashboard-provisioning` | `infra/grafana/provisioning/**` |
| data-validation-agent | 커스텀, general-purpose 기반 (`.claude/agents/data-validation-agent.md`) | 이상거래 비율/집계 정합성 통계 검증 | `pipeline-data-validation` | `_workspace/04_validation_report.md` |

## 워크플로우

### Phase 0: 컨텍스트 확인 (후속 작업 지원)

1. `_workspace/` 존재 여부 확인
2. 분기:
   - **미존재** → 초기 실행, Phase 1로 진행
   - **존재 + 사용자가 부분 수정 요청** ("스키마만 다시", "임계치만 조정", "대시보드 패널 추가" 등) → 부분 재실행. 해당 팀원만 스폰하고, 이전 산출물(`_workspace/`의 관련 파일)을 프롬프트에 포함해 기존 결과를 개선하도록 지시. 나머지 Phase는 건너뛴다.
   - **존재 + 새 입력/전면 재설계 요청** → 기존 `_workspace/`를 `_workspace_{YYYYMMDD_HHMMSS}/`로 이동 후 Phase 1부터 새로 진행
3. 부분 재실행이 아니면 인프라 상태 확인: `docker compose -f infra/docker-compose.yml ps` — 6개 서비스(postgres, kafka, kafka-ui, kafka-connect, clickhouse, grafana)가 안 떠 있으면 `docker compose -f infra/docker-compose.yml up -d`로 기동하고 안정화를 기다린다.

### Phase 1: 준비

1. 사용자 요청 분석 — 전체 파이프라인 구축인지, 특정 레이어(스키마/Flink/대시보드/검증) 중심 요청인지 파악
2. `_workspace/` 생성 (초기/새 실행 시)
3. 현재 상태 스냅샷을 `_workspace/00_baseline.md`에 기록: 현재 스키마, Flink 룰 임계치, Grafana provisioning 존재 여부

### Phase 2: 팀 구성

```
TeamCreate(
  team_name: "txn-pipeline-team",
  members: [
    { name: "clickhouse-schema-agent", agent_type: "clickhouse-schema-agent", model: "opus",
      prompt: "infra/clickhouse-schema.sql과 Flink JDBC sink 코드를 함께 검토해 스키마를 리뷰/개선하라. clickhouse-schema-design 스킬을 따르라. 결과를 _workspace/01_clickhouse_schema.md에 요약하고, sink 코드 영향이 있으면 flink-stream-agent에게 SendMessage로 알려라." },
    { name: "flink-stream-agent", agent_type: "flink-stream-agent", model: "opus",
      prompt: "flink-job의 이상거래 룰(대금액/속도)과 집계 로직을 리뷰/개선하라. flink-stream-processing 스킬을 따르라. clickhouse-schema-agent의 스키마 변경 통지를 기다렸다가 필요한 sink 수정을 반영하라. 임계치를 바꾸면 이론적 예상 발생률을 _workspace/02_flink_rules.md에 계산식과 함께 남겨라." },
    { name: "grafana-dashboard-agent", agent_type: "grafana-dashboard-agent", model: "opus",
      prompt: "clickhouse-schema-agent가 스키마를 확정했다는 통지를 받은 뒤, grafana-dashboard-provisioning 스킬에 따라 datasource + 4개 패널(잔액추이/윈도우집계/이상거래목록/이상거래비율) 대시보드를 provisioning으로 구성하라. 결과를 _workspace/03_grafana_dashboard.md에 요약하라." },
    { name: "data-validation-agent", agent_type: "data-validation-agent", model: "opus",
      prompt: "flink-stream-agent의 룰 확정 통지를 받은 뒤, pipeline-data-validation 스킬에 따라 이상거래 비율(이론/시뮬레이션 vs 실측)과 집계·잔액 정합성을 통계적으로 검증하라. 표본이 부족하면 generator를 일정 시간 더 돌릴 것을 리더에게 요청하라. 결과를 _workspace/04_validation_report.md에 남겨라." }
  ]
)
```

2. 작업 등록:
```
TaskCreate(tasks: [
  { title: "ClickHouse 스키마 리뷰/개선", assignee: "clickhouse-schema-agent" },
  { title: "Flink 이상거래 룰/집계 리뷰", assignee: "flink-stream-agent", depends_on: ["ClickHouse 스키마 리뷰/개선"] },
  { title: "Grafana 대시보드 provisioning", assignee: "grafana-dashboard-agent", depends_on: ["ClickHouse 스키마 리뷰/개선"] },
  { title: "이상거래 비율/정합성 검증", assignee: "data-validation-agent", depends_on: ["Flink 이상거래 룰/집계 리뷰"] }
])
```

> Flink와 Grafana는 스키마 확정에만 의존하고 서로 독립적이므로 병렬 진행 가능. 검증은 Flink 룰이 확정돼야 의미 있는 이론값을 계산할 수 있으므로 그 뒤에 진행.

### Phase 3: 팀 작업 (자체 조율)

**실행 방식:** 팀원들이 공유 작업 목록에서 작업을 요청하고 SendMessage로 조율하며 독립 수행.

1. 데이터가 실제로 흘러야 검증이 가능하므로, flink-stream-agent가 룰 확정 후 (또는 리더가 직접) Flink 잡을 백그라운드로 기동하고 `generator/generate_transactions.py`를 일정 시간(최소 수 분, 통계적으로 유의미한 표본이 쌓일 때까지) 백그라운드 실행한다.
2. 리더는 유휴 알림을 모니터링하고, 팀원이 막히면 SendMessage로 상태 확인 후 개입한다.
3. 진행률은 TaskGet으로 확인한다.

**산출물 저장 경로:**

| 팀원 | 경로 |
|---|---|
| clickhouse-schema-agent | `_workspace/01_clickhouse_schema.md` + `infra/clickhouse-schema.sql` |
| flink-stream-agent | `_workspace/02_flink_rules.md` + `flink-job/src/main/java/.../*.java` |
| grafana-dashboard-agent | `_workspace/03_grafana_dashboard.md` + `infra/grafana/provisioning/**` |
| data-validation-agent | `_workspace/04_validation_report.md` |

### Phase 4: 통합 및 피드백 루프

1. 모든 팀원의 작업 완료 대기 (TaskGet)
2. `_workspace/04_validation_report.md`를 Read해 불일치(이상거래 비율이 신뢰구간 밖 등)가 있는지 확인
3. 불일치가 있으면: data-validation-agent가 이미 원인 후보와 함께 flink-stream-agent(또는 clickhouse-schema-agent)에게 SendMessage로 전달했는지 확인하고, 해당 팀원에게 재작업을 요청 — **1회 재조정 루프**까지만 자동 진행하고, 그래도 불일치가 남으면 사용자에게 보고하고 판단을 구한다
4. 최종 요약을 `_workspace/05_summary.md`에 작성: 각 레이어 변경사항, 검증 결과, 남은 이슈

### Phase 5: 정리

1. 팀원들에게 종료 요청 (SendMessage)
2. `TeamDelete`로 팀 정리
3. `_workspace/` 보존 (감사 추적용)
4. 사용자에게 결과 요약 보고 + 개선 피드백 요청

## 데이터 흐름

```
[리더] → TeamCreate
             │
    clickhouse-schema-agent (스키마 확정)
        ├──SendMessage──▶ flink-stream-agent (sink 반영, 룰 확정 + 이론값 계산)
        └──SendMessage──▶ grafana-dashboard-agent (대시보드 구성)
                                │
                    flink-stream-agent ──SendMessage──▶ data-validation-agent (검증)
                                                              │
                                                    불일치 시 SendMessage로 재조정 요청
                                                              │
                                                       [리더: 통합 + 요약]
```

## 에러 핸들링

| 상황 | 전략 |
|---|---|
| 인프라 컨테이너 미기동 | Phase 0에서 `docker compose up -d` 후 재시도, 그래도 실패하면 사용자에게 로그와 함께 보고 |
| 팀원 1명 실패/중지 | 리더가 유휴 알림 감지 → SendMessage로 상태 확인 → 재시작, 안 되면 사용자에게 알림 |
| 검증 표본 부족 | data-validation-agent가 "판단 불가"로 보고 → 리더가 generator 실행 시간을 늘려 재검증 |
| 검증 불일치가 재조정 1회 후에도 지속 | 자동 재시도 중단, 사용자에게 이론값 계산/시뮬레이션 코드와 함께 판단 요청 |
| 팀원 간 데이터 충돌(예: 컬럼 타입 의견 불일치) | 삭제하지 않고 양쪽 근거를 병기해 사용자에게 보고 |

## 테스트 시나리오

### 정상 흐름
1. 사용자가 "파이프라인 구성해줘" 요청
2. Phase 0에서 `_workspace/` 없음 확인 → 초기 실행, 인프라 6개 서비스 기동 확인
3. Phase 2에서 4인 팀 구성 + 4개 작업 등록 (의존성: 스키마 → Flink/Grafana → 검증)
4. Phase 3에서 스키마 확정 후 Flink/Grafana 병렬 진행, Flink 확정 후 generator+잡 기동해 데이터 축적, 검증 수행
5. Phase 4에서 검증 결과 통합, 불일치 없으면 바로 요약
6. Phase 5에서 팀 정리, `_workspace/05_summary.md` 및 각 레이어 산출물 보고

### 에러 흐름
1. Phase 3에서 data-validation-agent가 표본 부족으로 "판단 불가" 보고
2. 리더가 generator를 추가로 5분 더 실행하도록 조율
3. 재검증 수행 → 이상거래 비율이 신뢰구간 밖으로 나옴
4. data-validation-agent가 flink-stream-agent에게 원인 후보(임계치 계산 오류 가능성)와 함께 SendMessage
5. flink-stream-agent가 임계치 재계산 후 수정, data-validation-agent가 재검증 (1회 재조정)
6. 여전히 불일치면 자동 재시도 중단, 사용자에게 두 팀원의 근거를 함께 보고
