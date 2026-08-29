---
name: grafana-dashboard-provisioning
description: "Grafana ClickHouse 데이터소스와 대시보드를 provisioning(코드 기반, UI 수동설정 아님)으로 구성한다. 계좌별 잔액 추이, 윈도우 거래량/거래액, 이상거래 알림 테이블/게이지 패널 설계 시 사용. \"Grafana 대시보드\", \"시각화\", \"패널 구성\" 언급 시 반드시 사용."
---

# Grafana 대시보드 프로비저닝 (실시간 뱅킹 파이프라인)

`infra/docker-compose.yml`의 Grafana 컨테이너는 `GF_INSTALL_PLUGINS: grafana-clickhouse-datasource`로 플러그인을 자동 설치하지만, 데이터소스와 대시보드는 아직 provisioning되어 있지 않다 — UI에서 수동 클릭으로 만들면 컨테이너 재생성 시 사라지므로, 반드시 파일 기반 provisioning으로 구성한다.

## 디렉토리 구조

```
infra/grafana/provisioning/
├── datasources/
│   └── clickhouse.yml
└── dashboards/
    ├── dashboards.yml          # 대시보드 자동 로딩 설정
    └── json/
        └── txn-pipeline-overview.json
```

`infra/docker-compose.yml`의 Grafana 서비스에 다음 볼륨을 추가해야 실제로 로드된다:
```yaml
volumes:
  - grafana_data:/var/lib/grafana
  - ./grafana/provisioning:/etc/grafana/provisioning
```

## 데이터소스 정의 (clickhouse.yml)

```yaml
apiVersion: 1
datasources:
  - name: ClickHouse
    type: grafana-clickhouse-datasource
    access: proxy
    isDefault: true
    jsonData:
      host: clickhouse   # docker 네트워크 내부 서비스명 (localhost 아님!)
      port: 9000
      protocol: native
      username: default
    editable: true
```

컨테이너 간 통신이므로 `localhost`가 아니라 docker-compose 서비스명(`clickhouse`)을 쓴다. 이건 Flink 잡(호스트에서 실행되므로 `localhost:8123` 사용)과 다른 점이니 혼동하지 않는다.

## 대시보드 패널 설계

| 패널 | 소스 테이블 | 쿼리 핵심 | 패널 타입 |
|---|---|---|---|
| 계좌별 잔액 추이 | `balance_snapshot` | `SELECT updated_at, account_id, balance FROM balance_snapshot WHERE $__timeFilter(updated_at)` — **ReplacingMergeTree라면 `FINAL` 필수** (예: `FROM balance_snapshot FINAL`) | Time series |
| 윈도우별 거래량/거래액 | `windowed_txn_stats` | `SELECT window_start, account_id, txn_count, txn_amount FROM windowed_txn_stats WHERE $__timeFilter(window_start)` | Time series / Bar chart |
| 이상거래 알림 목록 | `anomaly_flags` | `SELECT detected_at, account_id, reason FROM anomaly_flags ORDER BY detected_at DESC LIMIT 100` | Table |
| 이상거래 비율 | `anomaly_flags` + 전체 거래 대비 | 이상거래 건수 / windowed_txn_stats 총 거래건수 | Stat / Gauge |

- 시계열 패널은 `$__timeFilter(column)`, `$__interval` 등 Grafana 매크로를 써서 대시보드 시간 범위 위젯과 연동한다. 기간을 SQL에 하드코딩하지 않는다.
- 이상거래 테이블 패널은 `reason` 값별로 셀 색상을 다르게 해서(Field override) `large_amount`/`high_velocity`를 시각적으로 구분한다.
- ClickHouse 스키마(컬럼명/타입/엔진)는 반드시 clickhouse-schema-agent가 확정한 최신 `infra/clickhouse-schema.sql`을 기준으로 쿼리를 작성한다 — 스키마 확정 전에 대시보드 JSON을 먼저 작성하지 않는다.

## dashboards.yml (자동 로딩)

```yaml
apiVersion: 1
providers:
  - name: txn-pipeline
    type: file
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/provisioning/dashboards/json
```

## 검증 절차

파일만 작성하고 끝내지 않는다 — 반드시:
```
docker compose -f infra/docker-compose.yml up -d grafana   # 볼륨 마운트 반영 위해 재생성 필요할 수 있음
curl -s -u admin:admin http://localhost:3000/api/health
curl -s -u admin:admin http://localhost:3000/api/datasources | grep ClickHouse
curl -s -u admin:admin http://localhost:3000/api/search?query=
```
데이터소스가 목록에 나오고, 대시보드가 검색되며, 각 패널이 실제로 값을 그리는지 확인한다. 패널이 "No data"라면 쿼리를 `clickhouse-client`로 직접 실행해 원인이 쿼리인지 데이터 부재인지 구분한다.

## 흔한 실수

- 데이터소스 host를 `localhost`로 설정 (컨테이너 내부에서는 서비스명을 써야 함)
- `ReplacingMergeTree` 테이블에서 `FINAL` 누락 → 잔액 그래프가 지그재그로 보임
- 하드코딩된 시간 범위로 대시보드 타임피커가 동작하지 않음
- provisioning 디렉토리를 볼륨 마운트하지 않아 재시작 시 설정이 유지되지 않음
