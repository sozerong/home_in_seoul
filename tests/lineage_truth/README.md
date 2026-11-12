# 컬럼 리니지 정답 (P4-1, 결정 49, 59, 66)

모델 6개의 컬럼 간선을 사람이 SQL을 읽고 쓴 것이다. 파서 코드(`lineage/`)가 레포에 생기기 전에 커밋한다. 정답이 파서 결과에 끌려가지 않게 하기 위해서다.

작성: 2026-11-13, Claude Code. 사용자가 판단을 위임한 밤 작업이라 **사용자 검토 전**이다. 검토에서 바뀌는 줄은 커밋을 따로 남겨, 처음 정답과 검토 뒤 정답을 구분한다.

## 파일 모양

`<모델>.csv`: `target_column,source_table,source_column,kind,subkind,note`

- `target_column`: 그 모델의 출력 컬럼. 모델 이름은 파일 이름이다(`map_*`는 `wh`, 나머지는 `mart`).
- `source_table`: 정규화한 물리 표 이름(`wh.fact_congestion`, `mart.rent_listing`, seed는 `wh.congestion_station_manual`). 빌드 스키마(`wh_build`)가 아니라 실제 이름이다(설계서 1.7의 `lineage.edge`와 같은 이름).
- `source_column`: 컬럼 이름, 또는 `*`(`count(*)`처럼 표의 행 자체).
- `kind`, `subkind`: 설계서 1.4의 표. DIRECT는 IDENTITY / TRANSFORMATION / AGGREGATION, INDIRECT는 FILTER / JOIN / GROUP_BY / SORT / WINDOW / CONDITIONAL.

## 작성 원칙

1. **한 단계, 물리 표까지.** 모델 SQL이 직접 읽는 표(`ref`, `source`, seed)의 컬럼까지만 쓴다. CTE, 서브쿼리, LATERAL은 풀어서 그 안의 물리 표 컬럼으로 쓴다. 여러 모델을 건너는 경로는 뷰(`lineage.downstream`)가 이어 붙인다.
2. **의미로 판정한다.** "이 상류 컬럼의 값이 바뀌면 이 출력 컬럼의 값이나 행이 바뀌는가"로 판정한다. 설계서 1.4의 "붙이는 규칙"은 파서가 따르는 규칙이고, 정답은 그 규칙을 모른다고 치고 쓴다. 그래서 규칙과 다른 줄이 있다. `note`에 `규칙 밖`으로 표시했다.
   - 예: `dong_station.am_peak_congestion`은 LATERAL k가 고른 역의 peak을 붙인다. 그래서 k의 정렬(`dim_station.point`, `dim_dong.geom`)과 필터(covered)도 이 값을 바꾼다. 하지만 규칙 4("그 scope를 거쳐 나가는 출력 컬럼")로는 붙지 않는다.
3. **subkind는 경로 전체로 정한다.** 출처에서 출력까지 가는 길에 집계가 하나라도 있으면 AGGREGATION이다. 집계는 없고 계산이 있으면 TRANSFORMATION, 둘 다 없으면 IDENTITY다. 점수는 (target_column, source_table, source_column, kind)로 맞춘다. subkind는 맞춘 줄 안에서 일치율로 따로 센다.
4. **같은 쌍이 DIRECT와 INDIRECT 둘 다일 수 있다.** 예: `commute_od.avg_minutes`는 `fact_commute.population`을 값으로 쓰고(DIRECT), CASE 조건으로도 쓴다(INDIRECT CONDITIONAL). 두 줄로 쓴다.
5. **CASE 결과가 상수뿐이면 DIRECT가 없다.** `commute_od.basis`는 'age20_34' / 'all_ages' 중 하나라 값 출처가 없고, 조건만 INDIRECT CONDITIONAL로 쓴다.

## 정답에서 뺀 것 (알고 뺀 것)

- **상수 출력 컬럼.** `map_congestion_station.method`('padded' / 'manual')는 줄이 없다. union 첫 갈래의 `note`(null)도 같은 이유로 seed 쪽 DIRECT만 있다. 상수 컬럼의 행 수 영향은 같은 scope의 다른 컬럼에 붙은 INDIRECT로 이미 드러난다.
- **DISTINCT.** `map_congestion_station`의 첫 갈래 `select distinct`와 `dong_station`의 covered `distinct`는 행 관계로 쓰지 않았다. 중복 제거는 출력 컬럼 자기 자신의 값으로 정해져서, 이미 있는 DIRECT 줄과 같은 정보다.
- **퇴화한 경우.** 오른쪽 표가 비어 LATERAL cross join이 행을 없애는 경우, LEFT JOIN 오른쪽 키가 중복되어 왼쪽 행이 늘어나는 경우는 정답에 넣지 않았다. 지금 데이터의 키 제약(테스트)이 막고 있는 경우다.

## 확인한 것

- 정답에 나온 (표, 컬럼) 110쌍이 모두 실제 DB `information_schema.columns`에 있다(2026-11-13).
- 6개 모델의 출력 컬럼이 모두 정답에 한 줄 이상 있다. 예외는 상수 컬럼 `map_congestion_station.method` 하나다.

| 모델 | 줄 | DIRECT | INDIRECT |
|---|---|---|---|
| mart.dong_station | 50 | 13 | 37 |
| mart.commute_od | 145 | 13 | 132 |
| mart.dong_rent_summary | 24 | 8 | 16 |
| mart.dong_profile | 82 | 33 | 49 |
| wh.map_congestion_station | 19 | 7 | 12 |
| wh.map_dong_mois | 33 | 4 | 29 |
