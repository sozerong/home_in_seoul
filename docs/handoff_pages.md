# 지시서: 데이터 상품을 공개 정적 사이트(GitHub Pages)로 배포하기 (결정 113)

이 문서는 Claude Code가 읽고 한 단계씩 진행하는 지시서다. CLAUDE.md 1절 규칙(한 번에 하나, 만들기 전에 2~4줄 설명, 만든 뒤 멈춤, 설계 결정은 사용자)을 그대로 지킨다. 커밋과 푸시는 사용자가 한다. 이 문서는 2026-11-22에 작성했다.

## 0. 왜 하는가

- 사용자 방향 첫 줄은 "여러 출처의 데이터를 다른 사람이 쉽게 확인하고 가져가게 한다"이다. 지금 통로(Metabase, UI, Airflow, pgAdmin)는 모두 127.0.0.1에만 열려 있어(docker-compose.yml 75·157·181·201행), 작업 PC 밖에서 데이터를 가져간 증거가 없다. 이 단계가 그 증거를 만든다.
- 설계(docs/design_platform.md)의 B-b(목록), B-c(이용조건 검사), B-e(작업 PC 밖 경로), B-g(신선도 표시)의 첫 구현이 된다.
- 깊이 쪽 실측 하나가 생긴다: 정적 호스팅에 올린 Parquet를 DuckDB가 HTTP 범위 요청으로 필요한 열·행 그룹만 받는지, 받은 바이트로 잰다.

## 1. 사용자가 정한 것 (결정 113)

| 항목 | 결정 |
|---|---|
| 공개 범위 | 이용조건을 다시 찾아 출처를 붙여 공개 가능한 컬럼까지. 안 되는 컬럼만 뺀다 |
| 레포 | 데이터 전용 새 공개 레포(예: `sozerong/home_in_seoul-data`). 코드 레포에는 생성 코드만 둔다 |
| 가져가는 방법 | 카탈로그 페이지, 파일 + manifest, URL로 바로 조회하는 예시(pandas, DuckDB), 브라우저 안 SQL(DuckDB-WASM) |
| 확인 | 작업 PC 밖 에이전트가 공개 URL만 보고 인수 시험 질문을 푼다 |

## 2. 이용조건 (2026-11-22 공식 화면에서 다시 확인. 법률 검토 아님)

| 원천 | 확인한 표시 | 공개 판정 | 출처 문구에 넣을 것 |
|---|---|---|---|
| 국토교통부 전월세 실거래가(15126475, 15126473) | 공공데이터포털 "이용허락범위 제한 없음"(data_contract.md 83행) | 공개 | 국토교통부, 공공데이터포털 링크 |
| 서울 생활이동(서울특별시) | 공공데이터포털 15087677 "제3자 권리 포함: 저작권 표시, 공공저작물: 출처표시(제1유형)". 같은 계열 수도권 생활이동(OA-22657 등)도 열린데이터광장에서 공공누리 1유형 | 공개(출처와 저작권 표시) | "공공누리 제1유형에 따라 서울특별시 '서울 생활이동' 이용", 제3자 권리 저작권 표시, 열린데이터광장 링크 |
| 서울교통공사 혼잡도 | 열린데이터광장 OA-12928은 공공누리 3유형(변경금지). 같은 자료의 공공데이터포털 15071311은 "제한 없음" | 1차 제외. 6a에서 원천을 15071311로 바꾸고 값이 같음을 확인하면 공개 | 서울교통공사, 15071311 링크 |
| 서울시 역사마스터 | 공공누리 1유형(data_contract.md 5절) | 공개 | "공공누리 제1유형에 따라 서울특별시 역사마스터 정보 이용" |
| 소상공인 상가(상권)정보 | 공공데이터포털 "제한 없음" | 공개 | 소상공인시장진흥공단 |
| SGIS 행정동 경계 | 국가데이터처 통계정보 사이트 통합 이용약관 제15조: 사전 승낙 없이 서비스로 얻은 정보를 복사·복제·변경하거나 타인에게 제공할 수 없음, 영리행위 불가. 비영리여도 "타인 제공 금지"는 그대로다 | 도형을 직접 잰 값(area_km2, *_per_km2)은 1차 제외. 6b에서 서울시 행정구역(동별) 통계(공공누리 1유형) 면적으로 바꾸면 공개 | (바꾼 뒤) 서울특별시 행정구역(동별) 통계 |
| 행정동 코드·이름(dong_id, dong_nm, sgg_cd) | 통계청 한국행정구역분류로 공개된 코드 | 공개 | 통계청 한국행정구역분류 |
| 도형으로 위치만 정한 우리 계산 결과(어느 동에 속하는가, 대표점에서 역까지 거리, mois_dong_cd 대응) | SGIS 정보를 그대로 주는 것이 아니라 우리가 계산한 결과로 판단 | 공개(판단 근거를 계약에 적음) | 계산 방법 문서 링크 |

판단 기준(계약에 그대로 적는다):
- 공공누리 3유형은 상업·비상업 모두 이용할 수 있지만 2차적 저작물 작성(변경)을 금한다. 그래서 "비상업이라서 괜찮다"는 근거가 되지 않는다.
- 공공누리 출처 표시는 "공공누리에 따라 OOO의 공공저작물 이용" 형식에 링크를 붙인다. 공공기관이 우리를 후원하거나 특수한 관계인 것처럼 보이는 표시는 하지 않는다.
- 키·이름 컬럼이 불가·미정이 되면 빼지 않고 공개를 멈춘다(키가 빠지면 상품을 쓸 수 없다).
- GitHub Pages 쪽 조건: 게시 사이트 1GB 이하, 월 100GB 소프트 대역폭 한도, 온라인 사업용 무료 호스팅 금지(docs.github.com "GitHub Pages limits"). 버전당 약 22MB(v2026.11.16 실측)라 버전 40개쯤에서 1GB에 닿는다. 보존 버전 수는 B-i에서 정한다.

1차 공개에서 빠지는 컬럼: dong_profile의 area_km2, convenience_store_per_km2, supermarket_per_km2, pharmacy_per_km2, internal_pediatric_clinic_per_km2, laundry_per_km2, nearest_congestion_station_nm, nearest_congestion_station_route, nearest_congestion_station_distance_m, am_peak_congestion. 나머지 4개 표의 모든 컬럼은 공개한다.

## 3. 만들 것의 모양

데이터 레포(공개, Pages는 main 브랜치 루트에서):

```
index.html            카탈로그: 표마다 한 행의 뜻, 컬럼 뜻·타입·원천·이용조건, 버전 목록, 신선도(원천 수집일·기준월), 출처 문구, 빠진 컬럼과 이유
sql.html              브라우저 안 SQL(DuckDB-WASM). 최신 버전 파일을 뷰로 만들고 예시 질의 몇 개
                      (2026-11-22 포털로 바뀜, 8절: index.html은 포털 화면 + 정적 요약, sql.html은 index.html#/sql로 넘기는 파일)
catalog.json          기계가 읽는 목록(DCAT 참고): 표, 컬럼, 배포 파일 URL·sha256·바이트, 버전, 이용조건
latest.json           {"version": "v....", "manifest": "v..../manifest.json"} 최신을 가리키는 고정 주소
NOTICE.md             원천별 출처·이용조건 문구 전체
v2026.11.16/          공개 버전 폴더: 표마다 .parquet와 .csv, manifest.json(공개본)
```

코드 레포에 더할 것:
- `docs/public_columns.json`: 표·컬럼마다 공개 여부(공개/제외), 원천, 이용조건, 이유, 출처 문구 키. 이용조건 판단의 원본은 이 파일 하나다. docs/product_contract.json의 모든 컬럼이 정확히 한 번씩 있어야 한다.
- `warehouse/publish_site.py`: product/의 버전 하나를 읽어 공개 버전 폴더와 index·catalog·latest·NOTICE를 `--out` 폴더(사용자가 받아 둔 데이터 레포)에 쓴다.
  - product/는 읽기만 한다.
  - 공개 manifest는 원본 manifest에서 빠진 컬럼을 지운 schema, 공개 파일로 다시 잰 sha256·행 수, `excluded_columns`(이유 포함), 컬럼별 이용조건, 출처 문구를 담는다. created_at은 원본 값을 그대로 쓴다.
  - 같은 입력이면 같은 바이트를 낸다(JSON 키 정렬, 시각을 새로 찍지 않음, 행 순서는 원본 그대로). 이미 있는 공개 버전 폴더의 파일 sha256이 다르면 덮어쓰지 않고 실패한다(공개 버전은 불변).
  - `--selftest`: 빠진 컬럼이 parquet·csv 어디에도 없음, manifest sha256이 실제 파일과 같음, 두 번 돌리면 바이트가 같음, public_columns.json이 계약의 컬럼 집합과 같음, 키·이름 컬럼이 제외로 표시되면 실패함.
- git 명령(add, commit, push)은 Claude Code가 하지 않는다.

## 4. 단계 (각 단계 끝에서 멈추고 사용자 확인)

### 단계 P0. 이용조건 문서
- data_contract.md 5절을 2절 표로 갱신하고(확인 날짜 2026-11-22, 링크 포함), docs/public_columns.json을 만든다.
- 체크포인트: 사용자가 2절의 링크 두세 개를 직접 열어 표시와 비교한다.

### 단계 P1. 생성 코드
- warehouse/publish_site.py와 `--selftest`. 처음에는 index.html을 표와 링크만 있는 정적 HTML로 만든다(꾸미기는 뒤로). (2026-11-22 포털로 바뀜, 8절) index.html은 warehouse/site/index.html에 정적 요약을 넣어 쓴다(결정 114).
- 확인 방법: `python -m warehouse.publish_site --version v2026.11.16 --out <빈 폴더>`를 두 번 돌려 두 결과의 sha256 목록이 같은지, `--selftest` 통과.
- 체크포인트: 사용자가 공개 dong_profile.csv를 열어 1차 제외 컬럼이 없는지 본다.

### 단계 P2. 레포와 첫 배포 (사용자 일)
- 사용자가 공개 레포를 만들고, P1 결과를 넣어 푸시하고, Settings > Pages에서 main 브랜치 루트로 켠다.
- Claude Code는 할 일 목록과 확인 명령만 알려 준다.
- 체크포인트: 사용자가 `https://sozerong.github.io/<레포>/`에서 카탈로그를 연다.

### 단계 P3. URL로 가져가기와 범위 요청 실측
- index.html에 pandas·DuckDB 한 줄 예시를 넣는다.
- 실측(measurements.md에 조건과 함께):
  - `curl -sI` 응답에 Accept-Ranges가 있는지, `curl -r 0-99`가 206을 주는지.
  - DuckDB(httpfs)에서 공개 rent_listing.parquet의 한 컬럼 집계를 `EXPLAIN ANALYZE`로 돌려 HTTP 요청 수와 받은 바이트를 적고, 파일 전체 바이트와 비교한다. 3회.
- 예측을 먼저 적는다(CLAUDE.md, handoff_platform_cs.md 0.4): "범위 요청을 지원하면 한 컬럼 집계의 받은 바이트가 파일 전체보다 작다."
- 원인은 세 단계로 적는다: 현상(받은 바이트), 자료구조(Parquet 푸터·행 그룹·컬럼 청크), 운영체제·네트워크(HTTP Range, CDN 캐시).
- 체크포인트: 사용자가 같은 DuckDB 명령을 직접 돌려 본다.

### 단계 P4. 브라우저 안 SQL
- sql.html: DuckDB-WASM을 CDN에서 불러와 latest.json으로 최신 버전 파일을 뷰로 만들고, 예시 질의(구별 월세 중앙값, 출근 시간 짧은 출발 동)를 버튼으로 둔다. 같은 출처라 CORS 설정이 필요 없다. (2026-11-22 포털로 바뀜, 8절) SQL 화면은 index.html#/sql이고, sql.html은 그리로 넘기는 파일이다. 엔진 판은 결정 116.
- 체크포인트: 사용자가 브라우저에서 예시 질의를 돌리고 결과를 DuckDB CLI 결과와 비교한다.

### 단계 P5. 작업 PC 밖 인수 시험
- Claude Code가 공개 컬럼만으로 답할 수 있는 질문 2개와 정답(공개 버전 파일로 계산한 결과와 sha256)을 만든다. 정답은 레포 밖에 둔다. 질문 예:
  - "강남구(통계청 시군구 코드 11230) 행정동별, 이 버전 rent_listing의 오피스텔 월세 계약(lease_type = '월세', 월세 > 0) 월세 중앙값(percentile_cont 0.5). 계약 0건인 동도 행으로."
  - "commute_od에서 역삼1동이 직장인 20~34세 평균 이동 시간이 가장 짧은 출발 동 5개."
- 사용자가 질문 글만 클라우드 Claude에게 넘긴다. 클라우드 Claude는 공개 URL만 보고 결과 CSV와 걸린 단계·막힌 곳을 돌려준다. 정답과의 비교는 Claude Code가 로컬에서 한다(행 순서 무시, 상대 오차 0.1%).
- 체크포인트: 결과를 progress 문서에 적고, 막힌 곳을 카탈로그 개선 목록으로 남긴다.

### 단계 P6. 공개 범위 넓히기 (하나씩, 각각 선택지 표 → 사용자 결정 → decisions.md 기록)
- 6a 혼잡도: 15071311 파일 값과 지금 원천(OA-12928)이 같은지 비교한다. 같으면 수집 경로를 바꿀지, 출처 표기만 바꿀지 선택지로 올린다.
- 6b 면적: 서울시 행정구역(동별) 통계(1유형)의 동별 면적으로 area_km2를 바꾸는 방안. 동 이름을 dong_id로 잇는 규칙, 426개가 모두 이어지는지, 값이 SGIS 면적과 얼마나 다른지를 재서 선택지로 올린다. 계약 변경(schema_changes)이 따른다.

### 단계 P7. 갱신 절차
- export_product 뒤 publish_site를 돌리고 사용자가 푸시하는 순서를 README에 적는다. 자동화(Airflow 태스크)는 이 절차를 몇 번 돌린 뒤에 다시 본다.

## 5. 하지 않는 것
- .env, lake/, 원본 파일, 경계 도형은 데이터 레포에 넣지 않는다.
- 1차 제외 컬럼을 "출처만 붙여" 공개하지 않는다(2절 판정이 바뀌기 전까지).
- product/ 폴더를 고치지 않는다. 운영 DAG를 바꾸지 않는다.
- 대시보드나 웹 서비스(서버)를 만들지 않는다. 사이트는 정적 파일뿐이다(CLAUDE.md 6절 단계 9와 같음).

## 6. 끝난 기준
- 공개 URL에서 카탈로그, 파일, latest.json, sql.html이 열린다. (2026-11-22 포털로 바뀜, 8절) 지금 기준은 index.html 포털, 그 안의 SQL 편집기(#/sql), JavaScript 없이 보이는 정적 요약이다(handoff_portal.md 4절).
- `--selftest` 통과, 같은 입력 두 번에 같은 바이트.
- 범위 요청 실측이 measurements.md에 조건과 함께 있다.
- 작업 PC 밖 인수 시험 결과(정답 일치 횟수, 막힌 곳)가 기록돼 있다.
- data_contract.md 5절과 public_columns.json이 2절과 같다.

## 7. 관련 문서
- docs/design_platform.md: 전체 설계(v22). 이 지시서는 그중 결정 113 부분을 먼저 구현한다. 엔진 실험(단계 1·2·2b)은 이 지시서가 끝난 뒤 설계의 순서를 따른다.
- docs/evidence/: 설계가 인용한 직접 확인 기록(results.txt, pg_tests.sql, pg_tests.sh).

## 8. 포털 화면 확장 (2026-11-22, 사용자 요청: 분석팀이 실제로 쓰는 화면)
- 바꾼 파일: warehouse/site/index.html(새로 만듦, 서버 없는 한 페이지 포털), warehouse/publish_site.py(index.html을 생성하지 않고 이 파일을 복사, profiles/<버전>.json과 catalog.json의 title·relations·profiles를 더 씀, sql.html은 index.html#/sql로 넘기는 짧은 파일).
- 화면(참고한 것: Databricks Catalog Explorer의 표 개요·샘플 데이터·관계, 데이터 프로파일링 도구의 결측·분포 요약):
  - 데이터 목록: 표 카드(행·컬럼·크기·비공개 컬럼 수·원천), 신선도, 표끼리 잇는 키, 품질 지표. 왼쪽에서 표·컬럼·뜻 검색.
  - 표 화면 탭: 개요(파일, 한 행을 가르는 컬럼, 원천, 이어지는 표와 조인 SQL, 비공개 컬럼과 이유), 스키마(뜻·결측·고유값·범위), 미리보기(브라우저 안 DuckDB 100행), 프로필(히스토그램, 자주 나오는 값), 가져가기(URL·sha256, pandas·DuckDB·R·셸 코드), 버전.
  - SQL 편집기: 표 이름으로 바로 질의, Ctrl+Enter, 고른 부분만 실행, 예시 질의, 왼쪽 스키마 눌러 넣기, 결과 정렬, 차트, CSV 내려받기, 실행 계획(EXPLAIN ANALYZE), 링크 복사(질의가 주소에 들어감), 최근 질의(이 브라우저 localStorage).
  - 출처·이용조건: 원천별 이용조건·출처 문구 복사·쓰는 컬럼, 비공개 컬럼.
- 프로필은 publish_site가 pyarrow로 미리 계산한다(근사 없는 정확한 값, 숫자는 유효숫자 10자리). 공개 버전 폴더는 불변이라 그 밖(profiles/)에 두고 매번 다시 만든다. selftest 2b가 프로필 행 수·히스토그램 합을 확인한다.
- 확인(클라우드 Claude, 2026-11-22): 컨테이너에서 publish_site와 selftest 통과(1~7, 2b). 로컬 Range 지원 서버 + headless Chrome + DuckDB-WASM 1.33.1-dev57.0(로컬 사본)으로 모든 화면을 열고 예시 질의 실행, SUMMARIZE, 오류 표시, 정렬, 차트, 검색을 확인. 콘솔 오류 0. 실제 Pages(jsDelivr CDN)에서는 아직 열어 보지 않았다.
- warehouse/site/sql.html은 이제 쓰지 않는다(지워도 된다).
- 배포: `python -m warehouse.publish_site --version v2026.11.16 --out ../home_in_seoul-data` 뒤 데이터 레포에서 커밋·푸시(사용자). 공개 버전 폴더는 바이트가 같아 그대로 남고, index.html·catalog.json·profiles/·sql.html만 바뀐다.
