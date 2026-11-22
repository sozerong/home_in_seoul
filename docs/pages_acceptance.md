# 작업 PC 밖 인수 시험: 질문과 비교 규칙

목적: 공개 사이트 URL만 받은 바깥 에이전트가 공개 파일만으로 질문에 답할 수 있는지 확인한다([handoff_pages.md 단계 P5](handoff_pages.md#단계-p5-작업-pc-밖-인수-시험)). 이 문서는 2026-11-22에 작성했다.

- 대상 버전: v2026.11.16 (dong_profile 일부 컬럼을 뺀 공개본. 뺀 컬럼은 [public_columns.json](public_columns.json)의 `"public": false`)
- 이 문서에는 정답을 적지 않는다. 바깥 에이전트에게는 아래 "넘길 글"만 준다.
- 2026-11-22부터 사이트 첫 화면(index.html)은 JavaScript로 그리는 데이터 포털이다(handoff_pages.md 8절). JavaScript를 돌리지 않는 도구도 같은 주소에서 표, 공개 파일 8개 링크, 컬럼 사전을 평범한 HTML로 읽는다(결정 114). 기계가 읽는 입구는 `catalog.json`(표, 컬럼과 뜻·단위·코드 풀이, 파일 URL·sha256, 버전, 조인 키, 이용조건)과 `latest.json`이다.

## 1. 넘길 글

사이트 주소는 P2에서 만든 데이터 레포(sozerong/home_in_seoul-data)의 Pages 주소다. 두 질문을 한 번에 넘긴다.

```text
아래 공개 사이트에 있는 데이터 상품 버전 v2026.11.16의 파일만 써서 두 질문에 답해 주세요.
사이트: https://sozerong.github.io/home_in_seoul-data/
사이트 밖 자료는 쓰지 마세요. 더 새 버전이 있어도 v2026.11.16을 씁니다.
행정동은 dong_id(통계청 8자리 코드)로 구분하고, 표끼리는 dong_id로 잇습니다.

질문 1.
강남구(dong_profile.sgg_cd = '11230', 통계청 시군구 코드)에 속한 행정동 각각에 대해,
rent_listing의 오피스텔 월세 계약의 계약 수와 월세 중앙값을 구해 주세요.
- 대상 동: dong_profile에서 sgg_cd = '11230'인 행정동 전부. rent_listing에는 시군구 코드가 없으므로
  rent_listing.dong_id = dong_profile.dong_id로 이어서 강남구를 정합니다.
- 대상 계약: source_type = 'offi' 이고 lease_type = '월세' 이고 monthly_rent_10k_won > 0 인 행.
  rent_listing에 들어 있는 모든 계약월을 쓰고, 기간으로 더 거르지 않습니다.
- deals: 대상 계약 수.
- median_rent_10k_won: 대상 계약 monthly_rent_10k_won(만원)의 중앙값. 보간하는 중앙값
  (SQL의 percentile_cont(0.5), 건수가 짝수면 가운데 두 값의 평균). 반올림하지 않습니다.
- 대상 계약이 0건인 동도 한 행으로 넣습니다. 그 행은 deals = 0, median_rent_10k_won은 빈 칸(NULL).
  그래서 행 수는 강남구 행정동 수와 같아야 합니다.
- 출력 컬럼: dong_id, dong_nm, deals, median_rent_10k_won (dong_nm은 dong_profile의 값)

질문 2.
commute_od에서 직장(dest_dong_id)이 역삼1동인 행 가운데 avg_minutes_age20_34(20~34세 평균 이동 시간, 분)가
비어 있지 않은 행을 avg_minutes_age20_34 오름차순, 값이 같으면 origin_dong_id 오름차순으로 정렬해
앞에서 5개 행의 출발 동을 구해 주세요.
- 역삼1동의 dong_id는 dong_profile에서 dong_nm = '역삼1동'인 행으로 찾습니다.
- 출발 동이 역삼1동 자신인 행도 후보에서 빼지 않습니다.
- avg_minutes나 avg_minutes_all_ages가 아니라 avg_minutes_age20_34를 씁니다. 값은 파일 값 그대로 둡니다.
- origin_dong_nm: origin_dong_id = dong_profile.dong_id로 이은 dong_profile.dong_nm.
- 출력 컬럼: origin_dong_id, origin_dong_nm, avg_minutes_age20_34 (5행)

돌려줄 것:
1. 질문마다 결과 CSV 하나(UTF-8, 첫 줄은 위 출력 컬럼 이름 그대로, 빈 값은 빈 칸).
2. 거친 단계: 어느 페이지·파일·URL을 열었고, 어떤 도구와 질의(코드)를 썼는지 순서대로.
3. 막힌 곳: 찾기 어려웠던 것, 헷갈린 컬럼 뜻, 추측으로 정한 것, 실패한 시도와 그 오류.
```

## 2. 질문이 쓰는 컬럼

모두 public_columns.json에서 `"public": true`다. 정답 계산 스크립트도 공개 컬럼만 남긴 뷰로 계산하고, 쓰는 컬럼이 공개인지 먼저 확인한다.

| 표 | 컬럼 | 뜻의 근거 |
|---|---|---|
| dong_profile | dong_id, dong_nm, sgg_cd | data_contract.md 3절 dong_profile |
| rent_listing | dong_id, source_type, lease_type, monthly_rent_10k_won | data_contract.md 3절 rent_listing (`lease_type`: 월세(월세 > 0) / 전세(월세 = 0)) |
| commute_od | origin_dong_id, dest_dong_id, avg_minutes_age20_34 | data_contract.md 3절 commute_od |

## 3. 비교 규칙

정답 비교는 작업 PC에서 Claude Code가 한다.

- 행 순서는 무시한다. 키 컬럼으로 행을 맞춘다(질문 1은 dong_id, 질문 2는 origin_dong_id).
- 숫자 칸은 상대 오차 0.1% 이하면 같다(정답이 0이면 절대 차이 1e-9 이하).
- NULL(빈 칸)은 NULL과만 같다. 빈 칸과 0은 다르다.
- 숫자가 아닌 칸(dong_nm 등)은 앞뒤 공백을 뺀 글자가 같아야 한다.
- 컬럼 집합이 다르거나, 빠진 행·남는 행·다른 칸이 하나라도 있으면 그 질문은 불일치다.

실행:

```
python -m measure.pages_compare C:\Users\dudfo\Desktop\pages_acceptance_answers\q1.csv 받은_q1.csv
python -m measure.pages_compare C:\Users\dudfo\Desktop\pages_acceptance_answers\q2.csv 받은_q2.csv
```

같으면 종료 코드 0과 "같음", 다르면 종료 코드 1과 다른 점 목록이 나온다. 비교 규칙 자체는 `python -m measure.pages_compare --selftest`로 확인한다.

## 4. 정답 위치

`C:\Users\dudfo\Desktop\pages_acceptance_answers\` (코드 레포와 데이터 레포 밖)

- `q1.csv`, `q2.csv`: 정답
- `answers.json`: 버전, 원본 parquet sha256(manifest 값과 다시 잰 값), 계산 SQL, 행 수, 정답 CSV sha256, 계산 날짜

## 5. 처음 보는 사람 시험 결과 (handoff_portal.md Q6, 2026-11-22)

- 방법: 매번 새 하위 에이전트(이 대화 맥락 없음)에게 로컬 사이트 주소(127.0.0.1:8765, 범위 요청을 지원하는 표준 라이브러리 서버)만 주고 세 가지를 시켰다. 프로젝트 폴더는 열지 않게 했다.
  1. 월세 중앙값 컬럼의 단위와 뜻
  2. 강남구 동 목록을 SQL로
  3. 출퇴근 시간 값을 그대로 믿어도 되는가
- 보고 형식: A 답, B 단계, C 막힌 곳, D 뜻을 몰랐던 말, E 못 찾은 것. 회차 사이에 D·E를 고치고 사이트를 다시 만들었다. 최대 3회.
- 세 질문의 답은 세 회차 모두 맞았다:
  - 질문 1: dong_profile.monthly_median_rent_10k_won, 만원. 3회차는 426개 동 전부를 rent_listing에서 다시 계산해 모두 같았다.
  - 질문 2: `WHERE sgg_cd = '11230'`, 22개 동.
  - 질문 3: "그대로 믿으면 안 됨, 상대 비교와 직선거리 보완".

| 회차 | 뜻을 몰랐던 말(D) | 고친 것 |
|---|---|---|
| 1 | 신규 계약, 결정 113·OA-12928·3유형·6a·6b, 내부 문서 이름(링크 없음), 동 안쪽 대표점, 보정한 인구, house_type '연립다세대', 업종 코드, 필지, 공공누리 제1유형, commute_quarantine_rows, DuckDB-WASM·EXPLAIN ANALYZE·SUMMARIZE 등 11개 묶음 | 도움말 용어 풀이 추가, 근거 파일 이름을 코드 레포 링크로, 비공개 이유 글 용어 풀이(EXCLUDED_TERMS), 코드 값 풀이(구 코드 25개, house_type 등), 키 카드를 catalog.primary_key로, 5·6개월 문구 통일, 소표본·먼데 빠른 쌍·출근 방향만 있음을 notes에 |
| 2 | manifest·rent_months, "최대 6개월"(문구가 잘림, F-39), OA-12928·6a·6b(NOTICE.md·catalog), 약관 제15조, 근거 칸의 SQL 용어, 원천 키, 혼잡도 수집일, 거절된 달 등 8개 | grain 잘림 고침과 selftest 검사, 컬럼 설명의 manifest를 홈 '신선도' 표로, rent_listing에 기간 상한 note, NOTICE.md에 용어 풀이, 혼잡도 수집일·거절된 달 풀이, 근거 칸 안내, data_contract.md 6절 3·4를 v2026.11.16 수치로 |
| 3 | 이동인구 합의 실체(사람 수인지 누적인지), 6a·6b·OA-12928(catalog.json), HW, 보정계수, 근거 칸의 SQL·dbt 용어, 프로필 nulls, "시험 1회차" 근거 글, 제3자 권리 포함 저작권 표시 등 8개 | population_* 설명·단위를 "6개월 이동 횟수 합"으로(원천이 월마다 요일별로 더한 값, 매뉴얼 161행, spark_sql/living/30_commute.sql), "30명·10명" 문구를 숫자로, catalog.json에 excluded_terms, 근거 글 두 곳을 평범한 말로, 결정 22 근거를 "끝에 덧붙인 문단"으로, question.md 2절과 3절 어긋남을 잇는 문장 |

- 결과: 3회 안에 "뜻을 몰랐던 말"이 0이 되지 않았다. 3회차 뒤 고친 것은 다시 시험하지 않았다(최대 3회).
- 남은 것과 이유:
  - 근거 칸의 SQL·dbt 용어(percentile_cont, left join, dbt models): 근거는 확인하려는 분석가를 위한 코드 위치라 그대로 두고, 스키마 화면에 "확인하고 싶을 때만 보면 됩니다" 안내를 달았다.
  - HW(이동유형 코드): 사이트에는 나오지 않고 GitHub question.md에만 있다.
  - "제3자 권리 포함 저작권 표시": 원천 이용조건 글을 그대로 옮긴 것이라 바꾸지 않았다. 공공누리 제1유형과 어떻게 함께 적용되는지는 법률 판단이라 사용자 몫이다 [확인 필요].
  - 원천 키(derived_location 등): catalog.json에만 있고 화면은 한글 이름이다.
- 코드 레포 문서와의 차이: 2·3회차 시험자는 GitHub main의 data_contract.md(역삼1동 148.7분, "경로의 14%", "최근 6개월")가 사이트 수치와 다르다고 지적했다. 이 문서들은 로컬에서 고쳤지만 아직 커밋·푸시 전이다. docs/public_columns.json도 아직 커밋 전이라, 근거 링크가 GitHub에서 404다. 커밋·푸시 뒤에야 맞는다(사용자 몫).
- 시험 중 사이트 밖 사정: 시험자 둘 다 브라우저 SQL 편집기로 실행했고 콘솔 오류는 보고하지 않았다. 첫 실행은 엔진을 내려받느라 몇 초 기다렸다(측정 57).
