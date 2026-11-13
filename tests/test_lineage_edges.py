"""lineage/edges.py의 간선 추출 시험. DB, Spark, dbt 없이 돈다(CI).

패턴마다 작은 SQL 하나. 실제 모델 대조는 measure/lineage_score.py(정답 6개 모델)가 한다.
실행: python -m tests.test_lineage_edges
"""
from lineage.edges import extract_edges

SCHEMA = {"db": {"s": {
    "t": {"id": "text", "k": "text", "v": "int", "d": "text", "p": "text"},
    "u": {"id": "text", "w": "int", "typ": "text"},
    "st": {"sid": "text", "nm": "text", "pt": "text", "x": "text", "y": "text"},
}, "raw": {"c": {"row_no": "int", "raw": "jsonb"}}}}


def edges(sql):
    rows = extract_edges(sql, "db.s.out", SCHEMA)
    assert all(r["status"] == "ok" for r in rows), rows
    return {(r["target_column"], r["source_table"].split(".")[-1], r["source_column"], r["kind"], r["subkind"]) for r in rows}


def of(found, target):
    return {e[1:] for e in found if e[0] == target}


# 값 관계: 그대로 / 계산 / 집계, count(*)는 표의 행 '*'
e = edges("select t.id, upper(t.k) as k2, sum(t.v) as total, count(*) as n from db.s.t t group by t.id, t.k")
assert ("t", "id", "DIRECT", "IDENTITY") in of(e, "id")
assert ("t", "k", "DIRECT", "TRANSFORMATION") in of(e, "k2")
assert ("t", "v", "DIRECT", "AGGREGATION") in of(e, "total")
assert ("t", "*", "DIRECT", "AGGREGATION") in of(e, "n")
assert ("t", "k", "INDIRECT", "GROUP_BY") in of(e, "total")  # GROUP BY는 모든 출력에

# CASE 조건과 집계 FILTER는 그 출력에만 CONDITIONAL. 결과가 상수뿐인 CASE는 DIRECT가 없다
e = edges("select case when t.v > 1 then 'big' else 'small' end as size, "
          "sum(t.v) filter (where t.d = 'x') as vx, t.id from db.s.t t group by t.id")
assert of(e, "size") >= {("t", "v", "INDIRECT", "CONDITIONAL")}
assert not {x for x in of(e, "size") if x[2] == "DIRECT"}
assert ("t", "d", "INDIRECT", "CONDITIONAL") in of(e, "vx") and ("t", "v", "DIRECT", "AGGREGATION") in of(e, "vx")
assert ("t", "d", "INDIRECT", "CONDITIONAL") not in of(e, "id")

# 창 함수: PARTITION BY, ORDER BY는 WINDOW
e = edges("select t.id, row_number() over (partition by t.k order by t.v desc) as rk from db.s.t t")
assert of(e, "rk") == {("t", "k", "INDIRECT", "WINDOW"), ("t", "v", "INDIRECT", "WINDOW")}

# CTE 안의 WHERE는 그 CTE를 거쳐 나가는 출력에. LEFT JOIN의 ON은 오른쪽에서 온 출력에만(규칙 2)
e = edges("with a as (select t.id, t.v from db.s.t t where t.d = 'x') "
          "select a.id, a.v, u.w from a left join db.s.u u on u.id = a.id and u.typ = 'm'")
assert ("t", "d", "INDIRECT", "FILTER") in of(e, "id") and ("t", "d", "INDIRECT", "FILTER") in of(e, "v")
assert ("u", "typ", "INDIRECT", "JOIN") in of(e, "w")
assert not {x for x in of(e, "id") if x[0] == "u"}  # 왼쪽 행은 빠지지 않는다

# 결정 68: INNER JOIN 조건이 거쳐 간 CTE의 필터는 그 조인 결과 전체(다른 표에서 온 출력 포함)에 번진다
e = edges("with recent as (select t.id from db.s.t t where t.d = 'x') "
          "select u.w from db.s.u u join recent r on r.id = u.id")
assert ("t", "d", "INDIRECT", "FILTER") in of(e, "w") and ("t", "id", "INDIRECT", "JOIN") in of(e, "w")

# LATERAL 정렬 + LIMIT 1: s.*를 펼쳐 고른 컬럼만 값 출처로(과다 귀속 없음), 정렬 기준은 SORT. 상관 참조 c.p도 푼다
e = edges("with c as (select t.id, t.p from db.s.t t) "
          "select c.id, a.nm from c cross join lateral (select s.* from db.s.st s order by s.pt <-> c.p limit 1) a")
assert {x for x in of(e, "nm") if x[2] == "DIRECT"} == {("st", "nm", "DIRECT", "IDENTITY")}
assert ("st", "pt", "INDIRECT", "SORT") in of(e, "nm") and ("t", "p", "INDIRECT", "SORT") in of(e, "nm")
assert not {x for x in of(e, "id") if x[2] == "INDIRECT"}  # LATERAL의 정렬은 c.id에 붙지 않는다

# IN (서브쿼리)의 고른 값은 FILTER, 서브쿼리 안의 ORDER BY 별칭은 SORT로 그대로
e = edges("with m as (select distinct t.d from db.s.t t order by d desc limit 2) "
          "select u.w from db.s.u u where u.typ in (select m.d from m)")
assert of(e, "w") >= {("u", "typ", "INDIRECT", "FILTER"), ("t", "d", "INDIRECT", "FILTER"), ("t", "d", "INDIRECT", "SORT")}

# UNION ALL: 두 갈래 모두 출처. 상수 컬럼은 간선이 없다
e = edges("select t.id, 'a' as src from db.s.t t union all select u.id, 'b' from db.s.u u")
assert of(e, "id") == {("t", "id", "DIRECT", "IDENTITY"), ("u", "id", "DIRECT", "IDENTITY")}
assert of(e, "src") == set()

# 원천 모양 표(raw)의 jsonb 키는 키 단위, 함수 LATERAL(jsonb_each_text)의 출력은 인자 컬럼에서 온다(혼잡도 이전, 결정 60)
e = edges("select btrim(r.raw ->> 'DOW_SE', E' \t') as dow, r.raw, t.value from db.raw.c r "
          "cross join lateral jsonb_each_text(r.raw) t where t.key in ('TIME0530')")
assert of(e, "dow") == {("c", "raw.DOW_SE", "DIRECT", "TRANSFORMATION"), ("c", "raw", "INDIRECT", "FILTER")}, of(e, "dow")
assert ("c", "raw", "DIRECT", "IDENTITY") in of(e, "raw")
assert ("c", "raw", "DIRECT", "TRANSFORMATION") in of(e, "value")

# 파싱 실패는 예외 대신 parse_error 행 하나
rows = extract_edges("select from where", "db.s.out", SCHEMA)
assert len(rows) == 1 and rows[0]["status"] == "parse_error", rows

print("lineage edges 시험 통과")
