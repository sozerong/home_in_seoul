"""컴파일된 SQL 한 개에서 컬럼 간선을 뽑는다 (P4 2단계, 결정 46, 54, 67).

extract_edges(sql_text, model, schema)는 Spark를 모르는 순수 함수다. 엔진(Python 단독 / Spark)은 lineage/extract.py가 고른다(결정 44).

방법: sqlglot으로 qualify(별칭·* 풀기)한 뒤 build_scope로 쿼리를 scope(본문, CTE, 서브쿼리, LATERAL, UNION 갈래)로 나누고,
컬럼 하나를 scope를 건너 물리 표 컬럼까지 따라간다(resolve). 같은 해석기로 두 가지를 만든다.
  DIRECT   출력 식의 값 자리 컬럼. 경로에 집계가 있으면 AGGREGATION, 계산이 있으면 TRANSFORMATION, 아니면 IDENTITY.
  INDIRECT (1) 출력 식 안의 조건 자리: CASE WHEN 조건, 집계 FILTER(where) → CONDITIONAL, 창 PARTITION/ORDER → WINDOW.
              이 둘은 그 식이 든 출력 컬럼에만 붙는다.
           (2) scope의 절: WHERE·HAVING → FILTER, JOIN ON → JOIN, GROUP BY → GROUP_BY, ORDER BY → SORT.
              출력 컬럼을 따라가다 컬럼을 실제로 풀어 본 scope의 절만 그 출력에 붙는다(설계서 1.4 규칙 4).
              LEFT JOIN의 ON은 그 조인의 오른쪽 별칭을 거쳐 온 출력에만 붙는다(규칙 2).
count(*)처럼 컬럼이 없는 집계는 FROM 표의 행 '*'를 출처로 둔다. 풀지 못한 컬럼은 status='unresolved'로 남긴다.
"""
from typing import NamedTuple

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope

DIALECT = "postgres"
NESTED = (exp.Select, exp.Subquery, exp.SetOperation)  # 이 아래 컬럼은 다른 scope 것이다


class Leaf(NamedTuple):
    table: str | None       # 물리 표 원문 이름(seoul.wh_build.dim_dong). None이면 풀지 못함
    column: str             # 컬럼 이름 또는 '*'
    cond: str | None = None  # None(값 자리) / 'CONDITIONAL' / 'WINDOW'
    agg: bool = False
    transform: bool = False


def phys(table):
    return ".".join(p for p in (table.catalog, table.db, table.name) if p)


def own(node, cls):
    """node 안의 cls 노드 중 하위 쿼리(다른 scope)에 들지 않은 것."""
    return [n for n in node.walk(prune=lambda n: isinstance(n, NESTED) and n is not node) if isinstance(n, cls)]


def cond_kind(col, top):
    """col이 top 식 안에서 조건 자리이면 그 종류. 가장 바깥(top에 가까운) 조건이 이긴다."""
    kind, child, node = None, col, col.parent
    while node is not None and child is not top:
        if isinstance(node, exp.If) and child is node.args.get("this"):
            kind = "CONDITIONAL"
        elif isinstance(node, exp.Filter) and child is node.args.get("expression"):
            kind = "CONDITIONAL"
        elif isinstance(node, exp.Window) and child is not node.this:
            kind = "WINDOW"
        child, node = node, node.parent
    return kind


class Resolver:
    """한 쿼리의 scope 나무 위에서 컬럼을 물리 표까지 따라간다."""

    def __init__(self, root):
        self.root = root
        self.scope_of = {id(s.expression): s for s in root.traverse()}
        self._indirect = {}

    # ---- 값과 조건: 출력 컬럼 하나를 잎까지 ----
    def output(self, scope, name, seen):
        """scope가 내는 이름 name의 출처 잎. seen에는 (scope id, 거쳐 온 별칭)을 쌓는다."""
        node = scope.expression
        if isinstance(node, exp.SetOperation):  # UNION: 갈래마다 같은 자리의 select
            position = node.named_selects.index(name)
            return [leaf for branch in self.branches(node)
                    for leaf in self.select_leaves(self.scope_of[id(branch)], branch.selects[position], seen)]
        select = next(s for s in node.selects if s.alias_or_name == name)
        return self.select_leaves(scope, select, seen)

    def branches(self, node):
        if isinstance(node, exp.SetOperation):
            return self.branches(node.this) + self.branches(node.expression)
        return [node]

    def select_leaves(self, scope, select, seen):
        expr = select.unalias()
        agg = bool(own(expr, exp.AggFunc))
        transform = not isinstance(expr, exp.Column)
        leaves = []
        for col in own(expr, exp.Column):
            kind = cond_kind(col, expr)
            for leaf in self.column(scope, col, seen):
                leaves.append(leaf._replace(cond=kind or leaf.cond, agg=leaf.agg or agg,
                                            transform=leaf.transform or transform))
        for star in own(expr, exp.Star):  # count(*)
            kind = cond_kind(star, expr)
            leaves += [leaf._replace(cond=kind or leaf.cond, agg=True, transform=True)
                       for leaf in self.star(scope, seen)]
        for sub in own(expr, exp.Subquery):  # 스칼라 서브쿼리
            inner = self.scope_of[id(sub.this)]
            for s in sub.this.selects:
                leaves += [leaf._replace(agg=leaf.agg or agg, transform=True) for leaf in self.select_leaves(inner, s, seen)]
        return leaves

    def column(self, scope, col, seen):
        """scope 안의 컬럼 참조 하나를 물리 표 잎까지."""
        alias, source, here = col.table, None, scope
        if not alias and col.name in scope.expression.named_selects:  # ORDER BY "target_ym"처럼 select 별칭을 가리킴
            return self.output(scope, col.name, seen)
        while here is not None and source is None:  # 상관 참조(LATERAL 안의 c.p)는 바깥 scope에서 찾는다
            source = here.sources.get(alias)
            if source is None:
                here = here.parent
        if source is None:
            return [Leaf(None, f"{alias}.{col.name}")]
        seen.add((id(here), alias))
        if isinstance(source, exp.Table):
            # 원천 모양 표(raw 스키마)의 jsonb는 키 단위로 본다: raw ->> 'DOW_SE'는 'raw.DOW_SE' (결정 54의 보정, 60)
            return [Leaf(phys(source), json_key(col) if source.db == "raw" else col.name)]
        if is_function(source):  # jsonb_each_text(r.raw) t: t.key, t.value는 함수 인자 컬럼에서 계산된다
            return [leaf._replace(transform=True) for arg in own(source.expression, exp.Column)
                    for leaf in self.column(source, arg, seen)]
        return self.output(self.inner(source), col.name, seen)

    def inner(self, scope):
        """LATERAL(UDTF) scope는 안쪽 서브쿼리 scope로 바꾼다."""
        while not isinstance(scope.expression, (exp.Select, exp.SetOperation)):
            scope = scope.sources[""]  # 서브쿼리 LATERAL. 함수 LATERAL은 column()이 먼저 처리한다
        return scope

    def star(self, scope, seen):
        """count(*)의 출처: FROM 표의 행. FROM이 UNION이면 갈래마다 (F-10 처방 A)."""
        if isinstance(scope.expression, exp.SetOperation):
            return [leaf for branch in self.branches(scope.expression) for leaf in self.star(self.scope_of[id(branch)], seen)]
        source_expr = scope.expression.args["from_"].this  # sqlglot 30에서 인자 이름이 from_이다
        alias = source_expr.alias_or_name
        seen.add((id(scope), alias))
        source = scope.sources[alias]
        if isinstance(source, exp.Table):
            return [Leaf(phys(source), "*")]
        return self.star(self.inner(source), seen)

    # ---- 행 관계: scope의 절 ----
    def clauses(self, scope):
        """scope의 절에서 나온 (잎, subkind) 목록. {'all': [...], 'left': {오른쪽 별칭: [...]}}"""
        if id(scope) in self._indirect:
            return self._indirect[id(scope)]
        found = {"all": [], "left": {}}
        self._indirect[id(scope)] = found  # 먼저 넣어 두어 상관 참조가 돌아와도 끝없이 돌지 않는다
        node = scope.expression
        if isinstance(node, exp.Select):
            parts = [("FILTER", node.args.get("where"), None), ("FILTER", node.args.get("having"), None),
                     ("GROUP_BY", node.args.get("group"), None), ("SORT", node.args.get("order"), None)]
            for join in node.args.get("joins") or []:
                side = join.this.alias_or_name if join.side == "LEFT" else None
                parts.append(("JOIN", join.args.get("on"), side))
            for subkind, clause, side in parts:
                if clause is None:
                    continue
                seen = set()
                items = [(leaf, subkind) for leaf in self.clause_leaves(scope, clause, seen)]
                # 결정 68: 조건 컬럼이 거쳐 간 파생 표(CTE, LATERAL)가 행을 고른 절도 이 조건과 같은 범위에 붙는다.
                # 예: on b.origin_cd = ...의 b(by_code)가 최근 6개월로 걸렀으면, 그 필터는 이 조인의 결과 전체에 번진다
                items += self.attached(seen, skip=self.ancestors(scope))
                target = found["left"].setdefault(side, []) if side else found["all"]
                target += items
        return found

    def ancestors(self, scope):
        out = set()
        while scope is not None:
            out.add(id(scope))
            scope = scope.parent
        return out

    def attached(self, seen, skip=frozenset()):
        """seen(거쳐 온 (scope, 별칭))에 해당하는 절의 (잎, subkind). LEFT JOIN 절은 오른쪽 별칭을 거쳐 왔을 때만."""
        items = []
        for scope in self.root.traverse():
            if id(scope) in skip or not any(sid == id(scope) for sid, _ in seen):
                continue
            found = self.clauses(scope)
            items += found["all"]
            items += [item for side, side_items in found["left"].items() if (id(scope), side) in seen for item in side_items]
        return items

    def clause_leaves(self, scope, clause, seen):
        leaves = [leaf for col in own(clause, exp.Column) for leaf in self.column(scope, col, seen)]
        leaves += [leaf for star in own(clause, exp.Star) for leaf in self.star(scope, seen)]
        for sub in own(clause, exp.Subquery) + [q for q in own(clause, exp.Select) if q is not clause]:
            query = sub.this if isinstance(sub, exp.Subquery) else sub
            inner = self.scope_of[id(query)]
            for s in query.selects:  # in (select ...)의 고른 값. 하위 쿼리 자신의 절은 seen을 거쳐 attached()가 제 subkind로 붙인다
                leaves += self.select_leaves(inner, s, seen)
        return leaves


def is_function(scope):
    """LATERAL 함수(jsonb_each_text 등) scope. 서브쿼리 LATERAL과 달리 출력 컬럼이 인자에서 바로 나온다."""
    node = scope.expression
    return isinstance(node, exp.Lateral) and not isinstance(node.this, (exp.Subquery, exp.Query))


def json_key(col):
    """col ->> '키' / col -> '키'처럼 글자 키 하나로 꺼내면 'col.키', 아니면 컬럼 이름 그대로."""
    parent = col.parent
    if isinstance(parent, (exp.JSONExtract, exp.JSONExtractScalar)) and parent.this is col:
        path = parent.expression
        parts = path.expressions if isinstance(path, exp.JSONPath) else []
        if len(parts) == 2 and isinstance(parts[1], exp.JSONPathKey) and isinstance(parts[1].this, str):
            return f"{col.name}.{parts[1].this}"
    return col.name


def rowset_tables(root):
    """FROM·JOIN에 나온 물리 표(CTE, 파생 표, WHERE 안 하위 쿼리 포함). SELECT 목록의 스칼라 하위 쿼리와 그 안은 뺀다:
    값 하나를 계산할 뿐 행 집합을 정하지 않는다."""
    tables = set()
    for scope in root.traverse():
        if not any(in_select_list(s) for s in (scope, *parents(scope))):
            tables |= {phys(s) for s in scope.sources.values() if isinstance(s, exp.Table)}
    return tables


def parents(scope):
    while scope.parent is not None:
        scope = scope.parent
        yield scope


def in_select_list(scope):
    """scope가 부모 SELECT의 출력 식 안에 든 하위 쿼리인가."""
    if scope.parent is None or not scope.is_subquery:
        return False
    node, top = scope.expression, scope.parent.expression
    while node.parent is not None and node.parent is not top:
        node = node.parent
    return node.parent is top and node.arg_key == "expressions" and isinstance(top, exp.Select)


def subkind(leaf):
    return "AGGREGATION" if leaf.agg else "TRANSFORMATION" if leaf.transform else "IDENTITY"


def extract_edges(sql_text, model, schema, dialect=DIALECT):
    """컴파일된 SQL 하나의 간선 목록. 실패해도 예외를 던지지 않고 status='parse_error' 행 하나를 돌려준다.
    dialect: dbt 모델은 postgres, Spark SQL 묶음(lineage/spark_sql.py)은 spark."""
    base = {"target_table": model}
    try:
        tree = qualify(sqlglot.parse_one(sql_text, dialect=dialect), schema=schema, dialect=dialect,
                       validate_qualify_columns=False)
        root = build_scope(tree)
        resolver = Resolver(root)
        edges = set()
        for name in tree.named_selects:
            seen = set()
            leaves = resolver.output(root, name, seen)
            rows = [(leaf, "DIRECT", subkind(leaf)) if leaf.cond is None else (leaf, "INDIRECT", leaf.cond)
                    for leaf in leaves]
            rows += [(leaf, "INDIRECT", sk) for leaf, sk in resolver.attached(seen)]
            for leaf, kind, sk in rows:
                edges.add((name, leaf.table, leaf.column, kind, sk))
        # ROWSET (결정 80): 대상 행 집합 '*'의 출처. (1) FROM·JOIN 원천표의 행, (2) 행 조건(WHERE·JOIN·GROUP BY) 컬럼.
        # (2)는 위에서 출력 컬럼마다 붙인 INDIRECT 절 잎의 합이다. ponytail: 표 단위로 넓게 잡힌다(결정 80이 받아들인 대가)
        for table in rowset_tables(root):
            edges.add(("*", table, "*", "ROWSET", "FROM"))
        for target, table, column, kind, sk in list(edges):
            if kind == "INDIRECT" and sk in ("FILTER", "JOIN", "GROUP_BY") and table is not None:
                edges.add(("*", table, column, "ROWSET", sk))
    except Exception as e:  # 파싱 실패는 버리지 않고 남긴다 (CLAUDE.md 4절 4번)
        return [{**base, "target_column": None, "source_table": None, "source_column": None, "kind": None,
                 "subkind": None, "status": "parse_error", "detail": f"{type(e).__name__}: {e}"[:500]}]
    return [{**base, "target_column": t, "source_table": st, "source_column": sc, "kind": k, "subkind": sk,
             "status": "ok" if st is not None else "unresolved", "detail": None}
            for t, st, sc, k, sk in sorted(edges, key=lambda e: tuple("" if x is None else x for x in e))]
