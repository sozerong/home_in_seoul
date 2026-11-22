#!/bin/bash
# PostgreSQL 16.15, 소켓 /tmp 포트 5499. 매 시험 전에 pg_tests.sql로 초기화한다.
P="psql -h /tmp -p 5499 -U postgres -qtA"
now(){ date +%s.%N; }
reset(){ $P -f "$(dirname "$0")/pg_tests.sql" >/dev/null 2>&1; }
reset; echo "T1 리더가 t를 먼저 읽음 → TRUNCATE 대기"
($P -c "begin isolation level repeatable read; select 'before',count(*) from t; select pg_sleep(3); select 'after',count(*) from t; commit;") & sleep 0.5
$P -c "begin; truncate t; insert into t select g,'new' from generate_series(1,100000) g; commit;"; wait
reset; echo "T1b 스냅샷만 먼저(other), t는 커밋 뒤 첫 접근 → 빈 표"
($P -c "begin isolation level repeatable read; select 'snap',count(*) from other; select pg_sleep(3); select 'after',count(*) from t; commit;") & sleep 0.5
$P -c "begin; truncate t; insert into t select g,'new' from generate_series(1,100000) g; commit;"; wait
reset; echo "T2 DELETE+INSERT 중 읽기"
$P -c "begin; delete from t; insert into t select g,'new' from generate_series(1,100000) g; select pg_sleep(3); commit;" & sleep 0.5
s=$(now); $P -c "select 'reader',v,count(*) from t group by v"; echo "wait $(echo "$(now)-$s"|bc)"; wait
reset; echo "T3 긴 읽기 → DETACH+ATTACH(한 트랜잭션) → 새 읽기"
$P -c "begin; select count(*) from p; select pg_sleep(4); commit;" >/dev/null & sleep 0.5
$P -c "begin; alter table p detach partition p_1; alter table p attach partition p_1n for values in (1); commit;" & sleep 0.5
s=$(now); $P -c "select 'reader',v,count(*) from p group by v"; echo "wait $(echo "$(now)-$s"|bc)"; wait
reset; echo "T4 lock_timeout"
$P -c "begin; select count(*) from p; select pg_sleep(3); commit;" >/dev/null & sleep 0.5
$P -c "set lock_timeout='500ms'; begin; alter table p detach partition p_1; commit;" 2>&1 | head -1; wait
reset; echo "T5 긴 읽기 중 DETACH CONCURRENTLY, 붙이기 전 새 읽기"
$P -c "begin; select count(*) from p; select pg_sleep(4); commit;" >/dev/null & sleep 0.5
(d=$(now); $P -c "alter table p detach partition p_1 concurrently;"; echo "detach concurrently took $(echo "$(now)-$d"|bc)"; $P -c "alter table p attach partition p_1n for values in (1);") & sleep 0.5
s=$(now); $P -c "select 'reader_rows',count(*) from p"; echo "wait $(echo "$(now)-$s"|bc)"; wait
$P -c "select 'final',v,count(*) from p group by v"
