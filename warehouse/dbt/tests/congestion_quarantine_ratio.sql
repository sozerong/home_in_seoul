-- CF3: 수집일별 격리 칸 비율이 5%를 넘음. 행 단위 격리는 39칸으로 센다(옛 transform/congestion.py와 같다)
with bad as (
    select collected_date, sum(case when reasons = array['CV5']::text[] then 1 else 39 end) as cells
    from {{ ref('congestion_quarantine') }}
    group by collected_date
),
good as (
    select collected_date, count(*) as cells from {{ ref('congestion') }} group by collected_date
)
select coalesce(g.collected_date, b.collected_date) as collected_date, g.cells as good_cells, b.cells as bad_cells
from good g
full join bad b using (collected_date)
where coalesce(b.cells, 0)::numeric / nullif(coalesce(g.cells, 0) + coalesce(b.cells, 0), 0) > 0.05
