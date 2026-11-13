-- title: 출처별 품질
-- db: seoul
-- display: table
-- pos: 30 0 12 8
-- status: 판정
-- desc: 지금까지 넣은 전체 기준 비율. 사유 코드의 뜻은 docs/rules.md에 있다.
WITH reasons AS (
    SELECT source, string_agg(reason || ' ' || n, ', ' ORDER BY n DESC) AS reasons FROM (
        SELECT '전월세' AS source, r AS reason, count(*) AS n FROM stg.rent_quarantine, unnest(reasons) r GROUP BY 2
        UNION ALL SELECT '생활이동', r, count(*) FROM stg.living_migration_quarantine, unnest(reasons) r GROUP BY 2
        UNION ALL SELECT '혼잡도', r, count(*) FROM stg_dbt.congestion_quarantine, unnest(reasons) r GROUP BY 2
        UNION ALL SELECT '상가정보', r, count(*) FROM stg.sbiz_store_quarantine, unnest(reasons) r GROUP BY 2
        UNION ALL SELECT '역사마스터', r, count(*) FROM stg.station_master_quarantine, unnest(reasons) r GROUP BY 2
        UNION ALL SELECT '행정동 경계', r, count(*) FROM stg.sgis_dong_quarantine, unnest(reasons) r GROUP BY 2
        UNION ALL SELECT '연속지적도', r, count(*) FROM stg.parcel_quarantine, unnest(reasons) r GROUP BY 2
    ) t GROUP BY source
)
SELECT q.source AS "출처", CASE WHEN q.quarantined_pct > 5 THEN '초과' ELSE '정상' END AS "판정",
       q.good AS "통과 행", q.quarantined AS "격리 행", q.quarantined_pct AS "격리 비율(%)", coalesce(r.reasons, '-') AS "사유"
FROM ops.quality q LEFT JOIN reasons r USING (source) ORDER BY q.quarantined_pct DESC, q.source
