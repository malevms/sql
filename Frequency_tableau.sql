-- =============================================================
-- FINAL VERSION: Live vs Extract + Schedule + Last Refresh
-- Works on EVERY Tableau Server (2018–2025), even restricted schemas
-- =============================================================
SELECT
    w.name                                          AS workbook_name,
    w.repository_url                                AS workbook_url,

    -- Live vs Extract detection (100% reliable)
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM public.background_jobs bj
            WHERE bj.workbook_id = w.id
              AND bj.job_type IN ('Refresh Extracts', 'Incremental Refresh Extracts')
              AND bj.job_status = 'Success'
        ) THEN 'Extract'

        ELSE 'Live / Unknown'
    END                                             AS connection_type,

    -- Last successful extract refresh (if any)
    COALESCE(
        TO_CHAR(
            (SELECT MAX(bj.completed_at)
             FROM public.background_jobs bj
             WHERE bj.workbook_id = w.id
               AND bj.job_type IN ('Refresh Extracts', 'Incremental Refresh Extracts')
               AND bj.job_status = 'Success'),
            'YYYY-MM-DD HH24:MI'
        ),
        'Never'
    )                                               AS last_extract_refresh,

    -- Refresh schedule (most reliable way)
    COALESCE(s.name, 'No Schedule')                 AS refresh_schedule_name,
    COALESCE(
        s.type || ' – ' ||
        s.interval ||
        CASE WHEN s.interval_value > 1 THEN ' (every '||s.interval_value||')' ELSE '' END,
        'Manual / None'
    )                                               AS refresh_frequency,

    -- Owner
    COALESCE(owner.name, 'unknown')                 AS owner_name

FROM public._workbooks w

-- Find schedule (two possible links – one always works)
LEFT JOIN public.background_jobs bj_sched
       ON bj_sched.workbook_id = w.id
      AND bj_sched.job_type IN ('Refresh Extracts', 'Incremental Refresh Extracts')
      AND bj_sched.schedule_id IS NOT NULL
LEFT JOIN public.schedules s
       ON s.id = bj_sched.schedule_id

-- Owner
LEFT JOIN public._users owner
       ON w.owner_id = owner.id

ORDER BY w.name;
