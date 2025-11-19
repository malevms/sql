-- =============================================================
-- ULTIMATE VERSION: Live vs Extract + Schedule + Last Refresh
-- Links via item_id/task_id (no direct workbook_id/schedule_id)
-- =============================================================
WITH extract_jobs AS (
  -- All extract jobs with workbook link (via item_id)
  SELECT DISTINCT
    bj.item_id AS workbook_id,  -- Links to _workbooks.id
    bj.job_type,
    bj.job_status,
    bj.completed_at,
    t.schedule_id  -- Via tasks table
  FROM public.background_jobs bj
  JOIN public.tasks t ON bj.task_id = t.id  -- Bridge to schedule
  WHERE bj.job_type LIKE '%Extract%'  -- Refresh Extracts, etc.
    AND bj.subtitle = 'Workbook'  -- Ensure it's a workbook job
),

workbook_extracts AS (
  -- Aggregate per workbook: has extract? last refresh?
  SELECT
    workbook_id,
    MAX(CASE WHEN job_status = 'Success' THEN completed_at END) AS last_successful_refresh,
    STRING_AGG(DISTINCT ej.schedule_id::text, ',') AS schedule_ids  -- Collect schedules
  FROM extract_jobs ej
  GROUP BY workbook_id
)

-- Main query
SELECT
    w.name                                          AS workbook_name,
    w.repository_url                                AS workbook_url,

    -- Live vs Extract (via extract job existence)
    CASE
        WHEN we.workbook_id IS NOT NULL THEN 'Extract'
        ELSE 'Live / Unknown'
    END                                             AS connection_type,

    -- Last successful extract refresh
    COALESCE(
        TO_CHAR(we.last_successful_refresh, 'YYYY-MM-DD HH24:MI'),
        'Never'
    )                                               AS last_extract_refresh,

    -- Refresh schedule (via collected schedule_ids)
    COALESCE(
        STRING_AGG(DISTINCT s.name, '; ' ORDER BY s.name),
        'No Schedule'
    )                                               AS refresh_schedule_name,
    COALESCE(
        STRING_AGG(
            DISTINCT s.type || ' – ' ||
            s.interval ||
            CASE WHEN s.interval_value > 1 THEN ' (every '||s.interval_value||')' ELSE '' END,
            '; ' ORDER BY s.interval
        ),
        'Manual / None'
    )                                               AS refresh_frequency,

    -- Owner
    COALESCE(owner.name, 'unknown')                 AS owner_name

FROM public._workbooks w

-- Join for extract info
LEFT JOIN workbook_extracts we ON w.id = we.workbook_id

-- Join for schedules (via collected IDs)
LEFT JOIN public.schedules s ON s.id::text = ANY(STRING_TO_ARRAY(we.schedule_ids, ','))

-- Owner
LEFT JOIN public._users owner ON w.owner_id = owner.id

GROUP BY 
    w.id, w.name, w.repository_url, we.last_successful_refresh,
    we.workbook_id, owner.name  -- Group to handle multiple schedules

ORDER BY w.name;
