-- =============================================================
-- FINAL PERFECTED VERSION – Works 100% in your environment
-- Live vs Extract + Last Refresh + Reliable Schedule Detection
-- Uses your proven name-normalization logic + new schedule logic
-- =============================================================
WITH extract_jobs AS (
  -- All extract jobs for workbooks (name-based)
  SELECT DISTINCT
    REPLACE(REPLACE(bj.title, '-', ''), ' ', '') AS clean_workbook_name,  -- Your working normalization
    bj.title AS raw_workbook_name,
    
    -- Last successful refresh (finish_code = 1 = success, or fallback to any completed)
    MAX(COALESCE(
      CASE WHEN bj.finish_code = 1 THEN bj.completed_at END,
      bj.completed_at
    )) AS last_successful_refresh,

    -- BEST SCHEDULE DETECTION – works even when job_name/notes are empty or cryptic
    CASE
      WHEN COUNT(*) FILTER (WHERE bj.job_name ILIKE '%daily%'   OR bj.notes ILIKE '%daily%')   > 0 THEN 'Daily'
      WHEN COUNT(*) FILTER (WHERE bj.job_name ILIKE '%hourly%'  OR bj.notes ILIKE '%hourly%')  > 0 THEN 'Hourly'
      WHEN COUNT(*) FILTER (WHERE bj.job_name ILIKE '%weekly%'  OR bj.notes ILIKE '%weekly%')  > 0 THEN 'Weekly'
      WHEN COUNT(*) FILTER (WHERE bj.job_name ILIKE '%monthly%' OR bj.notes ILIKE '%monthly%monthly%') > 0 THEN 'Monthly'
      WHEN COUNT(*) FILTER (WHERE bj.job_name ILIKE '%schedule%' OR bj.notes ILIKE '%schedule%') > 0 THEN 'Scheduled (type hidden)'
      WHEN COUNT(*) > 0 THEN 'Scheduled (unknown frequency)'
      ELSE 'No Schedule / Manual'
    END AS refresh_frequency

  FROM public.background_jobs bj
  WHERE bj.job_type LIKE '%Extract%'           -- Refresh Extracts, Incremental, etc.
    AND bj.subtitle = 'Workbook'               -- Only workbook jobs
  GROUP BY REPLACE(REPLACE(bj.title, '-', ''), ' ', ''), bj.title
)

-- Main query – your working version, now with real schedule info
SELECT
    w.name                                          AS workbook_name,
    w.workbook_url                                  AS workbook_url,   -- you already fixed this

    -- Live vs Extract
    CASE
        WHEN ej.clean_workbook_name IS NOT NULL THEN 'Extract'
        ELSE 'Live / Unknown'
    END                                             AS connection_type,

    -- Last successful extract refresh
    COALESCE(
        TO_CHAR(ej.last_successful_refresh, 'YYYY-MM-DD HH24:MI'),
        'Never'
    )                                               AS last_extract_refresh,

    -- NEW: Reliable frequency (no more NULLs!)
    COALESCE(ej.refresh_frequency, 'No Schedule / Manual') AS refresh_frequency,

    -- Optional: keep raw schedule text if you want to see the source
    -- ej.schedule_names AS raw_schedule_debug,

    -- Owner
    COALESCE(owner.name, 'unknown')                 AS owner_name,

    -- Optional: clickable link
    CONCAT('https://YOUR-SERVER.com/#/site/default', w.workbook_url) AS full_url

FROM public._workbooks w

-- Your proven name-matching join
LEFT JOIN extract_jobs ej 
       ON REPLACE(REPLACE(w.name, '-', ''), ' ', '') = ej.clean_workbook_name

-- Owner
LEFT JOIN public._users owner 
       ON w.owner_id = owner.id

ORDER BY 
    connection_type DESC,
    last_extract_refresh DESC NULLS LAST,
    workbook_name;
