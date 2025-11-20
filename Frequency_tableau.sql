-- =============================================================
-- FALLBACK VERSION: Live vs Extract + Schedule (Name Matching)
-- Uses title/subtitle/job_type in background_jobs (no IDs needed)
-- =============================================================
WITH extract_jobs AS (
  -- All extract jobs for workbooks (name-based)
  SELECT DISTINCT
    REPLACE(REPLACE(bj.title, '-', ''), ' ', '') AS clean_workbook_name,  -- Normalize for matching
    bj.title AS raw_workbook_name,
    MAX(CASE WHEN bj.finish_code = 1 THEN bj.completed_at END) AS last_successful_refresh,  -- finish_code = 1 = success
    STRING_AGG(DISTINCT 
      CASE 
        WHEN bj.job_name ~ 'schedule' OR bj.notes ~ 'schedule' THEN 
          REGEXP_REPLACE(bj.job_name || ' ' || COALESCE(bj.notes, ''), '.*schedule[:\s]+(\w+).*', '\1')
        ELSE NULL 
      END, '; ') AS schedule_names  -- Parse schedule from job_name/notes
  FROM public.background_jobs bj
  WHERE bj.job_type LIKE '%Extract%'  -- Refresh Extracts, etc.
    AND bj.subtitle = 'Workbook'      -- Workbook-specific
    AND bj.finish_code = 1            -- Successful only
  GROUP BY REPLACE(REPLACE(bj.title, '-', ''), ' ', ''), bj.title
)

-- Main query: Match by normalized name
SELECT
    w.name                                          AS workbook_name,
    w.repository_url                                AS workbook_url,

    -- Live vs Extract (name match on title)
    CASE
        WHEN ej.clean_workbook_name IS NOT NULL THEN 'Extract'
        ELSE 'Live / Unknown'
    END                                             AS connection_type,

    -- Last successful extract refresh
    COALESCE(
        TO_CHAR(ej.last_successful_refresh, 'YYYY-MM-DD HH24:MI'),
        'Never'
    )                                               AS last_extract_refresh,

    -- Refresh schedule (parsed from jobs)
    COALESCE(ej.schedule_names, 'No Schedule')      AS refresh_schedule_name,
    COALESCE(
        CASE 
            WHEN ej.schedule_names ~ 'daily|day' THEN 'Daily'
            WHEN ej.schedule_names ~ 'hourly|hour' THEN 'Hourly (every 1)'
            WHEN ej.schedule_names ~ 'weekly|week' THEN 'Weekly'
            ELSE 'Manual / None'
        END,
        'Manual / None'
    )                                               AS refresh_frequency,

    -- Owner
    COALESCE(owner.name, 'unknown')                 AS owner_name

FROM public._workbooks w

-- Name-based join
LEFT JOIN extract_jobs ej 
       ON REPLACE(REPLACE(w.name, '-', ''), ' ', '') = ej.clean_workbook_name

-- Owner
LEFT JOIN public._users owner 
       ON w.owner_id = owner.id

ORDER BY w.name;
