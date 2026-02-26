-- =============================================================
-- Extract Refresh Runtime by Workbook and Extract Name
-- Shows how long each extract refresh took
-- =============================================================
SELECT 
    -- Main grouping
    COALESCE(w.name, 'Standalone / No Workbook') AS workbook_name,
    bj.title                                      AS extract_name,          -- usually contains datasource name
    -- Cleaned extract name (removes common prefixes like "Refresh Extracts: ")
    TRIM(REGEXP_REPLACE(bj.title, '^(Refresh Extracts|Incremental Refresh Extracts|Extract Refresh):?\s*', '')) 
        AS clean_extract_name,
    
    -- Runtime stats
    COUNT(*)                                      AS refresh_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0), 2) AS avg_runtime_minutes,
    ROUND(MAX(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0), 2) AS max_runtime_minutes,
    ROUND(MIN(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0), 2) AS min_runtime_minutes,
    
    -- Last few runtimes for quick glance
    STRING_AGG(
        TO_CHAR(bj.completed_at, 'YYYY-MM-DD HH24:MI') || ': ' || 
        ROUND(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0, 1) || ' min',
        ' | '
        ORDER BY bj.completed_at DESC
    ) AS recent_runtimes_sample,

    -- Owner who triggered it (if manual)
    STRING_AGG(DISTINCT u.name, ', ')             AS triggered_by_users

FROM public.background_jobs bj
LEFT JOIN public._workbooks w 
       ON bj.title ILIKE '%' || w.name || '%'
       OR REPLACE(REPLACE(bj.title, '-', ''), ' ', '') = REPLACE(REPLACE(w.name, '-', ''), ' ', '')
LEFT JOIN public._users u 
       ON bj.user_id = u.id

WHERE bj.job_type ILIKE '%Extract%' 
  AND bj.completed_at IS NOT NULL
  AND bj.created_at >= CURRENT_DATE - INTERVAL '90 days'   -- last 3 months; change as needed

GROUP BY 
    w.name,
    bj.title

HAVING COUNT(*) > 0   -- skip empty groups

ORDER BY 
    avg_runtime_minutes DESC,   -- slowest first
    refresh_count DESC;
