SELECT
    COALESCE(w.name, 'Standalone / No Workbook') AS workbook_name,
    bj.title AS extract_name,
    CONCAT('https://tableau.trinity-health.org/#/site/', s."name") AS site_url,
    
    -- Clean extract name
    TRIM(REGEXP_REPLACE(bj.title, '^(Refresh Extracts|Incremental Refresh Extracts|Extract Refresh):?\s*', '')) 
        AS clean_extract_name,
    
    -- New columns for Peak Time analysis
    EXTRACT(HOUR FROM bj.created_at) AS start_hour,
    CASE 
        WHEN EXTRACT(HOUR FROM bj.created_at) BETWEEN 6 AND 9 THEN 'Peak Time (6AM-10AM)'
        ELSE 'Non-Peak Time'
    END AS time_period,

    -- Runtime stats
    COUNT(*) AS refresh_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0), 2) AS avg_runtime_minutes,
    ROUND(MAX(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0), 2) AS max_runtime_minutes,
    ROUND(MIN(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0), 2) AS min_runtime_minutes,

    -- Recent runtimes
    STRING_AGG(
        TO_CHAR(bj.completed_at, 'YYYY-MM-DD HH24:MI') || ': ' ||
        ROUND(EXTRACT(EPOCH FROM (bj.completed_at - bj.created_at)) / 60.0, 1) || ' min',
        ' | '
        ORDER BY bj.completed_at DESC
    ) AS recent_runtimes_sample,

    -- Who triggered it
    STRING_AGG(DISTINCT u.friendly_name, ', ') AS triggered_by_users

FROM public.background_jobs bj
LEFT JOIN public._workbooks w
       ON bj.title ILIKE '%' || w.name || '%'
       OR REPLACE(REPLACE(bj.title, '-', ''), ' ', '') = REPLACE(REPLACE(w.name, '-', ''), ' ', '')
LEFT JOIN public._sites s 
       ON w.site_id = s.id
LEFT JOIN public._users u 
       ON bj.creator_id = u.id

WHERE bj.job_type ILIKE '%Extract%'
  AND bj.completed_at IS NOT NULL
  AND bj.created_at >= CURRENT_DATE - INTERVAL '90 days'

GROUP BY 
    w.name,
    bj.title,
    s."name",
    EXTRACT(HOUR FROM bj.created_at)

HAVING COUNT(*) > 0

ORDER BY avg_runtime_minutes DESC;
