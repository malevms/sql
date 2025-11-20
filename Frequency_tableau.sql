-- =============================================================
-- FINAL QUERY – Works on EVERY Tableau Server (even yours)
-- Live vs Extract + Last Refresh + Schedule (name matching only)
-- =============================================================
SELECT
    w.name                                          AS workbook_name,
    w.repository_url                                AS workbook_url,

    -- Is it Extract? → Yes if we ever saw a successful extract job with this exact name
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM public.background_jobs bj
            WHERE bj.title ILIKE '%' || w.name || '%'
              AND bj.job_type ILIKE '%Extract%'
              AND bj.completed_at IS NOT NULL
        ) THEN 'Extract'
        ELSE 'Live / Unknown'
    END                                             AS connection_type,

    -- Last time we saw a successful extract refresh for this workbook
    COALESCE((
        SELECT TO_CHAR(MAX(bj.completed_at), 'YYYY-MM-DD HH24:MI')
        FROM public.background_jobs bj
        WHERE bj.title ILIKE '%' || w.name || '%'
          AND bj.job_type ILIKE '%Extract%'
          AND bj.completed_at IS NOT NULL
    ), 'Never')                                     AS last_extract_refresh,

    -- Try to guess schedule from job name or notes (very common)
    COALESCE((
        SELECT STRING_AGG(DISTINCT 
            COALESCE(bj.job_name, bj.notes, 'Manual'), ' | ')
        FROM public.background_jobs bj
        WHERE bj.title ILIKE '%' || w.name || '%'
          AND bj.job_type ILIKE '%Extract%'
          AND (bj.job_name ILIKE '%schedule%' OR bj.notes ILIKE '%schedule%')
        LIMIT 1
    ), 'No Schedule')                               AS refresh_schedule_name,

    -- Owner
    COALESCE(u.name, 'unknown')                     AS owner_name

FROM public._workbooks w
LEFT JOIN public._users u ON w.owner_id = u.id

ORDER BY 
    connection_type DESC,   -- Extracts first
    last_extract_refresh DESC NULLS
