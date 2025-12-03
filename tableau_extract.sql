-- =============================================================
-- Extract Refresh Activity Growth – Week over Week (with Owner)
-- Shows who is driving extract growth (manual vs scheduled)
-- =============================================================
WITH weekly_extract_activity AS (
    SELECT
        DATE_TRUNC('week', bj.created_at)::date                  AS week_starting,        -- Monday of week
        COUNT(*)                                                  AS total_extract_refreshes,
        COUNT(DISTINCT w.id)                                      AS unique_workbooks_refreshed,
        
        -- Who triggered the refresh?
        COUNT(*) FILTER (WHERE u.name IS NULL OR u.name = 'system') AS scheduled_refreshes,
        COUNT(*) FILTER (WHERE u.name IS NOT NULL AND u.name != 'system') AS manual_refreshes_by_users,

        -- Top 5 owners this week (comma-separated)
        STRING_AGG(DISTINCT 
            CASE WHEN u.name IS NOT NULL AND u.name != 'system' THEN u.name END, 
            ', ' ORDER BY u.name
        )                                                         AS top_manual_refresh_users,

        -- Estimated row volume (if available in args JSON – safe fallback)
        SUM(
            COALESCE(
                (bj.args::jsonb ->> 'rows')::bigint,
                0
            )
        )                                                         AS estimated_rows_refreshed

    FROM public.background_jobs bj
    LEFT JOIN public._workbooks w 
           ON bj.title ILIKE '%' || w.name || '%' 
           OR REPLACE(REPLACE(bj.title, '-', ''), ' ', '') = REPLACE(REPLACE(w.name, '-', ''), ' ', '')
    LEFT JOIN public._users u 
           ON bj.user_id = u.id

    WHERE bj.job_type ILIKE '%Extract%' 
      AND bj.completed_at IS NOT NULL                                 -- Successful only
      AND bj.created_at >= CURRENT_DATE - INTERVAL '180 days'         -- Last ~6 months

    GROUP BY DATE_TRUNC('week', bj.created_at)
)

-- Final output with WoW growth
SELECT
    week_starting,
    total_extract_refreshes,
    unique_workbooks_refreshed,
    scheduled_refreshes,
    manual_refreshes_by_users,
    ROUND(100.0 * manual_refreshes_by_users / NULLIF(total_extract_refreshes, 0), 1) AS pct_manual_refreshes,

    top_manual_refresh_users,
    estimated_rows_refreshed,

    -- Week-over-Week Growth (refreshes)
    ROUND(
        100.0 * (total_extract_refreshes - LAG(total_extract_refreshes) OVER (ORDER BY week_starting))
        / NULLIF(LAG(total_extract_refreshes) OVER (ORDER BY week_starting), 0),
        2
    ) || '%' AS wow_growth_percent_refreshes,

    -- Growth in manual refreshes (most important for governance!)
    ROUND(
        100.0 * (manual_refreshes_by_users - LAG(manual_refreshes_by_users) OVER (ORDER BY week_starting))
        / NULLIF(LAG(manual_refreshes_by_users) OVER (ORDER BY week_starting), 0),
        2
    ) || '%' AS wow_growth_percent_manual

FROM weekly_extract_activity
ORDER BY week_starting DESC;
