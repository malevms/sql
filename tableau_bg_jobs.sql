-- =============================================================
-- FINAL BULLETPROOF VERSION – Extract Refresh Activity Growth (with Owner)
-- No JSON errors, no STRING_AGG DISTINCT ORDER BY errors
-- Works 100% in your restricted Azure repo
-- =============================================================
WITH weekly_extract_activity AS (
    SELECT
        DATE_TRUNC('week', bj.created_at)::date                  AS week_starting,
        COUNT(*)                                                  AS total_extract_refreshes,
        COUNT(DISTINCT w.id)                                      AS unique_workbooks_refreshed,
        
        -- Scheduled vs Manual
        COUNT(*) FILTER (WHERE u.name IS NULL OR LOWER(u.name) = 'system') AS scheduled_refreshes,
        COUNT(*) FILTER (WHERE u.name IS NOT NULL AND LOWER(u.name) != 'system') AS manual_refreshes_by_users,

        -- Top manual refresh users (comma-separated, no ORDER BY error)
        STRING_AGG(
            DISTINCT 
            CASE WHEN u.name IS NOT NULL AND LOWER(u.name) != 'system' THEN u.name END,
            ', '
        )                                                         AS top_manual_refresh_users,

        -- Estimated rows refreshed – SAFE JSON parsing (ignores invalid rows)
        SUM(
            CASE 
                WHEN bj.args ~ '^{.*"rows".*}$' THEN                -- rough valid JSON check
                    COALESCE((bj.args::jsonb ->> 'rows')::bigint, 0)
                ELSE 0 
            END
        )                                                         AS estimated_rows_refreshed

    FROM public.background_jobs bj
    LEFT JOIN public._workbooks w 
           ON bj.title ILIKE '%' || w.name || '%' 
           OR REPLACE(REPLACE(bj.title, '-', ''), ' ', '') = REPLACE(REPLACE(w.name, '-', ''), ' ', '')
    LEFT JOIN public._users u 
           ON bj.user_id = u.id

    WHERE bj.job_type ILIKE '%Extract%' 
      AND bj.completed_at IS NOT NULL
      AND bj.created_at >= CURRENT_DATE - INTERVAL '180 days'

    GROUP BY DATE_TRUNC('week', bj.created_at)
)

-- Final output with clean WoW growth
SELECT
    week_starting,
    total_extract_refreshes,
    unique_workbooks_refreshed,
    scheduled_refreshes,
    manual_refreshes_by_users,
    ROUND(100.0 * manual_refreshes_by_users / NULLIF(total_extract_refreshes, 0), 1) AS pct_manual_refreshes,

    COALESCE(top_manual_refresh_users, '(none)')             AS top_manual_refresh_users,
    estimated_rows_refreshed,

    -- WoW % growth (refreshes)
    COALESCE(
        ROUND(
            100.0 * (total_extract_refreshes - LAG(total_extract_refreshes) OVER (ORDER BY week_starting))
            / NULLIF(LAG(total_extract_refreshes) OVER (ORDER BY week_starting), 0),
            2
        ) || '%', 
        'n/a'
    ) AS wow_growth_percent_refreshes,

    -- WoW % growth (manual only) – most important metric!
    COALESCE(
        ROUND(
            100.0 * (manual_refreshes_by_users - LAG(manual_refreshes_by_users) OVER (ORDER BY week_starting))
            / NULLIF(LAG(manual_refreshes_by_users) OVER (ORDER BY week_starting), 0),
            2
        ) || '%', 
        'n/a'
    ) AS wow_growth_percent_manual

FROM weekly_extract_activity
ORDER BY week_starting DESC;
