-- Extract Refresh Activity Growth – Week over Week (with Owner)
-- Oracle version – compatible with Oracle 12c+
WITH weekly_extract_activity AS (
    SELECT
        TRUNC(bj.created_at, 'IW')                      AS week_starting,           -- ISO week (Monday start)
        COUNT(*)                                         AS total_extract_refreshes,
        COUNT(DISTINCT w.id)                             AS unique_workbooks_refreshed,
        
        -- Scheduled vs Manual
        SUM(CASE WHEN u.name IS NULL OR LOWER(u.name) = 'system' THEN 1 ELSE 0 END) 
            AS scheduled_refreshes,
        SUM(CASE WHEN u.name IS NOT NULL AND LOWER(u.name) <> 'system' THEN 1 ELSE 0 END) 
            AS manual_refreshes_by_users,

        -- Top manual refresh users (comma-separated)
        LISTAGG(
            CASE WHEN u.name IS NOT NULL AND LOWER(u.name) <> 'system' THEN u.name END,
            ', '
        ) WITHIN GROUP (ORDER BY u.name)                 AS top_manual_refresh_users,

        -- Estimated rows refreshed – safe parsing (assuming args is VARCHAR2/CLOB)
        SUM(
            CASE 
                WHEN REGEXP_LIKE(bj.args, '.*"rows".*') THEN
                    TO_NUMBER(REGEXP_SUBSTR(bj.args, '"rows":([0-9]+)', 1, 1, NULL, 1))
                ELSE 0 
            END
        )                                                AS estimated_rows_refreshed

    FROM background_jobs bj
    LEFT JOIN _workbooks w 
           ON bj.title LIKE '%' || w.name || '%'
           OR REPLACE(REPLACE(bj.title, '-', ''), ' ', '') = REPLACE(REPLACE(w.name, '-', ''), ' ', '')
    LEFT JOIN _users u 
           ON bj.user_id = u.id

    WHERE bj.job_type LIKE '%Extract%'
      AND bj.completed_at IS NOT NULL
      AND bj.created_at >= TRUNC(SYSDATE) - 180   -- last ~6 months

    GROUP BY TRUNC(bj.created_at, 'IW')
)

-- Final output with WoW growth
SELECT
    week_starting,
    total_extract_refreshes,
    unique_workbooks_refreshed,
    scheduled_refreshes,
    manual_refreshes_by_users,
    ROUND(manual_refreshes_by_users * 100 / NULLIF(total_extract_refreshes, 0), 1) AS pct_manual_refreshes,

    NVL(top_manual_refresh_users, '(none)')       AS top_manual_refresh_users,
    estimated_rows_refreshed,

    -- WoW % growth (refreshes)
    NVL(
        ROUND(
            (total_extract_refreshes - LAG(total_extract_refreshes) OVER (ORDER BY week_starting))
            / NULLIF(LAG(total_extract_refreshes) OVER (ORDER BY week_starting), 0) * 100,
            2
        ) || '%', 
        'n/a'
    ) AS wow_growth_percent_refreshes,

    -- WoW % growth (manual)
    NVL(
        ROUND(
            (manual_refreshes_by_users - LAG(manual_refreshes_by_users) OVER (ORDER BY week_starting))
            / NULLIF(LAG(manual_refreshes_by_users) OVER (ORDER BY week_starting), 0) * 100,
            2
        ) || '%', 
        'n/a'
    ) AS wow_growth_percent_manual

FROM weekly_extract_activity
ORDER BY week_starting DESC;
