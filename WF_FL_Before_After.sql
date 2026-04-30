WITH wb_performance AS (
    SELECT 
        wb.name AS "Name",
        prj.name AS "Project Name",
        COALESCE(su.display_name, u.name) AS "Owner",
        wb.updated_at AS "Last Updated At",
        DATE_TRUNC('day', hr.created_at) AS "Performance Date",
        AVG(EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at)) * 1000) AS "Avg Runtime (ms)",
        COUNT(*) AS "Load Count"
    FROM _http_requests hr
    JOIN _workbooks wb ON TRUE                     -- Cannot join directly, so we aggregate broadly
    JOIN _projects prj ON wb.project_id = prj.id
    JOIN _users u ON wb.owner_id = u.id
    LEFT JOIN _system_users su ON u.system_user_id = su.id
    WHERE hr.created_at >= NOW() - INTERVAL '3 weeks'
      AND hr.completed_at IS NOT NULL
    GROUP BY wb.name, prj.name, "Owner", wb.updated_at, DATE_TRUNC('day', hr.created_at)
),

before_after AS (
    SELECT 
        "Name",
        "Project Name",
        "Owner",
        "Last Updated At",
        AVG(CASE WHEN "Performance Date" < "Last Updated At" THEN "Avg Runtime (ms)" END) AS "Avg Before (ms)",
        AVG(CASE WHEN "Performance Date" >= "Last Updated At" THEN "Avg Runtime (ms)" END) AS "Avg After (ms)",
        SUM("Load Count") AS "Total Loads"
    FROM wb_performance
    GROUP BY "Name", "Project Name", "Owner", "Last Updated At"
)

SELECT 
    'Workbook' AS "Content Type",
    "Name",
    "Project Name",
    "Owner",
    "Last Updated At",
    "Avg Before (ms)" / 1000 AS "Avg Runtime Before (sec)",
    "Avg After (ms)" / 1000  AS "Avg Runtime After (sec)",
    CASE 
        WHEN "Avg Before (ms)" > 0 
        THEN ROUND( ("Avg After (ms)" - "Avg Before (ms)") * 100.0 / "Avg Before (ms)" , 1) 
    END AS "% Change",
    "Total Loads"
FROM before_after
WHERE "Last Updated At" >= NOW() - INTERVAL '3 weeks'
ORDER BY "% Change" ASC;   -- Negative = improvement
