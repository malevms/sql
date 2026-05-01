WITH recent_requests AS (
    SELECT 
        hr.created_at,
        EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at)) * 1000 AS runtime_ms
    FROM _http_requests hr
    WHERE hr.created_at >= NOW() - INTERVAL '3 weeks'
      AND hr.completed_at IS NOT NULL
      AND hr.site_id = (SELECT id FROM _sites WHERE name = 'Default' LIMIT 1)  -- Optional: filter by site
),

wb_list AS (
    SELECT 
        wb.id,
        wb.name AS "Name",
        prj.name AS "Project Name",
        COALESCE(su.display_name, u.name) AS "Owner",
        wb.updated_at AS "Last Updated At",
        wb.luid AS "LUID"
    FROM _workbooks wb
    JOIN _projects prj ON wb.project_id = prj.id
    JOIN _users u ON wb.owner_id = u.id
    LEFT JOIN _system_users su ON u.system_user_id = su.id
    WHERE wb.updated_at >= NOW() - INTERVAL '3 weeks'
      AND wb.is_deleted = FALSE
)

SELECT 
    'Workbook' AS "Content Type",
    wl."Name",
    wl."Project Name",
    wl."Owner",
    wl."Last Updated At",
    DATE_TRUNC('day', rr.created_at) AS "Performance Date",
    AVG(rr.runtime_ms) AS "Avg Runtime (ms)",
    COUNT(*) AS "Load Count",
    wl."LUID",
    'workbook' AS "Content Type for URL"
FROM recent_requests rr
-- Minimal cross-reference via date window only (approximate but fast)
CROSS JOIN wb_list wl
WHERE rr.created_at BETWEEN wl."Last Updated At" - INTERVAL '7 days' AND wl."Last Updated At" + INTERVAL '14 days'  -- Narrow window
GROUP BY wl."Name", wl."Project Name", wl."Owner", wl."Last Updated At", DATE_TRUNC('day', rr.created_at), wl."LUID"
HAVING AVG(rr.runtime_ms) IS NOT NULL
ORDER BY wl."Last Updated At" DESC, "Performance Date" DESC
LIMIT 50000;   -- Safety limit
