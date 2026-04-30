SELECT 
    'Workbook' AS "Content Type",
    wb.name AS "Name",
    prj.name AS "Project Name",
    COALESCE(su.display_name, u.name) AS "Owner",
    wb.updated_at AS "Last Updated At",          -- This is the key "update" timestamp
    DATE_TRUNC('day', hr.created_at) AS "Performance Date",
    AVG(EXTRACT(EPOCH FROM (hr.completed_at - hr.created_at)) * 1000) AS "Avg Runtime (ms)",   -- Load time in milliseconds
    COUNT(*) AS "Load Count",
    wb.luid AS "LUID",
    s.name AS "Site Name",
    'workbook' AS "Content Type for URL"
FROM _http_requests hr
JOIN _workbooks wb ON hr.workbook_id = wb.id          -- Direct link to workbook
JOIN _projects prj ON wb.project_id = prj.id
JOIN _users u ON wb.owner_id = u.id
LEFT JOIN _system_users su ON u.system_user_id = su.id
JOIN _sites s ON wb.site_id = s.id
WHERE hr.created_at >= NOW() - INTERVAL '3 weeks'     -- Adjust as needed (http_requests kept ~7-14 days)
  AND wb.is_deleted = FALSE
  AND hr.completed_at IS NOT NULL
GROUP BY 1,2,3,4,5,6,9,10,11
union all
SELECT 
    'Flow' AS "Content Type",
    f.name AS "Name",
    prj.name AS "Project Name",
    COALESCE(su.display_name, u.name) AS "Owner",
    f.updated_at AS "Last Updated At",
    DATE_TRUNC('day', COALESCE(br.started_at, fr.started_at)) AS "Performance Date",
    AVG(COALESCE(br.duration_ms, 
                 EXTRACT(EPOCH FROM (COALESCE(br.completed_at, fr.completed_at) - COALESCE(br.started_at, fr.started_at))) * 1000)) AS "Avg Runtime (ms)",
    COUNT(*) AS "Run Count",
    f.luid AS "LUID",
    s.name AS "Site Name",
    'flow' AS "Content Type for URL"
FROM _flows f
JOIN _projects prj ON f.project_id = prj.id
JOIN _users u ON f.owner_id = u.id
LEFT JOIN _system_users su ON u.system_user_id = su.id
JOIN _sites s ON f.site_id = s.id
LEFT JOIN _background_tasks br ON br.flow_id = f.id          -- Background task runs
LEFT JOIN _flow_runs fr ON fr.flow_id = f.id                 -- Newer flow run table
WHERE COALESCE(br.started_at, fr.started_at) >= NOW() - INTERVAL '3 weeks'
  AND f.is_deleted = FALSE
GROUP BY 1,2,3,4,5,6,9,10,11
