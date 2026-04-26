SELECT 
    'Workbook' AS "Content Type",
    wb.name AS "Name",
    prj.name AS "Project Name",
    COALESCE(su.display_name, u.name) AS "Owner",
    wb.created_at AS "Published At",          -- Initial publish time (most reliable for "published")
    wb.updated_at AS "Last Updated",
    wb.luid AS "LUID",
    s.name AS "Site Name",
    'workbook' AS "Content Type for URL"
FROM _workbooks wb
JOIN _projects prj ON wb.project_id = prj.id
JOIN _users u ON wb.owner_id = u.id
LEFT JOIN _system_users su ON u.system_user_id = su.id
JOIN _sites s ON wb.site_id = s.id
WHERE wb.created_at >= NOW() - INTERVAL '3 weeks'
  AND wb.is_deleted = FALSE

UNION ALL

SELECT 
    'Flow' AS "Content Type",
    f.name AS "Name",
    prj.name AS "Project Name",
    COALESCE(su.display_name, u.name) AS "Owner",
    f.created_at AS "Published At",
    f.updated_at AS "Last Updated",
    f.luid AS "LUID",
    s.name AS "Site Name",
    'flow' AS "Content Type for URL"
FROM _flows f
JOIN _projects prj ON f.project_id = prj.id
JOIN _users u ON f.owner_id = u.id
LEFT JOIN _system_users su ON u.system_user_id = su.id
JOIN _sites s ON f.site_id = s.id
WHERE f.created_at >= NOW() - INTERVAL '3 weeks'
  AND f.is_deleted = FALSE

ORDER BY "Published At" DESC;
