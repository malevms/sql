-- =============================================================
-- COMPLETE Teradata Audit – Every possible object type (2025-ready)
-- =============================================================
SELECT 'Published Data Source' AS object_type,
       ds.name,
       ds.id::text AS object_id,
       u.name AS owner,
       ds.created_at::date
FROM public._datasources ds
LEFT JOIN public._users u ON ds.owner_id = u.id
WHERE LOWER(ds.connection_type) = 'teradata'
   OR LOWER(ds.server) LIKE '%teradata%'
   OR LOWER(ds.dbname) LIKE '%teradata%'

UNION ALL

-- Embedded in workbooks
SELECT 'Workbook (embedded DS)' AS object_type,
       w.name || ' → ' || ds.name AS name,
       w.id::text,
       wu.name AS owner,
       w.created_at::date
FROM public._datasources ds
JOIN public._workbooks w ON ds.workbook_id = w.id
LEFT JOIN public._users wu ON w.owner_id = wu.id
WHERE LOWER(ds.connection_type) = 'teradata'
   OR LOWER(ds.server) LIKE '%teradata%'

UNION ALL

-- Tableau Prep Flows
SELECT 'Prep Flow' AS object_type,
       f.name,
       f.id::text,
       u.name AS owner,
       f.created_at::date
FROM public._flows f
LEFT JOIN public._users u ON f.owner_id = u.id
WHERE LOWER(f.connection_type) = 'teradata'
   OR LOWER(f.server) LIKE '%teradata%'

UNION ALL

-- Flow OUTPUTS published as data sources (HUGE blind spot!)
SELECT 'Flow Output → Published DS' AS object_type,
       ds.name || ' (output of flow)',
       ds.id::text,
       u.name AS owner,
       ds.created_at::date
FROM public._datasources ds
LEFT JOIN public._users u ON ds.owner_id = u.id
WHERE ds.is_flow_output = true
  AND (LOWER(ds.connection_type) = 'teradata' OR LOWER(ds.server) LIKE '%teradata%')

UNION ALL

-- Virtual Connections + their content tables
SELECT 'Virtual Connection' AS object_type,
       vc.name || ' → ' || vcd.table_name,
       vc.id::text,
       u.name AS owner,
       vc.created_at::date
FROM public.virtual_connections vc
JOIN public.virtual_connection_datasources vcd ON vc.id = vcd.virtual_connection_id
LEFT JOIN public._users u ON vc.owner_id = u.id
WHERE LOWER(vc.connection_type) = 'teradata'
   OR LOWER(vc.server) LIKE '%teradata%'

UNION ALL

-- Ask Data / Lens (rare, but exists)
SELECT 'Ask Data Lens' AS object_type,
       l.name,
       l.id::text,
       u.name AS owner,
       l.created_at::date
FROM public.lenses l
LEFT JOIN public._users u ON l.owner_id = u.id
WHERE LOWER(l.connection_type) = 'teradata'
   OR LOWER(l.server) LIKE '%teradata%'

ORDER BY object_type, name;
