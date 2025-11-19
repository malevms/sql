-- =============================================================
-- Workbook + Live vs Extract + Schedule + Last Refresh
-- =============================================================
WITH workbook_connections AS (
  -- Get all connections for workbooks (embedded datasources)
  SELECT DISTINCT
    c.workbook_id,
    dc.server,               -- DB server name (for Live)
    dc.name AS connection_name,
    eds.id IS NOT NULL AS has_extract  -- Flag if this connection has an extract
  FROM public.connections c
  LEFT JOIN public.data_connections dc ON c.data_connection_id = dc.id
  LEFT JOIN public.extract_data_sources eds ON eds.connection_id = c.id  -- Extract link
  WHERE c.workbook_id IS NOT NULL  -- Only workbook connections
),

workbook_types AS (
  -- Determine type per workbook (Live if any Live connection, else Extract if any extract)
  SELECT
    wc.workbook_id,
    CASE
      WHEN MIN(wc.server) IS NOT NULL AND NOT MAX(wc.has_extract) THEN 'Live'
      WHEN MAX(wc.has_extract) THEN 'Extract'
      ELSE 'Mixed / Unknown'
    END AS connection_type,
    MIN(wc.server) AS live_database_or_server  -- First server name for Live
  FROM workbook_connections wc
  GROUP BY wc.workbook_id
)

-- Main query: Join with workbooks, schedules, etc.
SELECT
    w.name                              AS workbook_name,
    w.repository_url                   AS workbook_url,  -- Relative path for URL building
    
    -- Live or Extract (from aggregated connections)
    wt.connection_type,

    -- If Live → show database/server name
    COALESCE(wt.live_database_or_server, 'n/a') AS live_database_or_server,

    -- If Extract → last successful refresh
    COALESCE(
        TO_CHAR(er.completed_at, 'YYYY-MM-DD HH24:MI'),
        'Never'
    )                                   AS last_extract_refresh,

    -- Next scheduled run (if any)
    COALESCE(s.name, 'No Schedule')     AS refresh_schedule_name,
    COALESCE(
        s.interval || 
        CASE WHEN s.interval_value > 1 THEN ' (every '||s.interval_value||')' ELSE '' END,
        'Manual / None'
    )                                   AS refresh_frequency,

    -- Owner
    owner.name                          AS owner_name

FROM public._workbooks w

-- Join for connection type
LEFT JOIN workbook_types wt ON w.id = wt.workbook_id

-- Extract refresh history (most recent successful)
LEFT JOIN public.background_jobs er 
       ON er.workbook_id = w.id 
      AND er.job_type LIKE '%Extract%' 
      AND er.job_status = 'Success'
      AND er.completed_at = (
            SELECT MAX(completed_at) 
            FROM public.background_jobs bj2 
            WHERE bj2.workbook_id = w.id 
              AND bj2.job_type LIKE '%Extract%'
              AND bj2.job_status = 'Success'
          )

-- Schedule (via extract_refresh_schedules or direct)
LEFT JOIN public.extract_refresh_schedules ers 
       ON ers.workbook_id = w.id
LEFT JOIN public.schedules s 
       ON s.id = COALESCE(ers.schedule_id, w.refresh_schedule_id)  -- Handles old/new versions

-- Owner
LEFT JOIN public._users owner 
       ON w.owner_id = owner.id

ORDER BY w.name;
