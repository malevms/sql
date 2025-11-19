-- =============================================================
-- Workbook + Live vs Extract + Schedule + Last Refresh
-- =============================================================
SELECT
    w.name                              AS workbook_name,
    w.repository_url                   AS workbook_url,
    
    -- Live or Extract?
    CASE 
        WHEN w.data_connection_type = 'live'      THEN 'Live'
        WHEN w.data_connection_type = 'extract'   THEN 'Extract'
        WHEN eds.id IS NOT NULL                   THEN 'Extract'
        ELSE 'Unknown / Mixed'
    END                                 AS connection_type,

    -- If Live → show database/server name
    COALESCE(dc.server, dc.name, 'n/a') AS live_database_or_server,

    -- If Extract → last successful refresh
    COALESCE(
        TO_CHAR(er.completed_at, 'YYYY-MM-DD HH24:MI'),
        'Never'
    )                                   AS last_extract_refresh,

    -- Next scheduled run (if any)
    COALESCE(s.name, 'No Schedule')     AS refresh_schedule_name,
    COALESCE(s.interval || 
        CASE WHEN s.interval_value > 1 THEN ' (every '||s.interval_value||')' ELSE '' END,
        'Manual / None'
    )                                   AS refresh_frequency,

    -- Owner
    owner.name                          AS owner_name

FROM public._workbooks w

-- Live connection details (if any)
LEFT JOIN public._datasources ds 
       ON ds.workbook_id = w.id 
      AND ds.is_live = true
LEFT JOIN public._data_connections dc 
       ON dc.id = ds.data_connection_id

-- Extract info
LEFT JOIN public.extract_data_sources eds 
       ON eds.workbook_id = w.id
LEFT JOIN public.background_jobs er 
       ON er.workbook_id = w.id 
      AND er.job_type LIKE '%Extract%' 
      AND er.completed_at = (
            SELECT MAX(completed_at) 
            FROM public.background_jobs bj2 
            WHERE bj2.workbook_id = w.id 
              AND bj2.job_type LIKE '%Extract%'
              AND bj2.job_status = 'Success'
          )

-- Schedule (newer versions use extract_refresh_schedules)
LEFT JOIN public.extract_refresh_schedules ers 
       ON ers.workbook_id = w.id
LEFT JOIN public.schedules s 
       ON s.id = ers.schedule_id

-- Owner
LEFT JOIN public._users owner 
       ON w.owner_id = owner.id

ORDER BY w.name;
