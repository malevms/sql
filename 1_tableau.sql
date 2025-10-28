-- ------------------------------------------------------------
-- 1. Base request data (runtime = completed_at - created_at)
-- ------------------------------------------------------------
WITH req AS (
  SELECT
    hr.id,
    hr.created_at,
    hr.completed_at,
    (hr.completed_at - hr.created_at)               AS runtime_sec,
    hr.user_id,
    hr.vizql_session,
    hr.action,
    hr.az_currentsheet,
    hr.site_id
  FROM public.http_requests hr
  WHERE hr.completed_at IS NOT NULL
    AND hr.action = 'show'          -- <-- focus on dashboard rendering
),

-- ------------------------------------------------------------
-- 2. User name
-- ------------------------------------------------------------
usr AS (
  SELECT id, name AS user_name
  FROM public._users
),

-- ------------------------------------------------------------
-- 3. Workbook + site (for URL building)
-- ------------------------------------------------------------
wb AS (
  SELECT
    w.id               AS workbook_id,
    w.name             AS workbook_name,
    w.workbook_url,
    s.name             AS site_name
  FROM public._workbooks w
  LEFT JOIN public._sites s ON w.site_id = s.id
),

-- ------------------------------------------------------------
-- 4. Views (dashboards)
-- ------------------------------------------------------------
vw AS (
  SELECT
    v.id               AS view_id,
    v.name             AS view_name,
    v.workbook_id
  FROM public._views v
),

-- ------------------------------------------------------------
-- 5. Match request → dashboard
--    • Try to pull a numeric view_id from vizql_session
--    • If that fails, fall back to az_currentsheet
-- ------------------------------------------------------------
matched AS (
  SELECT
    r.*,
    COALESCE(u.user_name, 'unknown')                     AS user_name,

    /* ---------- guess view_id from vizql_session ---------- */
    CASE
      WHEN r.vizql_session ~ '^\d+$'                     THEN r.vizql_session::bigint   -- exact number
      WHEN r.vizql_session ~ '\d+'                       THEN (regexp_matches(r.vizql_session, '\d+'))[1]::bigint
      ELSE NULL
    END                                                  AS guessed_view_id,

    /* ---------- join to real view / workbook ---------- */
    vw.view_name,
    wb.workbook_name,
    wb.workbook_url,
    wb.site_name,

    /* ---------- fallback dashboard name ---------- */
    COALESCE(vw.view_name, r.az_currentsheet, 'unknown') AS dashboard_name
  FROM req r
  LEFT JOIN usr u  ON r.user_id = u.id
  LEFT JOIN vw   vw ON vw.view_id = 
         CASE
           WHEN r.vizql_session ~ '^\d+$' THEN r.vizql_session::bigint
           WHEN r.vizql_session ~ '\d+'   THEN (regexp_matches(r.vizql_session, '\d+'))[1]::bigint
           ELSE NULL
         END
  LEFT JOIN wb   wb ON COALESCE(vw.workbook_id, r.guessed_view_id) = wb.workbook_id
)

-- ------------------------------------------------------------
-- 6. Final output (one row per request)
-- ------------------------------------------------------------
SELECT
  m.id,
  m.created_at,
  m.runtime_sec,
  m.user_name,
  m.dashboard_name,
  m.workbook_name,
  m.workbook_url,
  m.site_name,

  /* ---- absolute URL (replace YOUR_TABLEAU_SERVER.com) ---- */
  CONCAT(
    'https://YOUR_TABLEAU_SERVER.com',
    CASE WHEN m.site_name IS NOT NULL THEN '/#/site/'||m.site_name ELSE '' END,
    COALESCE(m.workbook_url, '')
  ) AS absolute_url,

  m.vizql_session,
  m.guessed_view_id          -- <-- now **available** here
FROM matched m
WHERE m.runtime_sec IS NOT NULL
ORDER BY m.created_at DESC;
