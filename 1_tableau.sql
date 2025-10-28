-- ------------------------------------------------------------
-- 1. Base request data
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
    AND hr.action = 'show'          -- dashboard rendering
),

-- ------------------------------------------------------------
-- 2. Guess view_id from vizql_session (scalar, no set-returning)
-- ------------------------------------------------------------
guessed AS (
  SELECT
    r.*,
    CASE
      WHEN r.vizql_session ~ '^\d+$' THEN r.vizql_session::bigint
      WHEN r.vizql_session ~ '\d+'   THEN (regexp_matches(r.vizql_session, '\d+'))[1]::bigint
      ELSE NULL
    END AS guessed_view_id
  FROM req r
),

-- ------------------------------------------------------------
-- 3. Users
-- ------------------------------------------------------------
usr AS (
  SELECT id, name AS user_name
  FROM public._users
),

-- ------------------------------------------------------------
-- 4. Workbooks + site (for URL)
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
-- 5. Views (dashboards)
-- ------------------------------------------------------------
vw AS (
  SELECT
    v.id               AS view_id,
    v.name             AS view_name,
    v.workbook_id
  FROM public._views v
),

-- ------------------------------------------------------------
-- 6. Join everything (guessed_view_id is now a plain column)
-- ------------------------------------------------------------
matched AS (
  SELECT
    g.*,
    COALESCE(u.user_name, 'unknown')                     AS user_name,
    vw.view_name,
    wb.workbook_name,
    wb.workbook_url,
    wb.site_name,
    COALESCE(vw.view_name, g.az_currentsheet, 'unknown') AS dashboard_name
  FROM guessed g
  LEFT JOIN usr u  ON g.user_id = u.id
  LEFT JOIN vw   vw ON vw.view_id = g.guessed_view_id          -- scalar join
  LEFT JOIN wb   wb ON wb.workbook_id = COALESCE(vw.workbook_id, g.guessed_view_id)
)

-- ------------------------------------------------------------
-- 7. Final output
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

  CONCAT(
    'https://YOUR_TABLEAU_SERVER.com',
    CASE WHEN m.site_name IS NOT NULL THEN '/#/site/'||m.site_name ELSE '' END,
    COALESCE(m.workbook_url, '')
  ) AS absolute_url,

  m.vizql_session,
  m.guessed_view_id
FROM matched m
WHERE m.runtime_sec IS NOT NULL
ORDER BY m.created_at DESC;
