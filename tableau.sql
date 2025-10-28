-- 1. Base request table (runtime = completed_at - created_at)
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
    AND hr.action = 'show'                     -- focus on rendering
),

-- 2. Resolve user name
usr AS (
  SELECT id, name AS user_name
  FROM public._users
),

-- 3. Resolve workbook & view (dashboard) name + URL
wb AS (
  SELECT
    w.id               AS workbook_id,
    w.name             AS workbook_name,
    w.workbook_url,
    s.name             AS site_name
  FROM public._workbooks w
  LEFT JOIN public._sites s ON w.site_id = s.id
),

vw AS (
  SELECT
    v.id               AS view_id,
    v.name             AS view_name,
    v.workbook_id
  FROM public._views v
),

-- 4. Try to match a request to a dashboard
--    (vizql_session is a string that often contains the view_id)
matched AS (
  SELECT
    r.*,
    COALESCE(u.user_name, 'unknown')                     AS user_name,
    COALESCE(wb.workbook_name, 'unknown')                AS workbook_name,
    COALESCE(wb.workbook_url,   '')                      AS workbook_url,
    COALESCE(vw.view_name,      r.az_currentsheet)       AS dashboard_name,
    CASE
      WHEN r.vizql_session ~ '\d+' THEN
        SUBSTRING(r.vizql_session FROM '\d+')::bigint
      ELSE NULL
    END                                                 AS guessed_view_id
  FROM req r
  LEFT JOIN usr  u  ON r.user_id = u.id
  LEFT JOIN vw   vw ON vw.id = r.guessed_view_id
  LEFT JOIN wb   wb ON COALESCE(vw.workbook_id, r.guessed_view_id) = wb.workbook_id
)

-- 5. Final data set (one row per request)
SELECT
  id,
  created_at,
  runtime_sec,
  user_name,
  workbook_name,
  dashboard_name,
  CONCAT('https://YOUR_TABLEAU_SERVER.com', 
         CASE WHEN site_name IS NOT NULL THEN '/#/site/'||site_name ELSE '' END,
         workbook_url)               AS absolute_url,
  vizql_session
FROM matched
WHERE runtime_sec IS NOT NULL;
