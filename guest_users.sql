-- True daily unique guest visitors (works in every restricted repo)
SELECT
    DATE_TRUNC('day', h.created_at)::date AS access_date,
    COUNT(*)                              AS total_guest_requests,
    COUNT(DISTINCT h.session_id)          AS unique_guest_visitors,
    COUNT(DISTINCT h.worker || h.session_id) AS unique_guest_devices   -- even better proxy
FROM public.http_requests h
WHERE 
    h.user_id IS NULL 
    OR h.user_id <= 0 
    OR EXISTS (SELECT 1 FROM public._users u WHERE u.id = h.user_id AND u.name ILIKE '%guest%')
  AND h.created_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_TRUNC('day', h.created_at)
ORDER BY access_date DESC;
