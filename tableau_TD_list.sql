-- =============================================================
-- Teradata Usage Summary – Prep Flows + Workbooks + Published DS
-- Works even in restricted schemas (no connections table needed)
-- =============================================================
SELECT
    'Published Data Sources'                AS object_type,
    COUNT(*)                                 AS teradata_count,
    STRING_AGG(DISTINCT ds.name, ' | ')      AS examples_top_10
FROM public._datasources ds
WHERE LOWER(ds.connection_type) = 'teradata'
   OR LOWER(ds.server) LIKE '%teradata%'
   OR LOWER(ds.dbname) LIKE '%teradata%'
   OR LOWER(ds.name) ILIKE '%teradata%'

UNION ALL

SELECT
    'Embedded in Workbooks' AS object_type,
    COUNT(*) AS teradata_count,
    STRING_AGG(DISTINCT w.name, ' | ')
FROM public._workbooks w
WHERE EXISTS (
    SELECT 1 FROM public._datasources ds
    WHERE ds.workbook_id = w.id
      AND (
          LOWER(ds.connection_type) = 'teradata'
          OR LOWER(ds.server) LIKE '%teradata%'
          OR LOWER(ds.dbname) LIKE '%teradata%'
      )
)
GROUP BY object_type

UNION ALL

SELECT
    'Tableau Prep Flows' AS object_type,
    COUNT(*) AS teradata_count,
    STRING_AGG(DISTINCT f.name, ' | ')
FROM public._flows f
WHERE LOWER(f.connection_type) = 'teradata'
   OR LOWER(f.server) LIKE '%teradata%'
   OR LOWER(f.dbname) LIKE '%teradata%'

UNION ALL

-- Grand total
SELECT
    'TOTAL Teradata Objects' AS object_type,
    (SELECT COUNT(*) FROM public._datasources ds WHERE LOWER(ds.connection_type) = 'teradata' OR LOWER(ds.server) LIKE '%teradata%')
    + (SELECT COUNT(*) FROM public._workbooks w WHERE EXISTS (SELECT 1 FROM public._datasources ds WHERE ds.workbook_id = w.id AND (LOWER(ds.connection_type) = 'teradata' OR LOWER(ds.server) LIKE '%teradata%')))
    + (SELECT COUNT(*) FROM public._flows f WHERE LOWER(f.connection_type) = 'teradata' OR LOWER(f.server) LIKE '%teradata%')
    AS teradata_count,
    '← Migrate these first!' AS examples_top_10;
