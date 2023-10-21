SELECT "fecha", "producto", "precio", "id" FROM "precios_lista_larga" 
where "producto" = 'mandarina'
order by "fecha" desc
LIMIT 300;

DELETE FROM "precios_lista_larga"
WHERE "producto" = 'mandarina'
  AND "fecha" BETWEEN '2023-10-01' AND '2023-10-18'
  AND "id" NOT IN (
    SELECT MIN("id")
    FROM "precios_lista_larga"
    WHERE "producto" = 'mandarina'
    AND "fecha" BETWEEN '2023-10-01' AND '2023-10-18'
    GROUP BY "fecha"
    HAVING COUNT(*) > 1
  );


INSERT INTO "precios_lista_larga" ("fecha", "producto", "precio")
SELECT
  d."fecha",
  'mandarina' AS "producto",
  CASE
    WHEN d."fecha" BETWEEN '2023-02-17' AND '2023-09-30' THEN 0
    ELSE 499
  END AS "precio"
FROM (
  SELECT generate_series('2023-02-17'::date, '2023-09-30'::date, '1 day'::interval) AS "fecha"
) d
LEFT JOIN "precios_lista_larga" p
ON d."fecha" = p."fecha" AND p."producto" = 'mandarina';

UPDATE "precios_lista_larga"
SET "precio" = 1
WHERE "producto" = 'mandarina'
AND "fecha" BETWEEN '2023-02-17' AND '2023-09-30';