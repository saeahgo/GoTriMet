-- query5.sql
COPY (
  SELECT
    b.latitude,
    b.longitude,
    b.speed,
    CASE
      WHEN DATE(b.tstamp) = '2023-01-12' THEN 'rainy'
      WHEN DATE(b.tstamp) = '2023-01-19' THEN 'non-rainy'
    END AS weather
  FROM breadcrumb b
  WHERE
    DATE(b.tstamp) IN ('2023-01-12', '2023-01-19')
    AND b.tstamp::time BETWEEN '10:00:00' AND '13:00:00'
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;
