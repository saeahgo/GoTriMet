-- query4.sql
COPY (
  SELECT
    b.tstamp,
    b.latitude,
    b.longitude,
    b.speed,
    b.trip_id
  FROM breadcrumb b
  WHERE b.tstamp::date = '2023-01-10'
    AND b.tstamp::time < '11:00:00'
    AND ST_DWithin(
      ST_SetSRID(ST_MakePoint(b.longitude, b.latitude), 4326)::geography,
      ST_SetSRID(ST_MakePoint(-122.649434, 45.508537), 4326)::geography,
      1000
    )
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;

