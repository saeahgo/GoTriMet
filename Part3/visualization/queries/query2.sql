COPY (
  SELECT b.longitude, b.latitude, b.speed
  FROM breadcrumb b
  JOIN trip t ON b.trip_id = t.trip_id
  WHERE t.route_id = 20
    AND b.trip_id = 235074028
    AND b.tstamp::time BETWEEN '16:00:00' AND '18:00:00'
  ORDER BY b.tstamp
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;
