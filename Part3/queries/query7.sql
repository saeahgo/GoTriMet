-- query7.sql
COPY (
    SELECT b.tstamp, b.latitude, b.longitude, b.speed, b.trip_id
    FROM breadcrumb b
    JOIN trip t ON b.trip_id = t.trip_id
    WHERE
        t.service_key = 'W'
        AND DATE(b.tstamp) >= '2023-01-09'
        AND b.tstamp::time BETWEEN '16:00:00' AND '18:00:00'
    ORDER BY b.tstamp
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;
