-- query6.sql
COPY (
    SELECT b.tstamp, b.latitude, b.longitude, b.speed, b.trip_id, t.service_key
    FROM breadcrumb b
    JOIN trip t ON b.trip_id = t.trip_id
    WHERE
        DATE(b.tstamp) BETWEEN '2023-01-15' AND '2023-01-21'
        AND b.tstamp::time BETWEEN '07:00:00' AND '09:00:00'
        AND b.latitude BETWEEN 45.50 AND 45.52
        AND b.longitude BETWEEN -122.69 AND -122.67
        AND t.service_key IN ('W', 'S', 'U')
    ORDER BY b.tstamp
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;
