-- query3.sql
COPY (
    SELECT b.tstamp, b.latitude, b.longitude, b.speed, b.trip_id
    FROM breadcrumb b
    WHERE b.trip_id IN (
        SELECT DISTINCT b.trip_id
        FROM breadcrumb b
        JOIN trip t ON b.trip_id = t.trip_id
        WHERE
            t.service_key = 'W' -- double check weekday
            AND DATE(b.tstamp) = '2023-01-09'
            AND b.tstamp::time BETWEEN '09:00:00' AND '11:00:00'
            AND b.latitude BETWEEN 45.5095 AND 45.5115
            AND b.longitude BETWEEN -122.6850 AND -122.6820
    )
    AND DATE(b.tstamp) = '2023-01-09'
    AND b.tstamp::time BETWEEN '09:00:00' AND '11:00:00'
    ORDER BY b.trip_id, b.tstamp
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;
