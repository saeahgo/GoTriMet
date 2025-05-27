COPY (
    SELECT longitude, latitude, speed
    FROM breadcrumb
    WHERE trip_id =  234279603 
    ORDER BY tstamp
) TO STDOUT WITH DELIMITER E'\t' CSV HEADER;
