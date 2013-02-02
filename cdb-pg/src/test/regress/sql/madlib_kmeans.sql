SET client_min_messages TO ERROR;
DROP SCHEMA IF EXISTS madlib_install_check_gpsql_kmeans CASCADE;
CREATE SCHEMA madlib_install_check_gpsql_kmeans;
SET search_path = madlib_install_check_gpsql_kmeans, madlib;

CREATE TABLE kmeans_2d(
	id SERIAL,
	x DOUBLE PRECISION,
	y DOUBLE PRECISION,
	position DOUBLE PRECISION[]
);

INSERT INTO kmeans_2d(x, y, position)
SELECT
	x, y,
	ARRAY[
		x + random() * 15.0,
		y + random() * 15.0
	]::DOUBLE PRECISION[] AS position
FROM (
	SELECT
		random() * 100.0 AS x,
		random() * 100.0 AS y
	FROM generate_series(1,10)
) AS centroids, generate_series(1,100) i;

CREATE TABLE centroids AS
SELECT position
FROM kmeans_2d
ORDER BY random()
LIMIT 10;

SELECT array_upper(centroids, 1) 
FROM kmeanspp('kmeans_2d', 'position', 10);

SELECT array_upper(centroids, 1) 
FROM kmeans_random('kmeans_2d', 'position', 10);
 
SELECT array_upper(centroids, 1) 
FROM kmeans('kmeans_2d', 'position', 'centroids', 'position');

SELECT array_upper(centroids, 1) 
FROM kmeans('kmeans_2d', 'position', ARRAY[
    ARRAY[10,10],
    ARRAY[20,20],
    ARRAY[30,30],
    ARRAY[40,40],
    ARRAY[50,50],
    ARRAY[60,60],
    ARRAY[70,70],
    ARRAY[80,80],
    ARRAY[90,90],
    ARRAY[10,10]
]::DOUBLE PRECISION[][]);
DROP SCHEMA madlib_install_check_gpsql_kmeans CASCADE;
