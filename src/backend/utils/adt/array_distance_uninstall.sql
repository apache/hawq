-- --------------------------------------------------------------------
--
-- array_distance_install.sql
--
-- Remove eculidean metric and cosine distance functions for array
--
-- --------------------------------------------------------------------

DROP FUNCTION IF EXISTS euclidean_metric_float4array(anyarray, anyarray);

DROP FUNCTION IF EXISTS euclidean_metric_float8array(anyarray, anyarray);

DROP FUNCTION IF EXISTS cosine_distance_float4array(anyarray, anyarray);

DROP FUNCTION IF EXISTS cosine_distance_float8array(anyarray, anyarray);
