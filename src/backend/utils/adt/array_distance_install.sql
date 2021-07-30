-- --------------------------------------------------------------------
--
-- array_distance_install.sql
--
-- Support eculidean metric and cosine distance functions for array
--
-- --------------------------------------------------------------------

CREATE OR REPLACE FUNCTION euclidean_metric_float4array(anyarray, anyarray) RETURNS float4 LANGUAGE internal IMMUTABLE AS 'euclidean_metric_float4array';

CREATE OR REPLACE FUNCTION euclidean_metric_float8array(anyarray, anyarray) RETURNS float8 LANGUAGE internal STABLE AS 'euclidean_metric_float8array';

CREATE OR REPLACE FUNCTION cosine_distance_float4array(anyarray, anyarray) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'cosine_distance_float4array';

CREATE OR REPLACE FUNCTION cosine_distance_float8array(anyarray, anyarray) RETURNS float8 LANGUAGE internal IMMUTABLE AS 'cosine_distance_float8array';
