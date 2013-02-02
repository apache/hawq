SET client_min_messages TO ERROR;
DROP SCHEMA IF EXISTS madlib_install_check_gpsql_linear CASCADE;
CREATE SCHEMA madlib_install_check_gpsql_linear;
SET search_path = madlib_install_check_gpsql_linear, madlib;

CREATE TABLE weibull (
    id INTEGER NOT NULL,
    x1 DOUBLE PRECISION,
    x2 DOUBLE PRECISION,
    y DOUBLE PRECISION
);

/*
 * We materialize the table here because on MPP systems we want to distribute
 * the data to the segments (and not do the calculations in memory).
 */
INSERT INTO weibull(id, x1, x2, y) VALUES
( 1, 41.9, 29.1, 251.3),
( 2, 43.4, 29.3, 251.3),
( 3, 43.9, 29.5, 248.3),
( 4, 44.5, 29.7, 267.5),
( 5, 47.3, 29.9, 273.0),
( 6, 47.5, 30.3, 276.5),
( 7, 47.9, 30.5, 270.3),
( 8, 50.2, 30.7, 274.9),
( 9, 52.8, 30.8, 285.0),
(10, 53.2, 30.9, 290.0),
(11, 56.7, 31.5, 297.0),
(12, 57.0, 31.7, 302.5),
(13, 63.5, 31.9, 304.5),
(14, 65.3, 32.0, 309.3),
(15, 71.1, 32.1, 321.7),
(16, 77.0, 32.5, 330.7),
(17, 77.8, 32.9, 349.0);

SELECT assert(
    relative_error(coef, ARRAY[-153.51, 1.24, 12.08]) < 1e-4 AND
    relative_error(t_stats[2], 3.1393) < 1e-4 AND
    relative_error(t_stats[3], 3.0726) < 1e-4 AND
    relative_error(p_values[3], 0.0083) < 1e-2,
    'Linear regression (weibull.com test): Wrong results'
) FROM (
    SELECT (linregr(y, ARRAY[1, x1, x2])).*
    FROM weibull
) q;


/*
 * The following example is taken from:
 * http://biocomp.health.unm.edu/biomed505/Course/Cheminformatics/advanced/data_classification_qsar/linear_multilinear_regression.pdf
 */
CREATE TABLE unm (
    id INTEGER NOT NULL,
    x1 DOUBLE PRECISION,
    x2 DOUBLE PRECISION,
    y DOUBLE PRECISION
);

INSERT INTO unm(id, x1, x2, y) VALUES
(1, 0,    0.30, 10.14),
(2, 0.69, 0.60, 11.93),
(3, 1.10, 0.90, 13.57),
(4, 1.39, 1.20, 14.17),
(5, 1.61, 1.50, 15.25),
(6, 1.79, 1.80, 16.15);

SELECT assert(
    relative_error(coef, ARRAY[9.69, 2.09, 1.50]) < 1e-2,
    'Linear regression (unm): Wrong coefficients'
) FROM (
    SELECT (linregr(y, ARRAY[1, x1, x2])).*
    FROM unm
) q;


/*
 * The following example is taken from:
 * http://www.stat.columbia.edu/~martin/W2110/SAS_7.pdf
 */
CREATE TABLE houses (
    id SERIAL NOT NULL,
    tax INTEGER,
    bedroom REAL,
    bath REAL,
    price INTEGER,
    size INTEGER,
    lot INTEGER
    --CONSTRAINT pk_houses PRIMARY KEY (id)
);

INSERT INTO houses(tax, bedroom, bath, price, size, lot) VALUES
( 590, 2, 1,    50000,  770, 22100),
(1050, 3, 2,    85000, 1410, 12000),
(  20, 3, 1,    22500, 1060, 3500 ),
( 870, 2, 2,    90000, 1300, 17500),
(1320, 3, 2,   133000, 1500, 30000),
(1350, 2, 1,    90500,  820, 25700),
(2790, 3, 2.5, 260000, 2130, 25000),
( 680, 2, 1,   142500, 1170, 22000),
(1840, 3, 2,   160000, 1500, 19000),
(3680, 4, 2,   240000, 2790, 20000),
(1660, 3, 1,    87000, 1030, 17500),
(1620, 3, 2,   118600, 1250, 20000),
(3100, 3, 2,   140000, 1760, 38000),
(2070, 2, 3,   148000, 1550, 14000),
( 650, 3, 1.5,  65000, 1450, 12000);

-- Values computed with MADlib
SELECT assert(
    relative_error(coef, ARRAY[27923.43, -35524.78, 2269.34, 130.79]) < 1e-4 AND
    relative_error(r2, 0.74537) < 1e-3 AND
    relative_error(std_err, ARRAY[56306, 25037, 22208, 36.209]) < 1e-4 AND
    relative_error(t_stats, ARRAY[0.49592, -1.4189, 0.10218, 3.6122]) < 1e-4 AND
    relative_error(p_values, ARRAY[0.62971, 0.18363, 0.92045, 0.0040816]) < 1e-4 AND
    relative_error(condition_no, 95707449) < 1e-4,
    'Linear regression (houses): Wrong results'
) FROM (
    SELECT (linregr(price, array[1, bedroom, bath, size])).*
    FROM houses
) q;

-- Merge functions must prodice valid results when being called with the
-- aggregate's initial state
SELECT assert(
    (linregr).r2 = 1,
    'Linear regression failed on singleton data set.'
) FROM (
    SELECT linregr_final(
        linregr_merge_states(
            linregr_transition(CAST('' AS bytea8), 3, ARRAY[5,2]),
            CAST('' AS bytea8)
        )
    ) AS linregr
) ignored;

SELECT assert(
    (linregr).r2 = 1,
    'Linear regression failed on singleton data set.'
) FROM (
    SELECT linregr_final(
        linregr_merge_states(
            CAST('' AS bytea8),
            linregr_transition(CAST('' AS bytea8), 3, ARRAY[5,2])
        )
    ) AS linregr
) ignored;

DROP SCHEMA madlib_install_check_gpsql_linear CASCADE;
