--
-- MONEY
--

CREATE TABLE MONEY_TBL (f1  money);

INSERT INTO MONEY_TBL(f1) VALUES ('    0.0');
INSERT INTO MONEY_TBL(f1) VALUES ('1004.30   ');
INSERT INTO MONEY_TBL(f1) VALUES ('     -34.84    ');
INSERT INTO MONEY_TBL(f1) VALUES ('123456789012345.67');

-- test money over and under flow
SELECT '12345678901234567890.12'::money = '-13639628150831692.60'::money as x;
SELECT '123.001'::money = '123'::money as x;

-- bad input
INSERT INTO MONEY_TBL(f1) VALUES ('xyz');
INSERT INTO MONEY_TBL(f1) VALUES ('5.0.0');
INSERT INTO MONEY_TBL(f1) VALUES ('5 . 0');
INSERT INTO MONEY_TBL(f1) VALUES ('5.   0');
INSERT INTO MONEY_TBL(f1) VALUES ('123            5');

-- queries
SELECT '' AS five, * FROM MONEY_TBL ORDER BY 2;

SELECT '' AS four, f.* FROM MONEY_TBL f WHERE f.f1 <> '1004.3' ORDER BY 2;

SELECT '' AS one, f.* FROM MONEY_TBL f WHERE f.f1 = '1004.3' ORDER BY 2;

SELECT '' AS three, f.* FROM MONEY_TBL f WHERE '1004.3' > f.f1 ORDER BY 2;

SELECT '' AS three, f.* FROM MONEY_TBL f WHERE  f.f1 < '1004.3' ORDER BY 2;

SELECT '' AS four, f.* FROM MONEY_TBL f WHERE '1004.3' >= f.f1 ORDER BY 2;

SELECT '' AS four, f.* FROM MONEY_TBL f WHERE  f.f1 <= '1004.3' ORDER BY 2;

SELECT '' AS three, f.f1, f.f1 * '-10' AS x FROM MONEY_TBL f
   WHERE f.f1 > '0.0' ORDER BY 2;

SELECT '' AS three, f.f1, f.f1 + '-10' AS x FROM MONEY_TBL f
   WHERE f.f1 > '0.0' ORDER BY 2;

SELECT '' AS three, f.f1, f.f1 / '-10' AS x FROM MONEY_TBL f
   WHERE f.f1 > '0.0' ORDER BY 2;

SELECT '' AS three, f.f1, f.f1 - '-10' AS x FROM MONEY_TBL f
   WHERE f.f1 > '0.0' ORDER BY 2;

SELECT SUM(f.f1) AS x FROM MONEY_TBL f;

-- test divide by zero
SELECT '' AS bad, f.f1 / '0.0' from MONEY_TBL f;

SELECT '' AS five, * FROM MONEY_TBL ORDER BY 2;

-- parquet table
CREATE TABLE MONEY_TBL_P (f1 money) with (appendonly=true, orientation=parquet);

INSERT INTO MONEY_TBL_P(f1) VALUES ('    0.0');
INSERT INTO MONEY_TBL_P(f1) VALUES ('1004.30   ');
INSERT INTO MONEY_TBL_P(f1) VALUES ('     -34.84    ');
INSERT INTO MONEY_TBL_P(f1) VALUES ('123456789012345.67');

SELECT f1 FROM MONEY_TBL_P f
   ORDER BY f1;

SELECT sum(f1) AS x, min(f1) as y, max(f1) as z FROM MONEY_TBL_P AS f;
