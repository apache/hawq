--Create table
CREATE TABLE cotable (id int, date date, amt decimal(10,2)) WITH (appendonly=true, compresstype=quicklz, compresslevel=1, orientation=column, blocksize = 8192) DISTRIBUTED BY (id);
--Insert date into table
INSERT INTO cotable values(0, '2008-01-02', 0.1);
INSERT INTO cotable values(0, '2008-01-03', 0.1);
INSERT INTO cotable values(0, '2008-01-04', 0.1);
INSERT INTO cotable values(0, '2008-01-05', 0.1);
INSERT INTO cotable values(0, '2008-07-02', 0.1);
INSERT INTO cotable values(0, '2008-07-03', 0.1);
INSERT INTO cotable values(0, '2008-07-04', 0.1);
INSERT INTO cotable values(0, '2008-07-05', 0.1);
INSERT INTO cotable values(0, '2009-01-02', 0.1);
INSERT INTO cotable values(0, '2009-01-03', 0.1);
INSERT INTO cotable values(0, '2009-01-04', 0.1);
INSERT INTO cotable values(0, '2009-01-05', 0.1);
INSERT INTO cotable values(0, '2009-07-02', 0.1);
INSERT INTO cotable values(0, '2009-07-03', 0.1);
INSERT INTO cotable values(0, '2009-07-04', 0.1);
INSERT INTO cotable values(0, '2009-07-05', 0.1);
