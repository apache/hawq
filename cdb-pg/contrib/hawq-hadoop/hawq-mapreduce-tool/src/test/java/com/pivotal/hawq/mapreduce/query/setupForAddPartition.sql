--Create table
CREATE TABLE addpartition (id int, date date, amt decimal(10,2)) WITH (appendonly=true, compresstype=quicklz, compresslevel=1, orientation=row, blocksize = 8192) DISTRIBUTED BY (id) PARTITION BY RANGE(date) (START (date '2008-01-01') END (date '2008-07-01') EVERY (INTERVAL '2 day') WITH (appendonly=true, compresstype=quicklz, compresslevel=1, orientation=row, blocksize = 8192),START (date '2008-07-02') END (date '2009-01-01') EVERY (INTERVAL '2 day') WITH (appendonly=true, compresstype=quicklz, compresslevel=1, orientation=row, blocksize = 8192));
--Alter table
ALTER TABLE addpartition ADD PARTITION START (date '2009-01-02') END (date '2009-08-01') WITH (appendonly=true, compresstype=zlib, compresslevel=1, orientation=row, blocksize = 10000);
ALTER TABLE addpartition ADD PARTITION START (date '2009-08-02') END (date '2009-12-01') WITH (appendonly=true, compresstype=quicklz, compresslevel=1, orientation=row, blocksize = 10000);
--Insert date into table
INSERT INTO addpartition values(0, '2008-01-02', 0.1);
INSERT INTO addpartition values(0, '2008-07-02', 0.1);
INSERT INTO addpartition values(0, '2009-01-02', 0.1);
INSERT INTO addpartition values(0, '2009-07-02', 0.1);
