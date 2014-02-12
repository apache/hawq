--Create table
CREATE TABLE truncateTable (id int, date date, amt decimal(10,2));
--Insert date into table
INSERT INTO truncateTable values(0, '2008-01-02', 0.1);
INSERT INTO truncateTable values(0, '2008-01-03', 0.1);
INSERT INTO truncateTable values(0, '2008-01-04', 0.1);
INSERT INTO truncateTable values(0, '2009-07-05', 0.1);

truncate truncateTable;
