Create Schema schema1;
Create Schema schema2;
Create table schematest(i int, s varchar(10));
Create table schema1.schematest(i int, s varchar(10));
Create table schema2.schematest(i int, s varchar(10));
Insert into schematest values (1, 'default');
Insert into schema1.schematest values (2, 'schema1');
Insert into schema2.schematest values (3, 'schema2');
