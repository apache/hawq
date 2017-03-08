CREATE WRITABLE EXTERNAL TABLE ext_t2 (i int) LOCATION ('gpfdist://localhost:7070/ranger2.out') FORMAT 'TEXT' ( DELIMITER '|' NULL ' ');

