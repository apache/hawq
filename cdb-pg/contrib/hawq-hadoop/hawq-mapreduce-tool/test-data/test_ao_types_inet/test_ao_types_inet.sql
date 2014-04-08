DROP TABLE IF EXISTS test_ao_types_inet;
CREATE TABLE test_ao_types_inet (c0 inet) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_inet values ('2001:db8:85a3:8d3:1319:8a2e:370:7344/64'),
('2001::7344'),
('172.20.143.0'),
('192.168.1.255/24'),
(null);