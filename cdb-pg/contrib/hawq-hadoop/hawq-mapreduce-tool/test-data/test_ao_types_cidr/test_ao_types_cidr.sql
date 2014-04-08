DROP TABLE IF EXISTS test_ao_types_cidr;
CREATE TABLE test_ao_types_cidr (c0 cidr) with (appendonly=true, orientation=row, compresstype=none, blocksize=32768, checksum=false, compresslevel=0);
INSERT INTO test_ao_types_cidr values ('2001:db8:85a3:8d3:1319:8a2e:370:7344/128'),
('192.168.1.255/32'),
('172.20.143.0/24'),
(null);