CREATE TABLE gpsnmpd_rdbmsDbTable (
	-- ideally this would reference pg_database.oid, but pgsql won't allow such a reference
	database_oid OID PRIMARY KEY,
	vendor_name VARCHAR(255) DEFAULT 'Greenplum Corporation',
	contact_name VARCHAR(255),
	last_backup TIMESTAMPTZ);

CREATE TABLE gpsnmpd_rdbmsSrvTable (
	vendor_name VARCHAR(255) DEFAULT 'Greenplum Corporation',
	product_name VARCHAR(255) DEFAULT 'GPDB',
	contact_name VARCHAR(255));
