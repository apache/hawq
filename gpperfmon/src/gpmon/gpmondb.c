#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "gpmonlib.h"
#include "gpmondb.h"
#include "libpq-fe.h"
#include "apr_strings.h"
#include "apr_file_io.h"
#include "apr_hash.h"
#include "time.h"

#define GPMON_HOSTTTYPE_HDW 1
#define GPMON_HOSTTTYPE_HDM 2
#define GPMON_HOSTTTYPE_ETL 3
#define GPMON_HOSTTTYPE_HBW 4
#define GPMON_HOSTTTYPE_HDC 5

#define MAX_SMON_PATH_SIZE (1024)

#define GPDB_CONNECTION_STRING "dbname='" GPMON_DB "' user='" GPMON_DBUSER "' connect_timeout='30'"

#ifdef USE_CONNECTEMC
int find_token_in_config_string(char* buffer, char**result, const char* token);
#else
int find_token_in_config_string(char* buffer, char**result, const char* token)
{
    return 1;
}
#endif

// assumes a valid connection already exists
static const char* gpdb_exec_only(PGconn* conn, PGresult** pres, const char* query)
{
	PGresult* res = 0;
	ExecStatusType status;

	TR1(("Query: %s\n", query));

	res = PQexec(conn, query);
	status = PQresultStatus(res);
	if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK)
		return PQerrorMessage(conn);

	*pres = res;
	return 0;
}

static const char* gpdb_exec(PGconn** pconn, PGresult** pres, const char* query)
{
	const char *connstr = "dbname='" GPMON_DB "' user='" GPMON_DBUSER
	"' connect_timeout='30'";
	PGconn *conn = NULL;

	conn = PQconnectdb(connstr);
	if (PQstatus(conn) != CONNECTION_OK)
		return PQerrorMessage(conn);

	*pconn = conn;

    return gpdb_exec_only(conn, pres, query);
}

// persistant_conn is optional if you are already holding an open connectionconn
// return 1 if more than 0 rows are returned from query
// return 0 if zero rows are returned from query
int gpdb_exec_search_for_at_least_one_row(const char* QUERY, PGconn* persistant_conn)
{
	PGconn* conn = 0;
	PGresult* result = 0;
	int rowcount;
	int res = 0;
	const char* errmsg;

    if (persistant_conn)
	    errmsg = gpdb_exec_only(persistant_conn, &result, QUERY);
    else
	    errmsg = gpdb_exec(&conn, &result, QUERY);

	if (errmsg)
	{
		gpmon_warning(FLINE, "GPDB error %s\n\tquery: %s\n", errmsg, QUERY);
	}
	else
	{
		rowcount = PQntuples(result);
		if (rowcount > 0)
			res = 1;
	}

	PQclear(result);

    if (conn)
	    PQfinish(conn);

	return res;
}

int gpdb_validate_gpperfmon(void)
{
	/* Check db */
	if (!gpdb_gpperfmon_db_exists())
		return 0;

	/* check post */
	if (!gpdb_get_gpmon_port())
		return 0;

	/* check external tables are accessable by gpmon user */
	if (!gpdb_validate_ext_table_access())
		return 0;

	return 1;
}

int gpdb_gpperfmon_db_exists(void)
{
	const char *connstr = "dbname='" GPMON_DB "' user='" GPMON_DBUSER
	"' connect_timeout='30'";
	PGconn *conn = NULL;
	int db_exists = 0;

	conn = PQconnectdb(connstr);
	if (PQstatus(conn) == CONNECTION_OK)
	{
		db_exists = 1;
		PQfinish(conn);
	}

	return db_exists;
}

int gpdb_gpperfmon_enabled(void)
{
	const char* QUERY = "SELECT 1 FROM pg_settings WHERE name = 'gp_enable_gpperfmon' and setting='on'";
	return gpdb_exec_search_for_at_least_one_row(QUERY, NULL);
}

int gpdb_validate_ext_table_access(void)
{
	const char* QUERY = "select * from master_data_dir";
	return gpdb_exec_search_for_at_least_one_row(QUERY, NULL);
}

int gpdb_get_gpmon_port(void)
{
	PGconn* conn = 0;
	PGresult* result = 0;
	char* curport = 0;
	int rowcount;
	const char* QUERY =
			"SELECT setting FROM pg_settings WHERE name = 'gpperfmon_port'";
	int port = 0;
	const char* errmsg = gpdb_exec(&conn, &result, QUERY);
	if (errmsg)
	{
		gpmon_warning(FLINE, "GPDB error %s\n\tquery: %s\n", errmsg, QUERY);
	}
	else
	{
		rowcount = PQntuples(result);
		if (rowcount > 0)
		{
			curport = PQgetvalue(result, 0, 0);
			if (curport)
			{
				port = atoi(curport);
			}
		}
	}
	PQclear(result);
	PQfinish(conn);

	if (!port)
	{
		gpmon_warning(FLINE, "Unable to retrieve gpperfmon_port GUC from GPDB\n");
	}
	return port;
}


struct hostinfo_holder_t
{
	addressinfo_holder_t* addressinfo_head;
	addressinfo_holder_t* addressinfo_tail;
	apr_uint32_t address_count;

	char* datadir;
	char* smon_dir;
	char* hostname;
	int is_master;
	int is_hdm;
	int is_hdw;
	int is_hbw;
	int is_hdc;
	int is_etl;
};




void initializeHostInfoDataWithAddress(struct hostinfo_holder_t* holder, char* address, int firstAddress)
{
	// USE permenant memory to store this data

	addressinfo_holder_t* aiholder = calloc(1, sizeof(addressinfo_holder_t));
	CHECKMEM(aiholder);

	aiholder->address = strdup(address);
	CHECKMEM(aiholder->address);

	if (firstAddress)
	{
		holder->addressinfo_head = holder->addressinfo_tail = aiholder;
	}
	else
	{
		holder->addressinfo_tail->next = aiholder;
		holder->addressinfo_tail = aiholder;
	}
}

void initializeHostInfoDataFromFileEntry(apr_pool_t* tmp_pool, struct hostinfo_holder_t* holder,
		char* primary_hostname, char* hostEntry, int hostType, char* smon_bin_dir, char* smon_log_dir)
{
	holder->hostname = apr_pstrdup(tmp_pool, primary_hostname);
	CHECKMEM(holder->hostname);

	if (smon_bin_dir && smon_log_dir) {		

		holder->smon_dir = apr_pstrdup(tmp_pool, smon_bin_dir);
		CHECKMEM(holder->smon_dir);

		holder->datadir = apr_pstrdup(tmp_pool, smon_log_dir);
		CHECKMEM(holder->datadir);

	} else {
		holder->smon_dir = NULL;
		holder->datadir = apr_pstrdup(tmp_pool, "/opt/dca/var/gpsmon");
		CHECKMEM(holder->datadir);
	}

 	switch(hostType){
		case GPMON_HOSTTTYPE_HDW:
			holder->is_hdw = 1;
		break;
		case GPMON_HOSTTTYPE_HDM:
			holder->is_hdm = 1;
		break;
		case GPMON_HOSTTTYPE_ETL:
			holder->is_etl = 1;
		break;
		case GPMON_HOSTTTYPE_HBW:
			holder->is_hbw = 1;
		break;
		case GPMON_HOSTTTYPE_HDC:
			holder->is_hdc = 1;
		break;
	}

	holder->address_count = 0;
	int firstAddress = 1;

	while(*hostEntry)
	{	
		char* location = strchr(hostEntry, ',');
		if (location)
			*location = 0;

		initializeHostInfoDataWithAddress(holder, hostEntry, firstAddress);
		holder->address_count++;
		if (!location)
			return; // there were no commas so this is the last address in the hostEntry
		*location = ',';
		hostEntry = location+1; 
		firstAddress = 0;
	}
}


void process_line_in_devices_cnf(apr_pool_t* tmp_pool, apr_hash_t* htab, char* line)
{
	if (!line)
	{
		gpmon_warningx(FLINE, 0, "Line in devices file is null, skipping");
		return; 
	}

	char* host;
    char* device;
    char* category;
	char primary_hostname[64];

	char* location = strchr(line, '#');
	if (location)
	{
		*location = 0;
		// remove comments from the line
	}

	if (!line)
	{
		gpmon_warningx(FLINE, 0, "Line in devices file is null after removing comments, skipping");
		return; 
	}

	// we do these in reverse order so inserting null chars does not prevent finding other tokens
    if (find_token_in_config_string(line, &host, "Host"))
	{
        return;
	}

    if (find_token_in_config_string(line, &device, "Device"))
	{
        return;
	}

    if (find_token_in_config_string(line, &category, "Category"))
	{
        return;
	}

	int monitored_device = 0;
	int hostType = 0;
	if (strcmp(device, "Spidey0001") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDW;
	}

	if (strcmp(device, "Spidey0002") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDM;
	}

	if (strcmp(device, "Spidey0003") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HBW;
	}

	if (strcmp(device, "EtlHost") == 0) 
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_ETL;
	}

	//For V2
	if (strcmp(device, "Locust0001") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDW;
	}

	if (strcmp(device, "Locust0002") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDM;
	}

	if (strcmp(device, "Locust0003") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDC;
	}

	if (strcmp(device, "EtlHostV2") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_ETL;
	}

	// segment host, switch, etc ... we are only adding additional hosts required for performance monitoring
	if (!monitored_device)
		return;

	strncpy(primary_hostname, host, sizeof(primary_hostname));
	primary_hostname[sizeof(primary_hostname) - 1] = 0;
	location = strchr(primary_hostname, ',');
	if (location)
		*location = 0;

	struct hostinfo_holder_t* hostinfo_holder = apr_hash_get(htab, primary_hostname, APR_HASH_KEY_STRING);
	if (hostinfo_holder) {
		gpmon_warningx(FLINE, 0, "Host '%s' is duplicated in devices.cnf", primary_hostname);
		return;
	}
	
	// OK Lets add this record at this point
	hostinfo_holder = apr_pcalloc(tmp_pool, sizeof(struct hostinfo_holder_t));
	CHECKMEM(hostinfo_holder);

	apr_hash_set(htab, primary_hostname, APR_HASH_KEY_STRING, hostinfo_holder);

	initializeHostInfoDataFromFileEntry(tmp_pool, hostinfo_holder, primary_hostname, host, hostType, NULL, NULL);
}


void process_line_in_hadoop_cluster_info(apr_pool_t* tmp_pool, apr_hash_t* htab, char* line, char* smon_bin_location, char* smon_log_location)
{
	if (!line) {
		gpmon_warningx(FLINE, 0, "Line in hadoop cluster info file is null, skipping");
		return;
	}

	char* host;
	char* category;

	char primary_hostname[64];

	char* location = strchr(line, '#');
	if (location) {
		*location = 0; // remove comments from the line
	}

	if (!line) {
		gpmon_warningx(FLINE, 0, "Line in devices file is null after removing comments, skipping");
		return;
	}

	// we do these in reverse order so inserting null chars does not prevent finding other tokens
	if (find_token_in_config_string(line, &category, "Categories")) {
		return;
	}
	location = strchr(category, ','); //remove the comma and extra categories
	if (location) {
		*location = 0;
	}

	if (find_token_in_config_string(line, &host, "Hostname")) {
		return;
	}
	TR1(("Found hadoop host %s\n",host ));
	// look for the 3 hadoop host types
	int monitored_device = 0;
	int hostType = 0;
	if (strcmp(category, "hdm") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDM;
	}

	if (strcmp(category, "hdw") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDW;
	}

	if (strcmp(category, "hdc") == 0)
	{
		monitored_device = 1;
		hostType = GPMON_HOSTTTYPE_HDC;
	}
	// The below code is the same as the devices file parsing code

	// segment host, switch, etc ... we are only adding additional hosts required for performance monitoring
	if (!monitored_device) {
		return;
	}

	strncpy(primary_hostname, host, sizeof(primary_hostname));
	primary_hostname[sizeof(primary_hostname) - 1] = 0;
	location = strchr(primary_hostname, ',');
	if (location) {
		*location = 0;
	}

	struct hostinfo_holder_t* hostinfo_holder = apr_hash_get(htab, primary_hostname, APR_HASH_KEY_STRING);
	if (hostinfo_holder) {
		gpmon_warningx(FLINE, 0, "Host '%s' is duplicated in clusterinfo.txt", primary_hostname);
		return;
	}

	// OK Lets add this record at this point
	hostinfo_holder = apr_pcalloc(tmp_pool, sizeof(struct hostinfo_holder_t));
	CHECKMEM(hostinfo_holder);

	apr_hash_set(htab, primary_hostname, APR_HASH_KEY_STRING, hostinfo_holder);

	initializeHostInfoDataFromFileEntry(tmp_pool, hostinfo_holder, primary_hostname, host, hostType, smon_bin_location, smon_log_location);
}

//Return 1 if not an appliance and 0 if an appliance
int get_appliance_hosts_and_add_to_hosts(apr_pool_t* tmp_pool, apr_hash_t* htab)
{
	// open devices.cnf and then start reading the data
	// populate all relevant hosts: Spidey0001, Spidey0002, EtlHost
	FILE* fd = fopen(PATH_TO_APPLIANCE_VERSION_FILE, "r");
	if (!fd)
	{
		TR0(("not an appliance ... not reading devices.cnf\n"));
		return 1;
	}
	fclose(fd);

	fd = fopen(PATH_TO_APPLAINCE_DEVICES_FILE, "r");
	if (!fd)
	{
		gpmon_warningx(FLINE, 0, "can not read %s, ignoring\n", PATH_TO_APPLAINCE_DEVICES_FILE);
		return 0;
	}

	char* line;
	char buffer[1024];

	while (NULL != fgets(buffer, sizeof(buffer), fd))
	{
		// remove new line 
		line = gpmon_trim(buffer);
		process_line_in_devices_cnf(tmp_pool, htab, line);
	}

	fclose(fd);
	return 0;
}

//Return 1 if not a hadoop software only cluster and 0 it is a hadoop software only cluster
int get_hadoop_hosts_and_add_to_hosts(apr_pool_t* tmp_pool, apr_hash_t* htab, mmon_options_t* opt)
{
	if (!opt->smon_hadoop_swonly_binfile){
		TR0(("hadoop_smon_path not specified in gpmmon config. not processing hadoop nodes\n"));
		return 1;
	}

	char* smon_log_dir;
	char* hadoop_cluster_file;
	if (opt->smon_hadoop_swonly_logdir){
		smon_log_dir = opt->smon_hadoop_swonly_logdir;
	}
	else{
		smon_log_dir = (char*)PATH_TO_HADOOP_SMON_LOGS;
	}
	if (opt->smon_hadoop_swonly_clusterfile){
		hadoop_cluster_file = opt->smon_hadoop_swonly_clusterfile;
	}
	else{
		hadoop_cluster_file = (char*)DEFAULT_PATH_TO_HADOOP_HOST_FILE;
	}

	FILE* fd = fopen(hadoop_cluster_file, "r");
	if (!fd)
	{
		TR0(("not a hadoop software only cluster ... not reading %s\n", hadoop_cluster_file));
		return 1;
	}

	char* line;
	char buffer[1024];

	// process the hostlines
	while (NULL != fgets(buffer, sizeof(buffer), fd)) {
		line = gpmon_trim(buffer);// remove new line
		process_line_in_hadoop_cluster_info(tmp_pool, htab, line, opt->smon_hadoop_swonly_binfile, smon_log_dir);
	}

	fclose(fd);
	return 0;
}


void gpdb_get_hostlist(int* hostcnt, host_t** host_table, apr_pool_t* global_pool, mmon_options_t* opt)
{
	apr_pool_t* pool;
	PGconn* conn = 0;
	PGresult* result = 0;
	int rowcount, i;
	unsigned int unique_hosts = 0;
	apr_hash_t* htab;
	struct hostinfo_holder_t* hostinfo_holder = NULL;
	host_t* hosts = NULL;
	int e;

	// 0 -- hostname, 1 -- address, 2 -- datadir, 3 -- is_master, 
	const char *QUERY = "SELECT distinct hostname, address, case when content < 0 then 1 else 0 end as is_master, MAX(fselocation) as datadir FROM pg_filespace_entry "
			    "JOIN gp_segment_configuration on (dbid = fsedbid) WHERE fsefsoid = (select oid from pg_filespace where fsname='pg_system') "
		  	    "GROUP BY (hostname, address, is_master) order by hostname";

	if (0 != (e = apr_pool_create(&pool, NULL)))
	{
		gpmon_fatalx(FLINE, e, "apr_pool_create failed");
	}

	const char* errmsg = gpdb_exec(&conn, &result, QUERY);

	TR2((QUERY));
	TR2(("\n"));

	if (errmsg)
	{
		gpmon_warning(FLINE, "GPDB error %s\n\tquery: %s\n", errmsg, QUERY);
	}
	else
	{
		// hash of hostnames to addresses
 		htab = apr_hash_make(pool);

		rowcount = PQntuples(result);

		for (i = 0; i < rowcount; i++)
		{
			char* curr_hostname = PQgetvalue(result, i, 0);

			hostinfo_holder = apr_hash_get(htab, curr_hostname, APR_HASH_KEY_STRING);

			if (!hostinfo_holder)
			{
				hostinfo_holder = apr_pcalloc(pool, sizeof(struct hostinfo_holder_t));
				CHECKMEM(hostinfo_holder);

				apr_hash_set(htab, curr_hostname, APR_HASH_KEY_STRING, hostinfo_holder);

				hostinfo_holder->hostname = curr_hostname;
				hostinfo_holder->is_master = atoi(PQgetvalue(result, i, 2));
				hostinfo_holder->datadir = PQgetvalue(result, i, 3);

				// use permenant memory for address list -- stored for duration

				// populate 1st on list and save to head and tail
				hostinfo_holder->addressinfo_head = hostinfo_holder->addressinfo_tail = calloc(1, sizeof(addressinfo_holder_t));
				CHECKMEM(hostinfo_holder->addressinfo_tail);

				// first is the hostname
				hostinfo_holder->addressinfo_tail->address = strdup(hostinfo_holder->hostname);
				CHECKMEM(hostinfo_holder->addressinfo_tail->address);


				// add a 2nd to the list
				hostinfo_holder->addressinfo_tail->next = calloc(1, sizeof(addressinfo_holder_t));
				CHECKMEM(hostinfo_holder->addressinfo_tail);
				hostinfo_holder->addressinfo_tail = hostinfo_holder->addressinfo_tail->next;

				// second is address
				hostinfo_holder->addressinfo_tail->address = strdup(PQgetvalue(result, i, 1));
				CHECKMEM(hostinfo_holder->addressinfo_tail->address);

				// one for hostname one for address
				hostinfo_holder->address_count = 2; 
			}
			else
			{
				// permenant memory for address list -- stored for duration
				hostinfo_holder->addressinfo_tail->next = calloc(1, sizeof(addressinfo_holder_t));
				CHECKMEM(hostinfo_holder->addressinfo_tail);

				hostinfo_holder->addressinfo_tail = hostinfo_holder->addressinfo_tail->next;

				// permenant memory for address list -- stored for duration
				hostinfo_holder->addressinfo_tail->address = strdup(PQgetvalue(result, i, 1));
				CHECKMEM(hostinfo_holder->addressinfo_tail->address);

				hostinfo_holder->address_count++;
			}

		}

		// if we have any appliance specific hosts such as hadoop nodes add them to the hash table
		if (get_appliance_hosts_and_add_to_hosts(pool, htab)){
			TR0(("Not an appliance: checking for SW Only hadoop hosts.\n"));
			get_hadoop_hosts_and_add_to_hosts(pool, htab, opt); // Not an appliance, so check for SW only hadoop nodes.
		} 

		unique_hosts = apr_hash_count(htab);

		// allocate memory for host list (not freed ever)
		hosts = calloc(unique_hosts, sizeof(host_t));

		apr_hash_index_t* hi;
		void* vptr;
		int hostcounter = 0;
		for (hi = apr_hash_first(0, htab); hi; hi = apr_hash_next(hi))
		{
			// sanity check
			if (hostcounter >= unique_hosts)
			{
				gpmon_fatalx(FLINE, 0, "host counter exceeds unique hosts");
			}

			apr_hash_this(hi, 0, 0, &vptr);
			hostinfo_holder = vptr;

			hosts[hostcounter].hostname = strdup(hostinfo_holder->hostname);
			hosts[hostcounter].data_dir = strdup(hostinfo_holder->datadir);
			if (hostinfo_holder->smon_dir){
				hosts[hostcounter].smon_bin_location = strdup(hostinfo_holder->smon_dir);
			}
			hosts[hostcounter].is_master = hostinfo_holder->is_master;
			hosts[hostcounter].addressinfo_head = hostinfo_holder->addressinfo_head;
			hosts[hostcounter].addressinfo_tail = hostinfo_holder->addressinfo_tail;
			hosts[hostcounter].address_count = hostinfo_holder->address_count;
			hosts[hostcounter].connection_hostname.current = hosts[hostcounter].addressinfo_head;
			hosts[hostcounter].snmp_hostname.current = hosts[hostcounter].addressinfo_head;

			if (hostinfo_holder->is_hdm)
				hosts[hostcounter].is_hdm = 1;
			
			if (hostinfo_holder->is_hdw)
				hosts[hostcounter].is_hdw = 1;

			if (hostinfo_holder->is_etl)
				hosts[hostcounter].is_etl = 1;

			if (hostinfo_holder->is_hbw)
				hosts[hostcounter].is_hbw = 1;

			if (hostinfo_holder->is_hdc)
				hosts[hostcounter].is_hdc = 1;

			apr_thread_mutex_create(&hosts[hostcounter].mutex, APR_THREAD_MUTEX_UNNESTED, global_pool); // use the global pool so the mutexes last beyond this function

			hostcounter++;
		}

		*hostcnt = hostcounter;
	}

	apr_pool_destroy(pool);
	PQclear(result);
	PQfinish(conn);

	if (!hosts || *hostcnt < 1)
	{
		gpmon_fatalx(FLINE, 0, "no valid hosts found");
	}

	*host_table = hosts;
}

void gpdb_get_master_data_dir(char** hostname, char** mstrdir, apr_pool_t* pool)
{
	PGconn* conn = 0;
	PGresult* result = 0;
	const char* QUERY = "select * from master_data_dir";
	char* dir = 0;
	char* hname = 0;
	int rowcount;
	const char* errmsg = gpdb_exec(&conn, &result, QUERY);
	if (errmsg)
	{
		gpmon_warning(FLINE, "GPDB error %s\n\tquery: %s\n", errmsg, QUERY);
	}
	else
	{
		rowcount = PQntuples(result);
		if (rowcount > 0)
		{
			hname = PQgetvalue(result, 0, 0);
			dir = PQgetvalue(result, 0, 1);
		}

		if (!hname || !dir)
		{
			gpmon_warning(FLINE, "unable to get master data directory");
		}
		else
		{
			hname = apr_pstrdup(pool, gpmon_trim(hname));
			CHECKMEM(hname);

			dir = apr_pstrdup(pool, gpmon_trim(dir));
			CHECKMEM(dir);
		}
	}

	PQclear(result);
	PQfinish(conn);

	*hostname = hname;
	*mstrdir = dir;
}

void gpdb_get_single_string_from_query(const char* QUERY, char** resultstring, apr_pool_t* pool)
{
	PGconn* conn = 0;
	PGresult* result = 0;
	char* tmpoutput = 0;
	int rowcount;
	const char* errmsg = gpdb_exec(&conn, &result, QUERY);
	if (errmsg)
	{
		gpmon_warning(FLINE, "GPDB error %s\n\tquery: %s\n", errmsg, QUERY);
	}
	else
	{
		rowcount = PQntuples(result);
		if (rowcount == 1)
		{
			tmpoutput = PQgetvalue(result, 0, 0);
		}
		else if (rowcount > 1)
		{
			gpmon_warning(FLINE, "unexpected number of rows returned from query %s", QUERY);
		}

		if (tmpoutput)
		{
			tmpoutput = apr_pstrdup(pool, gpmon_trim(tmpoutput));
			CHECKMEM(tmpoutput);
		}
	}

	PQclear(result);
	PQfinish(conn);

	*resultstring = tmpoutput;
}


static void check_and_add_partition(PGconn* conn, const char* tbl, int begin_year, int begin_month, int end_year, int end_month)
{
	PGresult* result = 0;
	const char* errmsg;
	const int QRYBUFSIZ = 1024;

	char qry[QRYBUFSIZ];
	const char* CHK_QRYFMT = "select dt from (select substring(partitionrangestart from 2 for 7) as dt from pg_partitions where tablename = '%s_history' ) as TBL where TBL.dt = '%d-%02d';";
	const char* ADD_QRYFMT = "alter table %s_history add partition start ('%d-%02d-01 00:00:00'::timestamp without time zone) inclusive end ('%d-%02d-01 00:00:00'::timestamp without time zone) exclusive;";

	snprintf(qry, QRYBUFSIZ, CHK_QRYFMT, tbl, begin_year, begin_month);
	if (!gpdb_exec_search_for_at_least_one_row(qry, conn))
	{
		// this partition does not exist, create it

		snprintf(qry, QRYBUFSIZ, ADD_QRYFMT, tbl, begin_year, begin_month, end_year, end_month);
		errmsg = gpdb_exec_only(conn, &result, qry);
		if (errmsg)
		{
			gpmon_warning(FLINE, "partion add response from server: %s\n", errmsg);
		}

		PQclear(result);
	}
}

static apr_status_t check_partition(const char* tbl, apr_pool_t* pool, PGconn* conn)
{
	struct tm tm;
	time_t now;

	unsigned short year[3];
	unsigned char month[3];

	TR0(("check partitions on %s_history\n", tbl));

    if (!conn)
        return APR_ENOMEM;

   	now = time(NULL);
	if (!localtime_r(&now, &tm))
	{
		gpmon_warning(FLINE, "error in check_partition getting current time\n");
		return APR_EGENERAL;
	}

	year[0] = 1900 + tm.tm_year;
	month[0] = tm.tm_mon+1;

	if (year[0] < 1 || month[0] < 1 || year[0] > 2030 || month[0] > 12)
	{
		gpmon_warning(FLINE, "invalid current month/year in check_partition %u/%u\n", month, year);
		return APR_EGENERAL;
	}

	if (month[0] < 11)
	{
		month[1] = month[0] + 1;
		month[2] = month[0] + 2;

		year[1] = year[0];
		year[2] = year[0];
	}
	else if (month[0] == 11)
	{
		month[1] = 12;
		month[2] = 1;

		year[1] = year[0];
		year[2] = year[0] + 1;
	}
	else 
	{
		month[1] = 1;
		month[2] = 2;

		year[1] = year[0] + 1;
		year[2] = year[0] + 1;
	}

	check_and_add_partition(conn, tbl, year[0], month[0], year[1], month[1]);
	check_and_add_partition(conn, tbl, year[1], month[1], year[2], month[2]);

	TR0(("check partitions on %s_history done\n", tbl));
	return APR_SUCCESS;
}

apr_status_t gpdb_harvest_healthdata()
{
	PGconn* conn = 0;
	PGresult* result = 0;
	const char* QRY = "insert into health_history select * from health_now;";
	const char* errmsg;
	apr_status_t res = APR_SUCCESS;

	errmsg = gpdb_exec(&conn, &result, QRY);
	if (errmsg)
	{
		res = 1;
		gpmon_warningx(FLINE, 0, "---- ARCHIVING HISTORICAL HEALTH DATA FAILED ---- on query %s with error %s\n", QRY, errmsg);
	}
	else
	{
		TR1(("load completed OK: health\n"));
	}

	PQclear(result);
	PQfinish(conn);
	return res;
}

static apr_status_t harvest(const char* tbl, apr_pool_t* pool, PGconn* conN)
{
	PGconn* conn = 0;
	PGresult* result = 0;
	const int QRYBUFSIZ = 1792;
	char qrybuf[QRYBUFSIZ];
	const char* QRYFMT = "insert into %s_history select * from _%s_tail;";
	const char* errmsg;
	apr_status_t res = APR_SUCCESS;

	if (strcmp(tbl, "iterators") == 0) { //this is to leave the cpu percentage out of iterators history
		const char* ITERQRYFMT = "insert into %s_history (ctime, tmid, ssid, ccnt, segid, pid, nid, pnid, hostname, ntype, nstatus, tstart, "
		"tduration, pmemsize, pmemmax, memsize, memresid, memshare, cpu_elapsed, cpu_currpct, phase, rows_out, rows_out_est, m0_name, m0_unit, m0_val, "
		"m0_est, m1_name, m1_unit, m1_val, m1_est, m2_name, m2_unit, m2_val, m2_est, m3_name, m3_unit, m3_val, m3_est, m4_name, m4_unit, "
		"m4_val, m4_est, m5_name, m5_unit, m5_val, m5_est, m6_name, m6_unit, m6_val, m6_est, m7_name, m7_unit, m7_val, m7_est, m8_name, "
		"m8_unit, m8_val, m8_est, m9_name, m9_unit, m9_val, m9_est, m10_name, m10_unit, m10_val, m10_est, m11_name, m11_unit, m11_val, "
		"m11_est, m12_name, m12_unit, m12_val, m12_est, m13_name, m13_unit, m13_val, m13_est, m14_name, m14_unit, m14_val, m14_est, m15_name, "
		"m15_unit, m15_val, m15_est, t0_name, t0_val) select ctime, tmid, ssid, ccnt, segid, pid, nid, pnid, hostname, ntype, nstatus, tstart, "
		"tduration, pmemsize, pmemmax, memsize, memresid, memshare, cpu_elapsed, 0, phase, rows_out, rows_out_est, m0_name, m0_unit, m0_val, "
		"m0_est, m1_name, m1_unit, m1_val, m1_est, m2_name, m2_unit, m2_val, m2_est, m3_name, m3_unit, m3_val, m3_est, m4_name, m4_unit, "
		"m4_val, m4_est, m5_name, m5_unit, m5_val, m5_est, m6_name, m6_unit, m6_val, m6_est, m7_name, m7_unit, m7_val, m7_est, m8_name, "
		"m8_unit, m8_val, m8_est, m9_name, m9_unit, m9_val, m9_est, m10_name, m10_unit, m10_val, m10_est, m11_name, m11_unit, m11_val, "
		"m11_est, m12_name, m12_unit, m12_val, m12_est, m13_name, m13_unit, m13_val, m13_est, m14_name, m14_unit, m14_val, m14_est, "
		"m15_name, m15_unit, m15_val, m15_est, t0_name, t0_val from _%s_tail;";

		snprintf(qrybuf, QRYBUFSIZ, ITERQRYFMT, tbl, tbl);
	}
	else {
		snprintf(qrybuf, QRYBUFSIZ, QRYFMT, tbl, tbl);
	}

	errmsg = gpdb_exec(&conn, &result, qrybuf);
	if (errmsg)
	{
		res = 1;
		gpmon_warningx(FLINE, 0, "---- HARVEST %s FAILED ---- on query %s with error %s\n", tbl, qrybuf, errmsg);
	}
	else
	{
		TR1(("load completed OK: %s\n", tbl));
	}

	PQclear(result);
	PQfinish(conn);
	return res;
}

/**
 * This function removes the not null constraint from the segid column so that we can set it to null when the segment aggregation flag is true
 */
apr_status_t remove_segid_constraint(void)
{
	PGconn* conn = 0;
	PGresult* result = 0;
	const char* ALTERSTR = "alter table iterators_history alter column segid drop not null;";
	const char* errmsg;
	apr_status_t res = APR_SUCCESS;

	errmsg = gpdb_exec(&conn, &result, ALTERSTR);
	if (errmsg) {
		res = 1;
		gpmon_warningx(FLINE, 0, "---- Alter FAILED ---- on command: %s with error %s\n", ALTERSTR, errmsg);
	} else {
		TR1(("remove_segid_constraint: alter completed OK\n"));
	}

	PQclear(result);
	PQfinish(conn);
	return res;
}

apr_status_t gpdb_harvest_one(const char* table)
{
	return harvest(table, NULL, NULL);
}



apr_status_t truncate_file(char* fn, apr_pool_t* pool)
{
	apr_file_t *fp = NULL;
	apr_status_t status;

	status = apr_file_open(&fp, fn, APR_WRITE|APR_CREATE|APR_TRUNCATE, APR_UREAD|APR_UWRITE, pool);

	if (status == APR_SUCCESS)
	{
		status = apr_file_trunc(fp, 0);
		apr_file_close(fp);
	}

	if (status != APR_SUCCESS)
	{
		gpmon_warningx(FLINE, 0, "harvest process truncate file %s failed", fn);
	}
	else
	{
		TR1(("harvest truncated file %s: ok\n", fn));
	}

	return status;
}


/* rename tail to stage */
static apr_status_t rename_tail_files(const char* tbl, apr_pool_t* pool, PGconn* conn)
{
	char srcfn[PATH_MAX];
	char dstfn[PATH_MAX];

	apr_status_t status;

	/* make the file names */
	snprintf(srcfn, PATH_MAX, "%s%s_tail.dat", GPMON_DIR, tbl);
	snprintf(dstfn, PATH_MAX, "%s%s_stage.dat", GPMON_DIR, tbl);

	status = apr_file_rename(srcfn, dstfn, pool);
	if (status != APR_SUCCESS)
	{
		gpmon_warningx(FLINE, status, "harvest failed renaming %s to %s", srcfn, dstfn);
		return status;
	}
	else
	{
		TR1(("harvest rename %s to %s success\n", srcfn, dstfn));
	}

	return status;
}

/* append stage data to _tail file */
static apr_status_t append_to_harvest(const char* tbl, apr_pool_t* pool, PGconn* conn)
{
	char srcfn[PATH_MAX];
	char dstfn[PATH_MAX];

	apr_status_t status;

	/* make the file names */
	snprintf(srcfn, PATH_MAX, "%s%s_stage.dat", GPMON_DIR, tbl);
	snprintf(dstfn, PATH_MAX, "%s_%s_tail.dat", GPMON_DIR, tbl);

	status = apr_file_append(srcfn, dstfn, APR_FILE_SOURCE_PERMS, pool);
	if (status != APR_SUCCESS)
	{
		gpmon_warningx(FLINE, status, "harvest failed appending %s to %s", srcfn, dstfn);
	}
	else
	{
		TR1(("harvest append %s to %s: ok\n", srcfn, dstfn));
	}

	return status;
}

typedef apr_status_t eachtablefunc(const char* tbl, apr_pool_t*, PGconn*);

char* all_tables[] = { "system", "queries", "iterators", "database", "segment", "filerep", "diskspace" };

apr_status_t call_for_each_table(eachtablefunc func, apr_pool_t* pool, PGconn* conn)
{ 
 	apr_status_t status = APR_SUCCESS;
	apr_status_t r;
	int num_tables = sizeof(all_tables) / sizeof (char*);
	int i;

	for (i = 0; i < num_tables; ++i)
	{
		r = func(all_tables[i], pool, conn);
		if (r != APR_SUCCESS)
		{
			status = r;
		}
	}
	
	return status;
}

/* rename tail files to stage files */
apr_status_t gpdb_rename_tail_files(apr_pool_t* pool)
{
	return call_for_each_table(rename_tail_files, pool, NULL);
}

/* copy data from stage files to harvest files */
apr_status_t gpdb_copy_stage_to_harvest_files(apr_pool_t* pool)
{
	return call_for_each_table(append_to_harvest, pool, NULL);
}

/* truncate _tail files */
apr_status_t empty_harvest_file(const char* tbl, apr_pool_t* pool, PGconn* conn)
{
	char fn[PATH_MAX];
	snprintf(fn, PATH_MAX, "%s_%s_tail.dat", GPMON_DIR, tbl);
	return truncate_file(fn, pool);
}

/* truncate tail files */
apr_status_t truncate_tail_file(const char* tbl, apr_pool_t* pool, PGconn* conn)
{
	char fn[PATH_MAX];
	snprintf(fn, PATH_MAX, "%s%s_tail.dat", GPMON_DIR, tbl);
	return truncate_file(fn, pool);
}

/* truncate _tail files to clear data already loaded into the DB */
apr_status_t gpdb_truncate_tail_files(apr_pool_t* pool)
{
	return call_for_each_table(truncate_tail_file, pool, NULL);
}

/* truncate _tail files to clear data already loaded into the DB */
apr_status_t gpdb_empty_harvest_files(apr_pool_t* pool)
{
	return call_for_each_table(empty_harvest_file, pool, NULL);
}

/* insert _tail data into history table */
apr_status_t gpdb_harvest(void)
{
	return call_for_each_table(harvest, NULL, NULL);
}

/* insert _tail data into history table */
apr_status_t gpdb_check_partitions(void)
{
	// health is not a full table and needs to be added to the list

	apr_status_t r1, r2, r3;

    // open a connection
    PGconn* conn = NULL;
	conn = PQconnectdb(GPDB_CONNECTION_STRING);

	if (PQstatus(conn) != CONNECTION_OK)
    {
		gpmon_warning(FLINE, "error creating GPDB client connection to dynamically check/create gpperfmon partitions: %s", 
            PQerrorMessage(conn));

        return APR_EINVAL;
    }

	r1 = check_partition("health", NULL, conn);

	r2 = check_partition("emcconnect", NULL, conn);
	
	r3 = call_for_each_table(check_partition, NULL, conn);

    // close connection
	PQfinish(conn);

	if (r1  != APR_SUCCESS)
		return r1;
    else if (r2 != APR_SUCCESS)
		return r2;
    else
        return r3;
}

