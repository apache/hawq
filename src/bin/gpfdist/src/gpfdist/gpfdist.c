/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifdef WIN32
/* exclude transformation features on windows for now */
#undef GPFXDIST
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0501
#endif
#endif

#include <apr_getopt.h>
#include <apr_env.h>
#include <apr_file_info.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_strings.h>
#include <apr_time.h>
#include <event.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#ifdef GPFXDIST
#include <regex.h>
#include <gpfxdist.h>
#endif
#include <fstream/fstream.h>

#ifndef WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#define SOCKET int
#ifndef closesocket
#define closesocket(x)   close(x)
#endif
#else
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <io.h>
#define SHUT_WR SD_SEND
#define SHUT_RD SD_RECEIVE
#define socklen_t int
#ifndef ECONNRESET
#define ECONNRESET   WSAECONNRESET
#endif

#endif

#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/err.h>

/*  A data block */
typedef struct blockhdr_t blockhdr_t;
struct blockhdr_t
{
	char 	hbyte[293];
	int 	hbot, htop;
};

/*
 * Data that is sent from server to client
 */
typedef struct block_t block_t;
struct block_t
{
	blockhdr_t 	hdr;
	int 		bot, top;
	char*      	data;
};

#define FLINE		__FILE__":"APR_STRINGIFY(__LINE__)

int gprint(const char* fmt, ...);
void gfatal(const char* fline, const char* fmt, ...);
int gwarning(const char* fline, const char* fmt, ...);

static void base16_decode(char* data);

/* SSL additions */
#define SSL_RENEGOTIATE_TIMEOUT_SEC	(600) /* 10 minutes */
const char* const CertificateFilename = "server.crt";
const char* const PrivateKeyFilename = "server.key";
const char* const TrustedCaFilename = "root.crt";
static SSL_CTX *initialize_ctx(void);
static void handle_ssl_error(SOCKET sock, BIO *sbio, SSL *ssl);
static void flush_ssl_buffer(int fd, short event, void* arg);
/* SSL end */

/**************

 NOTE on GP_PROTO
 ================
 When a gpdb segment connects to gpfdist, it provides the following parameters:
 X-GP-XID   - transaction ID
 X-GP-CID   - command ID
 X-GP-SN    - session ID
 X-GP-PROTO - protocol number

 X-GP-PROTO = 0 or not provided:
 return the content of the file without any kind of meta info

 X-GP-PROTO = 1
 each data block is tagged by meta info like this:
 byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 byte 1-4: length. # bytes of following data block. in network-order.
 byte 5-X: the block itself.

 The stream is terminated by a Data block of length 0. If the stream is
 not property terminated, then gpfdist encountered some error, and caller
 should check the gpfdist error log.

 **************/

typedef struct gnet_request_t gnet_request_t;
struct gnet_request_t
{
	int 	argc;
	char** 	argv;
	int 	hc;
	char* 	hname[50];
	char* 	hvalue[50];
};

static gnet_request_t* gnet_parse_request(const char* buf, int* len,
										  apr_pool_t* pool);
static char* gstring_trim(char* s);
static void percent_encoding_to_char(char* p, char* pp, char* path);

#ifndef GP_VERSION
#define GP_VERSION unknown
#endif

#define GP_VERSIONX APR_STRINGIFY(GP_VERSION)

/* CR-2723 */
#define GPFDIST_MAX_LINE_LOWER_LIMIT (32*1024)
#ifdef GPFXDIST
#define GPFDIST_MAX_LINE_UPPER_LIMIT (256*1024*1024)
#define GPFDIST_MAX_LINE_MESSAGE     "Error: -m max row length must be between 32KB and 256MB"
#else
#define GPFDIST_MAX_LINE_UPPER_LIMIT (1024*1024)
#define GPFDIST_MAX_LINE_MESSAGE     "Error: -m max row length must be between 32KB and 1MB"
#endif


/*	Struct of command line options */
static struct
{
	int			p; /* port */
	int			last_port;
	int			v; /* verbose */
	int			V; /* very verbose */
	const char* d; /* directory */
	const char* l; /* log filename */
	const char*	f; /* forced filename */
	int			g; /* gp_proto (0 or 1) (internal, not documented) */
	int			t; /* timeout in seconds */
	const char *b; /* IP address to bind (internal, not documented) */
	int			m; /* max data line len */
	int			S; /* use O_SYNC when opening files for write  */
	int			z; /* listen queue size (hidden option currently for debuging) */
	const char* c; /* config file */
	struct transform* trlist; /* transforms from config file */
	const char* ssl; /* path to certificates in case we use gpfdist with ssl */
	int 		sslclean; /* Defines the time to wait [sec] untill cleanup the SSL resources (internal, not documented) */
} opt = { 8080, 8080, 0, 0, ".", 0, 0, -1, 5, 0, 32768, 0, 256, 0, 0, 0, 5 };

#if APR_IS_BIGENDIAN
#define local_htonll(n)  (n)
#define local_ntohll(n)  (n)
#else
#define local_htonll(n)  ((((apr_uint64_t) htonl(n)) << 32LL) | htonl((n) >> 32LL))
#define local_ntohll(n)  ((((apr_uint64_t) ntohl(n)) << 32LL) | (apr_uint32_t) ntohl(((apr_uint64_t)n) >> 32LL))
#endif

/* if VERBOSE mode, print log information */
#define TR(x)  while (opt.V){printf x;break;}

/*
 * block_fill_header
 *
 * Prepare a block header for sending to the client. It includes various meta
 * data information such as filename, initial linenumber, etc. This will only
 * get used in PROTO-1. We store this header in block_t->hdr (a blockhdr_t)
 * and PROTO-0 never uses it.
 */
static void block_fill_header(block_t* b,
							  const struct fstream_filename_and_offset* fos)
{
	blockhdr_t*		h = &b->hdr;
	apr_int32_t 	len;
	apr_int64_t 	len8;
	char*			p = h->hbyte;
	int 			fname_len = strlen(fos->fname);

	h->hbot = 0;

	/* FILENAME: 'F' + len + fname */
	*p++ = 'F';
	len = htonl(fname_len);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, fos->fname, fname_len);
	p += fname_len;
	TR(("F %u %s\n", (unsigned int)ntohl(len), fos->fname));

	/* OFFSET: 'O' + len + foff */
	*p++ = 'O';
	len = htonl(8);
	len8 = local_htonll(fos->foff);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len8, 8);
	p += 8;
	TR(("O %llu\n",(unsigned long long) local_ntohll(len8)));

	/* LINENUMBER: 'L' + len + linenumber */
	*p++ = 'L';
	len8 = local_htonll(fos->line_number);
	memcpy(p, &len, 4);
	p += 4;
	memcpy(p, &len8, 8);
	p += 8;
	TR(("L %llu\n", (unsigned long long)local_ntohll(len8)));

	/* DATA: 'D' + len */
	*p++ = 'D';
	len = htonl(b->top-b->bot);
	memcpy(p, &len, 4);
	p += 4;
	TR(("D %u\n", (unsigned int)ntohl(len)));
	h->htop = p - h->hbyte;
	if (h->htop > sizeof(h->hbyte))
		gfatal(FLINE, "assert failed, h->htop = %d, max = %d\n", h->htop,
				sizeof(h->hbyte));
	TR(("header size: %d\n",h->htop-h->hbot));
}

/*  Global constrol block */
static struct
{
	apr_pool_t* 	pool;
	int				listen_sock_count;
	SOCKET 			listen_socks[6];
	struct event 	listen_events[6];
	struct
	{
		int 		gen;
		apr_hash_t* tab;
	} session;
	apr_int64_t 	read_bytes;
	apr_int64_t 	total_bytes;
	int 			total_sessions;
	BIO 			*bio_err;	/* for SSL */
	SSL_CTX 		*server_ctx;/* for SSL */
} gcb;

/*  A session */
typedef struct session_t session_t;
struct session_t
{
	apr_pool_t* 	pool;
	const char* 	key;
	const char* 	tid;
	const char* 	path;			/* path requested */
	fstream_t* 		fstream;
	int 			is_error;		/* error flag */
	int 			nrequest;		/* # requests attached to this session */
	int				is_get;     	/* true for GET, false for POST */
	int*			active_segids;	/* array indexed by segid. used for write operations
									   to indicate which segdbs are writing and when each
									   is done (sent a final request) */
	int				maxsegs; 		/* same as request->totalsegs. length of active_segs arr */
	apr_time_t		mtime; 			/* time when nrequest was modified */
};

/*  An http request */
typedef struct request_t request_t;
struct request_t
{
	SOCKET 			sock; 		/* the socket */
	apr_pool_t* 	pool; 		/* memory pool container */
	struct timeval 	tm; 		/* timeout for struct event */
	struct event 	ev; 		/* event we are watching for this request */
	const char* 	peer; 		/* peer IP:port string */
	const char* 	path; 		/* path to file */
	const char* 	tid; 		/* transaction id */
	const char* 	csvopt; 	/* options for csv file */

#ifdef GPFXDIST
	struct
	{
		const char* name;		/* requested transformation */
		char*		command;	/* command associated with transform */
		int			paths;		/* 1 if filename passed to transform should contain paths to data files */
		const char* errfilename; /* name of temporary file holding stderr to send to server */
		apr_file_t* errfile;	/* temporary file holding stderr to send to server */
	} trans;
#endif

	session_t* 		session; 	/* the session this request is attached to */
	int 			gp_proto; 	/* the protocol to use, sent from client */
	int				is_get;     /* true for GET, false for POST */
	int				is_final;	/* the final POST request. a signal from client to end session */
	int				segid;		/* the segment id of the segdb with the request */
	int				totalsegs;	/* the total number of segdbs */

	struct
	{
		char* 	hbuf; 		/* buffer for raw incoming HTTP request */
		int 	hbuftop; 	/* # bytes used in hbuf */
		int 	hbufmax; 	/* size of hbuf[] */
		gnet_request_t* req; /* a parsed HTTP request, NULL if still incomplete. */
		int		davailable;	/* number of data bytes available to consume */
		char*	dbuf;		/* buffer for raw data from a POST request */
		int 	dbuftop; 	/* # bytes used in dbuf */
		int 	dbufmax; 	/* size of dbuf[] */
	} in;

	block_t	outblock;	/* next block to send out */
	char*           line_delim_str;
	int             line_delim_length;
	
	/* SSL related */
	BIO			*io;		/* for the i.o. */
	BIO			*sbio;		/* for the accept */
	BIO			*ssl_bio;
	SSL			*ssl;
};

/* send gp-proto==1 ctl info */
static void gp1_send_eof(request_t* r);
static void gp1_send_errmsg(request_t* r, const char* msg);
#ifdef GPFXDIST
static void gp1_send_errfile(request_t* r, apr_file_t* errfile);
#endif

static int setup_read(request_t* r);
static int setup_write(request_t* r);
static int session_attach(request_t* r);
static void session_detach(request_t* r);
static void session_end(session_t* s, int error);
static void session_free(session_t* s);
static void session_active_segs_dump(session_t* session);
static int session_active_segs_isempty(session_t* session);
static int request_validate(request_t *r);
static int request_set_path(request_t *r, const char* d, char* p, char* pp, char* path);
static int request_parse_gp_headers(request_t *r, int opt_g);
#ifdef GPFXDIST
static int request_set_transform(request_t *r);
#endif
static void handle_post_request(request_t *r, int header_end);
static void handle_get_request(request_t *r);

static int gpfdist_socket_send(const request_t *r, const void *buf, const size_t buflen);
static int gpfdist_SSL_send(const request_t *r, const void *buf, const size_t buflen);
static int (*gpfdist_send)(const request_t *r, const void *buf, const size_t buflen); /* function pointer */
static int gpfdist_socket_receive(const request_t *r, void *buf, const size_t buflen);
static int gpfdist_SSL_receive(const request_t *r, void *buf, const size_t buflen);
static int (*gpfdist_receive)(const request_t *r, void *buf, const size_t buflen); /* function pointer */
static void free_SSL_resources(const request_t *r);
static void setup_flush_ssl_buffer(request_t* r);
static void request_cleanup(request_t *r);
static void request_cleanup_and_free_SSL_resources(int fd, short event, void* arg);

/* Print usage */
static void usage_error(const char* msg, int print_usage)
{
	if (print_usage)
	{
		char* GPHOME = 0;
		FILE* fp = 0;

		if (gcb.pool && apr_env_get(&GPHOME, "GPHOME", gcb.pool))
			GPHOME = 0;

		if (GPHOME)
		{
			char* path = apr_psprintf(gcb.pool, "%s/docs/cli_help/gpfdist_help",
					GPHOME);
			if (path)
				fp = fopen(path, "r");
		}

		if (fp)
		{
			int i;
			while ((i = getc(fp)) != EOF)
				putchar(i);
			fclose(fp);
		}
		else
		{
			fprintf(stderr,
					"gpfdist -- file distribution web server\n\n"
						"usage: gpfdist [--ssl <certificates_directory>] [-d <directory>] [-p <http(s)_port>] [-l <log_file>] [-t <timeout>] [-v | -V] [-m <maxlen>]"
#ifdef GPFXDIST
					    "[-c file]"
#endif
					    "\n\n"
						"       gpfdist [-? | --help] | --version\n\n"
						"        -?, --help : print this screen\n"
						"        -v         : verbose mode\n"
						"        -V         : more verbose\n"
						"        -p port    : port to serve HTTP(S), default is 8080\n"
						"        -d dir     : serve files under the specified directory,  default is '.'\n"
						"        -l logfn   : log filename\n"
						"        -t tm      : timeout in seconds \n"
						"        -m maxlen  : max data row length expected, in bytes. default is 32768\n"
						"        --ssl dir  : start HTTPS server. Use the certificates from the specified directory\n"
#ifdef GPFXDIST
					    "        -c file    : configuration file for transformations\n"
#endif
						"        --version  : print version information\n\n");
		}
	}

	if (msg)
		fprintf(stderr, "%s\n", msg);

	exit(msg ? 1 : 0);
}

static void print_version(void)
{
	printf("gpfdist version \"%s\"\n", GP_VERSIONX);
	exit(0);
}

static void print_q_x_h_are_gone(void)
{
	fprintf(stderr, "The -q, -h and -x options are gone.  Please specify these as in this example:\n");
	fprintf(stderr, "create external table a (a int) location ('gpfdist://...') format 'csv' (escape as '\"' quote as '\"' header);\n");
	exit(1);
}

/* Parse command line */
static void parse_command_line(int argc, const char* const argv[],
		apr_pool_t* pool)
{
	apr_getopt_t* 	os;
	const char* 	arg;
	char 			apr_errbuf[256];
	int				status;
	int 			ch;
	int 			e;

	char* current_directory = NULL;

	static const apr_getopt_option_t option[] =
	{
	/* long-option, short-option, has-arg flag, description */
	{ "help", '?', 0, "print help screen" },
	{ NULL, 'V', 0, "very verbose" },
	{ NULL, 'v', 0, "verbose mode" },
	{ NULL, 'p', 1, "which port to serve HTTP(S)" },
	{ NULL, 'P', 1, "last port of range of ports to serve HTTP(S)" },
	{ NULL, 'd', 1, "serve files under this directory" },
	{ NULL, 'f', 1, "internal - force file to be given file name" },
	{ NULL, 'b', 1, "internal - bind to ip4 address" },
	{ NULL, 'q', 0, "gone" },
	{ NULL, 'x', 0, "gone" },
	{ NULL, 'h', 0, "gone" },
	{ NULL, 'l', 1, "log filename" },
	{ NULL, 't', 5, "timeout in seconds" },
	{ NULL, 'g', 1, "internal - gp_proto number" },
	{ NULL, 'm', 1, "max data row length expected" },
	{ NULL, 'S', 0, "use O_SYNC when opening files for write" },
	{ NULL, 'z', 1, "internal - queue size for listen call" },
	{ "ssl", 257, 1, "ssl - certificates files under this directory" },
	{ "sslclean", 258, 1, "Defines the time to wait [sec] untill cleanup the SSL resources" },
#ifdef GPFXDIST
	{ NULL, 'c', 1, "transform configuration file" },
#endif
	{ "version", 256, 0, "print version number" },
	{ 0 } };

	status = apr_getopt_init(&os, pool, argc, argv);

	if (status != APR_SUCCESS)
		gfatal(FLINE, "apt_getopt_init failed: %s",
			   apr_strerror(status, apr_errbuf, sizeof(apr_errbuf)));

	while (APR_SUCCESS == (e = apr_getopt_long(os, option, &ch, &arg)))
	{
		switch (ch)
		{
		case '?':
			usage_error(0, 1);
			break;
		case 'v':
			opt.v = 1;
			break;
		case 'V':
			opt.v = opt.V = 1;
			break;
		case 'h':
			print_q_x_h_are_gone();
			break;
		case 'd':
			opt.d = arg;
			break;
		case 'f':
			opt.f = arg;
			break;
		case 'q':
			print_q_x_h_are_gone();
			break;
		case 'x':
			print_q_x_h_are_gone();
			break;
		case 'p':
			opt.last_port = opt.p = atoi(arg);
			break;
		case 'P':
			opt.last_port = atoi(arg);
			break;
		case 'l':
			opt.l = arg;
			break;
		case 't':
			opt.t = atoi(arg);
			break;
		case 'g':
			opt.g = atoi(arg);
			break;
		case 'b':
			opt.b = arg;
			break;
		case 'm':
			opt.m = atoi(arg);
			break;
		case 'S':
			opt.S = 1;
			break;
		case 'z':
			opt.z = atoi(arg);
			break;
		case 'c':
			opt.c = arg;
			break;
		case 257:
			opt.ssl = arg;
			break;
		case 258:
			opt.sslclean = atoi(arg);
			break;
		case 256:
			print_version();
			break;
		}
	}

	if (e != APR_EOF)
		usage_error("Error: illegal arguments", 1);

	if (!(0 < opt.p && opt.p < (1 << 16)))
		usage_error("Error: please specify a valid port number for -p switch", 0);

	if (-1 != opt.g)
	{
		if (!(0 == opt.g || 1 == opt.g))
			usage_error("Error: please specify 0 or 1 for -g switch (note: this is internal)", 0);
	}

	if (! ((2 <= opt.t && opt.t <= 30) || opt.t == 0))
		usage_error("Error: -t timeout must be between 2 and 30, or 0 for no timeout", 0);

	/* validate max row length */
    if (! ((GPFDIST_MAX_LINE_LOWER_LIMIT <= opt.m) && (opt.m <= GPFDIST_MAX_LINE_UPPER_LIMIT)))
    	usage_error(GPFDIST_MAX_LINE_MESSAGE, 0);

    if (! (16 <= opt.z && opt.t <= 512) )
		usage_error("Error: -z listen queue size must be between 16 and 512 (default is 256)", 0);

    /* get current directory, for ssl directory validation */
    if (0 != apr_filepath_get(&current_directory, APR_FILEPATH_NATIVE, pool))
		usage_error(apr_psprintf(pool, "Error: cannot access directory '.'\n"
									   "Please run gpfdist from a different location"), 0);

	/* validate opt.d */
	{
		char* p = gstring_trim(apr_pstrdup(pool, opt.d));

		/* collapse // */
		while (p[0] == '/' && p[1] == '/')
			p++;

		/* disallow / */
		if (0 == strcmp(p, "/"))
			usage_error("Security Error:  You cannot specify the root"
						" directory (/) as the source files directory.", 0);

		/* strip ending / */
		while (p[0] && p[strlen(p) - 1] == '/')
			p[strlen(p) - 1] = 0;
		opt.d = p;

		if (0 == strlen(opt.d))
			opt.d = ".";

		/* check that the dir exists */
		if (0 != apr_filepath_set(opt.d, pool))
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
				"Please specify a valid directory for -d switch", opt.d), 0);

		if (0 != apr_filepath_get(&p, APR_FILEPATH_NATIVE, pool))
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
				"Please specify a valid directory for -d switch", opt.d), 0);
		opt.d = p;
	}

	/* validate opt.l */
	if (opt.l)
	{
		FILE *f;

		char* p = gstring_trim(apr_pstrdup(pool, opt.l));

		/* collapse // */
		while (p[0] == '/' && p[1] == '/')
			p++;

		/* disallow / */
		if (0 == strcmp(p, "/"))
			usage_error("Security Error: You cannot specify the root"
						" directory (/) as the log file directory.", 0);

		/* strip ending / */
		while (p[0] && p[strlen(p) - 1] == '/')
			p[strlen(p) - 1] = 0;
		opt.l = p;

		if (0 == strlen(opt.l))
			opt.l = ".";

		/* check that the file exists */
		f = fopen(opt.l, "a");
		if (!f)
		{
			fprintf(stderr, "unable to create log file %s: %s\n",
					opt.l, strerror(errno));
			exit(1);
		}
		fclose(f);

	}

	/* validate opt.ssl */
	if (opt.ssl)
	{
		char* p = gstring_trim(apr_pstrdup(pool, opt.ssl));

		/* collapse // */
		while (p[0] == '/' && p[1] == '/')
			p++;

		/* disallow / */
		if (0 == strcmp(p, "/"))
			usage_error("Security Error: You cannot specify the root" 
						" directory (/) as the certificates directory", 0);

		/* strip ending / */
		while (p[0] && p[strlen(p) - 1] == '/')
			p[strlen(p) - 1] = 0;
		opt.ssl = p;

		/* change current directory to original one (after -d changed it) */
		if (0 != apr_filepath_set(current_directory, pool))
				usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
											   "Please run gpfdist from a different location", current_directory), 0);
		/* check that the dir exists */
		if ( (0 != apr_filepath_set(opt.ssl, pool)) || (0 != apr_filepath_get(&p, APR_FILEPATH_NATIVE, pool)) )
			usage_error(apr_psprintf(pool, "Error: cannot access directory '%s'\n"
				"Please specify a valid directory for --ssl switch", opt.ssl), 0);
		opt.ssl = p;
	}

	/* Validate opt.sslclean*/
	if ( (opt.sslclean < 0) || (opt.sslclean > 300) )
		usage_error("Error: -sslclean timeout must be between 0 and 300 [sec] (default is 5[sec])", 0);

#ifdef GPFXDIST
    /* validate opt.c */
    if (opt.c)
    {
        extern int transform_config(const char* filename, struct transform** trlistp, int verbose);

		if (transform_config(opt.c, &opt.trlist, opt.V))
		{
			/* transform_config has already printed a message to stderr on failure */
			exit(1);
        }
    }
#endif

	/* there should not be any more args left */
	if (os->ind != argc)
		usage_error("Error: illegal arguments", 1);
}

/* http error codes used by gpfdist */
#define FDIST_OK 200
#define FDIST_BAD_REQUEST 400
#define FDIST_INTERNAL_ERROR 500

/* send an error response */
static void http_error(request_t* r, int code, const char* msg)
{
	char buf[1024];
	int n;
	gprint("%s - %d %s\n", r->peer, code, msg);
	n = apr_snprintf(buf, sizeof(buf), "HTTP/1.0 %d %s\r\n"
		"Content-length: 0\r\n"
		"Expires: 0\r\n"
		"X-GPFDIST-VERSION: " GP_VERSIONX "\r\n"
		"Cache-Control: no-cache\r\n"
		"Connection: close\r\n\r\n", code, msg);

	gpfdist_send(r, buf, n);
}

/* send an empty response */
static void http_empty(request_t* r)
{
	static const char buf[] = "HTTP/1.0 200 ok\r\n"
		"Content-type: text/plain\r\n"
		"Content-length: 0\r\n"
		"Expires: 0\r\n"
		"X-GPFDIST-VERSION: " GP_VERSIONX "\r\n"
		"Cache-Control: no-cache\r\n"
		"Connection: close\r\n\r\n";
	gprint("%s %s %s - OK\n", r->peer, r->in.req->argv[0], r->in.req->argv[1]);
	gpfdist_send(r, buf, sizeof buf -1);
}

/* send a Continue response */
static void http_continue(request_t* r)
{
	static const char buf[] = "HTTP/1.1 100 Continue\r\n\r\n";

	gprint("%s %s %s - Continue\n", r->peer, r->in.req->argv[0], r->in.req->argv[1]);

	gpfdist_send(r, buf, sizeof buf -1);
}


/* send an OK response */
static apr_status_t http_ok(request_t* r)
{
	const char* fmt = "HTTP/1.0 200 ok\r\n"
		"Content-type: text/plain\r\n"
		"Expires: 0\r\n"
		"X-GPFDIST-VERSION: " GP_VERSIONX "\r\n"
		"X-GP-PROTO: %d\r\n"
		"Cache-Control: no-cache\r\n"
		"Connection: close\r\n\r\n";
	char buf[1024];
	int m, n;

	n = apr_snprintf(buf, sizeof(buf), fmt, r->gp_proto);
	if (n >= sizeof(buf) - 1)
		gfatal(FLINE, "internal error - buffer overflow during http_ok");

	m = gpfdist_send(r, buf, n);
	if (m != n)
	{
		gprint("%s - socket error\n", r->peer);
		return APR_EGENERAL;
	}
	gprint("%s %s %s - OK\n", r->peer, r->in.req->argv[0], r->in.req->argv[1]);

	return 0;
}

/*
 * send_gpfdist_status
 *
 * send some server status back to the client. This is a debug utility and is
 * not normally used in normal production environment unless triggered for
 * debugging purposes. For more information see do_read, search for
 * 'gpfdist/status'.
 */
static apr_status_t send_gpfdist_status(request_t* r)
{
	char	buf[1024];
	int 	m, n;

	n = apr_snprintf(buf, sizeof(buf),	"HTTP/1.0 200 ok\r\n"
										"Content-type: text/plain\r\n"
										"Expires: 0\r\n"
										"X-GPFDIST-VERSION: " GP_VERSIONX "\r\n"
										"Cache-Control: no-cache\r\n"
										"Connection: close\r\n\r\n"
										"read_bytes %"APR_INT64_T_FMT"\r\n"
										"total_bytes %"APR_INT64_T_FMT"\r\n"
										"total_sessions %d\r\n",
										gcb.read_bytes,
										gcb.total_bytes,
										gcb.total_sessions);

	if (n >= sizeof buf - 1)
		gfatal(FLINE, "internal error - buffer overflow during send_gpfdist_status");

	m = gpfdist_send(r, buf, n);

	if (m != n)
	{
		gprint("%s - socket error\n", r->peer);
		return APR_EGENERAL;
	}

	return 0;
}

/*
 * request_end
 *
 * Finished a request. Close socket and cleanup.
 * Note: ending a request does not mean that the session is ended.
 * Maybe there is a request out there (of the same session), that still
 * has a block to send out.
 */
static void request_end(request_t* r, int error, const char* errmsg)
{
	session_t* s = r->session;

#ifdef GPFXDIST
	if (r->trans.errfile)
	{
		apr_status_t rv;

		/*
		 * close and then re-open (for reading) the temporary file we've used to capture stderr
		 */
		apr_file_flush(r->trans.errfile);
		apr_file_close(r->trans.errfile);
		TR(("[%d] request closed stderr file %s\n", r->sock, r->trans.errfilename));

		/*
		 * send the first 8K of stderr to the server
		 */
		if ((rv = apr_file_open(&r->trans.errfile, r->trans.errfilename, APR_READ|APR_BUFFERED, APR_UREAD, r->pool)) == APR_SUCCESS)
		{
			gp1_send_errfile(r, r->trans.errfile);
			apr_file_close(r->trans.errfile);
		}

		/*
		 * remove the temp file
		 */
		apr_file_remove(r->trans.errfilename, r->pool);
		TR(("[%d] request removed stderr file %s\n", r->sock, r->trans.errfilename));

		r->trans.errfile = NULL;
	}

#endif

	if (r->gp_proto == 1)
	{
		if (!error)
			gp1_send_eof(r);
		else if (errmsg)
			gp1_send_errmsg(r, errmsg);
	}

	TR(("[%d] request end\n", r->sock));

	/* If we still have a block outstanding, the session is corrupted. */
	if (r->outblock.top != r->outblock.bot)
	{
		gwarning(FLINE, "request failure resulting in session failure");
		if (s)
			session_end(s, 1);
	}
	else
	{
		/* detach this request from its session */
		session_detach(r);
	}

	/* If we still have data in the buffer - flush it */
	if ( opt.ssl )
	{
		flush_ssl_buffer(r->sock, 0, r);
	}
	else
	{
		request_cleanup(r);
	}
}

static int local_send(request_t *r, const char* buf, int buflen)
{
	int n = gpfdist_send(r, buf, buflen);

	if (n < 0)
	{
#ifdef WIN32
		int e = WSAGetLastError();
		int ok = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
		int e = errno;
		int ok = (e == EINTR || e == EAGAIN);
#endif
		if ( e == EPIPE )
		{
			gprint(FLINE,"gpfdist_send failed - the connection was terminated by the client (%d)\n",e);
		}
		return ok ? 0 : -1;
	}

	return n;
}

static int local_sendall(request_t* r, const char* buf, int buflen)
{
	int oldlen = buflen;

	while (buflen)
	{
		int i = local_send(r, buf, buflen);

		if (i < 0)
			return i;

		buf += i;
		buflen -= i;
	}

	return oldlen;
}

/*
 * gp1_send_header
 *
 * In PROTO-1 we can send all kinds of data blocks to the client. each data
 * block is tagged by meta info like this:
 * byte 0: type (can be 'F'ilename, 'O'ffset, 'D'ata, 'E'rror, 'L'inenumber)
 * byte 1-4: length. # bytes of following data block. in network-order.
 * byte 5-X: the block itself.
 *
 * this function creates and sends the meta info according to the passed in
 * arguments. It does not send the block itself (bytes 5-X).
 */
static int
gp1_send_header(request_t*r, char letter, int length)
{
	char 		hdr[5];
	const char*	p = hdr;

	hdr[0] = letter;
	length = htonl(length);

	memcpy(hdr + 1, &length, 4);

	return local_sendall(r, p, 5) < 0 ? -1 : 0;
}

/*
 * Send a message to the client to indicate EOF - no more data. This is done
 * by sending a 'D' message type (Data) with length 0.
 */
static void
gp1_send_eof(request_t* r)
{
	if (!gp1_send_header(r, 'D', 0))
		TR(("[%d] sent EOF\n", r->sock));
}

/*
 * Send an error message to the client, using the 'E' message type.
 */
static void 
gp1_send_errmsg(request_t* r, const char* errmsg)
{
	apr_int32_t len = strlen(errmsg);
	if (!gp1_send_header(r, 'E', len))
		local_sendall(r, errmsg, len);
}

#ifdef GPFXDIST
/*
 * Send the first 8k of the specified file as an error message
 */
static void gp1_send_errfile(request_t* r, apr_file_t* errfile)
{
	char         buf[8192];
	apr_size_t   nbytes = sizeof(buf);
	apr_status_t rv;

	if ((rv = apr_file_read(errfile, buf, &nbytes)) == APR_SUCCESS)
	{
		if (nbytes > 0)
		{
			if (! gp1_send_header(r, 'E', nbytes))
			{
				local_sendall(r, buf, nbytes);
				TR(("[%d] request sent %"APR_SIZE_T_FMT" stderr bytes to server\n", r->sock, nbytes));
			}
		}
	}
}
#endif

/*
 * session_get_block
 *
 * Get a block out of the session. return error string. This includes a block
 * header (metadata for client such as filename, etc) and the data itself.
 */
static const char*
session_get_block(session_t* session, block_t* retblock, char* line_delim_str, int line_delim_length)
{
	int 		size;
	const int 	whole_rows = 1; /* gpfdist must not read data with partial rows */
	struct fstream_filename_and_offset fos;

	retblock->bot = retblock->top = 0;

	if (session->is_error || 0 == session->fstream)
	{
		session_end(session, 0);
		return 0;
	}

	gcb.read_bytes -= fstream_get_compressed_position(session->fstream);

	/* read data from our filestream as a chunk with whole data rows */
	
	size = fstream_read(session->fstream, retblock->data, opt.m, &fos, whole_rows, line_delim_str, line_delim_length);

	if (size == 0)
	{
		gcb.read_bytes += fstream_get_compressed_size(session->fstream);
		session_end(session, 0);
		return 0;
	}

	gcb.read_bytes += fstream_get_compressed_position(session->fstream);

	if (size < 0)
	{
		const char* ferror = fstream_get_error(session->fstream);
		session_end(session, 1);
		return ferror;
	}

	retblock->top = size;

	/* fill the block header with meta data for the client to parse and use */
	block_fill_header(retblock, &fos);

	return 0;
}

/* finish the session - close the file */
static void session_end(session_t* session, int error)
{
	if (error)
		session->is_error = error;

	if (session->fstream)
	{
		fstream_close(session->fstream);
		session->fstream = 0;
	}
}

/* deallocate session, remove from hashtable */
static void session_free(session_t* session)
{
	gprint("free session %s\n", session->key);

	if (session->fstream)
	{
		fstream_close(session->fstream);
		session->fstream = 0;
	}

	apr_hash_set(gcb.session.tab, session->key, APR_HASH_KEY_STRING, 0);
	apr_pool_destroy(session->pool);
}

/* detach a request from a session */
static void session_detach(request_t* r)
{
	session_t* session = r->session;

	r->session = 0;

	if (session)
	{
		if (session->nrequest <= 0)
			gfatal(FLINE, "internal error - detaching a request from an empty session");

		session->nrequest--;
		session->mtime = apr_time_now();

		/* for auto-tid sessions, we can free it now */
		if (0 == strncmp("auto-tid.", session->tid, 9))
		{
			if (session->nrequest != 0)
				gfatal(FLINE, "internal error - expected an empty auto-tid session"
							  "but saw %d requests", session->nrequest);

			session_free(session);
		}
		else if(!session->is_get &&
				session->nrequest == 0 &&
				session_active_segs_isempty(session))
		{
			/*
			 * free the session if this is a POST request and it's
			 * the last request for this session (we can tell is all
			 * segments sent a "done" request by calling session_active_isempty.
			 * (nrequest == 0 test isn't sufficient by itself).
			 *
			 * this is needed in order to make sure to close the out file
			 * when we're done writing. (only in write operations, not in read).
			 */
			session_free(session);
		}
	}
}

static void sessions_cleanup(void)
{
	apr_hash_index_t*	hi;
	int 				i, n = 0;
	void* 				entry;
	session_t**			session;
	session_t*			s;
	int 				numses;

	numses = apr_hash_count(gcb.session.tab);

	if (numses == 0)
		return;

	if (!(session = malloc(sizeof(session_t *) * numses)))
		gfatal(FLINE, "out of memory in sessions_cleanup");

	hi = apr_hash_first(gcb.pool, gcb.session.tab);

	for (i = 0; hi && i < numses; i++, hi = apr_hash_next(hi))
	{
		apr_hash_this(hi, 0, 0, &entry);
		s = (session_t*) entry;

		if (s->nrequest == 0 && (s->mtime < apr_time_now() - 300
				* APR_USEC_PER_SEC))
		{
			session[n++] = s;
		}
	}

	for (i = 0; i < n; i++)
	{
		gprint("remove out-dated session %s\n", session[i]->key);
		session_free(session[i]);
		session[i] = 0;
	}

	free(session);
}

/*
 * session_attach
 *
 * attach a request to a session (create the session if not already exists).
 */
static int session_attach(request_t* r)
{
	char key[1024];
	session_t* session = 0;

	/*
	 * create the session key (tid:path)
	 */
	if (sizeof(key) - 1 == apr_snprintf(key, sizeof(key), "%s:%s",
										r->tid, r->path))
	{
		http_error(r, FDIST_BAD_REQUEST, "path too long");
		request_end(r, 1, 0);
		return -1;
	}


	/* check if such session already exists in hashtable */
	session = apr_hash_get(gcb.session.tab, key, APR_HASH_KEY_STRING);

	if (!session)
	{
		/* not in hashtable - create new session */

		fstream_t* 	fstream = 0;
		apr_pool_t* pool;
		int 		response_code;
		const char*	response_string;
		struct fstream_options fstream_options;

		/* remove any outdated sessions*/
		sessions_cleanup();

		/*
		 * this is the special WET "session-end" request. Another similar
		 * request must have already came in from another segdb and finished
		 * the session we were at. we don't want to create a new session now,
		 * so just exit instead
		 */
		if (r->is_final)
		{
			gprint("got a final write request. skipping session creation\n");
			http_empty(r);
			request_end(r, 0, 0);
			return -1;
		}

		if (apr_pool_create(&pool, gcb.pool))
		{
			gwarning(FLINE, "out of memory");
			http_error(r, FDIST_INTERNAL_ERROR, "internal error - out of memory");
			request_end(r, 1, 0);
			return -1;
		}

		/* parse csvopt header */
		memset(&fstream_options, 0, sizeof fstream_options);
		fstream_options.verbose = opt.v;
		fstream_options.bufsize = opt.m;

		{
			int quote = 0;
			int escape = 0;

			sscanf(r->csvopt, "m%dx%dq%dh%d", &fstream_options.is_csv, &escape,
					&quote, &fstream_options.header);
			fstream_options.quote = quote;
			fstream_options.escape = escape;
		}

		/* set fstream for read (GET) or write (PUT) */
		if (r->is_get)
			fstream_options.forwrite = 0; /* GET request */
		else
		{
			fstream_options.forwrite = 1; /* PUT request */
			fstream_options.usesync = opt.S;
		}

#ifdef GPFXDIST
		/* set transformation options */
        if (r->trans.command)
        {
            fstream_options.transform = (struct gpfxdist_t*) apr_pcalloc(pool, sizeof(struct gpfxdist_t));

            if (! fstream_options.transform)
                    gfatal(FLINE, "out of memory in session_attach");

            fstream_options.transform->cmd        = r->trans.command;
			fstream_options.transform->pass_paths = r->trans.paths;
            fstream_options.transform->for_write  = fstream_options.forwrite;
            fstream_options.transform->mp         = pool;
			fstream_options.transform->errfile    = r->trans.errfile;
        }
		gprint("r->path %s\n", r->path);
#endif

		/* try opening the fstream */
		gprint("new session trying to open the data stream\n");
		fstream = fstream_open(r->path, &fstream_options, &response_code, &response_string);

		if (!fstream)
		{
			gwarning(FLINE, "reject request from %s, path %s\n", r->peer,
					r->path);
			http_error(r, response_code, response_string);
			request_end(r, 1, 0);
			apr_pool_destroy(pool);
			return -1;
		}

		gprint("new session successfully opened the data stream\n");

		gcb.total_sessions++;
		gcb.total_bytes += fstream_get_compressed_size(fstream);

		/* allocate session */
		session = apr_palloc(pool, sizeof(session_t));

		if (session == 0)
			gfatal(FLINE, "out of memory in session_attach");

		memset(session, 0, sizeof(*session));

		/* allocate active_segdb array (session member) */
		session->active_segids = (int *) apr_palloc(pool, sizeof(int) * r->totalsegs);

		if (session->active_segids == 0)
			gfatal(FLINE, "out of memory when allocating active_segids array");

		memset(session->active_segids, 0, sizeof(int) * r->totalsegs);

		/* initialize session values */
		session->tid = apr_pstrdup(pool, r->tid);
		session->path = apr_pstrdup(pool, r->path);
		session->key = apr_pstrdup(pool, key);
		session->fstream = fstream;
		session->pool = pool;
		session->is_get = r->is_get;
		session->active_segids[r->segid] = 1; /* mark this segid as active */
		session->maxsegs = r->totalsegs;

		if (session->tid == 0 || session->path == 0 || session->key == 0)
			gfatal(FLINE, "out of memory in session_attach");

		/* insert into hashtable */
		apr_hash_set(gcb.session.tab, session->key, APR_HASH_KEY_STRING, session);

		gprint("new session (%s, %s)\n", session->path, session->tid);
	}

	/* found a session in hashtable*/

	/* if error, send an error and close */
	if (session->is_error)
	{
		http_error(r, FDIST_INTERNAL_ERROR, "session error");
		request_end(r, 1, 0);
		return -1;
	}

	/* session already ended. send an empty response, and close. */
	if (0 == session->fstream)
	{
		gprint("session already ended. return empty response (OK)\n");

		http_empty(r);
		request_end(r, 0, 0);
		return -1;
	}

	/*
	 * disallow mixing GET and POST requests in one session.
	 * this will protect us from an infinitely running
	 * INSERT INTO ext_t SELECT FROM ext_t
	 */
	if (r->is_get != session->is_get)
	{
		http_error(r, FDIST_BAD_REQUEST, "can\'t write to and read from the same "
										 "gpfdist server simultaneously");
		request_end(r, 1, 0);
		return -1;
	}

	gprint("joined session (%s, %s)\n", session->path, session->tid);

	/* one more request for session */
	session->nrequest++;
	session->active_segids[r->segid] = !r->is_final;
	session->mtime = apr_time_now();
	r->session = session;

	if(!session->is_get)
		session_active_segs_dump(session);

	return 0;
}

/*
 * Dump all the segdb ids that currently participate
 * in this session.
 */
static void session_active_segs_dump(session_t* session)
{
	if(opt.v)
	{
		int i = 0;

		gprint("active segids in session: ");

		for (i = 0 ; i < session->maxsegs ; i++)
		{
			if(session->active_segids[i] == 1)
				printf("%d ", i);
		}
		printf("\n");
	}
}

/*
 * Is there any segdb still sending us data? or are
 * all of them done already? if empty all are done.
 */
static int session_active_segs_isempty(session_t* session)
{
	int i = 0;

	for (i = 0 ; i < session->maxsegs ; i++)
	{
		if(session->active_segids[i] == 1)
			return 0; /* not empty */
	}

	return 1; /* empty */
}

/*
 * do_write
 *
 * Callback when the socket is ready to be written
 */
void gfile_printf_then_putc_newline(const char *format, ...);

static void do_write(int fd, short event, void* arg)
{
	request_t* 	r = (request_t*) arg;
	int 		n, i;
	block_t* 	datablock;

	if (fd != r->sock)
		gfatal(FLINE, "internal error - non matching fd (%d) "
					  "and socket (%d)", fd, r->sock);

	/* Loop at most 3 blocks or until we choke on the socket */
	for (i = 0; i < 3; i++)
	{
		/* get a block (or find a remaining block) */
		if (r->outblock.top == r->outblock.bot)
		{
			const char* ferror = session_get_block(r->session, &r->outblock, r->line_delim_str, r->line_delim_length);

			if (ferror)
			{
				request_end(r, 1, ferror);
				gfile_printf_then_putc_newline("%s", ferror);
				return;
			}
			if (!r->outblock.top)
			{
				request_end(r, 0, 0);
				return;
			}
		}

		datablock = &r->outblock;

		/*
		 * If PROTO-1: first write out the block header (metadata).
		 */
		if (r->gp_proto == 1)
		{
			n = datablock->hdr.htop - datablock->hdr.hbot;

			if (n > 0)
			{
				n = local_send(r, datablock->hdr.hbyte + datablock->hdr.hbot, n);
				if (n < 0)
				{
					if (errno == EPIPE || errno == ECONNRESET)
						r->outblock.bot = r->outblock.top;
					request_end(r, 1, 0);
					return;
				}

				TR(("[%d] send header bytes %d .. %d (top %d)\n",
					fd, datablock->hdr.hbot, datablock->hdr.hbot + n, datablock->hdr.htop));

				datablock->hdr.hbot += n;
				n = datablock->hdr.htop - datablock->hdr.hbot;
				if (n > 0)
					break; /* network chocked */
			}
		}

		/*
		 * write out the block data
		 */
		n = datablock->top - datablock->bot;
		n = local_send(r, datablock->data + datablock->bot, n);
		if (n < 0)
		{
			/*
			 * EPIPE (or ECONNRESET some computers) indicates remote socket
			 * intentionally shut down half of the pipe.  If this was because
			 * of something like "select ... limit 10;", then it is fine that
			 * we couldn't transmit all the data--the segment didn't want it
			 * anyway.  If it is because the segment crashed or something like
			 * that, hopefully we would find out about that in some other way
			 * anyway, so it is okay if we don't poison the session.
			 */
			if (errno == EPIPE || errno == ECONNRESET)
				r->outblock.bot = r->outblock.top;
			request_end(r, 1, 0);
			return;
		}

		TR(("[%d] send data bytes off buf %d .. %d (top %d)\n",
			fd, datablock->bot, datablock->bot + n, datablock->top));

		datablock->bot += n;

		if (datablock->top != datablock->bot)
		{ /* network chocked */
			TR(("[%d] network full\n", fd));
			break;
		}
	}

	/* Set up for this routine to be called again */
	if (setup_write(r))
		request_end(r, 1, 0);
}

/*
 * do_read_request
 *
 * Callback when a socket is ready to be read. Read the
 * socket for a complete HTTP request.
 */
static void do_read_request(int fd, short event, void* arg)
{
	request_t* 	r = (request_t*) arg;
	int 		n, i;
	char*		p = NULL;
	char*		pp = NULL;
	char*		path = NULL;

	/* If we timeout, close the request. */
	if (event & EV_TIMEOUT)
	{
		http_error(r, FDIST_BAD_REQUEST, "time out");
		request_end(r, 1, 0);
		return;
	}

	/* Execute only once */
	if (opt.ssl && !r->io && !r->ssl_bio)
	{
		r->io = BIO_new(BIO_f_buffer());
		r->ssl_bio = BIO_new(BIO_f_ssl());
		BIO_set_ssl(r->ssl_bio, r->ssl, BIO_CLOSE);
		BIO_push(r->io, r->ssl_bio);

		/* Set the renegotiate timeout in seconds. 	*/
		/* When the renegotiate timeout elapses the */
		/* session is automatically renegotiated	*/
		BIO_set_ssl_renegotiate_timeout(r->ssl_bio, SSL_RENEGOTIATE_TIMEOUT_SEC);
	}

	/* how many bytes left in the header buf */
	n = r->in.hbufmax - r->in.hbuftop;
	if (n <= 0)
	{
		http_error(r, FDIST_INTERNAL_ERROR, "internal error");
		request_end(r, 1, 0);
		return;
	}

	/* read into header buf */
	n = gpfdist_receive(r, r->in.hbuf + r->in.hbuftop, n);

	if (n < 0)
	{
#ifdef WIN32
		int e = WSAGetLastError();
		int ok = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
		int e = errno;
		int ok = (e == EINTR || e == EAGAIN);
#endif
		if (!ok)
		{
			request_end(r, 1, 0);
			return;
		}
	}
	else if (n == 0)
	{
		/* socket close by peer will return 0 */
		request_end(r, 1, 0);
		return;
	}
	else
	{
		/* check if a complete HTTP request is available in header buf */
		r->in.hbuftop += n;
		n = r->in.hbuftop;
		r->in.req = gnet_parse_request(r->in.hbuf, &n, r->pool);
		if (!r->in.req && r->in.hbuftop >= r->in.hbufmax)
		{
			/* not available, but headerbuf is full - send error and close */
			http_error(r, FDIST_BAD_REQUEST, "forbidden");
			request_end(r, 1, 0);
			return;
		}
	}

	/*
	 * if we don't yet have a complete request, set up this function to be
	 * called again for
	 */
	if (!r->in.req)
	{
		if (setup_read(r))
		{
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
		}
		return;
	}

	/* Hurray, got a request !!! */
	gprint("%s requests %s\n", r->peer, r->in.req->argv[1] ? r->in.req->argv[1]
			: "(none)");

	/* print the complete request to the log if in verbose mode */
	if (opt.V)
	{
		gprint("[%d] got a request:", r->sock);
		for (i = 0; i < r->in.req->argc; i++)
			TR((" %s", r->in.req->argv[i]));
		TR(("\n"));
		gprint("request headers:");
		for (i = 0; i < r->in.req->hc; i++)
			TR(("%s:%s\n", r->in.req->hname[i], r->in.req->hvalue[i] ));
	}

	/* check that the request is validly formatted */
	if(request_validate(r))
		return;

	/* mark it as a GET or PUT request */
	if (0 == strcmp("GET", r->in.req->argv[0]))
		r->is_get = 1;

	/* if GET, we don't need to read from the socket anymore */
	if (r->is_get)
		shutdown(r->sock, SHUT_RD);


	/* make a copy of the path */
	path = apr_pstrdup(r->pool, r->in.req->argv[1]);

	/* decode %xx to char */
	percent_encoding_to_char(p, pp, path);

	/*
	 * This is a debug hook. We'll get here By creating an external table with
	 * name(a text) location('gpfdist://<host>:<port>/gpfdist/status').
	 * Show some state of gpfdist (num sessions, num bytes).
	 */
	if (!strcmp(path, "/gpfdist/status"))
	{
		send_gpfdist_status(r);
		request_end(r, 0, 0);
		return;
	}

	/*
	 * set up the requested path
	 */
	if (opt.f)
	{
		/* we forced in a filename with the hidden -f option. use it */
		r->path = opt.f;
	}
	else
	{
		if(request_set_path(r, opt.d, p, pp, path) != 0)
			return;
	}

	/* parse gp variables from the request */
	if(request_parse_gp_headers(r, opt.g) != 0)
		return;

#ifdef GPFXDIST
	/* setup transform */
	if(request_set_transform(r) != 0)
		return;
#endif

	/* Attach the request to a session */
	if(session_attach(r) != 0)
		return;

	if (r->is_get)
	{
		/* handle GET */
		handle_get_request(r);
	}
	else
	{
		/* handle PUT */
		handle_post_request(r, n);
	}
}


/* Callback when the listen socket is ready to accept connections. */
static void do_accept(int fd, short event, void* arg)
{
	struct sockaddr_storage 	a;
	socklen_t 			len = sizeof(a);
	SOCKET 				sock;
	request_t* 			r;
	apr_pool_t* 		pool;
	int 				on = 1;
	struct linger		linger;
	BIO 				*sbio = NULL;	/* only for SSL */
	SSL 				*ssl = NULL;	/* only for SSL */
	int 				rd;				/* only for SSL */

	/* do the accept */
	if ((sock = accept(fd, (struct sockaddr*) &a, &len)) < 0)
	{
		gwarning(FLINE, "accept failed");
		goto failure;
	}
	if (opt.ssl)
	{
		sbio = BIO_new_socket(sock, BIO_NOCLOSE);
		ssl = SSL_new(gcb.server_ctx);
		SSL_set_bio(ssl, sbio, sbio);
		if ( (rd = SSL_accept(ssl) <= 0) )
		{
			handle_ssl_error(sock, sbio, ssl);
			/* Close the socket that was allocated by accept 			*/
			/* We also must perform this, in case that a user 			*/
			/* accidentaly connected via gpfdist, instead of gpfdits	*/
			closesocket(sock);
			return;
		}

		gprint("[%d] Using SSL\n", (int)sock);
	}
	/* set to non-blocking, and close-on-exec */
#ifdef WIN32
	{
		unsigned long nonblocking = 1;
		ioctlsocket(sock, FIONBIO, &nonblocking);
	}
#else
	if (fcntl(sock, F_SETFL, O_NONBLOCK) == -1)
	{
		gwarning(FLINE, "fcntl(F_SETFL, O_NONBLOCK) failed");
		if ( opt.ssl )
		{
			handle_ssl_error(sock, sbio, ssl);
		}
		closesocket(sock);
		goto failure;
	}
	if (fcntl(sock, F_SETFD, 1) == -1)
	{
		gwarning(FLINE, "fcntl(F_SETFD) failed");
		if ( opt.ssl )
		{
			handle_ssl_error(sock, sbio, ssl);
		}
		closesocket(sock);
		goto failure;
	}
#endif

	/* set keepalive, reuseaddr, and linger */
	setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void*) &on, sizeof(on));
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void*) &on, sizeof(on));
	linger.l_onoff = 1;
	linger.l_linger = 5;
	setsockopt(sock, SOL_SOCKET, SO_LINGER, (void*) &linger, sizeof(linger));

	/* create a pool container for this socket */
	if (apr_pool_create(&pool, gcb.pool))
		gfatal(FLINE, "out of memory in do_accept");

	/* create the request in pool */
	r = apr_pcalloc(pool, sizeof(request_t));
	r->pool = pool;
	r->sock = sock;

    r->outblock.data = apr_palloc(pool, opt.m); /* use the block size specified by -m option */

	r->line_delim_str = "";
	r->line_delim_length = -1;

	r->in.hbufmax = 1024 * 4; /* 4K for reading the headers */
	r->in.hbuf = apr_palloc(pool, r->in.hbufmax);
	r->is_final = 0;	/* initialize */
	r->ssl = ssl;
	r->sbio = sbio;

	{
		char host[128];
		getnameinfo((struct sockaddr *)&a, len, host, sizeof(host), NULL, 0, NI_NUMERICHOST
#ifdef NI_NUMERICSERV
				| NI_NUMERICSERV
#endif
				);
		r->peer = apr_psprintf(r->pool, "%s", host);
	}


	/* set up for callback when socket ready for reading the http request */
	if (setup_read(r))
	{
		http_error(r, FDIST_INTERNAL_ERROR, "internal error");
		request_end(r, 1, 0);
	}

	return;

failure: gwarning(FLINE, "accept failed");
	return;
}

/*
 * setup_write
 *
 * setup the write event to write data to the socket. It uses
 * the callback function 'do_write'.
 */
static int setup_write(request_t* r)
{
	if (r->sock < 0)
		gfatal(FLINE, "internal error in setup_write - no socket to use");
	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_WRITE, do_write, r);
	return (event_add(&r->ev, 0));
}


/*
 * setup_read
 *
 * setup the read event to read data from a socket.
 *
 * we expect to be reading either:
 * 1) a GET or PUT request. or,
 * 2) the body of a PUT request (the raw data from client).
 *
 * this is controller by 'is_request' as follows:
 * -- if set to true, use the callback function 'do_read_request'.
 * -- if set to false, use the callback function 'do_read_body'.
 */
static int setup_read(request_t* r)
{
	if (r->sock < 0)
		gfatal(FLINE, "internal error in setup_read - no socket to use");

	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_READ, do_read_request, r);

	if(opt.t == 0)
	{
		return (event_add(&r->ev, NULL)); /* no timeout */
	}
	else
	{
		r->tm.tv_sec = opt.t;
		r->tm.tv_usec = 0;
		return (event_add(&r->ev, &r->tm));
	}
}

static void
print_listening_address(struct addrinfo *rp)
{
    char full_address[220] = {0};
	
	if (rp->ai_family == AF_INET)
	{
#ifndef WIN32
		struct sockaddr_in *ain = (struct sockaddr_in*)rp->ai_addr;
		char stradd[200] = {0};
		inet_ntop(AF_INET, (const void*)&ain->sin_addr, stradd, 100);
		sprintf(full_address, "IPV4 socket: %s:%d", stradd, opt.p);
#else
		/*
		 * there is no alternative for inet_ntop in windows that works for all Win platforms
		 * and for IPV6. inet_ntop transform the integer representation of the IP addr. into a string 
	     */
		sprintf(full_address, "IPV4 socket: IPv4:%d", opt.p);
#endif

	}
	else if (rp->ai_family == AF_INET6)
	{
#ifndef WIN32
		struct sockaddr_in6 *ain = (struct sockaddr_in6*)rp->ai_addr;
		char stradd[200] = {0};
		inet_ntop(AF_INET6, (const void*)&ain->sin6_addr, stradd, 100);
		sprintf(full_address, "IPV6 socket: [%s]:%d", stradd, opt.p);
#else
		sprintf(full_address, "IPV6 socket: [IPV6]:%d", opt.p);
#endif

	}
	else 
	{
		sprintf(full_address, "unknown protocol - %d", rp->ai_family);
	}
	
	gprint("%s\n", full_address);
}

/*
 * Search linked list (head) for first element with family (first_family).
 * Moves first matching element to head of the list.
 */
static struct
addrinfo* rearrange_addrs(struct addrinfo *head, int first_family)
{
	struct addrinfo* iter;
	struct addrinfo* new_head = head;
	struct addrinfo* holder = NULL;

	if (head->ai_family == first_family)
		return head;

	for (iter = head; iter != NULL && iter->ai_next != NULL; iter = iter->ai_next)
	{
		if ( iter->ai_next->ai_family == first_family )
		{
			holder = iter->ai_next;
			iter->ai_next = iter->ai_next->ai_next;
			/*
			 * we don't break here since if there are more addrinfo structure that belong to first_family
			 * in the list, we want to remove them all and keep only one in the holder.
			 * and then we will put the holder in the front
			 */
		}
	}

	if ( holder != NULL )
	{
		holder->ai_next = new_head;
		new_head = holder;
	}

	return new_head;
}


static void
print_addrinfo_list(struct addrinfo *head)
{
	struct addrinfo *iter;
	for (iter = head; iter != NULL; iter = iter->ai_next)
	{ 
		print_listening_address(iter);
	}
}

/* Create HTTP port and start to receive request */
static void
http_setup(void)
{
	SOCKET f;
	int on = 1;
	struct linger linger;
	struct addrinfo hints;
	struct addrinfo *addrs, *rp;
	int  s;
	int  i;

	char service[32];
	const char *hostaddr = NULL;

	if (opt.ssl)
	{
		/* Build our SSL context*/
		gcb.server_ctx 	= initialize_ctx();
		gpfdist_send 	= gpfdist_SSL_send;
		gpfdist_receive = gpfdist_SSL_receive;
	}
	else
	{
		gcb.server_ctx 	= NULL;
		gpfdist_send 	= gpfdist_socket_send;
		gpfdist_receive = gpfdist_socket_receive;
	}

	gcb.listen_sock_count = 0;
	if (opt.b != NULL && strlen(opt.b) > 1)
		hostaddr = opt.b;

	/* setup event priority */
	if (event_priority_init(10))
		gwarning(FLINE, "event_priority_init failed");


	/* Try each possible port from opt.p to opt.last_port */
	for (;;)
	{
		snprintf(service,32,"%d",opt.p);
		memset(&hints, 0, sizeof(struct addrinfo));
		hints.ai_family = AF_UNSPEC;	/* Allow IPv4 or IPv6 */
		hints.ai_socktype = SOCK_STREAM; /* tcp socket */
		hints.ai_flags = AI_PASSIVE;	/* For wildcard IP address */
		hints.ai_protocol = 0;			/* Any protocol */

		s = getaddrinfo(hostaddr, service, &hints, &addrs);
		if (s != 0)
#if (!defined(WIN32)) || defined(gai_strerror)
			gfatal(FLINE,"getaddrinfo says %s",gai_strerror(s));
#else
			/* Broken mingw header file from old version of mingw doesn't have gai_strerror */
			gfatal(FLINE,"getaddrinfo says %d",s);
#endif

		addrs = rearrange_addrs(addrs, AF_INET6);

		gprint("Before opening listening sockets - following listening sockets are available:\n");
		print_addrinfo_list(addrs);
		
		/*
		 * getaddrinfo() returns a list of address structures,
		 * one for each valid address and family we can use.
		 *
		 * Try each address until we successfully bind.
		 * If socket (or bind) fails, we (close the socket
		 * and) try the next address.  This can happen if
		 * the system supports IPv6, but IPv6 is disabled from
		 * working, or if it supports IPv6 and IPv4 is disabled.
		 */
		for (rp = addrs; rp != NULL; rp = rp->ai_next)
		{
			gprint("Trying to open listening socket:\n");
			print_listening_address(rp);
			
			/*
			 * getaddrinfo gives us all the parameters for the socket() call
			 * as well as the parameters for the bind() call.
			 */
			f = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);

			if (f == -1)
			{
				gwarning(FLINE, "Creating the socket failed\n");
				continue;
			}
			
#ifndef WIN32
			if (fcntl(f, F_SETFD, 1) == -1)
				gfatal(FLINE, "cannot create socket - fcntl(F_SETFD) failed");

			/* For the Windows case, we could use SetHandleInformation to remove
			 the HANDLE_INHERIT property from fd.
			 But for our purposes this does not matter,
			 as by default handles are *not* inherited. */

#endif
			setsockopt(f, SOL_SOCKET, SO_KEEPALIVE, (void*) &on, sizeof(on));

			/*
			 * We cannot use SO_REUSEADDR on win32 because it results in different
			 * behaviour -- it allows multiple servers to bind to the same port,
			 * resulting in totally unpredictable behaviour. What a silly operating
			 * system.
			 */
#ifndef WIN32
			setsockopt(f, SOL_SOCKET, SO_REUSEADDR, (void*) &on, sizeof(on));
#endif
			linger.l_onoff = 1;
			linger.l_linger = 5;
			setsockopt(f, SOL_SOCKET, SO_LINGER, (void*) &linger, sizeof(linger));

			if (bind(f, rp->ai_addr, rp->ai_addrlen) != 0)
			{
				/*
				 * EADDRINUSE warning appears only if the -v or -V option is on,
				 * All the other warnings will appear anyway
				 * EADDRINUSE is not defined in win32, so all the warnings will always appear.
				 */
#ifdef WIN32
				if ( 1 )
#else
				if ( errno == EADDRINUSE )
#endif
				{
					if ( opt.v )
					{
						gwarning(FLINE, "%s (errno = %d), port: %d",
					               		strerror(errno), errno, opt.p);
					}
				}
				else
			    {
					gwarning(FLINE, "%s (errno=%d), port: %d",strerror(errno), errno, opt.p);
				}

				/* failed on bind, maybe this address family isn't supported */
				close(f);
				continue;
			}

			/* listen with a big queue */
			if (listen(f, opt.z))
			{
				int saved_errno = errno;
				close(f);
				gwarning(FLINE, "listen with queue size %d on socket (%d) using port %d failed with error code (%d): %s",
							  opt.z,
							  (int)f,
							  opt.p,
							  saved_errno,
							  strerror(saved_errno));
				continue;
			}
			gcb.listen_socks[gcb.listen_sock_count++] = f;

			gprint("Opening listening socket succeeded\n");
		}

		/* When we get here, we have either succeeded, or tried all address families for this port */

		if (addrs != NULL)
		{
			/* don't need this any more */
			freeaddrinfo(addrs);
		}

		if (gcb.listen_sock_count > 0)
			break;

		if (opt.p >= opt.last_port)
			gfatal(FLINE, "cannot create socket on port %d "
						  "(last port is %d)", opt.p, opt.last_port);

		opt.p++;
		if (opt.v)
			putchar('\n'); /* this is just to beautify the print outs */
	}

	for (i = 0; i < gcb.listen_sock_count; i++)
	{
		/* when this socket is ready, do accept */
		event_set(&gcb.listen_events[i], gcb.listen_socks[i], EV_READ | EV_PERSIST,
				  do_accept, 0);

		/* high priority so we accept as fast as possible */
		if (event_priority_set(&gcb.listen_events[i], 0))
			gwarning(FLINE, "event_priority_set failed");

		/* start watching this event */
		if (event_add(&gcb.listen_events[i], 0))
			gfatal(FLINE, "cannot set up event on listen socket: %s",
						   strerror(errno));
	}
}

void
process_signal(int sig)
{
	if (sig == SIGINT || sig == SIGTERM)
	{
		int i;
		gwarning(FLINE, "signal %d received. gpfdist exits", sig);
		for (i = 0; i < gcb.listen_sock_count; i++)
			if (gcb.listen_socks[i] > 0)
			{
				closesocket(gcb.listen_socks[i]);
			}
		exit(1);
	}
}


static gnet_request_t*
gnet_parse_request(const char* buf, int* len, apr_pool_t* pool)
{
	int n = *len;
	int empty, completed;
	const char* p;
	char* line;
	char* last;
	char* colon;
	gnet_request_t* req = 0;

	/* find an empty line */
	*len = 0;
	empty = 1, completed = 0;
	for (p = buf; n > 0 && *p; p++, n--)
	{
		int ch = *p;
		/* skip spaces */
		if (ch == ' ' || ch == '\t' || ch == '\r')
			continue;
		if (ch == '\n')
		{
			if (!empty)
			{
				empty = 1;
				continue;
			}
			p++;
			completed = 1;
			break;
		}
		empty = 0;
	}
	if (!completed)
		return 0;

	/* we have a complete HTTP-style request (terminated by empty line) */
	*len = n = p - buf; /* consume it */
	line = apr_pstrndup(pool, buf, n); /* dup it */
	req = apr_pcalloc(pool, sizeof(gnet_request_t));

	/* for first line */
	line = apr_strtok(line, "\n", &last);

	if (!line)
		line = apr_pstrdup(pool, "");

	line = gstring_trim(line);

	if (0 != apr_tokenize_to_argv(line, &req->argv, pool))
		return req;

	while (req->argv[req->argc])
		req->argc++;

	/* for each subsequent lines */
	while (0 != (line = apr_strtok(0, "\n", &last)))
	{
		if (*line == ' ' || *line == '\t')
		{
			/* continuation */
			if (req->hc == 0) /* illegal - missing first header */
				break;

			line = gstring_trim(line);
			if (*line == 0) /* empty line */
				break;

			/* add to previous hvalue */
			req->hvalue[req->hc - 1] = gstring_trim(apr_pstrcat(pool,
					req->hvalue[req->hc - 1], " ", line, (char*)NULL));
			continue;
		}
		/* find a colon, and break the line in two */
		if (!(colon = strchr(line, ':')))
			colon = line + strlen(line);
		else
			*colon++ = 0;

		line = gstring_trim(line);
		if (*line == 0) /* empty line */
			break;

		/* save name, value pair */
		req->hname[req->hc] = line;
		req->hvalue[req->hc] = gstring_trim(colon);
		req->hc++;

		if (req->hc >= sizeof(req->hname) / sizeof(req->hname[0]))
			break;
	}

	return req;
}

static char *gstring_trim(char* s)
{
	char* p;
	s += strspn(s, " \t\r\n");
	for (p = s + strlen(s) - 1; p > s; p--)
	{
		if (strchr(" \t\r\n", *p))
			*p = 0;
		else
			break;
	}
	return s;
}

static char* datetime(void)
{
	static char 	buf[100];
	apr_time_exp_t 	t;

	apr_time_exp_lt(&t, apr_time_now());

	sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", 1900 + t.tm_year, 1
			+ t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);

	return buf;
}

int gprint(const char *fmt, ...)
{
	va_list ap;

	if (opt.v)
	{
		va_start(ap, fmt);
		printf("[%s] ", datetime());
		vprintf(fmt, ap);
		va_end(ap);
	}
	return 0;
}

void gfatal(const char* fline, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	printf("[%s] [INTERNAL ERROR %s] ", datetime(), fline);
	vprintf(fmt, ap);
	va_end(ap);
	printf("\n          ... exiting\n");

	exit(1);
}

int gwarning(const char* fline, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	printf("[%s] [WRN %s] ", datetime(), fline);
	vprintf(fmt, ap);
	va_end(ap);
	printf("\n");

	return 0;
}

void gfile_printf_then_putc_newline(const char *format, ...)
{
	va_list va;

	va_start(va,format);
	vprintf(format, va);
	va_end(va);
	putchar('\n');
}

void *gfile_malloc(size_t size)
{
	return malloc(size);
}

void gfile_free(void *a)
{
	free(a);
}

/*
 * percent_encoding_to_char
 *
 * decode any percent encoded characters that may be included in the http
 * request into normal characters ascii characters.
 */
void percent_encoding_to_char(char* p, char* pp, char* path)
{
	/*   - decode %xx to char */
	for (p = pp = path; *pp; p++, pp++)
	{
		if ('%' == (*p = *pp))
		{
			if (pp[1] && pp[2])
			{
				int x = pp[1];
				int y = pp[2];

				if ('0' <= x && x <= '9')
					x -= '0';
				else if ('a' <= x && x <= 'f')
					x = x - 'a' + 10;
				else if ('A' <= x && x <= 'F')
					x = x - 'A' + 10;
				else
					x = -1;

				if ('0' <= y && y <= '9')
					y -= '0';
				else if ('a' <= y && y <= 'f')
					y = y - 'a' + 10;
				else if ('A' <= y && y <= 'F')
					y = y - 'A' + 10;
				else
					y = -1;

				if (x >= 0 && y >= 0)
				{
					x = (x << 4) + y;
					*p = (char) x;
					pp++, pp++;
				}
			}
		}
	}

	*p = 0;
}

static void handle_get_request(request_t *r)
{
	/* setup to receive EV_WRITE events to write to socket */
	if (setup_write(r))
	{
		http_error(r, FDIST_INTERNAL_ERROR, "internal error");
		request_end(r, 1, 0);
		return;
	}

	if (0 != http_ok(r))
		request_end(r, 1, 0);
}

static void handle_post_request(request_t *r, int header_end)
{
	int h_count = r->in.req->hc;
	char** h_names = r->in.req->hname;
	char** h_values = r->in.req->hvalue;
	int i = 0;
	int b_continue = 0;
	char *data_start = 0;
	int data_bytes_in_req = 0;
	int wrote = 0;

	/*
	 * If this request is a "done" request (has GP-DONE header set)
	 * it has already marked this segment as inactive in this session.
	 * This is all that a "done" request should do. no data to process.
	 * we send our success response and end the request.
	 */
	if(r->is_final)
		goto done_processing_request;

	for(i = 0 ; i < h_count ; i++)
	{
		/* the request include a "Expect: 100-continue" header? */
		if(strcmp("Expect", h_names[i]) == 0 && strcmp("100-continue", h_values[i]) == 0)
			b_continue = 1;

		/* find out how long is our data by looking at "Content-Length" header*/
		if(strcmp("Content-Length", h_names[i]) == 0)
			r->in.davailable = atoi(h_values[i]);
	}

	/* if client asked for 100-Continue, send it. otherwise, move on. */
	if(b_continue)
		http_continue(r);

	gprint("available data to consume %d, starting at offset %d\n", r->in.davailable, r->in.hbuftop);

	/* create a buffer to hold the incoming raw data */
	r->in.dbufmax = opt.m; /* size of max line size */
	r->in.dbuf = apr_palloc(r->pool, r->in.dbufmax);
	r->in.dbuftop = 0;

	/* if some data come along with the request, copy it first */
	data_start = strstr(r->in.hbuf, "\r\n\r\n");
	if(data_start)
	{
		data_start += 4;
		data_bytes_in_req = (r->in.hbuf + r->in.hbuftop) - data_start;
	}

	if(data_bytes_in_req > 0)
	{
		/* we have data after the request headers. consume it */
		memcpy(r->in.dbuf, data_start, data_bytes_in_req);
		r->in.dbuftop += data_bytes_in_req;
		r->in.davailable -= data_bytes_in_req;

		/* only write it out if no more data is expected */
		if(r->in.davailable == 0)
			wrote = fstream_write(r->session->fstream, r->in.dbuf, data_bytes_in_req, 1, r->line_delim_str, r->line_delim_length);
	}

	/*
	 * we've consumed all data that came in the first buffer (with the request)
	 * if we're still expecting more data, get it from socket now and process it.
	 */
	while(r->in.davailable > 0)
	{
		size_t want;
		ssize_t n;
		size_t buf_space_left = r->in.dbufmax - r->in.dbuftop;

		if (r->in.davailable >= buf_space_left)
			want = buf_space_left;
		else
			want = r->in.davailable;

		/* read from socket into data buf */
		n = gpfdist_receive(r, r->in.dbuf + r->in.dbuftop, want);

		if (n < 0)
		{
#ifdef WIN32
			int e = WSAGetLastError();
			int ok = (e == WSAEINTR || e == WSAEWOULDBLOCK);
#else
			int e = errno;
			int ok = (e == EINTR || e == EAGAIN);
#endif
			if (!ok)
			{
				request_end(r, 1, 0);
				return;
			}
		}
		else if (n == 0)
		{
			/* socket close by peer will return 0 */
			request_end(r, 1, 0);
			return;
		}
		else
		{
			/*gprint("received %d bytes from client\n", n);*/

			r->in.davailable -= n;
			r->in.dbuftop += n;

			/* if filled our buffer or no more data expected, write it */
			if (r->in.dbufmax == r->in.dbuftop || r->in.davailable == 0)
			{
				/* only write up to end of last row */
				wrote = fstream_write(r->session->fstream, r->in.dbuf, r->in.dbuftop, 1, r->line_delim_str, r->line_delim_length);
				gprint("wrote %d bytes to file\n", wrote);

				if(wrote == -1)
				{
					/* write error */
					http_error(r, FDIST_INTERNAL_ERROR, fstream_get_error(r->session->fstream));
					request_end(r, 1, 0);
				}
				else if(wrote == r->in.dbuftop)
				{
					/* wrote the whole buffer. clean it for next round */
					r->in.dbuftop = 0;
				}
				else
				{
					/* wrote up to last line, some data left over in buffer. move to front */
					int bytes_left_over = r->in.dbuftop - wrote;

					memmove(r->in.dbuf, r->in.dbuf + wrote, bytes_left_over);
					r->in.dbuftop = bytes_left_over;
				}
			}
		}

	}

done_processing_request:

	/* send our success response and end the request */
	if (0 != http_ok(r))
		request_end(r, 1, 0);
	else
		request_end(r, 0, 0); /* we're done! */

}

static int request_set_path(request_t *r, const char* d, char* p, char* pp, char* path)
{

	/*
	 * disallow using a relative path in the request
	 */
	if (strstr(path, ".."))
	{
		gwarning(FLINE, "reject invalid request from %s [%s %s] - request "
						"is using a relative path",
						r->peer,
						r->in.req->argv[0],
						r->in.req->argv[1]);

		http_error(r, FDIST_BAD_REQUEST, "invalid request due to relative path");
		request_end(r, 1, 0);

		return -1;
	}

	r->path = 0;

	/*
	 * make the new path relative to the user's specified dir (opt.d)
	 */
	do
	{
		while (*path == ' ')
			path++;

		p = strchr(path, ' ');

		if (p)
			*p++ = 0;

		while (*path == '/')
			path++;

		if (*path)
		{
			if (r->path)
				r->path = apr_psprintf(r->pool, "%s %s/%s", r->path, d,
									   path);
			else
				r->path = apr_psprintf(r->pool, "%s/%s", d, path);
		}

		path = p;

	} while (path);


	if (!r->path)
	{
		http_error(r, FDIST_BAD_REQUEST, "invalid request (unable to set path)");
		request_end(r, 1, 0);
		return -1;
	}

	return 0;
}

static int request_validate(request_t *r)
{
	/* parse the HTTP request. Expect "GET /path HTTP/1.X" or "PUT /path HTTP/1.X" */
	if (r->in.req->argc != 3)
	{
		gwarning(FLINE, "reject invalid request from %s", r->peer);
		http_error(r, FDIST_BAD_REQUEST, "invalid request");
		request_end(r, 1, 0);
		return -1;
	}
	if (0 != strncmp("HTTP/1.", r->in.req->argv[2], 7))
	{
		gwarning(FLINE, "reject invalid protocol from %s [%s]", r->peer,
				r->in.req->argv[2]);
		http_error(r, FDIST_BAD_REQUEST, "invalid request");
		request_end(r, 1, 0);
		return -1;
	}
	if (0 != strcmp("GET", r->in.req->argv[0]) &&
		0 != strcmp("POST", r->in.req->argv[0]))
	{
		gwarning(FLINE, "reject invalid request from %s [%s %s]", r->peer,
				r->in.req->argv[0], r->in.req->argv[1]);
		http_error(r, FDIST_BAD_REQUEST, "invalid request");
		request_end(r, 1, 0);
		return -1;
	}

	return 0;
}

static void base16_decode(char* data)
{
	int i = 0;
	char *encoded_bytes = data;

	char buf[3];
	buf[2] = '\0';

	while (encoded_bytes[0])
	{
		buf[0] = encoded_bytes[0];
		buf[1] = encoded_bytes[1];

        data[i] = strtoul(buf, NULL, 16);

		i++;
        encoded_bytes += 2;
     }
     data[i] = '\0';
}

/*
 * request_parse_gp_headers
 *
 * Extract all X-GP-* variables from the HTTP headers.
 * Create a unique X-GP-TID value from it.
 */
static int request_parse_gp_headers(request_t *r, int opt_g)
{
	const char* xid = 0;
	const char* cid = 0;
	const char* sn = 0;
	const char* gp_proto = "0";
	int 		i;

	r->csvopt = "";
	r->is_final = 0;

	for (i = 0; i < r->in.req->hc; i++)
	{
		if (0 == strcmp("X-GP-XID", r->in.req->hname[i]))
			xid = r->in.req->hvalue[i];
		else if (0 == strcmp("X-GP-CID", r->in.req->hname[i]))
			cid = r->in.req->hvalue[i];
		else if (0 == strcmp("X-GP-SN", r->in.req->hname[i]))
			sn = r->in.req->hvalue[i];
		else if (0 == strcmp("X-GP-CSVOPT", r->in.req->hname[i]))
			r->csvopt = r->in.req->hvalue[i];
		else if (0 == strcmp("X-GP-PROTO", r->in.req->hname[i]))
			gp_proto = r->in.req->hvalue[i];
		else if (0 == strcmp("X-GP-DONE", r->in.req->hname[i]))
			r->is_final = 1;
		else if (0 == strcmp("X-GP-SEGMENT-COUNT", r->in.req->hname[i]))
			r->totalsegs = atoi(r->in.req->hvalue[i]);
		else if (0 == strcmp("X-GP-SEGMENT-ID", r->in.req->hname[i]))
			r->segid = atoi(r->in.req->hvalue[i]);
		else if (0 == strcmp("X-GP-LINE-DELIM-STR", r->in.req->hname[i]))
		{
			if (NULL == r->in.req->hvalue[i] || ((int)strlen(r->in.req->hvalue[i]))%2 == 1)
			{
				gwarning(FLINE, "reject invalid request from %s, invalid EOL encoding: %s", r->peer, r->in.req->hvalue[i]);
				http_error(r, FDIST_BAD_REQUEST, "invalid EOL encoding");
				request_end(r, 1, 0);
			}
			base16_decode(r->in.req->hvalue[i]);
			r->line_delim_str = r->in.req->hvalue[i];
		}
		else if (0 == strcmp("X-GP-LINE-DELIM-LENGTH", r->in.req->hname[i]))
			r->line_delim_length = atoi(r->in.req->hvalue[i]);
#ifdef GPFXDIST
		else if (0 == strcmp("X-GP-TRANSFORM", r->in.req->hname[i]))
			r->trans.name = r->in.req->hvalue[i];
#endif
	}

	if (r->line_delim_length > 0)
	{
		if (NULL == r->line_delim_str || (((int)strlen(r->line_delim_str)) != r->line_delim_length))
		{
			gwarning(FLINE, "reject invalid request from %s, invalid EOL length: %s, EOL: %s", r->peer, r->line_delim_length, r->line_delim_str);
			http_error(r, FDIST_BAD_REQUEST, "invalid EOL length");
			request_end(r, 1, 0);
		}
	}
	r->gp_proto = strtol(gp_proto, 0, 0);

	if (r->gp_proto != 0 && r->gp_proto != 1)
	{
		gwarning(FLINE,
				"reject invalid request from %s [%s %s] - X-GP-PROTO "
				"invalid '%s'",
				r->peer, r->in.req->argv[0], r->in.req->argv[1], gp_proto);
		http_error(r, FDIST_BAD_REQUEST, "invalid request (invalid gp-proto)");
		request_end(r, 1, 0);
		return -1;
	}

	if (opt_g != -1) /* override?  */
		r->gp_proto = opt_g;

	if (xid && cid && sn)
	{
		r->tid = apr_psprintf(r->pool, "%s.%s.%s.%d", xid, cid, sn,
							  r->gp_proto);
	}
	else if (xid || cid || sn)
	{
		gwarning(FLINE,
				 "reject invalid request from %s [%s %s] - missing X-GP-* "
				 "header",
				 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
		http_error(r, FDIST_BAD_REQUEST, "invalid request (missing X-GP-* header)");
		request_end(r, 1, 0);
		return -1;
	}
	else
	{
		r->tid = apr_psprintf(r->pool, "auto-tid.%d", gcb.session.gen++);
	}

	return 0;
}


#ifdef GPFXDIST
static int request_set_transform(request_t *r)
{
    extern struct transform* transform_lookup(struct transform* trlist, const char* name, int for_write, int verbose);
	extern char* transform_command(struct transform* tr);
	extern int transform_stderr_server(struct transform* tr);
	extern int transform_content_paths(struct transform* tr);
	extern char* transform_safe(struct transform* tr);
	extern regex_t* transform_saferegex(struct transform* tr);

    struct transform* tr;
	char* safe;

	/*
	 * Requests involving transformations should have a #transform=name in the external
	 * table URL.  In Rio, GPDB moves the name into an X-GP-TRANSFORM header.  However
	 * #transform= may still appear in the url in post requests.
	 *
	 * Note that ordinary HTTP clients and browsers do not typically transmit the portion
	 * of the URL after a #.  RFC 2396 calls this part the fragment identifier.
	 */

	char* param = "#transform=";
	char* start = strstr(r->path, param);
	if (start)
	{
		/*
		 * we have a transformation request encoded in the url
		 */
		*start = 0;
		if (! r->trans.name)
			r->trans.name = start + strlen(param);
	}

	if (! r->trans.name)
		return 0;

    /*
     * at this point r->trans.name is the name of the transformation requested
     * in the url and r->is_get tells us what kind (input or output) to look for.
     * attempt to look it up.
     */
	tr = transform_lookup(opt.trlist, r->trans.name, r->is_get ? 0 : 1, opt.V);
    if (! tr)
    {
        if (r->is_get)
        {
            gwarning(FLINE, "reject invalid request from %s [%s %s] - unsppported input #transform",
                     r->peer, r->in.req->argv[0], r->in.req->argv[1]);
            http_error(r, FDIST_BAD_REQUEST, "invalid request (unsupported input #transform)");
        }
        else
        {
            gwarning(FLINE, "reject invalid request from %s [%s %s] - unsppported output #transform",
                     r->peer, r->in.req->argv[0], r->in.req->argv[1]);
            http_error(r, FDIST_BAD_REQUEST, "invalid request (unsupported output #transform)");
        }
        request_end(r, 1, 0);
        return -1;
    }

	TR(("[%d] transform: %s\n", r->sock, r->trans.name));

	/*
	 * propagate details for this transformation
	 */
	r->trans.command = transform_command(tr);
	r->trans.paths   = transform_content_paths(tr);

	/*
	 * if safe regex is specified, check that the path matches it
	 */
	safe = transform_safe(tr);
	if (safe)
	{
		regex_t* saferegex = transform_saferegex(tr);
		int rc = regexec(saferegex, r->path, 0, NULL, 0);
		if (rc)
		{
			char buf[1024];
			regerror(rc, saferegex, buf, sizeof(buf));

			gwarning(FLINE, "reject invalid request from %s [%s %s] - path does not match safe regex %s: %s",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1], safe, buf);
			http_error(r, FDIST_BAD_REQUEST, "invalid request (path does not match safe regex)");
			request_end(r, 1, 0);
			return -1;
		}
		else
		{
			TR(("[%d] safe regex %s matches %s\n", r->sock, safe, r->path));
		}
	}

	/*
	 * if we've been requested to send stderr output to the server,
	 * we prepare a temporary file to hold it.	when the request is
	 * done we'll forward the output as error messages.
	 */
	if (transform_stderr_server(tr))
	{
		apr_pool_t*	 mp = r->pool;
		apr_file_t*	 f = NULL;
		const char*	 tempdir = NULL;
		char*		 tempfilename = NULL;
		apr_status_t rv;

		if ((rv = apr_temp_dir_get(&tempdir, mp)) != APR_SUCCESS)
		{
			gwarning(FLINE, "request failed from %s [%s %s] - failed to get temporary directory for stderr",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
			return -1;
		}

		tempfilename = apr_pstrcat(mp, tempdir, "/stderrXXXXXX", NULL);
		if ((rv = apr_file_mktemp(&f, tempfilename, APR_CREATE|APR_WRITE|APR_EXCL, mp)) != APR_SUCCESS)
		{
			gwarning(FLINE, "request failed from %s [%s %s] - failed to create temporary file for stderr",
					 r->peer, r->in.req->argv[0], r->in.req->argv[1]);
			http_error(r, FDIST_INTERNAL_ERROR, "internal error");
			request_end(r, 1, 0);
			return -1;
		}

		TR(("[%d] request opened stderr file %s\n", r->sock, tempfilename));

		r->trans.errfilename = tempfilename;
		r->trans.errfile	 = f;
	}

    return 0;
}
#endif


/*
 * gpfdist main
 *
 * 1) get command line options from user
 * 2) setup internal memory pool, and signal handlers
 * 3) init event handler (libevent)
 * 4) create the requested HTTP port and start listening for requests.
 * 5) create the gpfdist log file and handle stderr/out redirection.
 * 6) sit and wait for an event.
 */
int gpfdist_init(int argc, const char* const argv[])
{
	/*
	 * Comment
	 */
	if (0 != apr_app_initialize(&argc, &argv, 0))
		gfatal(FLINE, "apr_app_initialize failed");
	atexit(apr_terminate);

	if (0 != apr_pool_create(&gcb.pool, 0))
		gfatal(FLINE, "apr_app_initialize failed");

	apr_signal_init(gcb.pool);

	gcb.session.tab = apr_hash_make(gcb.pool);

	parse_command_line(argc, argv, gcb.pool);

#ifndef WIN32
#ifdef SIGPIPE
	signal(SIGPIPE, SIG_IGN);
#endif
#endif
	/*
	 * apr_signal(SIGINT, process_signal);
	 */
	apr_signal(SIGTERM, process_signal);
	if (opt.V)
		putenv("EVENT_SHOW_METHOD=1");
	putenv("EVENT_NOKQUEUE=1");

	event_init();
	http_setup();

	if (opt.ssl)
		printf("Serving HTTPS on port %d, directory %s\n", opt.p, opt.d);
	else
		printf("Serving HTTP on port %d, directory %s\n", opt.p, opt.d);

	fflush(stdout);

	/* redirect stderr and stdout to log */
	if (opt.l)
	{
		freopen(opt.l, "a", stderr);
#ifndef WIN32
		setlinebuf(stderr);
#endif
		freopen(opt.l, "a", stdout);
#ifndef WIN32
		setlinebuf(stdout);
#endif
	}

	/*
	 * must identify errors in calls above and return non-zero for them
	 * behaviour required for the Windows service case
	 */
	return 0;
}

int gpfdist_run()
{
	return event_dispatch();
}

#ifndef WIN32

int main(int argc, const char* const argv[])
{
	gpfdist_init(argc, argv);
	return gpfdist_run();
}


#else   /* in Windows gpfdist may run as a Windows service or a console application  */


SERVICE_STATUS          ServiceStatus;
SERVICE_STATUS_HANDLE   hStatus;

#define CMD_LINE_ARG_MAX_SIZE 1000
#define CMD_LINE_ARG_SIZE 500
#define CMD_LINE_ARG_NUM 40
char* cmd_line_buffer[CMD_LINE_ARG_NUM];
int cmd_line_args;

void  ServiceMain(int argc, char** argv);
void  ControlHandler(DWORD request);


/* gpfdist service registration on the WINDOWS command line
 * sc create gpfdist binpath= "c:\temp\gpfdist.exe param1 param2 param3"
 * sc delete gpfdist
 */

/* HELPERS - START */
void report_event(LPCTSTR _error_msg)
{
	HANDLE hEventSource;
	LPCTSTR lpszStrings[2];
	TCHAR Buffer[100];

	hEventSource = RegisterEventSource(NULL, TEXT("gpfdist"));

	if( NULL != hEventSource )
	{
		memcpy(Buffer, _error_msg, 100);

		lpszStrings[0] = TEXT("gpfdist");
		lpszStrings[1] = Buffer;

		ReportEvent(hEventSource,        /* event log handle */
					EVENTLOG_ERROR_TYPE, /* event type */
					0,                   /* event category */
					((DWORD)0xC0020100L),           /* event identifier */
					NULL,                /* no security identifier */
					2,                   /* size of lpszStrings array */
					0,                   /* no binary data */
					lpszStrings,         /* array of strings */
					NULL);               /* no binary data */

		DeregisterEventSource(hEventSource);
	}
}

int verify_buf_size(char** pBuf, const char* _in_val)
{
	int val_len, new_len;

	val_len = (int)strlen(_in_val);
	if (val_len >= CMD_LINE_ARG_SIZE)
	{
		new_len = ((val_len+1) >= CMD_LINE_ARG_MAX_SIZE) ? CMD_LINE_ARG_MAX_SIZE : (val_len+1);
		free(*pBuf);
		*pBuf = (char*)malloc(new_len);
		memset(*pBuf, 0, new_len);
	}
	else
	{
		new_len = val_len;
	}


	return new_len;
}

void init_cmd_buffer(int argc, const char* const argv[])
{
	int i;
	/* 1. initialize command line params buffer*/
	for (i = 0; i < CMD_LINE_ARG_NUM; i++)
	{
		cmd_line_buffer[i] = (char*)malloc(CMD_LINE_ARG_SIZE);
		memset(cmd_line_buffer[i], 0, CMD_LINE_ARG_SIZE);
	}

	/* 2. the number of variables cannot be higher than a
	 *    a predifined const, that is because - down the line
	 *    this values get to a const buffer whose size is
	 *    defined at compile time
	 */
	cmd_line_args = (argc <= CMD_LINE_ARG_NUM) ? argc : CMD_LINE_ARG_NUM;
	if (argc > CMD_LINE_ARG_NUM)
	{
		char msg[200] = {0};
		sprintf(msg, "too many parameters - maximum allowed: %d.", CMD_LINE_ARG_NUM);
		report_event(TEXT("msg"));
	}


	for (i = 0; i < cmd_line_args; i++)
	{
		int len;
		len = verify_buf_size(&cmd_line_buffer[i], argv[i]);
		memcpy(cmd_line_buffer[i], argv[i], len);
	}
}

void clean_cmd_buffer()
{
	int i;
	for (i = 0; i < CMD_LINE_ARG_NUM; i++)
	{
		free(cmd_line_buffer[i]);
	}
}

void init_service_status()
{
	ServiceStatus.dwServiceType = SERVICE_WIN32;
	ServiceStatus.dwCurrentState = SERVICE_START_PENDING;
	ServiceStatus.dwControlsAccepted   =  SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	ServiceStatus.dwWin32ExitCode = 0;
	ServiceStatus.dwServiceSpecificExitCode = 0;
	ServiceStatus.dwCheckPoint = 0;
	ServiceStatus.dwWaitHint = 0;
}

void do_set_srv_status(DWORD _currentState, DWORD _exitCode)
{
	ServiceStatus.dwCurrentState = _currentState;
	ServiceStatus.dwWin32ExitCode = _exitCode;
	SetServiceStatus(hStatus, &ServiceStatus);
}

void init_services_table(SERVICE_TABLE_ENTRY* ServiceTable)
{
	ServiceTable[0].lpServiceName = (LPSTR)"gpfdist";
	ServiceTable[0].lpServiceProc = (LPSERVICE_MAIN_FUNCTIONA)ServiceMain;
	ServiceTable[1].lpServiceName = (LPSTR)NULL;
	ServiceTable[1].lpServiceProc = (LPSERVICE_MAIN_FUNCTIONA)NULL;

}
/* HELPERS - STOP */

int main(int argc, const char* const argv[])
{
	int main_ret = 0, srv_ret;

	/*
	 * 1. command line parameters transfer to a global buffer - for ServiceMain
	 */
	init_cmd_buffer(argc, argv);

	/*
	 * 2. services table init
	 */
	SERVICE_TABLE_ENTRY ServiceTable[2];
	init_services_table(ServiceTable);

	/*
	 * 3. Start the control dispatcher thread for our service
	 */
	srv_ret = StartServiceCtrlDispatcher(ServiceTable);
	if (0 == srv_ret) /* program is being run as a Windows console application */
	{
		gpfdist_init(argc, argv);
		main_ret = gpfdist_run();
	}


	return main_ret;
}

void ServiceMain(int argc, char** argv)
{
	int error = 0;
	init_service_status();

	hStatus = RegisterServiceCtrlHandler((LPCSTR)"gpfdist", (LPHANDLER_FUNCTION)ControlHandler);
	if (hStatus == (SERVICE_STATUS_HANDLE)0)
	{
		/*
		 * Registering Control Handler failed
		 */
		return;
	}
	/*
	 * Initialize Service
	 * If we don't pass a const char* const [], to gpfdist_init
	 * we will get a warning that will fail the build
	 */
	const char* const buf[CMD_LINE_ARG_NUM] = {
		cmd_line_buffer[0],
		cmd_line_buffer[1],
		cmd_line_buffer[2],
		cmd_line_buffer[3],
		cmd_line_buffer[4],
		cmd_line_buffer[5],
		cmd_line_buffer[6],
		cmd_line_buffer[7],
		cmd_line_buffer[8],
		cmd_line_buffer[9],
		cmd_line_buffer[10],
		cmd_line_buffer[11],
		cmd_line_buffer[12],
		cmd_line_buffer[13],
		cmd_line_buffer[14],
		cmd_line_buffer[15],
		cmd_line_buffer[16],
		cmd_line_buffer[17],
		cmd_line_buffer[18],
		cmd_line_buffer[19],
		cmd_line_buffer[20],
		cmd_line_buffer[21],
		cmd_line_buffer[22],
		cmd_line_buffer[23],
		cmd_line_buffer[24],
		cmd_line_buffer[25],
		cmd_line_buffer[26],
		cmd_line_buffer[27],
		cmd_line_buffer[28],
		cmd_line_buffer[29],
		cmd_line_buffer[30],
		cmd_line_buffer[31],
		cmd_line_buffer[32],
		cmd_line_buffer[33],
		cmd_line_buffer[34],
		cmd_line_buffer[35],
		cmd_line_buffer[36],
		cmd_line_buffer[37],
		cmd_line_buffer[38],
		cmd_line_buffer[39]
	};
	error = gpfdist_init(cmd_line_args, buf);
	if (error != 0)
	{
		/*
		 * Initialization failed
		 */
		do_set_srv_status(SERVICE_STOPPED, -1);
		return;
	}
	else
	{
		do_set_srv_status(SERVICE_RUNNING, 0);
	}

	/*
	 * free the command line arguments buffer - it's not used anymore
	 */
	clean_cmd_buffer();

	/*
	 * actual service work
	 */
	gpfdist_run();
}

void ControlHandler(DWORD request)
{
	switch(request)
	{
		case SERVICE_CONTROL_STOP:
		case SERVICE_CONTROL_SHUTDOWN:
		{
			do_set_srv_status(SERVICE_STOPPED, 0);
			return;
		}

		default:
			break;
	}

	/*
	 * Report current status
	 */
	do_set_srv_status(SERVICE_RUNNING, 0);
}
#endif

#define find_max(a,b) ((a) >= (b) ? (a) : (b))

static SSL_CTX *initialize_ctx(void)
{
	int			stringSize;
	char 		*fileName, slash;
	SSL_CTX 	*ctx;

	if (!gcb.bio_err){
		/* Global system initialization*/
		SSL_library_init();
		SSL_load_error_strings();
		/* An error write context */
		gcb.bio_err=BIO_new_fp(stderr, BIO_NOCLOSE);
	}

	/* Create our context*/
	ctx = SSL_CTX_new( TLSv1_server_method() );

	/* Generate random seed */
	if ( RAND_poll() == 0 )
		gfatal(FLINE,"Can't generate random seed for SSL");

	/* The size of the string will consist of the path and the filename (the longest one) */
	// +1 for the '/' charachter (/filename)
	// +1 for the \0,
	stringSize = find_max( strlen(CertificateFilename), find_max(strlen(PrivateKeyFilename),strlen(TrustedCaFilename)) ) + strlen(opt.ssl) + 2;
	/* Allocate the memory for the file name */
	fileName = (char *) calloc( (stringSize), sizeof(char) );
	if ( fileName == NULL )
	{
		gfatal (FLINE,"Unable to allocate memory for SSL initialization");
	}

#ifdef WIN32
	slash = '\\';
#else
	slash = '/';
#endif

	/* Copy the path + the filename */
	snprintf(fileName,stringSize,"%s%c%s",opt.ssl,slash,CertificateFilename);

	/* Load our keys and certificates*/
	if (!(SSL_CTX_use_certificate_chain_file(ctx,fileName)))
	{
		gfatal(FLINE,"Unable to load the certificate from file: \"%s\"", fileName);
	}
	else
	{
		if ( opt.v )
		{
			gprint("The certificate was successfully loaded from \"%s\"\n",fileName);
		}
	}

	/* Copy the path + the filename */
	snprintf(fileName,stringSize,"%s%c%s",opt.ssl,slash,PrivateKeyFilename);

	if (!(SSL_CTX_use_PrivateKey_file(ctx,fileName,SSL_FILETYPE_PEM)))
	{
		gfatal (FLINE,"Unable to load the private key from file: \"%s\"", fileName);
	}
	else
	{
		if ( opt.v )
		{
			gprint("The private key was successfully loaded from \"%s\"\n",fileName);
		}
	}

	/* Copy the path + the filename */
	snprintf(fileName,stringSize,"%s%c%s",opt.ssl,slash,TrustedCaFilename);

	/* Load the CAs we trust*/
	if (!(SSL_CTX_load_verify_locations(ctx, fileName,0)))
	{
		gfatal (FLINE,"Unable to to load CA from file: \"%s\"", fileName);
	}
	else
	{
		if ( opt.v )
		{
			gprint("The CA file successfully loaded from \"%s\"\n",fileName);
		}
	}

	/* Set the verification flags for ctx 	*/
	/* We always require client certificate	*/
	SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, 0);

	/* Consider using these - experinments on Mac showed no improvement,
	 * but perhaps it will on other platforms, or when opt.m is very big
	 */
	//SSL_CTX_set_mode(ctx, SSL_MODE_AUTO_RETRY | SSL_MODE_ENABLE_PARTIAL_WRITE);

	free(fileName);
	return ctx;
}


/*
 * gpfdist_socket_send
 *
 * Sends the requested buf, of size buflen to the network
 * via appropriate socket
 */
static int gpfdist_socket_send(const request_t *r, const void *buf, const size_t buflen)
{
	return send(r->sock, buf, buflen, 0);
}


/*
 * gpfdist_SSL_send
 *
 * Sends the requested buf, of size len to the network via SSL
 */
static int gpfdist_SSL_send(const request_t *r, const void *buf, const size_t buflen)
{

	/* Write the data to socket */
	int n = BIO_write(r->io, buf, buflen);
	/* Try to flush */
	(void)BIO_flush(r->io);

	/* If we could not write to BIO */
	if ( n < 0)
	{
		/* If BIO indicates retry => we should retry later, this is not an error */
		if ( BIO_should_retry(r->io) > 1 )
		{
			/* Do not indicate error */
			n = 0;
		}
		else
		{
			/* If errno == 0 => this is not a real error */
			if ( errno == 0 )
			{
				/* Do not indicate error */
				n = 0;
			}
			else
			{
				/* If errno == EPIPE, it means that the client has closed the connection   	*/
				/* This error will be handled in the calling function, do not print it here	*/
				if (errno != EPIPE)
				{
					gwarning(FLINE, "Error during SSL gpfdist_send (Error = %d. errno = %d)", SSL_get_error(r->ssl,n), (int)errno);
					ERR_print_errors(gcb.bio_err);
				}
			}
		}
	}

	return n;
}


/*
 * gpfdist_socket_receive
 *
 * read from a socket
 */
static int gpfdist_socket_receive(const request_t *r, void *buf, const size_t buflen)
{
	return ( recv(r->sock, buf, buflen, 0) );
}


/*
 * gpfdist_SSL_receive
 *
 * read from an SSL socket
 */
static int gpfdist_SSL_receive(const request_t *r, void *buf, const size_t buflen)
{
	return ( BIO_read(r->io, buf, buflen) );
	/* todo: add error checks here */
}


/*
 * free_SSL_resources
 *
 * Frees all SSL resources that were allocated per request r
 */
static void free_SSL_resources(const request_t *r)
{
	BIO_ssl_shutdown(r->sbio);
	BIO_vfree(r->io);
	BIO_vfree(r->sbio);
	//BIO_vfree(r->ssl_bio);
	SSL_free(r->ssl);

}


/*
 * handle_ssl_error
 *
 * Frees SSL resources that were allocated during do_accept
 */
static void handle_ssl_error(SOCKET sock, BIO *sbio, SSL *ssl)
{
	gwarning(FLINE, "SSL accept failed");
	if (opt.v)
	{
		ERR_print_errors(gcb.bio_err);
	}

	BIO_ssl_shutdown(sbio);
	SSL_free(ssl);
}

/*
 * flush_ssl_buffer
 *
 * Flush all the data that is still pending in the current buffer
 */
static void flush_ssl_buffer(int fd, short event, void* arg)
{
	request_t* r = (request_t*)arg;

	(void)BIO_flush(r->io);

	if ( event & EV_TIMEOUT )
	{
		gwarning(FLINE,"Buffer flush timeout");
	}

	if ( BIO_wpending(r->io) )
	{
		setup_flush_ssl_buffer(r);
	}
	else
	{
		// Prepare and start a 5 seconds timer.
		// While working with BIO_SSL, we are working with 3 buffers:
		// [1] BIO layer buffer [2] SSL buffer [3] Socket buffer
		// Sometimes we have a delay somewhere between these buffers (MPP-16402)
		// So, even if the BIO_wpending() shows that there is no more pending data in the BIO_SSL buffer,
		// we might still have data in the socket's buffer that wasn't sent yet.

		TR(("[%s] [%d] SSL cleanup started\n", datetime(), r->sock));

		event_del(&r->ev);
		evtimer_set(&r->ev, request_cleanup_and_free_SSL_resources, r);
		r->tm.tv_sec  = opt.sslclean;
		r->tm.tv_usec = 0;
		(void)evtimer_add(&r->ev, &r->tm);
	}
}


/*
 * setup_flush_ssl_buffer
 *
 * Create event that will call to 'flush_ssl_buffer', with 5 seconds timeout
 */
static void setup_flush_ssl_buffer(request_t* r)
{
	event_del(&r->ev);
	event_set(&r->ev, r->sock, EV_WRITE, flush_ssl_buffer, r);
	r->tm.tv_sec  = 5;
	r->tm.tv_usec = 0;
	(void)event_add(&r->ev, &r->tm);
}


/*
 * request_cleanup
 *
 * Cleanup request related resources
 */
static void request_cleanup(request_t *r)
{
	event_del(&r->ev);
	shutdown(r->sock, SHUT_WR | SHUT_RD);
	closesocket(r->sock);
	r->sock = -1;
	apr_pool_destroy(r->pool);
}


/*
 * request_cleanup_and_free_SSL_resources
 *
 */
static void request_cleanup_and_free_SSL_resources(int fd, short event, void* arg)
{
	request_t *r = (request_t *)arg;

	TR(("[%s] [%d] SSL cleanup ended\n", datetime(), r->sock));

	/* Clean up request resources */
	request_cleanup(r);

	/* Release SSL related memory */
	free_SSL_resources(r);
}
