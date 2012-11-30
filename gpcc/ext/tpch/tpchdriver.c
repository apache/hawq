/*
* tpchdriver.c  This program is a master driver program that runs
* the entire TPC-H benchmark.  It first runs the power tests, then
* it runs the throughput tests.  It includes the queries and the 
* update functions UF1 and UF2.  
* This program incorporates all of QGEN.  It also has some of
* DBGEN incorporated so it can generate update data on the fly.
*/
#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#define MAXSESS 500
#define EVERYTHING        0x10000

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <math.h>
#include <sys/types.h>
#include <sys/timeb.h>
#include <signal.h>
#include <errno.h>
#ifndef WIN32
#include <unistd.h>
#include <sys/wait.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))

#pragma warning(disable:4201)
#pragma warning(disable:4214)
#pragma warning(disable:4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <process.h>
#include <io.h>
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#ifndef timeb
#define timeb _timeb
#endif
#ifndef itoa
#define itoa _itoa
#endif
#ifndef ftime
#define ftime _ftime
#endif
#ifndef sleep
#define sleep(x) Sleep(x*1000)
typedef unsigned long DWORD;
__declspec(dllimport) void __stdcall Sleep(DWORD dwMillseconds);
#define INLINE __inline
#else
#define INLINE inline
#endif

#endif
#include <ctype.h>
#include <time.h>
#include <assert.h>

#undef FAR
#define FAR


#undef VERSION

/* includes from QGEN */
#include "config.h"
#include "release.h"
#include "dss.h"
#include "tpcd.h"
#include "permute.h"
/* These two from DBGEN */
#include "dsstypes.h"
#include "bcd2.h"

#undef LIFENOISE
#define LIFENOISE(cnt) if (i % cnt == 0 && (flags & VERBOSE)) fprintf(stderr, ".")



#ifndef BOOL
typedef int BOOL;
#endif
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

long throughputstreams;
int ac1;
char **av1;
int *pids;


#define LINE_SIZE 512

/*
* Function Protoypes
*/
void	usage (void);
int prep_direct(char *);
int close_direct(void);
int ld_line(order_t * o, int mode);
int ld_order(order_t * o, int mode);
int ld_order_line(order_t * o, int mode);
int ld_deletes (DSS_HUGE  okey, int mode);

void vsub PROTO ((char *SQLRequest, int qnum, int vnum, int flags));
int strip_comments PROTO ((char *line));

int process_options PROTO ((int cnt, char **args));
int setup PROTO ((void));
void qsub PROTO ((char *qtag, int flags));
void gen_updates (long start, long count, long upd_num);
int gen_deletes (long min, long cnt, long num, int special);



extern int optind, opterr;
extern char *optarg;

char **mk_ascdate (void);
extern seed_t Seed[];

char **asc_date;
int snum = -1;

char *prog;
char *p1 = NULL;
FILE *fdetail;
FILE *fsummary;




/*
* seed generation functions; used with '-O s' option
*/
long sd_cust (int child, DSS_HUGE skip_count);
long sd_line (int child, DSS_HUGE skip_count);
long sd_order (int child, DSS_HUGE skip_count);
long sd_part (int child, DSS_HUGE skip_count);
long sd_psupp (int child, DSS_HUGE skip_count);
long sd_supp (int child, DSS_HUGE skip_count);
long sd_order_line (int child, DSS_HUGE skip_count);
long sd_part_psupp (int child, DSS_HUGE skip_count);

tdef tdefs[] =
{
	{"part.tbl", "part table", 200000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"partsupp.tbl", "partsupplier table", 200000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"supplier.tbl", "suppliers table", 10000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"customer.tbl", "customers table", 150000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"order.tbl", "order table", 150000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, sd_order, NO_FUNC, NONE, 0},
	{"lineitem.tbl", "lineitem table", 150000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"order.tbl", "order/lineitem tables", 150000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, LINE, 0},
	{"part.tbl", "part/partsupplier tables", 200000, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, PSUPP, 0},
	{"time.tbl", "time table", 2557, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"nation.tbl", "nation table", NATIONS_MAX, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0},
	{"region.tbl", "region table", NATIONS_MAX, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NO_FUNC, NONE, 0}
	/*,
	{"TPCDSEED", NULL, 0, NO_FUNC,
	{NO_FUNC, NO_FUNC}, NO_LFUNC, NONE, 0}*/
};

seed_t SaveSeed[MAX_STREAM + 1];
seed_t tempSeed;
long initSeeds[MAX_STREAM + 1];
long num_seeds = 0;
long rndm;
int rowcnt = 0, minrow = 0;
long upd_num = 0;
int setsPerReq = 1;
int maxSess = 4;
int specialtest = 0;
distribution q13a, q13b;
double flt_scale;
struct timeb StartTime;
double QueryTime[QUERIES_PER_SET + 1];
double UF1time;
double UF2time;
double Throughputtime;



char SQLRequest[32768];
//char UsingBuffer[32768];




/*
* FUNCTION strip_comments(line)
*
* remove all comments from 'line'; recognizes both {} and -- comments
*/
int
strip_comments (char *line)
{
	static int in_comment = 0;
	char *cp1, *cp2;

	cp1 = line;


	for(;;)					/* traverse the entire string */
	{

		if (in_comment)
		{
			if ((cp2 = strchr (cp1, '}')) != NULL)	/* comment ends */
			{
				strcpy (cp1, cp2 + 1);
				in_comment = 0;
				continue;
			}
			else
			{
				*cp1 = '\0';
				break;
			}
		}
		else
			/* not in_comment */
		{
			if ((cp2 = strchr (cp1, '-')) != NULL)
			{
				if (*(cp2 + 1) == '-')	/* found a '--' comment */
				{
					*cp2 = '\0';
					break;
				}
			}
			if ((cp2 = strchr (cp1, '{')) != NULL)	/* comment starts */
			{
				in_comment = 1;
				*cp2 = ' ';
				continue;
			}
			else
				break;
		}
	}
	return (0);
}

/*
* FUNCTION qsub(char *qtag, int flags)
*
* based on the settings of flags, and the template file $QDIR/qtag.sql
* make the following substitutions to turn a query template into EQT
*
*  String      Converted to            Based on
*  ======      ============            ===========
*  first line  database <db_name>;      -n from command line
*  second line set explain on;         -x from command line
*   :<number>  parameter <number>
*  :k          set number
*  :o          output to outpath/qnum.snum
*                                      -o from command line, SET_OUTPUT
*  :s          stream number
*  :b          BEGIN WORK;             -a from command line, START_TRAN
*  :e          COMMIT WORK;            -a from command line, END_TRAN
*  :q          query number
*  :n<number>                          sets rowcount to be returned
*/
void
qsub (char *qtag, int flags)
{
	static char *line = NULL, *qpath = NULL;
	int qnum;
	int i;
	FILE *qfp;
	char *cptr, *mark, *qroot = NULL;
	char tempStr[128];

	qnum = atoi (qtag);
	if (line == NULL)
	{
		line = malloc (BUFSIZ);
		qpath = malloc (BUFSIZ);
		if (line == NULL || qpath == NULL)
		{
			fprintf (stderr, "Malloc failed (qsub)\n");
			fflush (stderr);
			fclose (fdetail);
			fclose (fsummary);
			exit (2);
		}
	}

	qroot = env_config (QDIR_TAG, QDIR_DFLT);
	sprintf (qpath, "%s/%s.sql", qroot, qtag);
	qfp = fopen (qpath, "r");
	if (qfp == NULL)
	{
		fprintf (stderr, "open failed for query template %s, errno=%d\n", qpath, errno);
		fflush (stderr);
		fclose (fdetail);
		fclose (fsummary);
		exit (1);
	}

	SQLRequest[0] = '\0';
	rowcnt = rowcnt_dflt[qnum];
	vsub (SQLRequest, qnum, 0, flags);	/* set the variables */

	/* 
		if (flags & DFLT_NUM)
	     { strcat(SQLRequest, SET_ROWCOUNT); strcat(SQLRequest, itoa(rowcnt,tempStr,10)); } 
	*/
	while (fgets (line, BUFSIZ, qfp) != NULL)
	{
		if (line[0] == '.' || line[0] == '\\')	/* BTEQ command or PSQL command? */
			strcpy (line, " \n");

		if (!(flags & COMMENT))
			strip_comments (line);

		mark = line;
		while ((cptr = strchr (mark, VTAG)) != NULL)
		{
			*cptr = '\0';
			cptr++;
			strcat (SQLRequest, mark);
			switch (*cptr)
			{
			case 'b':
			case 'B':
				if ((flags & ANSI) == 0)
				{
					strcat (SQLRequest, START_TRAN);
					strcat (SQLRequest, "\n");
				}
				cptr++;
				break;
			case 'c':
			case 'C':
				//if (flags & DBASE)
				//   { strcat(SQLRequest, SET_DBASE); strcat(SQLRequest, db_name);   }

				cptr++;
				break;
			case 'e':
			case 'E':
				if ((flags & ANSI) == 0)
				{
					strcat (SQLRequest, END_TRAN);
					strcat (SQLRequest, "\n");
				}
				cptr++;
				break;
			case 'n':
			case 'N':
				if (!(flags & DFLT_NUM))
				{
					rowcnt = atoi (++cptr);
					while (isdigit (*cptr) || *cptr == ' ')
						cptr++;
					if (rowcnt > 0)
					{
						sprintf(tempStr, SET_ROWCOUNT, rowcnt); 
						strcat(SQLRequest, tempStr);
					}
				}
				continue;
			case 'o':
			case 'O':
				if (flags & OUTPUT)
				{	/*fprintf(ofp,"%s '%s/%s.%d'", SET_OUTPUT, osuff,
					qtag, (snum < 0)?0:snum);  */
				}
				cptr++;
				break;
			case 'q':
			case 'Q':
				strcat (SQLRequest, qtag);
				cptr++;
				break;
			case 's':
			case 'S':
				sprintf (tempStr, "%d", (snum < 0) ? 0 : snum);
				strcat (SQLRequest, tempStr);
				cptr++;
				break;
			case 'X':
			case 'x':
				if (flags & EXPLAIN)
				{
					strcat (SQLRequest, GEN_QUERY_PLAN);
					if (flags & EXPLAINANALYZE)
						strcat(SQLRequest, " ANALYZE");
					strcat (SQLRequest, " \n");
				}
				cptr++;
				break;
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				vsub (SQLRequest, qnum, atoi (cptr), flags & DFLT);
				while (isdigit(*++cptr));
				break;
			default:
				fprintf(stderr, "-- unknown flag '%c%c' ignored\n", 
					VTAG, *cptr);
				cptr++;
				break;

			}
			mark = cptr;
		}
		strcat (SQLRequest, mark);
	}
	fclose (qfp);
	for (i = strlen (SQLRequest) - 1; i > 0; i--)
		if (SQLRequest[i] == ' ' ||
			SQLRequest[i] == '\r' ||
			SQLRequest[i] == '\n' ||
			SQLRequest[i] == '\t')
			SQLRequest[i] = '\0';
		else
			break;
	if (SQLRequest[strlen (SQLRequest) - 1] == ';')
		SQLRequest[strlen (SQLRequest) - 1] = '\0';
	return;

}

void
usage (void)
{
	printf("TPC-H Benchmark Driver, matched to DBGEN/QGEN (Version %s %d.%d.%d build %d)\n",
					  NAME, VERSION, RELEASE, PATCH, BUILD);

	printf ("Portions Copyright %s %s\n", TPC, C_DATES);
	printf ("USAGE: %s <options> [ queries ]\n", prog);
	printf ("Options:\n");
	printf ("\t-c\t\t-- retain comments found in template.\n");
	printf ("\t-d\t\t-- use default substitution values.\n");
	printf ("\t-e\t\t-- run everything: Power test and throughput test.\n");
	printf ("\t-h\t\t-- print this usage summary.\n");
	/*printf("\t-i <str>\t-- use file <str> for initialization.\n");
	//printf("\t-l <str>\t-- log parameters to <str>.\n"); */
	printf ("\t-m <n>\t\t-- number of sessions to use for updates.\n");
	printf ("\t-M <n>\t\t-- number of transactions per multi-statement request for updates.\n");
	printf ("\t-n <str>\t-- connect to database using logon <str>.\n");
	printf ("\t-N\t\t-- use default rowcounts and ignore :n directive.\n");
	printf ("\t-o <str>\t-- set the output file base path to <str>.\n");
	printf ("\t-p <n>\t\t-- use the query permutation for stream <n>\n");
	printf ("\t-r <n,n,>\t\t-- seed the random number generator with <n>\n");
	printf ("\t-R  \t\t-- seed the random number generator with the current time of day\n");
	printf ("\t-s <n>\t\t-- base substitutions and updates on an SF of <n>\n");
	printf ("\t-S <n>\t\t-- set number of streams for throughput test to <n>\n");
	printf ("\t-U <s>\t\t-- generate <s> update sets.  Use 0 to avoid updates\n");
	printf ("\t-C <n>\t\t-- use seed file generated for <n> load processes to generate updates\n");
	printf ("\t-v\t\t-- verbose.\n");
	printf ("\t-x\t\t-- enable EXPLAIN in each query.\n");
}

int
process_options (int cnt, char **args)
{
	int flag;
	int i;

	while ((flag = getopt (cnt, args, "acC:dehi:M:m:n:Nl:o:p:P:r:s:S:t:TU:vw:x")) != -1)
		switch (flag)
	{
		case 'a':			/* special test mode */
			specialtest = 1;
			flags |= VERBOSE;
			break;
		case 'c':			/* retain comments in EQT */
			flags |= COMMENT;
			break;
		case 'C':
			children = atoi (optarg);

			break;
		case 'd':			/* use default substitution values */
			flags |= DFLT;
			break;
		case 'e':			/* Run everything, power test + multisession test. */
			snum = 0;
			flags |= EVERYTHING;
			break;

		case 'h':			/* just generate the usage summary */
			usage ();
			exit (0);
			break;
		case 'i':			/* set stream initialization file name */
			ifile = malloc (strlen (optarg) + 1);
			MALLOC_CHECK (ifile);
			strcpy (ifile, optarg);
			flags |= INIT;
			break;
		case 'l':			/* log parameter usages */
			lfile = malloc (strlen (optarg) + 1);
			MALLOC_CHECK (lfile);
			strcpy (lfile, optarg);
			flags |= LOG;
			break;
		case 'm':			/* sessions to use for updates. */
			maxSess = atoi (optarg);
			if (maxSess < 1 || maxSess > MAXSESS)
			{
				fprintf (stderr, " Max sessions out of range, ignored\n");
				maxSess = 4;
			}
			break;

		case 'M':			/* transactions per multi-statement req. */
			setsPerReq = atoi (optarg);
			if (setsPerReq < 1 || setsPerReq > 9)
			{
				fprintf (stderr, " Sets per Request out of range, ignored\n");
				setsPerReq = 1;
			}
			break;

		case 'N':			/* use default rowcounts */
			flags |= DFLT_NUM;
			break;
		case 'n':			/* set database name */
			db_name = malloc (strlen (optarg) + 1);
			MALLOC_CHECK (db_name);
			strcpy (db_name, optarg);
			flags |= DBASE;
			break;
		case 'o':			/* set the output path */
			osuff = malloc (strlen (optarg) + 1);
			MALLOC_CHECK (osuff);
			strcpy (osuff, optarg);
			flags |= OUTPUT;
			break;
		case 'p':			/* permutation for a given stream */
			snum = atoi (optarg);
			break;

		case 'r':			/* set random number seed for parameter gen */
			flags |= SEED;
			num_seeds = sscanf (optarg, "%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld",
				&initSeeds[0], &initSeeds[1], &initSeeds[2], &initSeeds[3], &initSeeds[4], &initSeeds[5],
				&initSeeds[6], &initSeeds[7], &initSeeds[8], &initSeeds[9], &initSeeds[10], &initSeeds[11]);
			rndm = (long) initSeeds[0];
			if (num_seeds > 1)
			{
				for (i = 0; i < num_seeds; i++)
				{
					//printf (" -- Using %ld as seed for stream %d\n", initSeeds[i], i);
					if (initSeeds[i] == 0)
					{
						fprintf (stderr, " -- Seeds cannot be zero!\n");
						exit (1);
					}
				}
			}
			//else
			//	printf (" -- Using %ld as seed\n", rndm);
			break;

		case 's':			/* scale of data set to run against */
			flt_scale = (double) atof (optarg);
			if (flt_scale < 1.0)
			{
				int i;

				scale = 1;
				for (i = PART; i < UPDATE; i++)
				{
					tdefs[i].base = (long) (tdefs[i].base * flt_scale);
					if (tdefs[i].base < 1)
						tdefs[i].base = 1;
				}
			}
			else
				scale = (long) flt_scale;
			if (scale < 1)
			{
				fprintf (stderr,
					"WARNING: Scale set to its lower bound (1)\n");
				scale = 1;
			}
			break;

		case 'S':
			throughputstreams = atoi (optarg);
			if (throughputstreams > 10)
			{
				fprintf (stderr, "Throughput streams reset to limit of %d\n",
					10);
				throughputstreams = 10;
			}
			break;
		case 't':			/* set termination file name */
			tfile = malloc (strlen (optarg) + 1);
			MALLOC_CHECK (tfile);
			strcpy (tfile, optarg);
			flags |= TERMINATE;
			break;
		case 'U':			/* generate for update stream */
			updates = atoi (optarg);
			break;
		case 'v':			/* verbose */
			flags |= VERBOSE;
			break;
		case 'w':
			schema_name = (char *) malloc (strlen(optarg)+1);
			strcpy(schema_name, optarg);
			break;
		case 'x':			/* set explain in the queries */
			if (flags & EXPLAIN)
				flags |= EXPLAINANALYZE;
			flags |= EXPLAIN;
			break;
		default:
			printf ("unknown option '%s' ignored\n", args[optind]);
			usage ();
			exit (1);
			break;
	}
	if (children > 999 && step < 0)		/* limitation of current seed file names */
	{
		printf ("Child process counts of > 999 not supported.\n");
		exit (1);
	}
	if (children > 1 && step >= 0)
		pids = malloc (children * sizeof (pid_t));
	return (0);
}

int
setup (void)
{
	asc_date = mk_ascdate();
	read_dist(env_config(DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "colors", &colors);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "nations", &nations);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "nations2", &nations2);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "regions", &regions);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "o_oprio", 
		&o_priority_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "instruct", 
		&l_instruct_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "category", 
		&l_category_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "Q13a", &q13a);
	read_dist(env_config(DIST_TAG, DIST_DFLT), "Q13b", &q13b);


	/* load the distributions that contain text generation */
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nouns", &nouns);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "verbs", &verbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adjectives", &adjectives);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adverbs", &adverbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "auxillaries", &auxillaries);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "terminators", &terminators);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "articles", &articles);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "prepositions", &prepositions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "grammar", &grammar);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "np", &np);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "vp", &vp);

	return (0);
}

BOOL canRetry(const char * SqlState)
{
	if (SqlState == NULL)
		return FALSE;

	if (strcmp(SqlState,"40000")==0 || /* Transaction rolled back? */
		strcmp(SqlState,"40003")==0 || /* completion unknown? */
		strcmp(SqlState,"40P01")==0 ) /* deadlock detected */
		return TRUE;
	else
		return FALSE;



}





DSS_HUGE start;
DSS_HUGE last;

long orderRowsDeleted = 0;
long lineitemRowsDeleted = 0;
long orderRowsToBeDeleted = 0;
long orderRowsInError = 0;
int OrdersInReq[MAXSESS + 1] = {0};




int LinesPerOrder[MAXSESS + 1][20];
long orderRowsAdded = 0;
long lineitemRowsAdded = 0;
long orderRowsToBeAdded = 0;
long lineitemRowsToBeAdded = 0;
long duplicates = 0;
order_t o[MAXSESS + 1][20];



/* From print.c */
/* 
* NOTE: this routine does NOT use the BCD2_* routines. As a result,
* it WILL fail if the keys being deleted exceed 32 bits. Since this
* would require ~660 update iterations, this seems an acceptable
* oversight
*/
int
gen_deletes (long min, long cnt, long num, int special)
{

	PGresult   *res = 0;
	static int last_num = 0;
	static FILE *dfp = NULL;
	DSS_HUGE child = -1;
	DSS_HUGE start, last;
	DSS_HUGE sk; /* called "new" in dbgen, but changed for c++ compatibility here */
	struct timeb EndTime;
	struct timeb difTime;
	char tempStr[200];

	DSS_HUGE i;
	DSS_HUGE temp; 


	conn = conn_list[1];

	orderRowsDeleted = 0;
	orderRowsInError = 0;
	lineitemRowsDeleted = 0;
	orderRowsToBeDeleted = 0;

	if (special == 0)
		fprintf (fdetail, "\n Running update function UF2 (deletes)\n");
	else
		fprintf (fdetail, "\n Running special cleanup deletes\n");
	if (flags & VERBOSE)
	{
		if (special == 0)
			fprintf (stdout, " Running update function UF2 (deletes)\n");
		else
			fprintf (stdout, " Doing special cleanup deletes\n");
		//fprintf(stderr," Running update function UF2 (deletes)\n");
	}
	ftime (&StartTime);
	sk = 0;
	i = 0;
	last = 0;
	start = 0;
	temp = 0;

	if (last_num != num)
	{
		// Do first time init here 
		last_num = num;

	}
	gen_rng = 1;

	start = MK_SPARSE(min, (special + num/ (10000 / refresh)));


	last = start - 1;

	for (child = min; cnt > 0; child++, cnt--)
	{

		sk =  MK_SPARSE(child, (special + num/ (10000 / refresh)));
		if (gen_rng == 1 && sk - last == 1 && cnt > 1)
		{
			last = sk;
			continue;
		}
		else
		{
			if (cnt <= 1 && sk - last == 1)
				last = sk;

			SQLRequest[0] = '\0';
			if (flags & EXPLAIN)
				strcat (SQLRequest, "EXPLAIN ");
			sprintf(tempStr,"DELETE FROM ORDERS WHERE O_ORDERKEY BETWEEN %ld AND %ld", (long)start,(long)last);
			strcat(SQLRequest,tempStr);
			strcat(SQLRequest,";");
			if (flags & EXPLAIN)
				strcat (SQLRequest, "EXPLAIN ");
			sprintf(tempStr,"DELETE FROM LINEITEM WHERE L_ORDERKEY BETWEEN %ld AND %ld", (long)start,(long)last);
			strcat(SQLRequest,tempStr);
			{
				static int p = 0;
				if (p<10 || (flags & EXPLAIN))
					fprintf(fdetail,"%s\n",SQLRequest);
				p++;
			}
			
			res = PQexec(conn, SQLRequest);

			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				if (PQresultStatus(res) == PGRES_TUPLES_OK)
				{
					Fetch_and_Display_All_Rows (res, -1);
				}
				else
				{
					fprintf(stderr, "DELETE failed: %s", PQerrorMessage(conn));
					PQclear(res);
					PQfinish(conn);
					exit(1);
				}
			}
			else
			{
				char * rowsaffected = PQcmdTuples(res);
				orderRowsDeleted = atol(rowsaffected);
				lineitemRowsDeleted = atol(rowsaffected);
			}

			if (flags & EXPLAIN)
				break;


			start = sk;
			last = sk;

		}
	}


	conn = conn_list[1];
	ftime (&EndTime);
	difTime.time = EndTime.time - StartTime.time;
	if (EndTime.millitm >= StartTime.millitm)
	{
		difTime.millitm = (unsigned short) (EndTime.millitm - StartTime.millitm);
	}
	else
	{
		difTime.millitm = (unsigned short) ((1000 + EndTime.millitm) - StartTime.millitm);
		difTime.time -= 1;
	};
	UF2time = difTime.time + difTime.millitm / 1000.0 + 0.005;

	StartTime.time = EndTime.time;
	StartTime.millitm = EndTime.millitm;

	if (orderRowsDeleted == 0 && special)
		UF2time = 0.0;
	else
	{
		fprintf (fdetail, " UF2 Time was %.2f seconds.  Ended at %s\n\n", UF2time, ctime (&StartTime.time));


		fprintf (fdetail, " Total %ld deleted from Order table\n", orderRowsDeleted);
		if (orderRowsDeleted != orderRowsToBeDeleted)
			fprintf (fdetail, " Should have been %ld deleted from Order table!!\n", orderRowsToBeDeleted);
		fprintf (fdetail, " Total %ld deleted from LineItem table\n", lineitemRowsDeleted);
		if (orderRowsInError > 0)
			fprintf (fdetail, " Total %ld deletes failed due to database errors\n", orderRowsInError);

	}



	return (0);
}






/*
* generate a particular table
*/
void
gen_updates (long start, long count, long upd_num)
{
	long i;

	


	PGresult   *res = 0;
	PGconn	   *order_conn = 0;
	PGconn	   *line_conn = 0;

	static char buffer[5000];
	static char * b;
	int dollars,
		cents;
	int dollars2,
		cents2;
	int dollars3,
		cents3;

	static int completed = 0;

	struct timeb EndTime;
	struct timeb difTime;

	DSS_HUGE sk;
	
	static order_t o;


	orderRowsAdded = 0;
	orderRowsInError = 0;
	lineitemRowsAdded = 0;
	orderRowsToBeAdded = 0;
	lineitemRowsToBeAdded = 0;
	duplicates = 0;



	fprintf (fdetail, "\n Running update function UF1 (inserts)\n");
	if (flags & VERBOSE)
	{
		fprintf (stdout, " Running update function UF1 (inserts)\n");
		//fprintf(stderr," Running update function UF1 (inserts)\n");
	}
	ftime (&StartTime);



	sk = 0;

	conn = conn_list[1];


	order_conn = conn_list[1];

	line_conn = conn_list[2];


	res = PQexec(order_conn, "COPY ORDERS FROM STDIN DELIMITER '|'"			);
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		fprintf(stderr, "COPY failed: %s", PQerrorMessage(order_conn));
		PQclear(res);
		PQfinish(order_conn);
		exit(1);
	}
	PQclear(res);






	res = PQexec(line_conn, "COPY LINEITEM FROM STDIN DELIMITER '|'");
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		fprintf(stderr, "COPY failed: %s", PQerrorMessage(line_conn));
		PQclear(res);
		PQfinish(line_conn);
		exit(1);
	}
	PQclear(res);

	for (i = start; count; count--, i++)
	{
		LIFENOISE (1000);

		mk_order ((DSS_HUGE)i, &o, upd_num % 10000);


		/* orders row */
		dollars = (int)(o.totalprice / 100);
		if (o.totalprice < 0)
			cents = (int) ((- o.totalprice) % 100);
		else
			cents = (int)(o.totalprice % 100);

		b = buffer + sprintf(buffer, HUGE_FORMAT "|" HUGE_FORMAT "|%c|%d.%02d|%s|%s|%s|%ld|%s\n", 
			o.okey,o.custkey,o.orderstatus,dollars,cents,o.odate,o.opriority,o.clerk,o.spriority,o.comment);

		orderRowsAdded++;

		if (PQputCopyData(order_conn,buffer, b-buffer) != 1)  
		{
			fprintf(stderr, "putCOPYData failed: %s", PQerrorMessage(order_conn));
			PQfinish(order_conn);
			exit(1);
		}



		/* lineitem rows */
		b = buffer;
		for (i = 0; i < o.lines; i++)
		{

			dollars = (int)(o.l[i].eprice / 100);
			if (o.l[i].eprice < 0)
				cents = (int) ((- o.l[i].eprice) % 100);
			else
				cents = (int)(o.l[i].eprice % 100);

			dollars2 = (int)(o.l[i].discount / 100);
			if (o.l[i].discount < 0)
				cents2 = (int) ((- o.l[i].discount) % 100);
			else
				cents2 = (int)(o.l[i].discount % 100);

			dollars3 = (int)(o.l[i].tax / 100);
			if (o.l[i].tax < 0)
				cents3 = (int) ((- o.l[i].tax) % 100);
			else
				cents3 = (int)(o.l[i].tax % 100);

			b = b + sprintf(b, HUGE_FORMAT "|" HUGE_FORMAT "|" HUGE_FORMAT "|" HUGE_FORMAT "|" HUGE_FORMAT  		
				"|%d.%02d|%d.%02d|%d.%02d|%c|%c|%s|%s|%s|%s|%s|%s\n", 
				o.l[i].okey,o.l[i].partkey,o.l[i].suppkey,o.l[i].lcnt,o.l[i].quantity,
				dollars,cents,dollars2,cents2,dollars3,cents3,
				o.l[i].rflag[0],o.l[i].lstatus[0],o.l[i].sdate,o.l[i].cdate,o.l[i].rdate,o.l[i].shipinstruct,o.l[i].shipmode,  
				o.l[i].comment);

			lineitemRowsAdded++;
		}

		if (PQputCopyData(line_conn,buffer, b-buffer) != 1)  
		{
			fprintf(stderr, "putCOPYData failed: %s", PQerrorMessage(line_conn));
			PQfinish(line_conn);
			exit(1);
		}

	}


	if (PQputCopyEnd(order_conn,0) == -1)
	{
		fprintf(stderr, "putCOPYEnd failed: %s", PQerrorMessage(order_conn));
		PQfinish(order_conn);
		exit(1);
	}
	res = PQgetResult(order_conn);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "copy into ORDERS failed: %s", PQerrorMessage(order_conn));
		fprintf(fdetail, "copy into ORDERS failed: %s", PQerrorMessage(order_conn));
	}
	/*else
	{
		char * rowsaffected = PQcmdTuples(res);
		fprintf(stderr, "copy says : %s\n",rowsaffected);
	}*/
	PQclear(res);


	if (PQputCopyEnd(line_conn,0) == -1)
	{
		fprintf(stderr, "putCOPYEnd failed: %s", PQerrorMessage(line_conn));
		PQfinish(order_conn);
		exit(1);
	}
	res = PQgetResult(line_conn);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "copy into LINEITEM failed: %s", PQerrorMessage(line_conn));
		fprintf(fdetail, "copy into LINEITEM failed: %s", PQerrorMessage(line_conn));
		 

	}
	/*else
	{
		char * rowsaffected = PQcmdTuples(res);
		fprintf(stderr, "copy says : %s\n",rowsaffected);
	}*/
	PQclear(res);


	conn = conn_list[1];

	ftime (&EndTime);
	difTime.time = EndTime.time - StartTime.time;
	if (EndTime.millitm >= StartTime.millitm)
	{
		difTime.millitm = (unsigned short) (EndTime.millitm - StartTime.millitm);
	}
	else
	{
		difTime.millitm = (unsigned short) ((1000 + EndTime.millitm) - StartTime.millitm);
		difTime.time -= 1;
	};
	UF1time = difTime.time + difTime.millitm / 1000.0 + 0.005;

	StartTime.time = EndTime.time;
	StartTime.millitm = EndTime.millitm;

	fprintf (fdetail, " UF1 Insert Time was %.2f seconds.  Ended at %s\n\n", UF1time, ctime (&StartTime.time));



	fprintf (fdetail, " Total %ld inserted into Order table\n", orderRowsAdded);
	//if (orderRowsAdded != orderRowsToBeAdded)
	//	fprintf (fdetail, " Should have been %d inserted into Order table!!\n", orderRowsToBeAdded);
	fprintf (fdetail, " Total %ld inserted into LineItem table\n", lineitemRowsAdded);
	//if (lineitemRowsAdded != lineitemRowsToBeAdded)
	//	fprintf (fdetail, " Should have been %d inserted into LineItem table\n", lineitemRowsToBeAdded);

	//if ((flags & VERBOSE) || (duplicates > 0))
	//	fprintf (fdetail, " Total %d inserts ignored due to duplicate unique primary index\n", duplicates);

	//if (orderRowsInError > 0)
	//	fprintf (fdetail, " Total %d inserts failed due to database errors\n", orderRowsInError);

}



void
stop_proc (int signum)
{
	fflush (fdetail);
	fflush (fsummary);
	fflush (stdout);
	fprintf (stderr, " Program terminating abnormally! signal=%d\n", signum);
	fflush (stderr);
	fclose(fsummary);
	fclose(fdetail);


	exit (1);
}


int 
Connect_Sessions ()
{
	PGresult   *res = 0;

	int i;
	char szLogon[240];
	char szDSN[32];
	char szUID[32];
	char szPWD[32];
	

	char *place;
	char *place2;

	szDSN[0] = '\0';
	szUID[0] = '\0';
	szPWD[0] = '\0';
	szLogon[0] = '\0';

	strcpy (szLogon, db_name);


	if (strstr (szLogon, "/") != NULL)
	{

		place = strstr (szLogon, "/");
		place[0] = '\0';
		strcpy ((char *) szDSN, szLogon);
		place++;
		place2 = strstr (place, ",");
		place2[0] = '\0';
		strcpy ((char *) szUID, place);
		place2++;
		strcpy ((char *) szPWD, place2);

	}

	/*****************
	if (snum == 0)
	{
	sprintf (tempstr, "rsh %scop1 /tpasw/bin/fsuflusher", szDSN);
	if (system (tempstr) != 0)
	{
	fprintf (stderr, " Tried to '%s' but it didn't work\n", tempstr);
	sleep (5);
	}
	else
	{
	fprintf (fdetail, " Called fsuflusher to flush memory\n");
	fprintf (stdout, " Called fsuflusher to flush memory\n");
	sleep (15);
	}
	}
	***********************/



	for (i = 1; i <= maxSess; i++)
	{
		conn = PQconnectdb(db_name);

		/* Check to see that the backend connection was successfully made */
		if (PQstatus(conn) != CONNECTION_OK)
		{
			if (i > 1)
				fprintf (stderr, " When trying to log on the %dth session, \n", i);
			fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));

			if (i < 8)
			{


				PQfinish(conn);
				exit(1);
			}
			fprintf (stderr, " Continuing with %d sessions only\n", i - 1);
			fflush (stderr);
			maxSess = i - 1;
			conn = conn_list[1];

			return (0);
		}


		if (schema_name && strlen(schema_name)>0)
		{
			char schema[250];
			strcpy(schema,"set search_path=");
			strcat(schema,schema_name);
			/*strcat(schema,",public,pg_catalog");*/
			res = PQexec(conn, schema);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, "SET SEARCH_PATH failed: %s", PQerrorMessage(conn));
				PQclear(res);
				PQfinish(conn);
				exit(1);
			}
			PQclear(res);
		}

		conn_list[i] = conn;
		if (updates == 0)
			break;

	}

	conn = conn_list[1];

	return (0);
}
int 
Disconnect_Sessions ()
{
	int i;
	for (i = 1; i <= maxSess; i++)
	{
		PQfinish(conn_list[i]);
		conn_list[i] = NULL;

		if (updates == 0)
			break;
	}

	if (flags & VERBOSE)
		fprintf (stdout, "Sessions are now logged off\n");
	return (0);
}

int 
Run_Query (const char *SQLReq, int qnum)
{
	PGresult   *res = 0;
	static char SQLR[2048];
	char SqlState[6];

	BOOL retry;

	struct timeb EndTime;
	struct timeb difTime;
	int i;
	char *SQ1;
	char *SQ2;
	const char *SQ;
	strcpy (SQLR, SQLReq);

	fprintf (fdetail, "\n Submitting SQL Request #%d:\n\n", qnum);
	fprintf (stdout, " Submitting SQL Request #%d:\n", qnum);

	SQ2 = SQLR;

	while (SQ2 != NULL)
	{
		SQ1 = SQ2;
		for (i = strlen (SQ1) - 1; i > 0; i--)	/* Trim trailing spaces */
			if (SQ1[i] == ' ' ||
				SQ1[i] == '\r' ||
				SQ1[i] == '\t')
				SQ1[i] = '\0';
			else
				break;
		if (SQ1[strlen (SQ1) - 1] == ';')	/* Trim trailing semicolon and spaces */
		{
			SQ1[strlen (SQ1) - 1] = '\0';
			for (i = strlen (SQ1) - 1; i > 0; i--)
				if (SQ1[i] == ' ' ||
					SQ1[i] == '\r' ||
					SQ1[i] == '\t')
					SQ1[i] = '\0';
				else
					break;
		}

		if (strstr (SQ1, "/*") != NULL)
			SQ2 = strstr (strstr (strstr (SQ1, "/*"), "*/"), ";");	/* look past the comment strings! */
		else
			SQ2 = strstr (SQ1, ";");	/* If semicolon, this is a multi-statement request */
		if (SQ2 != NULL)
		{
			*SQ2 = '\0';	/* Break out the first SQL statement in SQ1 */
			SQ2++;		/* SQ2 points to the next statement */
		}

		SQ = SQ1;
		while (SQ[0] != 0)
		{

			if (SQ[0] == '\r')
				fputc ('\n', fdetail);
			else
				fputc (SQ[0], fdetail);
			SQ++;
		}
		fprintf (fdetail, ";\n\n");

		retry = TRUE;
		while (retry)
		{
			retry = FALSE;
			res = PQexec(conn,  SQ1);
			SqlState[0] = '\0';
			if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf (fdetail, " Query #%d failed\n", qnum);

				if (PQresultErrorField(res, PG_DIAG_SQLSTATE) != NULL)
					strcpy(SqlState, PQresultErrorField(res, PG_DIAG_SQLSTATE));

				fprintf (fdetail, " %s: %s\n", SqlState, PQerrorMessage(conn));
				fprintf (stdout, " Query #%d failed\n", qnum);
				fprintf (stdout, " %s: %s\n", SqlState, PQerrorMessage(conn));
				PQclear(res);

				if (canRetry(SqlState))	// Transaction ABORTed due to DeadLock or crash 
				{
					retry = TRUE;
					fprintf (fdetail, " Attempting to resubmit Query #%d\n", qnum);
					fprintf (stdout, " Attempting to resubmit Query #%d\n", qnum);
				}
				else
					if (strcmp(SqlState,"42P07")!=0 )	// table already exists 
					{
						QueryTime[qnum] = 0.0;

						if (strcmp(SqlState,"57P01")==0 || /* admin killed your backend process */
							strcmp(SqlState,"57P02")==0 || /* backend process crashed */
							strncmp(PQerrorMessage(conn),"server closed the connection",strlen("server closed the connection"))==0)
						{
							// Need to reconnect to db.
							conn = PQconnectdb(db_name);
			
							if (PQstatus(conn) != CONNECTION_OK)
							{
								fprintf(stderr, "Reconnection to database failed: %s",
									PQerrorMessage(conn));

								PQfinish(conn);
								return(-1);
							}

							if (schema_name && strlen(schema_name)>0)
							{
								char schema[250];
								strcpy(schema,"set search_path=");
								strcat(schema,schema_name);
								 
								res = PQexec(conn, schema);
								if (PQresultStatus(res) != PGRES_COMMAND_OK)
								{
									fprintf(stderr, "SET SEARCH_PATH failed: %s", PQerrorMessage(conn));
									PQclear(res);
									PQfinish(conn);
									exit(1);
								}
								PQclear(res);
							}
						}
						return (-1);
					}
			}
			else
			{
				Fetch_and_Display_All_Rows (res, qnum);
			}
		}
	}

	ftime (&EndTime);
	difTime.time = EndTime.time - StartTime.time;
	if (EndTime.millitm >= StartTime.millitm)
	{
		difTime.millitm = (unsigned short) (EndTime.millitm - StartTime.millitm);
	}
	else
	{
		difTime.millitm = (unsigned short) ((1000 + EndTime.millitm) - StartTime.millitm);
		difTime.time -= 1;
	};

	QueryTime[qnum] = difTime.time + difTime.millitm / 1000.0 + 0.005;

	StartTime.time = EndTime.time;
	StartTime.millitm = EndTime.millitm;

	fprintf (fdetail, "Time was %.2f seconds.  Query ended at %s\n\n", QueryTime[qnum], ctime (&StartTime.time));
	fflush(fdetail);
	fflush(fsummary);
#ifdef WIN32
	_commit(_fileno(fdetail));
	_commit(_fileno(fsummary));
#else
	fsync(fileno(fdetail));
	fsync(fileno(fsummary));
#endif


	return (0);

}


int 
Verify_Database ()
{
	PGresult   *res = 0;

	long crowcount;
	double newSF;

	char * version;

	int isMPP;
	int isOLD;

	isMPP = 0;
	isOLD = 0;

	res = PQexec(conn, "SELECT VERSION()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SELECT VERSION() failed: %s", PQerrorMessage(conn));
		PQclear(res);
		PQfinish(conn);
		exit(1);
	}

	version = PQgetvalue(res, 0, 0);

	fprintf (fdetail, " Version of system:  %s\n", version);
	fprintf (stdout, " Version of system:  %s\n", version);


	if (strstr(version,"Bizgres MPP")!= 0 || strstr(version,"Greenplum database")!= 0 || strstr(version,"Greenplum Database")!= 0)
		isMPP = 1;
	else
		isMPP = 0;

	if (strstr(version,"PostgreSQL 8.1.")!=0)
		isOLD = 1;
	else
		isOLD = 0;

	PQclear(res);


	if (isMPP)
	{
		char * segdbcount = "";
		char * nodecount = "";
		if (isOLD)
			res = PQexec(conn, "SELECT COUNT(*) FROM pg_catalog.mpp_configuration WHERE content >=0 and valid=true");
		else
			res = PQexec(conn, "SELECT COUNT(*) FROM pg_catalog.gp_configuration WHERE content >=0 and valid=true");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT VERSION() failed: %s", PQerrorMessage(conn));


		}
		else
		{
			segdbcount = strdup(PQgetvalue(res, 0, 0))	;
			PQclear(res);
			if (isOLD)
				res = PQexec(conn, "SELECT COUNT(DISTINCT hostname) FROM pg_catalog.mpp_configuration WHERE content >=0 and valid=true");
			else
				res = PQexec(conn, "SELECT COUNT(DISTINCT hostname) FROM pg_catalog.gp_configuration WHERE content >=0 and valid=true");
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				fprintf(stderr, "SELECT VERSION() failed: %s", PQerrorMessage(conn));


			}
			else
			{

				nodecount = PQgetvalue(res, 0, 0);
				fprintf (fdetail, " System has %s segment databases on %s nodes\n", segdbcount, nodecount);
				fprintf (fsummary, " System has %s segment databases on %s nodes\n", segdbcount, nodecount);
				fprintf (stdout, " System has %s segment databases on %s nodes\n", segdbcount, nodecount);
			}
			free(segdbcount);
		}


		PQclear(res);

	}


		res = PQexec(conn, "SHOW shared_buffers");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SHOW failed: %s", PQerrorMessage(conn));
		}
		else
		{
			char * value;
			value = PQgetvalue(res, 0, 0);
			fprintf (fdetail, " shared_buffers = %s   ", value);
			fprintf (fsummary, " shared_buffers = %s   ", value);
			fprintf (stdout, " shared_buffers = %s   ", value);
		}


		PQclear(res);


		res = PQexec(conn, "SHOW work_mem");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SHOW failed: %s", PQerrorMessage(conn));
		}
		else
		{
			char * value;
			value = PQgetvalue(res, 0, 0);
			fprintf (fdetail, " work_mem = %s    ", value);
			fprintf (fsummary, " work_mem = %s    ", value);
			fprintf (stdout, " work_mem = %s    ", value);
		}

		PQclear(res);

		res = PQexec(conn, "SHOW random_page_cost");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SHOW failed: %s", PQerrorMessage(conn));
		}
		else
		{
			char * value;
			value = PQgetvalue(res, 0, 0);
			fprintf (fdetail, " random_page_cost = %s\n", value);
			fprintf (fsummary, " random_page_cost = %s\n", value);
			fprintf (stdout, " random_page_cost = %s\n", value);
		}


		PQclear(res);

		res = PQexec(conn, "SHOW effective_cache_size");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SHOW failed: %s", PQerrorMessage(conn));
		}
		else
		{
			char * value;
			value = PQgetvalue(res, 0, 0);
			fprintf (fdetail, " effective_cache_size = %s   ", value);
			fprintf (fsummary, " effective_cache_size = %s   ", value);
			fprintf (stdout, " effective_cache_size = %s   ", value);
		}


		PQclear(res);

		res = PQexec(conn, "SHOW cpu_tuple_cost");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SHOW failed: %s", PQerrorMessage(conn));
		}
		else
		{
			char * value;
			value = PQgetvalue(res, 0, 0);
			fprintf (fdetail, " cpu_tuple_cost = %s   ", value);
			fprintf (fsummary, " cpu_tuple_cost = %s   ", value);
			fprintf (stdout, " cpu_tuple_cost = %s   ", value);
		}


		PQclear(res);

		res = PQexec(conn, "SHOW cpu_index_tuple_cost");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SHOW failed: %s", PQerrorMessage(conn));
		}
		else
		{
			char * value;
			value = PQgetvalue(res, 0, 0);
			fprintf (fdetail, " cpu_index_tuple_cost = %s\n", value);
			fprintf (fsummary, " cpu_index_tuple_cost = %s\n", value);
			fprintf (stdout, " cpu_index_tuple_cost = %s\n", value);
		}


		PQclear(res);



	res = PQexec(conn, "SELECT COUNT(*) FROM PG_STAT_ACTIVITY");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SELECT COUNT(*) FROM PG_STAT_ACTIVITY failed: %s", PQerrorMessage(conn));	 
	}
	else
	{
		char * count = PQgetvalue(res, 0, 0);
		crowcount = atol(count);
		if (crowcount > maxSess)
		{
			fprintf (fdetail, " There are %ld other sessions logged on to the system under test\n", crowcount - maxSess);
			fprintf (stdout, " There are %ld other sessions logged on to the system under test\n", crowcount - maxSess);
		}
	}

	PQclear(res);

	if (flags & VERBOSE)
		fprintf (stdout, "Checking the catalog to see what tables and indexes exist in this schema\n");

	res = PQexec(conn, "SELECT n.nspname as \"Schema\","
		"c.relname as \"Name\","
		"CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' END as \"Type\","
		"r.rolname as \"Owner\","
		"c2.relname as \"Table\" "
		" FROM pg_catalog.pg_class c "
		" LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner "
		" LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
		" LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid "
		" LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid "
		" WHERE c.relkind IN ('r','i') "
		" AND n.nspname NOT IN ('pg_catalog', 'pg_toast') "
		" AND pg_catalog.pg_table_is_visible(c.oid) "
		" ORDER BY 1,2");

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Catalog check failed: %s", PQerrorMessage(conn));	 
	}
	else
	{
		int currow;
		int icol;
		int totalRows = PQntuples(res);
		int maxcol = PQnfields(res);
		fprintf(fdetail,"\n Schema         Name           Type           Owner          Table\n");
		fprintf(fdetail,  " -------------- -------------- -------------- -------------- --------------\n");
		for (currow = 0; currow < totalRows; currow++)	 
		{

			int actualLen = 0;

			for (icol = 0; icol < maxcol; icol++)
			{
				char * dValue;
				dValue = PQgetvalue(res, currow, icol);
				if (dValue)
					actualLen = strlen(dValue);
				else
					actualLen = 0;

				fprintf (fdetail, "%c", ' ');
				 
				fprintf (fdetail, "%s", dValue);

	 
				while (14 > actualLen++)
						fprintf (fdetail, "%c", ' ');

			}
			fprintf (fdetail, " \n");
		}
		fprintf(fdetail, "\n");
	}

	PQclear(res);

	if (flags & VERBOSE)
		fprintf (stdout, "Submitting SQL Requests to verify scale factor\n");

	res = PQexec(conn, "SELECT COUNT(*) FROM CUSTOMER");
	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		char * count = PQgetvalue(res, 0, 0);
		crowcount = atol(count);
		if (crowcount > maxSess)
		{
			newSF = ((double) crowcount) / 150000.0;
			fprintf (stdout, " Customer table exists with scale factor %g\n", newSF);
			if ((long) (newSF * 1000 + 0.5) != (long) (flt_scale * 1000))
				fprintf (stdout, " Scale factor does not match!\n");
		}
	}
	else
	{
		fprintf (stderr, " Customer table does not exist or cant be accessed\n");
		fprintf (stderr, " %s\n", PQerrorMessage(conn));

	}
	PQclear(res);

	res = PQexec(conn, "SELECT COUNT(*) FROM PART");
	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		char * count = PQgetvalue(res, 0, 0);
		crowcount = atol(count);
		if (crowcount > maxSess)
		{
			newSF = ((double) crowcount) / 200000.0;
			fprintf (stdout, " Part table exists with scale factor %g\n", newSF);
			if ((long) (newSF * 1000 + 0.5) != (long) (flt_scale * 1000))
				fprintf (stdout, " Scale factor does not match!\n");
		}
	}
	else
	{
		fprintf (stderr, " Part table does not exist or cant be accessed\n");
		fprintf (stderr, " %s\n", PQerrorMessage(conn));

	}
	PQclear(res);


	res = PQexec(conn, "SELECT COUNT(*) FROM ORDERS");
	if (PQresultStatus(res) == PGRES_TUPLES_OK)
	{
		char * count = PQgetvalue(res, 0, 0);
		crowcount = atol(count);
		if (crowcount > maxSess)
		{
			newSF = ((double) crowcount) / 1500000.0;
			fprintf (stdout, " Order table exists with scale factor %g\n", newSF);
			if ((long) (newSF * 1000 + 0.5) != (long) (flt_scale * 1000))
				fprintf (stdout, " Scale factor does not match!\n");
		}
	}
	else
	{
		fprintf (stderr, " Orders table does not exist or cant be accessed\n");
		fprintf (stderr, " %s\n", PQerrorMessage(conn));

	}
	PQclear(res);




	return (0);

}

int 
Run_Query_Stream ()
{
	int i;
	ftime (&StartTime);
	fprintf (stdout, " Starting query run for stream %d on %s\n", snum, ctime (&StartTime.time));
	fprintf (fdetail, "\n Starting query run for stream %d on %s\n", snum, ctime (&StartTime.time));

	ftime (&StartTime);



	if (snum >= 0)
		if (optind < ac1)
			for (i = optind; i < ac1; i++)
			{
				char qname[10];
				sprintf (qname, "%ld", SEQUENCE (snum, atoi (av1[i])));
				qsub (qname, flags);
				Run_Query (SQLRequest, SEQUENCE (snum, atoi (av1[i])));
			}
		else
			for (i = 1; i <= QUERIES_PER_SET; i++)
			{
				char qname[10];
				sprintf (qname, "%ld", SEQUENCE (snum, i));
				qsub (qname, flags);
				Run_Query (SQLRequest, SEQUENCE (snum, i));
			}
	else if (optind < ac1)
		for (i = optind; i < ac1; i++)
		{
			qsub (av1[i], flags);
			Run_Query (SQLRequest, atoi (av1[i]));
		}
	else
		for (i = 1; i <= QUERIES_PER_SET; i++)
		{
			char qname[10];
			sprintf (qname, "%d", i);
			qsub (qname, flags);
			Run_Query (SQLRequest, i);
		}

		ftime (&StartTime);
		fprintf (stdout, " Finished query run for stream %d on %s\n", snum, ctime (&StartTime.time));
		fprintf (fdetail, " Finished query run for stream %d on %s\n", snum, ctime (&StartTime.time));

		return (0);
}

void 
Print_Stream_Summary ()
{
	int i;
	double totalquery = 0.0;

	fprintf (fsummary, "\nQuery timing results:\n\n");
	for (i = 1; i <= QUERIES_PER_SET; i++)
		if (QueryTime[i] != 0.0)
			fprintf (fsummary, "Query #%d time was %.2f seconds.\n", i, QueryTime[i]);
		else
			fprintf (fsummary, "Query #%d was not run.\n", i);

	for (i = 1; i <= QUERIES_PER_SET; i++)
		totalquery += QueryTime[i];
	fprintf (fsummary, "Total for all queries was %.2f minutes.\n", totalquery/60.0);

	if (UF1time != 0.0)
		fprintf (fsummary, "UF1 (insert) time was %.2f seconds.\n", UF1time);
	else
		fprintf (fsummary, "UF1 (insert) was not run.\n");

	if (UF2time != 0.0)
		fprintf (fsummary, "UF2 (delete) time was %.2f seconds.\n", UF2time);
	else
		fprintf (fsummary, "UF2 (delete) was not run.\n");

	totalquery += (UF1time + UF2time);

	if (UF1time != 0.0 && UF2time != 0.0)
		fprintf (fsummary, "Total for stream time was %.2f minutes.\n", totalquery/60.0);

}

void 
Run_UF1_Inserts ()
{
	DSS_HUGE i;
	

	ftime (&StartTime);
	fprintf (stdout, " Starting UF1 insert run for set %ld on %s\n", upd_num, ctime (&StartTime.time));
	fprintf (fdetail, "\n Starting UF1 insert run for set %ld on %s\n", upd_num, ctime (&StartTime.time));

	set_state (ORDER, scale, 1, 1 + 1, &i); 

	rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * scale * refresh);

	if (rowcnt != 1500 * scale && flt_scale >= 1.0)
		fprintf (stderr, " Why is rowcnt = %d\n", rowcnt);

	minrow = rowcnt * upd_num + 1;
	if (flags & VERBOSE)
		fprintf (fdetail, " Inserting from %d for %d txns, upd_num =%ld\n", minrow, rowcnt, upd_num);


	if (!(flags & EXPLAIN))
	gen_updates (minrow, rowcnt, upd_num + 1);

	upd_num++;

}

void 
Run_UF2_Deletes (int special)
{

	ftime (&StartTime);
	if (special == 0)
	{
		fprintf (stdout, " Starting UF2 delete run for set %ld on %s\n", upd_num - 1, ctime (&StartTime.time));
		fprintf (fdetail, "\n Starting UF2 delete run for set %ld on %s\n", upd_num - 1, ctime (&StartTime.time));
	}

	
	rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * scale * refresh);
	if (rowcnt != 1500 * scale && flt_scale >= 1.0)
		fprintf (stderr, " Why is rowcnt = %d\n", rowcnt);

	minrow = (upd_num-1) * rowcnt + 1;
	if (flags & VERBOSE)
		fprintf (fdetail, " Deleting from %d for %d txns, upd_num =%ld\n", minrow, rowcnt, upd_num);


	gen_deletes (minrow, rowcnt, upd_num, special);


}


int
main (int ac, char **av)
{
	int i;
	int c;
	int status;
	FILE *ifp;
	char line[LINE_SIZE];
	double QppH;
	double QthH;
	double QphH;
	double sumoflogs;
	double maxtime;
	double mintime;
	struct timeb ThroughputStartTime;
	struct timeb ThroughputEndTime;
	struct timeb difTime;
	char tempstr[200];
	char tempstr2[20];
	char tempstr3[128];

	prog = av[0];
	rowcnt = 0;
	table = (1 << CUST) |
		(1 << SUPP) |
		(1 << NATION) |
		(1 << REGION) |
		(1 << PART_PSUPP) |
		(1 << ORDER_LINE);
	scale = 1;
	flt_scale = (float) 1.0;
	flags = 0;
	updates = 1;
	refresh = UPD_PCT;
	gen_sql = 0;
	gen_rng = 1;
	direct = 1;
	tdefs[ORDER].base *=
		ORDERS_PER_CUST;		/* have to do this after init */
	tdefs[LINE].base *=
		ORDERS_PER_CUST;		/* have to do this after init */
	tdefs[ORDER_LINE].base *=
		ORDERS_PER_CUST;		/* have to do this after init */
	children = 1;
	throughputstreams = 3;
	upd_num = 0;
	QppH = 0.0;
	QthH = 0.0;
	QphH = 0.0;

	ac1 = ac;
	av1 = av;


	process_options (ac, av);
	if (flags & VERBOSE)
		printf ("TPC-H Benchmark Driver, (matched to DBGEN/QGEN Version %s %d.%d.%d build %d)\n",
					  NAME, VERSION, RELEASE, PATCH, BUILD);
	fflush (stdout);



	if ((flags & SEED) == 0)
		if (flags & EVERYTHING)
			num_seeds = throughputstreams + 1;

	if (num_seeds > 1 && num_seeds < throughputstreams + 1 && (flags & EVERYTHING))
	{
		fprintf (stderr, " You didn't supply enough seed values \n");
		exit (1);
	}
	if (num_seeds > 1 && !(flags & EVERYTHING))
	{
		fprintf (stderr, " Supplying multiple seeds only makes sense if you are also use -e \n");
		exit (1);
	}

	setup ();
	/* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;

	for (i = 0; i <= QUERIES_PER_SET; i++)
		SaveSeed[i] = Seed[i];

	if (!(flags & DFLT))        /* preturb the RNG */
	{
		if (!(flags & SEED))
		{
			int j;
			for (j = MAX_STREAM; j >= 0; j--)
			{
				rndm = (long)((unsigned)time(NULL) * DSS_PROC) + j;
				if (rndm < 0)
					rndm += 2147483647;
				if (rndm == 0)
					rndm = 12345;
				
				initSeeds[j] = rndm;
			}
			
		}

		Seed[0].value = rndm;
		for (i=1; i <= QUERIES_PER_SET; i++)
		{
			Seed[0].value = NextRand(Seed[0].value);
			Seed[i].value = Seed[0].value;
		}
		if (num_seeds > 1 && (flags & EVERYTHING))
		{
			for (i = 0; i < num_seeds; i++)
			{
				printf (" -- Using %ld as seed for stream %d\n", initSeeds[i], i);
				if (initSeeds[i] == 0)
				{
					fprintf (stderr, " -- Seeds cannot be zero!\n");
					exit (1);
				}
			}
		}
		else
			printf("-- using %ld as a seed to the RNG\n", rndm);

		/*for (i=1; i <= QUERIES_PER_SET; i++)
		{
			Seed[i].value = rndm;
			UnifInt((DSS_HUGE)0L, (DSS_HUGE)100L, i);
			UnifInt((DSS_HUGE)0L, (DSS_HUGE)100L, i);
			UnifInt((DSS_HUGE)0L, (DSS_HUGE)100L, i);
			UnifInt((DSS_HUGE)0L, (DSS_HUGE)100L, i);
			UnifInt((DSS_HUGE)0L, (DSS_HUGE)100L, i);
		}*/


	}
	else
		printf("-- using default substitutions\n");



	if (!(flags & DBASE))
	{
		fprintf (stderr, " You must use the -n option to enter the logon to use\n");

		exit (1);
	}


	if (!(flags & OUTPUT))
	{
		osuff = malloc (strlen (".") + 1);
		MALLOC_CHECK (osuff);
		strcpy (osuff, ".");
	}


	if (snum >= 0)
		sprintf (tempstr, "%s/tpch_stream_%d_detailed_output.txt", osuff, snum);
	else
		sprintf (tempstr, "%s/tpch_detailed_output.txt", osuff);
#ifdef WIN32
	fdetail = fopen (tempstr, "wc");
#else
	fdetail = fopen (tempstr, "w");
#endif

	if (snum >= 0)
		sprintf (tempstr, "%s/tpch_stream_%d_summary_output.txt", osuff, snum);
	else
		sprintf (tempstr, "%s/tpch_summary_output.txt", osuff);
#ifdef WIN32
	fsummary = fopen (tempstr, "wc");
#else
	fsummary = fopen (tempstr, "w");
#endif

	if (fdetail == NULL || fsummary == NULL)
	{
		fprintf (stderr, " **** Fatal error, cannot open output files. errno=%d\n", errno);
		if (flags & OUTPUT)
			fprintf (stderr, " **** Probably directory %s does not exist or is not writable\n", osuff);
		exit (1);
	}

	signal (SIGABRT, stop_proc);
	signal (SIGTERM, stop_proc);
	signal (SIGINT, stop_proc);
#ifdef _WIN32
	signal (SIGBREAK, stop_proc);
#else
	signal (SIGTSTP, stop_proc);
#endif


	fprintf (fdetail, "\n TPC-H Benchmark Execution, detailed results\n");
	fprintf (fsummary, "\n TPC-H Benchmark Execution, summary results\n");
	{
		ftime (&StartTime);
		fprintf (fdetail, "\n Benchmark stream %d started on %s\n", snum, ctime (&StartTime.time));
		fprintf (fsummary, "\n Benchmark stream %d started on %s\n", snum, ctime (&StartTime.time));
	
	}

	fprintf (fdetail, "\n Using connection '%s' with schema name '%s'\n", db_name, schema_name);
	fprintf (fsummary, "\n Using connection '%s' with schema name '%s'\n", db_name, schema_name);

	fprintf (fsummary, " Using %ld as seed \n", rndm);
	fprintf (fdetail, " Using %ld as seed \n", rndm);

	fprintf (fsummary, " Scale factor is %g\n", flt_scale);
	fprintf (fdetail, " Scale factor is %g\n", flt_scale);

	/* Log on all sessions needed */
	Connect_Sessions ();

	/* Test to see the size of the tables */
	if (snum <= 0 && (optind >= ac1) && !(flags & EXPLAIN))
		Verify_Database ();

	for (i = 0; i <= QUERIES_PER_SET; i++)
	{
		QueryTime[i] = 0.0;
	}
	UF1time = 0.0;
	UF2time = 0.0;
	/* Run Power test warm-up, optional */
	/* run update function UF1 once on same query stream! */
	if (updates > 0)
	{

		/* Save Query Seeds */
		for (i = 0; i <= QUERIES_PER_SET; i++)
		{
			tempSeed = SaveSeed[i];
			SaveSeed[i] = Seed[i];
			Seed[i] = tempSeed;
		}

		{
			DSS_HUGE extra;
			set_state (ORDER, scale, children, children + 1, &extra); 
		}
		rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * scale * refresh);

		/*if (load_state (scale, children, children))
		{
		fprintf (stderr, "Unable to load seeds (%d scale)\n",
		scale);
		fprintf (stderr, "Either you need to use the -C option to match\n");
		fprintf (stderr, "the value used in DBGEN, or DBGEN was not run\n");
		fprintf (stderr, "Run './DBGEN -O s -s %d' and then rerun this program\n", scale);
		exit (-1);
		}*/


		/*if (specialtest)
		{
			upd_num = 0;
			system ("prfld");
			system ("prfstat");

			for (i = 1; i < 5; i++)
			{
				setsPerReq = i;
				fprintf (fdetail, " Testing with setsPerReq = %d \n", setsPerReq);

				upd_num = 1;
				Run_UF2_Deletes (1);
				fflush (stdout);
				fflush (stderr);
				fflush (fdetail);
				fflush (fsummary);

				sprintf (tempstr, "prfsnap InsertText%dtxns.log", setsPerReq);
				system (tempstr);
				upd_num = 0;
				Run_UF1_Inserts ();
				system (tempstr);
				fflush (stdout);
				fflush (stderr);
				fflush (fdetail);
				fflush (fsummary);

			}
			fflush (stdout);
			fflush (stderr);
			fclose (fsummary);
			fclose (fdetail);

			exit (1);
		}*/

		if ((flags & DFLT) || snum < 0 || flt_scale < 1.0)
		{
			/* Temp:  Pre-delete to clean up everything from previous failed runs */
			/* normally, this should delete zero rows */
			upd_num = updates;
			Run_UF2_Deletes (1);
			UF2time = 0.0;
		}
		upd_num = updates - 1;
		fflush (fdetail);
		fflush (fsummary);

		Run_UF1_Inserts ();


		/* Restore Query Seeds, Save Insert seeds */
		for (i = 0; i <= QUERIES_PER_SET; i++)
		{
			tempSeed = SaveSeed[i];
			SaveSeed[i] = Seed[i];
			Seed[i] = tempSeed;
		}
	}

	fflush (fdetail);
	fflush (fsummary);

	if (flags & INIT)			/* init stream with ifile */
	{
		ifp = fopen (ifile, "r");
		if (ifp == NULL)
		{
			fprintf (stderr, "Failed to open file '%s'\n", ifile);
			exit (1);
		}
		while (fgets (line, LINE_SIZE, ifp) != NULL)
			fprintf (stdout, "%s", line);
	}


	/* Run Power test, stream=0! */

	Run_Query_Stream ();

	/* Restore Insert seeds */
	for (i = 0; i <= QUERIES_PER_SET; i++)
	{
		tempSeed = SaveSeed[i];
		SaveSeed[i] = Seed[i];
		Seed[i] = tempSeed;
	}

	fflush (fdetail);
	fflush (fsummary);

	/* Run update function UF2 once on the same query stream! */

	if (updates > 0)
		if (snum < 0 || (flags & DFLT))
			Run_UF2_Deletes (1);
		else
			Run_UF2_Deletes (0);

	fprintf (stdout, " Stream %d finished on %s\n", snum, ctime (&StartTime.time));
	fprintf (fdetail, "\n Stream %d finished on %s\n", snum, ctime (&StartTime.time));

	Print_Stream_Summary ();

	fflush (fdetail);
	fflush (fsummary);
#ifdef WIN32
	_commit(_fileno(fdetail));
	_commit(_fileno(fsummary));
#else
	fsync(fileno(fdetail));
	fsync(fileno(fsummary));
#endif

	if (snum > 0)
	{
		if (flags & EVERYTHING)
		{
			Disconnect_Sessions ();

			fclose (fsummary);
			fclose (fdetail);

			return (0);
		}
	}

	maxtime = 0.0;
	mintime = 9e50;
	for (i = 1; i <= QUERIES_PER_SET; i++)
	{
		if (maxtime < QueryTime[i])
			maxtime = QueryTime[i];
		if (mintime > QueryTime[i] && QueryTime[i] > 0.0)
			mintime = QueryTime[i];
	}
	if (maxtime > 0.0)
	{
		if ((maxtime / mintime) > 1000.0)
		{
			fprintf (fsummary, " Ratio of max query time to min query time over 1000, special processing in effect\n");
			fprintf (fdetail, " Ratio of max query time to min query time over 1000, special processing in effect\n");
			for (i = 1; i < QUERIES_PER_SET; i++)
				if (QueryTime[i] < maxtime / 1000.0  && QueryTime[i] > 0.0)
					QueryTime[i] = maxtime / 1000.0;
		}
	}

	if (maxtime > 0.0)
	{
		int n = 0;
		sumoflogs = 0.0;
		for (i = 1; i <= QUERIES_PER_SET; i++)
		{
			if (QueryTime[i] > 0.0)
			{
				sumoflogs += log (QueryTime[i]);
				n++;
			}
		}
		if (updates!=0 || UF1time != 0.0 || UF2time != 0.0)
		{
			sumoflogs += log (UF1time) + log (UF2time);
			n += 2;
		}
		QppH = (3600.0 / exp (sumoflogs / (double)n)) * flt_scale;

		fprintf (fsummary, "\n TPC-H Power@%.1fGB = %.2f\n", flt_scale, QppH);
		fprintf (fdetail, "\n TPC-H Power@%.1fGB = %.2f\n", flt_scale, QppH);
		if (updates==0 && UF1time == 0.0 && UF2time == 0.0)
		{
			fprintf (fsummary, "(not including UF1 or UF2)\n");
			fprintf (fdetail, "(not including UF1 or UF2)\n");
		}
		if (n < 22)
		{
			fprintf (fsummary, "(not including all queries)\n");
			fprintf (fdetail, "(not including all queries)\n");
		}
	}

	/* Power test complete!  */


	if (snum == 0 &&
		(flags & EVERYTHING))
	{
		/*note: nothing is legal between power test and throughput test */
		/* Run throughput in parallel with a single update stream */
		/*note: stream 1 to s, where s= #streams */

		ftime (&StartTime);
		ThroughputStartTime.time = StartTime.time;
		ThroughputStartTime.millitm = StartTime.millitm;
		fprintf (stdout, " Beginning throughput test using %ld query streams on %s\n", throughputstreams, ctime (&StartTime.time));
		fprintf (fdetail, "\n Beginning throughput test using %ld query streams on %s\n", throughputstreams, ctime (&StartTime.time));

		pids = malloc (throughputstreams * sizeof (pid_t));

		for (c = 0; c < throughputstreams; c++)
		{
			sprintf (tempstr, "%d", c + 1);
			sprintf (tempstr2, "%f", flt_scale);
			if (c < num_seeds)
				sprintf (tempstr3, "%ld", initSeeds[c + 1]);
			else
				sprintf (tempstr3, "%ld", rndm);


#if (defined(WIN32)&&!defined(_POSIX_))
			pids[c] = _spawnl (_P_NOWAIT, av[0], av[0], "-p", tempstr, "-s", tempstr2, "-U", "0", "-r", tempstr3, "-n", db_name,
				"-w", schema_name,
				"-o", osuff,
				"-v",
				(char *) NULL);
			fprintf (stdout, " Spawned task, pid is %d\n", pids[c]);
			if (pids[c] == -1)
			{
				fprintf (stderr, "Child query stream not created %d\n", errno);
				exit (-1);
			}
#else
			pids[c] = SPAWN ();
			if (pids[c] == -1)
			{
				fprintf (stderr, "Child query stream not created");
				exit (-1);
			}
			else if (pids[c] == 0)	/* CHILD */
			{
				execl (av[0], av[0], "-p", tempstr, "-s", tempstr2, "-U", "0", "-r", tempstr3, "-n", db_name,
					"-w", schema_name,
					"-o", osuff,
					(char *) NULL);
				fprintf (stderr, " Could not execl to start the new task %d\n", errno);

				exit (1);
			}
			else
#endif
				if (flags & VERBOSE)	/* PARENT */
					fprintf (stdout, ".");
		}

		if (flags & VERBOSE)
			fprintf (stdout, "waiting...");

		fflush (stderr);
		fflush (fdetail);
		fflush (fsummary);
#ifdef WIN32
	_commit(_fileno(fdetail));
	_commit(_fileno(fsummary));
#else
	fsync(fileno(fdetail));
	fsync(fileno(fsummary));
#endif

		c = throughputstreams;
		while (c)
		{
#if (defined(WIN32)&&!defined(_POSIX_))
			i = _cwait (&status, pids[c - 1], _WAIT_CHILD);
			if (i == -1 && throughputstreams)
			{
				if (errno == ECHILD)
					fprintf (stderr, "Could not wait on pid %d\n", pids[c - 1]);
				else if (errno == EINTR)
					fprintf (stderr, "Process %d stopped abnormally\n", pids[c - 1]);
				else if (errno == EINVAL)
					fprintf (stderr, "Program bug\n");
			}
			else
			{
				fprintf (stdout, "Process %d: STOPPED\n", pids[c - 1]);
			}
#else
			i = wait (&status);
			if (i == -1 && throughputstreams)
			{
				fprintf (stderr, "We lost one\n");
				exit (-2);
			}
			if (status & 0xFF)
			{
				if ((status & 0xFF) == 0117)
					printf ("Process %d: STOPPED\n", i);
				else
					printf ("Process %d: rcvd signal %d\n",
					i, status & 0x7F);
			}
#endif
			c--;
		}

		ftime (&StartTime);
		fprintf (stdout, " Throughput query streams ended on %s, starting updates\n", ctime (&StartTime.time));
		fprintf (fdetail, "\n  Throughput query streams ended on %s, starting updates\n", ctime (&StartTime.time));

		if (updates > 0)
		{
			for (c = 0; c < throughputstreams; c++)
			{
				Run_UF1_Inserts ();
				if (snum < 0 || (flags & DFLT))
					Run_UF2_Deletes (1);
				else
					Run_UF2_Deletes (0);
			}
		}

		ftime (&ThroughputEndTime);

		difTime.time = ThroughputEndTime.time - ThroughputStartTime.time;
		if (ThroughputEndTime.millitm >= ThroughputStartTime.millitm)
		{
			difTime.millitm = (unsigned short) (ThroughputEndTime.millitm - ThroughputStartTime.millitm);
		}
		else
		{
			difTime.millitm = (unsigned short) ((1000 + ThroughputEndTime.millitm) - ThroughputStartTime.millitm);
			difTime.time -= 1;
		};
		Throughputtime = difTime.time + difTime.millitm / 1000.0 + 0.005;

		fprintf (stdout, " Throughput test ended on %s\n", ctime (&ThroughputEndTime.time));
		fprintf (fdetail, "\n  Throughput test ended on %s\n", ctime (&ThroughputEndTime.time));

		fprintf (fdetail, "\n  Throughput test took %.2f seconds\n", Throughputtime);

		//if (updates > 0)
		{
			QthH = ((throughputstreams * 22.0 * 3600.0) / Throughputtime) * flt_scale;

			fprintf (fsummary, "\n TPC-H Throughput@%.1fGB = %.2f\n", flt_scale, QthH);
			fprintf (fdetail, "\n TPC-H Throughput@%.1fGB = %.2f\n", flt_scale, QthH);
			if (updates==0)
			{
				fprintf (fsummary, "(not including UF1 or UF2)\n");
				fprintf (fdetail, "(not including UF1 or UF2)\n");
			}
		

			QphH = sqrt (QppH * QthH);

			fprintf (fsummary, "\n QphH@%.1fGB = %.2f\n", flt_scale, QphH);
			fprintf (fdetail, "\n QphH@%.1fGB = %.2f\n", flt_scale, QphH);
		}


	}







	if (flags & TERMINATE)		/* terminate stream with tfile */
	{
		ifp = fopen (tfile, "r");
		if (ifp == NULL)
		{
			fprintf (stderr, "Failed to open terminate file '%s'\n",
				tfile);
			exit (1);
		}
		while (fgets (line, LINE_SIZE, ifp) != NULL)
			fprintf (stdout, "%s", line);
	}

	fflush (fdetail);
	fflush (fsummary);

	Disconnect_Sessions ();

	fclose (fsummary);
	fclose (fdetail);

	return (0);
}
