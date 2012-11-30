/*
 * $Id$
 *
 * Revision History
 * ===================
 * $Log: driver.c,v $
 * Revision 1.6  2007/10/27 07:03:59  cktan
 * add -b load distributions path
 *
 * Revision 1.5  2007/10/27 01:29:56  cktan
 * output partial table
 *
 * Revision 1.4  2007/10/26 03:17:45  elam
 * remove -b -C -D -f -F -n flag
 *
 * Revision 1.3  2007/10/25 05:44:25  cktan
 * more cleanup
 *
 * Revision 1.2  2007/10/24 21:22:38  cktan
 * make dbgen only, no direct load
 *
 * Revision 1.1  2007/10/24 20:25:23  cktan
 * new
 *
 * Revision 1.6  2007/09/12 21:55:05  cmcdevitt
 * fix a lot of incorrect format specifiers, and fix some 64-bit issues
 *
 * Revision 1.5  2007/04/10 00:08:54  cmcdevitt
 * Load directly into partitioned orders and lineitem with -p option
 *
 * Revision 1.4  2007/04/08 19:32:46  cmcdevitt
 * Fix problem where we were loading too much data
 *
 * Revision 1.3  2007/04/07 08:10:40  cmcdevitt
 * Fixes for dbgen with large scale factors
 *
 * Revision 1.4  2006/04/26 23:01:10  jms
 * address update generation problems
 *
 * Revision 1.3  2005/10/28 02:54:35  jms
 * add release.h changes
 *
 * Revision 1.2  2005/01/03 20:08:58  jms
 * change line terminations
 *
 * Revision 1.1.1.1  2004/11/24 23:31:46  jms
 * re-establish external server
 *
 * Revision 1.5  2004/04/07 20:17:29  jms
 * bug #58 (join fails between order/lineitem)
 *
 * Revision 1.4  2004/02/18 16:26:49  jms
 * 32/64 bit changes for overflow handling needed additional changes when ported back to windows
 *
 * Revision 1.3  2004/01/22 05:49:29  jms
 * AIX porting (AIX 5.1)
 *
 * Revision 1.2  2004/01/22 03:54:12  jms
 * 64 bit support changes for customer address
 *
 * Revision 1.1.1.1  2003/08/08 21:50:33  jms
 * recreation after CVS crash
 *
 * Revision 1.3  2003/08/08 21:35:26  jms
 * first integration of rng64 for o_custkey and l_partkey
 *
 * Revision 1.2  2003/08/07 17:58:34  jms
 * Convery RNG to 64bit space as preparation for new large scale RNG
 *
 * Revision 1.1.1.1  2003/04/03 18:54:21  jms
 * initial checkin
 *
 *
 */
/* main driver for dss banchmark */

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#include "config.h"
#include "release.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))
#include <process.h>
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
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#endif

#include "dss.h"
#include "dsstypes.h"

/*
 * Function prototypes
 */
void	usage (void);
void	kill_load (void);
void	gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num);
int		pr_drange (int tbl, DSS_HUGE min, DSS_HUGE cnt, long num);
int		set_files (int t, int pload);
int		partial (int, int);


extern int optind, opterr;
extern char *optarg;
DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif


/*
 * general table descriptions. See dss.h for details on structure
 * NOTE: tables with no scaling info are scaled according to
 * another table
 *
 *
 * the following is based on the tdef structure defined in dss.h as:
 * typedef struct
 * {
 * char     *name;            -- name of the table; 
 *                               flat file output in <name>.tbl
 * long      base;            -- base scale rowcount of table; 
 *                               0 if derived
 * int       (*header) ();    -- function to prep output
 * int       (*loader[2]) (); -- functions to present output
 * long      (*gen_seed) ();  -- functions to seed the RNG
 * int       (*verify) ();    -- function to verfiy the data set without building it
 * int       child;           -- non-zero if there is an associated detail table
 * unsigned long vtotal;      -- "checksum" total 
 * }         tdef;
 *
 */

/*
 * flat file print functions; used with -F(lat) option
 */
int pr_cust (customer_t * c, int mode);
int pr_line (order_t * o, int mode);
int pr_order (order_t * o, int mode);
int pr_part (part_t * p, int mode);
int pr_psupp (part_t * p, int mode);
int pr_supp (supplier_t * s, int mode);
int pr_order_line (order_t * o, int mode);
int pr_part_psupp (part_t * p, int mode);
int pr_nation (code_t * c, int mode);
int pr_region (code_t * c, int mode);


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

/*
 * header output functions); used with -h(eader) option
 */
int hd_cust (FILE * f);
int hd_line (FILE * f);
int hd_order (FILE * f);
int hd_part (FILE * f);
int hd_psupp (FILE * f);
int hd_supp (FILE * f);
int hd_order_line (FILE * f);
int hd_part_psupp (FILE * f);
int hd_nation (FILE * f);
int hd_region (FILE * f);

/*
 * data verfication functions; used with -O v option
 */
int vrf_cust (customer_t * c, int mode);
int vrf_line (order_t * o, int mode);
int vrf_order (order_t * o, int mode);
int vrf_part (part_t * p, int mode);
int vrf_psupp (part_t * p, int mode);
int vrf_supp (supplier_t * s, int mode);
int vrf_order_line (order_t * o, int mode);
int vrf_part_psupp (part_t * p, int mode);
int vrf_nation (code_t * c, int mode);
int vrf_region (code_t * c, int mode);


tdef tdefs[] =
{
    {"part.tbl", "part table", 200000, hd_part,
     {pr_part, 0}, sd_part, vrf_part, PSUPP, 0},
    {"partsupp.tbl", "partsupplier table", 200000, hd_psupp,
     {pr_psupp, 0}, sd_psupp, vrf_psupp, NONE, 0},
    {"supplier.tbl", "suppliers table", 10000, hd_supp,
     {pr_supp, 0}, sd_supp, vrf_supp, NONE, 0},
    {"customer.tbl", "customers table", 150000, hd_cust,
     {pr_cust, 0}, sd_cust, vrf_cust, NONE, 0},
    {"orders.tbl", "order table", 150000, hd_order,
     {pr_order, 0}, sd_order, vrf_order, LINE, 0},
    {"lineitem.tbl", "lineitem table", 150000, hd_line,
     {pr_line, 0}, sd_line, vrf_line, NONE, 0},
    {"orders.tbl", "orders/lineitem tables", 150000, hd_order_line,
     {pr_order_line, 0}, sd_order, vrf_order_line, LINE, 0},
    {"part.tbl", "part/partsupplier tables", 200000, hd_part_psupp,
     {pr_part_psupp, 0}, sd_part, vrf_part_psupp, PSUPP, 0},
    {"nation.tbl", "nation table", NATIONS_MAX, hd_nation,
     {pr_nation, 0}, NO_LFUNC, vrf_nation, NONE, 0},
    {"region.tbl", "region table", NATIONS_MAX, hd_region,
     {pr_region, 0}, NO_LFUNC, vrf_region, NONE, 0},
};



/*
 * routines to handle the graceful cleanup of multi-process loads
 */

void
    stop_proc (int signum)
{
    exit (0);
}


/*
 * re-set default output file names 
 */
int
    set_files (int i, int pload)
{
    char line[80], *new_name;
	
    if (o_table & (1 << i))
	child_table:
	{
	    if (pload != -1)
		sprintf (line, "%s.%d", tdefs[i].name, pload);
	    else {
		printf ("Enter new destination for %s data: ",
			tdefs[i].name);
		if (fgets (line, sizeof (line), stdin) == NULL)
		    return (-1);;
		if ((new_name = strchr (line, '\n')) != NULL)
		    *new_name = '\0';
		if (strlen (line) == 0)
		    return (0);
	    }
	    new_name = (char *) malloc (strlen (line) + 1);
	    MALLOC_CHECK (new_name);
	    strcpy (new_name, line);
	    tdefs[i].name = new_name;
	    if (tdefs[i].child != NONE) {
		i = tdefs[i].child;
		tdefs[i].child = NONE;
		goto child_table;
	    }
	}
	
    return (0);
}



/*
 * read the distributions needed in the benchamrk
 */
void
    load_dists (void)
{
    read_dist (env_config (DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "colors", &colors);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "nations", &nations);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "regions", &regions);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "o_oprio",
	       &o_priority_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "instruct",
	       &l_instruct_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "category",
	       &l_category_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
    read_dist (env_config (DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

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
	
}

/*
 * generate a particular table
 */
void
    gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
{
    static order_t o;
    supplier_t supp;
    customer_t cust;
    part_t part;
    code_t code;
    static int completed = 0;
    DSS_HUGE i;

    DSS_HUGE rows_per_segment=0;
    DSS_HUGE rows_this_segment=-1;
    DSS_HUGE residual_rows=0;

    if (insert_segments) {
	rows_per_segment = count / insert_segments;
	residual_rows = count - (rows_per_segment * insert_segments);
    }

    for (i = start; count; count--, i++) {
	LIFENOISE (1000, i);
	row_start(tnum);
		
	switch (tnum) {
	case LINE:
	case ORDER:
	case ORDER_LINE: 
	    if (mk_order (i, &o, upd_num % 10000)==1)
		continue;
			
	    if (insert_segments  && (upd_num > 0)) {
		if((upd_num / 10000) < residual_rows) {
		    if((++rows_this_segment) > rows_per_segment) {						
			rows_this_segment=0;
			upd_num += 10000;					
		    }
		}
		else {
		    if((++rows_this_segment) >= rows_per_segment) {
			rows_this_segment=0;
			upd_num += 10000;
		    }
		}
	    }

	    if (o_set_seeds == 0) {
		if (o_validate)
		    tdefs[tnum].verify(&o, 0);
		else
		    tdefs[tnum].loader[o_direct] (&o, upd_num);
	    }
	    break;
	case SUPP:
	    if (mk_supp (i, &supp)==1)
		continue;
			
	    if (o_set_seeds == 0) {
		if (o_validate)
		    tdefs[tnum].verify(&supp, 0);
		else
		    tdefs[tnum].loader[o_direct] (&supp, upd_num);
	    }
	    break;
	case CUST:
	    if (mk_cust (i, &cust)==1)
		continue;
				
	    if (o_set_seeds == 0) {
		if (o_validate)
		    tdefs[tnum].verify(&cust, 0);
		else
		    tdefs[tnum].loader[o_direct] (&cust, upd_num);
	    }
	    break;
	case PSUPP:
	case PART:
	case PART_PSUPP: 
	    if (mk_part (i, &part)==1)
		continue;
			
	    if (o_set_seeds == 0) {
		if (o_validate)
		    tdefs[tnum].verify(&part, 0);
		else
		    tdefs[tnum].loader[o_direct] (&part, upd_num);
	    }
	    break;
	case NATION:
	    if (mk_nation (i, &code)==1)
		continue;
	    if (o_set_seeds == 0) {
		if (o_validate)
		    tdefs[tnum].verify(&code, 0);
		else
		    tdefs[tnum].loader[o_direct] (&code, 0);
	    }
	    break;
	case REGION:
	    if (mk_region (i, &code)==1)
		continue;
	    if (o_set_seeds == 0) {
		if (o_validate)
		    tdefs[tnum].verify(&code, 0);
		else
		    tdefs[tnum].loader[o_direct] (&code, 0);
	    }
	    break;
	}
	row_stop(tnum);
	if (o_set_seeds && (i % tdefs[tnum].base) < 2) {
	    printf("\nSeeds for %s at rowcount %lld\n", tdefs[tnum].comment, i);
	    dump_seeds(tnum);
	}
    }
    completed |= 1 << tnum;
}



void
    usage (void)
{
    fprintf (stderr, "%s\n%s\n\t%s\n%s %s\n\n",
	     "USAGE:",
	     "dbgen [-{vfFD}] [-O {fhmsv}][-T {pcsoPSOL}]",
	     "[-s <scale>][-C <procs>][-S <step>]",
	     "dbgen [-v] [-O {dfhmr}] [-s <scale>]",
	     "[-U <updates>] [-r <percent>]");
    fprintf (stderr, "-v     -- enable VERBOSE mode\n");
    fprintf (stderr, "-b <s> -- load distributions for <s>\n");
    fprintf (stderr, "-d <n> -- split deletes between <n> files\n");
    fprintf (stderr, "-F     -- generate flat files output\n");
    fprintf (stderr, "-h     -- display this message\n");
    fprintf (stderr, "-i <n> -- split inserts between <n> files\n");
    fprintf (stderr, "-O d   -- generate SQL syntax for deletes\n");
    fprintf (stderr, "-O f   -- over-ride default output file names\n");
    fprintf (stderr, "-O h   -- output files with headers\n");
    fprintf (stderr, "-O m   -- produce columnar output\n");
    fprintf (stderr, "-O r   -- generate key ranges for deletes.\n");
    fprintf (stderr, "-O v   -- Verify data set without generating it.\n");
    fprintf (stderr, "-q     -- enable QUIET mode\n");
    fprintf (stderr, "-r <n> -- updates refresh (n/100)%% of the\n");
    fprintf (stderr, "          data set\n");
    fprintf (stderr, "-s <n> -- set Scale Factor (SF) to  <n> \n");
    fprintf (stderr, "-T c   -- generate cutomers ONLY\n");
    fprintf (stderr, "-T L   -- generate lineitem ONLY\n");
    fprintf (stderr, "-T n   -- generate nation ONLY\n");
    fprintf (stderr, "-T O   -- generate orders ONLY\n");
    fprintf (stderr, "-T P   -- generate parts ONLY\n");
    fprintf (stderr, "-T r   -- generate region ONLY\n");
    fprintf (stderr, "-T s   -- generate suppliers ONLY\n");
    fprintf (stderr, "-T S   -- generate partsupp ONLY\n");
    fprintf (stderr, "-U <s> -- generate <s> update sets\n");

    fprintf (stderr, "-n <n> -- build the <n>th step of the N data/update set\n");
    fprintf (stderr, "-N <n> -- split work into N steps\n");

    fprintf (stderr,
	     "\nTo generate the SF=1 (1GB), validation database population, use:\n");
    fprintf (stderr, "\tdbgen -vfF -s 1\n");
    fprintf (stderr, "\nTo generate updates for a SF=1 (1GB), use:\n");
    fprintf (stderr, "\tdbgen -v -U 1 -s 1\n");
}

/*
 * int partial(int tbl, int s) -- generate the s-th part of the named tables data
 */
int
    partial (int tbl, int s)
{
    DSS_HUGE rowcnt;
    DSS_HUGE extra;
	
    if (o_verbose > 0) {
	fprintf (stderr, "\tStarting to load stage %d of %ld for %s...",
		 s, o_stepmax, tdefs[tbl].comment);
    }
	
    if (o_direct == 0)
	set_files (tbl, s);
	
    rowcnt = set_state(tbl, o_scale, o_stepmax, s, &extra);

    if (s == o_stepmax)
	gen_tbl (tbl, rowcnt * (s - 1) + 1, rowcnt + extra, upd_num);
    else
	gen_tbl (tbl, rowcnt * (s - 1) + 1, rowcnt, upd_num);
	
    if (o_verbose > 0)
	fprintf (stderr, "done.\n");
	
    return (0);
}


void
    process_options (int count, char **vector)
{
    int option;

	
    while ((option = getopt (count, vector,
			     "b:hr:U:O:s:P:T:vN:n:d:i:q")) != -1)
	switch (option) {
	case 'b':				/* load distributions from named file */
	    o_d_path = (char *)malloc(strlen(optarg) + 1);
	    MALLOC_CHECK(o_d_path);
	    strcpy(o_d_path, optarg);
	    break;
	case 'q':				/* all prompts disabled */
	    o_verbose = -1;
	    break;
	case 'i':
	    insert_segments = atoi (optarg);
	    break;
	case 'd':
	    delete_segments = atoi (optarg);
	    break;
	case 'n':				/* generate a particular STEP */
	    o_step = atoi (optarg);
	    break;
	case 'N':				/* total steps */
	    o_stepmax = atoi (optarg);
	    break;
	case 'v':				/* life noises enabled */
	    o_verbose = 1;
	    break;
	case 'T':				/* generate a specifc table */
	    switch (*optarg) {
	    case 'c':			/* generate customer ONLY */
		o_table = 1 << CUST;
		break;
	    case 'L':			/* generate lineitems ONLY */
		o_table = 1 << LINE;
		break;
	    case 'n':			/* generate nation table ONLY */
		o_table = 1 << NATION;
		break;
	    case 'O':			/* generate orders ONLY */
		o_table = 1 << ORDER;
		break;
	    case 'P':			/* generate part ONLY */
		o_table = 1 << PART;
		break;
	    case 'r':			/* generate region table ONLY */
		o_table = 1 << REGION;
		break;
	    case 'S':			/* generate partsupp ONLY */
		o_table = 1 << PSUPP;
		break;
	    case 's':			/* generate suppliers ONLY */
		o_table = 1 << SUPP;
		break;
	    default:
		fprintf (stderr, "Unknown table name %s\n",
			 optarg);
		usage ();
		exit (1);
	    }
	    break;
	case 's':				/* scale by Percentage of base rowcount */
	case 'P':				/* for backward compatibility */
	    flt_scale = atof (optarg);
	    if (flt_scale < MIN_SCALE) {
		int i;
		int int_scale;
				
		o_scale = 1;
		int_scale = (int)(1000 * flt_scale);
		for (i = PART; i < REGION; i++) {
		    tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base)/1000;
		    if (tdefs[i].base < 1)
			tdefs[i].base = 1;
		}
	    }
	    else
		o_scale = (long) flt_scale;
	    if (o_scale > MAX_SCALE) {
		fprintf (stderr, "%s %5.0f %s\n\t%s\n\n",
			 "NOTE: Data generation for scale factors >",
			 MAX_SCALE,
			 "GB is still in development,",
			 "and is not yet supported.\n");
		fprintf (stderr,
			 "Your resulting data set MAY NOT BE COMPLIANT!\n");
	    }
	    break;
	case 'O':				/* optional actions */
	    switch (tolower (*optarg)) {
	    case 'd':			/* generate SQL for deletes */
		o_gen_sql = 1;
		break;
	    case 'f':			/* over-ride default file names */
		o_fnames = 1;
		break;
	    case 'h':			/* generate headers */
		o_header = 1;
		break;
	    case 'm':			/* generate columnar output */
		o_columnar = 1;
		break;
	    case 'r':			/* generate key ranges for delete */
		o_gen_rng = 1;
		break;
	    case 's':			/* calibrate the RNG usage */
		o_set_seeds = 1;
		break;
	    case 'v':			/* validate the data set */
		o_validate = 1;
		break;
	    default:
		fprintf (stderr, "Unknown option name %s\n",
			 optarg);
		usage ();
		exit (1);
	    }
	    break;
	case 'U':				/* generate flat files for update stream */
	    o_updates = atoi (optarg);
	    break;
	case 'r':				/* set the refresh (update) percentage */
	    o_refresh = atoi (optarg);
	    break;
	default:
	    printf ("ERROR: option '%c' unknown.\n",
		    *(vector[optind] + 1));
	case 'h':				/* something unexpected */
	    fprintf (stderr,
		     "%s Population Generator (Version %d.%d.%d build %d)\n",
		     NAME, VERSION, RELEASE, PATCH, BUILD);
	    fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);
	    usage ();
	    exit (1);
	}

    return;
}

/*
 * MAIN
 *
 * assumes the existance of getopt() to clean up the command 
 * line handling
 */
int
    main (int ac, char **av)
{
    DSS_HUGE i;
	
    o_table = 0;
    o_force = 0;
    insert_segments=0;
    delete_segments=0;
    insert_orders_segment=0;
    insert_lineitem_segment=0;
    delete_segment=0;
    o_verbose = 0;
    o_columnar = 0;
    o_set_seeds = 0;
    o_header = 0;
    o_direct = 0;
    o_scale = 1;
    flt_scale = 1.0;
    o_updates = 0;
    o_refresh = UPD_PCT;
    tdefs[ORDER].base *=
	ORDERS_PER_CUST;			/* have to do this after init */
    tdefs[LINE].base *=
	ORDERS_PER_CUST;			/* have to do this after init */
    tdefs[ORDER_LINE].base *=
	ORDERS_PER_CUST;			/* have to do this after init */
    o_fnames = 0;
    o_db_name = NULL;
    o_schema_name = NULL;
    o_gen_sql = 0;
    o_gen_rng = 0;
    o_d_path = NULL;

    o_step = 1;
    o_stepmax = 1;

	
#ifdef NO_SUPPORT
    signal (SIGINT, exit);
#endif /* NO_SUPPORT */

    process_options (ac, av);
    if (o_table == 0) {
	usage();
	fprintf(stderr, "Error: please specify -T option\n");
	exit(1);
    }
    if (o_stepmax <= 0) {
	usage();
	fprintf(stderr, "Error: invalid -N option\n");
	exit(1);
    }
    if (! (0 < o_step && o_step <= o_stepmax)) {
	usage();
	fprintf(stderr, "Error: invalid -n option\n");
	exit(1);
    }
    if (o_verbose >= 0) {
	fprintf (stderr,
		 "%s Population Generator (Version %d.%d.%d build %d)\n",
		 NAME, VERSION, RELEASE, PATCH, BUILD);
	fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);
    }
	
    load_dists ();
    /* have to do this after init */
    tdefs[NATION].base = nations.count;
    tdefs[REGION].base = regions.count;
	
    /* 
     * updates are never parallelized 
     */
    if (o_updates) {
	/* 
	 * set RNG to start generating rows beyond SF=scale
	 */
	set_state (ORDER, o_scale, o_stepmax, o_stepmax + 1, &i); 
	rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * o_scale * o_refresh);
	if (o_step > 0) {
	    /* 
	     * adjust RNG for any prior update generation
	     */
	    for (i=1; i < o_step; i++) {
		sd_order(0, rowcnt);
		sd_line(0, rowcnt);
	    }
	    upd_num = o_step - 1;
	}
	else
	    upd_num = 0;

	while (upd_num < o_updates) {
	    if (o_verbose > 0)
		fprintf (stderr,
			 "Generating update pair #%ld for %s [pid: %d]",
			 upd_num + 1, tdefs[ORDER_LINE].comment, (int)DSS_PROC);
	    insert_orders_segment=0;
	    insert_lineitem_segment=0;
	    delete_segment=0;
	    minrow = upd_num * rowcnt + 1;
	    gen_tbl (ORDER_LINE, minrow, rowcnt, upd_num + 1);
	    if (o_verbose > 0)
		fprintf (stderr, "done.\n");
	    pr_drange (ORDER_LINE, minrow, rowcnt, upd_num + 1);
	    upd_num++;
	}

	exit (0);
    }
	
    /**
     ** actual data generation section starts here
     **/
    /*
     * open database connection or set all the file names, as appropriate
     */
    if (o_fnames)
	for (i = PART; i <= REGION; i++) {
	    if (o_table & (1 << i))
		if (set_files ((int)i, -1)) {
		    fprintf (stderr, "Load aborted!\n");
		    exit (1);
		}
	}
		
    /*
     * traverse the tables, invoking the appropriate data generation routine for any to be built
     */
    for (i = PART; i <= REGION; i++) {
	if (0 == (o_table & (1 << i)))
	    continue;
		
	minrow = 1;
	if (i < NATION)
	    rowcnt = tdefs[i].base * o_scale;
	else
	    rowcnt = tdefs[i].base;
	if (o_verbose > 0)
	    fprintf (stderr, "%s data for %s [pid: %ld, step: %d]",
		     (o_validate)?"Validating":"Generating", tdefs[i].comment, (long)DSS_PROC, o_step);

	if (i < NATION) {
	    partial(i, o_step);
	}
	else {
	    gen_tbl ((int)i, minrow, rowcnt, upd_num);
	}

	if (o_verbose > 0)
	    fprintf (stderr, "done.\n");
		
	if (o_validate)
	    printf("Validation checksum for %s at %ld GB: %0x\n", 
		   tdefs[i].name, o_scale, (unsigned int) tdefs[i].vtotal);
    }
	
    return (0);
}

