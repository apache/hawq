/*-------------------------------------------------------------------------
 *
 * pg_regress --- regression test driver
 *
 * This is a C implementation of the previous shell script for running
 * the regression tests, and should be mostly compatible with it.
 * Initial author of C translation: Magnus Hagander
 *
 * This code is released under the terms of the PostgreSQL License.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/test/regress/pg_regress.c,v 1.36 2007/07/18 21:19:17 alvherre Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "pg_regress.h"

#include <ctype.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/time.h>
#include <sys/resource.h>
#endif

#include "getopt_long.h"
#include "pg_config_paths.h"

/* for resultmap we need a list of pairs of strings */
typedef struct _resultmap
{
	char	   *test;
	char	   *type;
	char	   *resultfile;
	struct _resultmap *next;
}	_resultmap;

/*
 * Values obtained from pg_config_paths.h and Makefile.
 * In non-temp_install mode, the only thing we need is the location of psql,
 * which we expect to find in psqldir, or in the PATH if psqldir isn't given.
 *
 * XXX Because pg_regress is not installed in bindir, we can't support
 * this for relocatable trees as it is.  --psqldir would need to be
 * specified in those cases.
 */
char	   *bindir = PGBINDIR;
char	   *libdir = LIBDIR;
char	   *host_platform = HOST_TUPLE;

#ifndef WIN32					/* not used in WIN32 case */
static char *shellprog = SHELLPROG;
#endif

/* currently we can use the same diff switches on all platforms */
/* MPP:  Add stuff to ignore all the extra NOTICE messages we give */
const char *basic_diff_opts = "-w -I NOTICE: -I HINT: -I CONTEXT: -I GP_IGNORE:";
const char *pretty_diff_opts = "-w -I NOTICE: -I HINT: -I CONTEXT: -I GP_IGNORE: -C3";

/* options settable from command line */
_stringlist *dblist = NULL;
bool		debug = false;
char	   *inputdir = ".";
char	   *outputdir = ".";
char	   *psqldir = PGBINDIR;
bool 		optimizer_enabled = false;
static _stringlist *loadlanguage = NULL;
static int	max_connections = 0;
static char *encoding = NULL;
static _stringlist *schedulelist = NULL;
static _stringlist *extra_tests = NULL;
static char *top_builddir = NULL;
static int	temp_port = 65432;
static bool nolocale = false;
static char *hostname = NULL;
static int	port = -1;
static char *user = NULL;
static char *srcdir = NULL;
static _stringlist *extraroles = NULL;
static char *initfile = "./init_file";
static char *tablespace = "";

/* internal variables */
static const char *progname;
static char *logfilename;
static FILE *logfile;
static char *difffilename;

static _resultmap *resultmap = NULL;

static int	success_count = 0;
static int	fail_count = 0;
static int	fail_ignore_count = 0;

static bool directory_exists(const char *dir);
static void make_directory(const char *dir);

static void create_database(const char *dbname);
static void drop_database_if_exists(const char *dbname);

static int
run_diff(const char *cmd, const char *filename);

static void
header(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));
static void
status(const char *fmt,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 1, 2)));
static void
psql_command(const char *database, const char *query,...)
/* This extension allows gcc to check the format string for consistency with
   the supplied arguments. */
__attribute__((format(printf, 2, 3)));

#ifdef WIN32
typedef BOOL (WINAPI * __CreateRestrictedToken) (HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);

/* Windows API define missing from MingW headers */
#define DISABLE_MAX_PRIVILEGE	0x1
#endif

/*
 * allow core files if possible.
 */
#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
static void
unlimit_core_size(void)
{
	struct rlimit lim;

	getrlimit(RLIMIT_CORE, &lim);
	if (lim.rlim_max == 0)
	{
		fprintf(stderr,
				_("%s: could not set core size: disallowed by hard limit\n"),
				progname);
		return;
	}
	else if (lim.rlim_max == RLIM_INFINITY || lim.rlim_cur < lim.rlim_max)
	{
		lim.rlim_cur = lim.rlim_max;
		setrlimit(RLIMIT_CORE, &lim);
	}
}
#endif


/*
 * Add an item at the end of a stringlist.
 */
void
add_stringlist_item(_stringlist ** listhead, const char *str)
{
	_stringlist *newentry = malloc(sizeof(_stringlist));
	_stringlist *oldentry;

	newentry->str = strdup(str);
	newentry->next = NULL;
	if (*listhead == NULL)
		*listhead = newentry;
	else
	{
		for (oldentry = *listhead; oldentry->next; oldentry = oldentry->next)
			 /* skip */ ;
		oldentry->next = newentry;
	}
}

/*
 * Free a stringlist.
 */
static void
free_stringlist(_stringlist ** listhead)
{
	if (listhead == NULL || *listhead == NULL)
		return;
	if ((*listhead)->next != NULL)
		free_stringlist(&((*listhead)->next));
	free((*listhead)->str);
	free(*listhead);
	*listhead = NULL;
}

/*
 * Split a delimited string into a stringlist
 */
static void
split_to_stringlist(const char *s, const char *delim, _stringlist ** listhead)
{
	char	   *sc = strdup(s);
	char	   *token = strtok(sc, delim);

	while (token)
	{
		add_stringlist_item(listhead, token);
		token = strtok(NULL, delim);
	}
	free(sc);
}

/*
 * Print a progress banner on stdout.
 */
static void
header(const char *fmt,...)
{
	char		tmp[64];
	va_list		ap;

	va_start(ap, fmt);
	vsnprintf(tmp, sizeof(tmp), fmt, ap);
	va_end(ap);

	fprintf(stdout, "============== %-38s ==============\n", tmp);
	fflush(stdout);
}

/*
 * Print "doing something ..." --- supplied text should not end with newline
 */
static void
status(const char *fmt,...)
{
	va_list		ap;

	va_start(ap, fmt);
	vfprintf(stdout, fmt, ap);
	fflush(stdout);
	va_end(ap);

	if (logfile)
	{
		va_start(ap, fmt);
		vfprintf(logfile, fmt, ap);
		va_end(ap);
	}
}

/*
 * Done "doing something ..."
 */
static void
status_end(void)
{
	fprintf(stdout, "\n");
	fflush(stdout);
	if (logfile)
		fprintf(logfile, "\n");
}

/*
 * Always exit through here, not through plain exit()
 */
void
exit_nicely(int code)
{
	exit(code);
}

/*
 * Check whether string matches pattern
 *
 * In the original shell script, this function was implemented using expr(1),
 * which provides basic regular expressions restricted to match starting at
 * the string start (in conventional regex terms, there's an implicit "^"
 * at the start of the pattern --- but no implicit "$" at the end).
 *
 * For now, we only support "." and ".*" as non-literal metacharacters,
 * because that's all that anyone has found use for in resultmap.  This
 * code could be extended if more functionality is needed.
 */
static bool
string_matches_pattern(const char *str, const char *pattern)
{
	while (*str && *pattern)
	{
		if (*pattern == '.' && pattern[1] == '*')
		{
			pattern += 2;
			/* Trailing .* matches everything. */
			if (*pattern == '\0')
				return true;

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.
			 */
			while (*str)
			{
				/*
				 * Optimization to prevent most recursion: don't recurse
				 * unless first pattern char might match this text char.
				 */
				if (*str == *pattern || *pattern == '.')
				{
					if (string_matches_pattern(str, pattern))
						return true;
				}

				str++;
			}

			/*
			 * End of text with no match.
			 */
			return false;
		}
		else if (*pattern != '.' && *str != *pattern)
		{
			/*
			 * Not the single-character wildcard and no explicit match? Then
			 * time to quit...
			 */
			return false;
		}

		str++;
		pattern++;
	}

	if (*pattern == '\0')
		return true;			/* end of pattern, so declare match */

	/* End of input string.  Do we have matching pattern remaining? */
	while (*pattern == '.' && pattern[1] == '*')
		pattern += 2;
	if (*pattern == '\0')
		return true;			/* end of pattern, so declare match */

	return false;
}

/*
 * Replace all occurances of a string in a string with a different string.
 * NOTE: Assumes there is enough room in the target buffer!
 */
void
replace_string(char *string, char *replace, char *replacement)
{
	char	   *ptr;

	while ((ptr = strstr(string, replace)) != NULL)
	{
		char	   *dup = strdup(string);

		strlcpy(string, dup, ptr - string + 1);
		strcat(string, replacement);
		strcat(string, dup + (ptr - string) + strlen(replace));
		free(dup);
	}
}

/*
 * Convert *.source found in the "source" directory, replacing certain tokens
 * in the file contents with their intended values, and put the resulting files
 * in the "dest" directory, replacing the ".source" prefix in their names with
 * the given suffix.
 */
static void
convert_sourcefiles_in(char *source, char *dest, char *suffix)
{
	char	abs_srcdir[MAXPGPATH];
	char	abs_builddir[MAXPGPATH];
	char	testtablespace[MAXPGPATH];
	char	indir[MAXPGPATH];
	char    outdir[MAXPGPATH];
	char  **name;
	char  **names;
	int		count = 0;
#ifdef WIN32
	char *c;
#endif

	if (!getcwd(abs_builddir, sizeof(abs_builddir)))
	{
		fprintf(stderr, _("%s: could not get current directory: %s\n"),
			progname, strerror(errno));
		exit_nicely(2);
	}

	/*
	 * in a VPATH build, use the provided source directory; otherwise, use the
	 * current directory.
	 */
	if (srcdir)
		strlcpy(abs_srcdir, srcdir, MAXPGPATH);
	else
		strlcpy(abs_srcdir, abs_builddir, MAXPGPATH);

	snprintf(indir, MAXPGPATH, "%s/%s", abs_srcdir, source);
	names = pgfnames(indir);
	if (!names)
		/* Error logged in pgfnames */
		exit_nicely(2);

	/* also create the output directory if not present */
	snprintf(outdir, sizeof(outdir), "%s/%s", abs_srcdir, dest);
	if (!directory_exists(outdir))
		make_directory(outdir);

#ifdef WIN32
	/* in Win32, replace backslashes with forward slashes */
	for (c = abs_builddir; *c; c++)
		if (*c == '\\')
			*c = '/';
	for (c = abs_srcdir; *c; c++)
		if (*c == '\\')
			*c = '/';
#endif

	/* try to create the test tablespace dir if it doesn't exist */
	snprintf(testtablespace, MAXPGPATH, "%s/testtablespace", abs_builddir);

#ifdef WIN32
	/*
	 * On Windows only, clean out the test tablespace dir, or create it if it
	 * doesn't exist.  On other platforms we expect the Makefile to take
	 * care of that.  (We don't migrate that functionality in here because
	 * it'd be harder to cope with platform-specific issues such as SELinux.)
	 *
	 * XXX it would be better if pg_regress.c had nothing at all to do with
	 * testtablespace, and this were handled by a .BAT file or similar on
	 * Windows.  See pgsql-hackers discussion of 2008-01-18.
	 */
	if (directory_exists(testtablespace))
		rmtree(testtablespace, true);
	make_directory(testtablespace);
#endif

	/* finally loop on each file and do the replacement */
	for (name = names; *name; name++)
	{
		char		srcfile[MAXPGPATH];
		char		destfile[MAXPGPATH];
		char		prefix[MAXPGPATH];
		FILE	   *infile,
				   *outfile;
		char		line[1024];

		/* reject filenames not finishing in ".source" */
		if (strlen(*name) < 8)
			continue;
		if (strcmp(*name + strlen(*name) - 7, ".source") != 0)
			continue;

		count++;

		/* build the full actual paths to open */
		snprintf(prefix, strlen(*name) - 6, "%s", *name);
		snprintf(srcfile, MAXPGPATH, "%s/%s", indir, *name);
		snprintf(destfile, MAXPGPATH, "%s/%s.%s", outdir, prefix, suffix);

		infile = fopen(srcfile, "r");
		if (!infile)
		{
			fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
					progname, srcfile, strerror(errno));
			exit_nicely(2);
		}
		outfile = fopen(destfile, "w");
		if (!outfile)
		{
			fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"),
					progname, destfile, strerror(errno));
			exit_nicely(2);
		}
		while (fgets(line, sizeof(line), infile))
		{
			replace_string(line, "@abs_srcdir@", abs_srcdir);
			replace_string(line, "@abs_builddir@", abs_builddir);
			replace_string(line, "@testtablespace@", testtablespace);
			replace_string(line, "@DLSUFFIX@", DLSUFFIX);
			fputs(line, outfile);
		}
		fclose(infile);
		fclose(outfile);
		{
			char		cmd[MAXPGPATH * 3];
			snprintf(cmd, sizeof(cmd),
					 SYSTEMQUOTE "gpstringsubs.pl %s" SYSTEMQUOTE, destfile);
			if (run_diff(cmd, destfile) != 0)
			{
				fprintf(stderr, _("%s: could not convert %s\n"),
						progname, destfile);
			}
		}

	}

	/*
	 * If we didn't process any files, complain because it probably means
	 * somebody neglected to pass the needed --inputdir argument.
	 */
	if (count <= 0)
	{
		fprintf(stderr, _("%s: no *.source files found in \"%s\"\n"),
				progname, indir);
		exit_nicely(2);
	}

	pgfnames_cleanup(names);
}

/* Create the .sql and .out files from the .source files, if any */
static void
convert_sourcefiles(void)
{
	struct stat	st;
	int		ret;

	ret = stat("input", &st);
	if (ret == 0 && S_ISDIR(st.st_mode))
		convert_sourcefiles_in("input", "sql", "sql");

	ret = stat("output", &st);
	if (ret == 0 && S_ISDIR(st.st_mode))
		convert_sourcefiles_in("output", "expected", "out");
}

/*
 * Scan resultmap file to find which platform-specific expected files to use.
 *
 * The format of each line of the file is
 *		   testname/hostplatformpattern=substitutefile
 * where the hostplatformpattern is evaluated per the rules of expr(1),
 * namely, it is a standard regular expression with an implicit ^ at the start.
 * (We currently support only a very limited subset of regular expressions,
 * see string_matches_pattern() above.)  What hostplatformpattern will be
 * matched against is the config.guess output.	(In the shell-script version,
 * we also provided an indication of whether gcc or another compiler was in
 * use, but that facility isn't used anymore.)
 */
static void
load_resultmap(void)
{
	char		buf[MAXPGPATH];
	FILE	   *f;

	/* scan the file ... */
	snprintf(buf, sizeof(buf), "%s/resultmap", inputdir);
	f = fopen(buf, "r");
	if (!f)
	{
		/* OK if it doesn't exist, else complain */
		if (errno == ENOENT)
			return;
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, buf, strerror(errno));
		exit_nicely(2);
	}

	while (fgets(buf, sizeof(buf), f))
	{
		char	   *platform;
		char	   *file_type;
		char	   *expected;
		int			i;

		/* strip trailing whitespace, especially the newline */
		i = strlen(buf);
		while (i > 0 && isspace((unsigned char) buf[i - 1]))
			buf[--i] = '\0';

		/* parse out the line fields */
		file_type = strchr(buf, ':');
		if (!file_type)
		{
			fprintf(stderr, _("incorrectly formatted resultmap entry: %s\n"),
					buf);
			exit_nicely(2);
		}
		*file_type++ = '\0';

		platform = strchr(file_type, ':');
		if (!platform)
		{
			fprintf(stderr, _("incorrectly formatted resultmap entry: %s\n"),
					buf);
			exit_nicely(2);
		}
		*platform++ = '\0';
		expected = strchr(platform, '=');
		if (!expected)
		{
			fprintf(stderr, _("incorrectly formatted resultmap entry: %s\n"),
					buf);
			exit_nicely(2);
		}
		*expected++ = '\0';

		/*
		 * if it's for current platform, save it in resultmap list. Note: by
		 * adding at the front of the list, we ensure that in ambiguous cases,
		 * the last match in the resultmap file is used. This mimics the
		 * behavior of the old shell script.
		 */
		if (string_matches_pattern(host_platform, platform))
		{
			_resultmap *entry = malloc(sizeof(_resultmap));

			entry->test = strdup(buf);
			entry->type = strdup(file_type);
			entry->resultfile = strdup(expected);
			entry->next = resultmap;
			resultmap = entry;
		}
	}
	fclose(f);
}

/*
 * Check in resultmap if we should be looking at a different file
 */
static
const char *
get_expectfile(const char *testname, const char *file)
{
	char	   *file_type;
	_resultmap *rm;

	/*
	 * Determine the file type from the file name. This is just what is
	 * following the last dot in the file name.
	 */
	if (!file || !(file_type = strrchr(file, '.')))
		return NULL;

	file_type++;

	for (rm = resultmap; rm != NULL; rm = rm->next)
	{
		if (strcmp(testname, rm->test) == 0 && strcmp(file_type, rm->type) == 0)
		{
			return rm->resultfile;
		}
	}

	return NULL;
}

/*
 * Handy subroutine for setting an environment variable "var" to "val"
 */
static void
doputenv(const char *var, const char *val)
{
	char	   *s = malloc(strlen(var) + strlen(val) + 2);

	sprintf(s, "%s=%s", var, val);
	putenv(s);
}

/*
 * Prepare environment variables for running regression tests
 */
static void
initialize_environment(void)
{
	if (nolocale)
	{
		/*
		 * Clear out any non-C locale settings
		 */
		unsetenv("LC_COLLATE");
		unsetenv("LC_CTYPE");
		unsetenv("LC_MONETARY");
		unsetenv("LC_NUMERIC");
		unsetenv("LC_TIME");
		unsetenv("LANG");
		/* On Windows the default locale cannot be English, so force it */
#if defined(WIN32) || defined(__CYGWIN__)
		putenv("LANG=en");
#endif
	}

	/*
	 * Set translation-related settings to English; otherwise psql will
	 * produce translated messages and produce diffs.  (XXX If we ever support
	 * translation of pg_regress, this needs to be moved elsewhere, where psql
	 * is actually called.)
	 */
	unsetenv("LANGUAGE");
	unsetenv("LC_ALL");
	putenv("LC_MESSAGES=C");

	/*
	 * Set multibyte as requested
	 */
	if (encoding && strlen(encoding) > 0)
		doputenv("PGCLIENTENCODING", encoding);
	else
		unsetenv("PGCLIENTENCODING");

	/*
	 * Set timezone and datestyle for datetime-related tests
	 */
	putenv("PGTZ=PST8PDT");
	putenv("PGDATESTYLE=Postgres, MDY");

	/*
	 * Likewise set intervalstyle to ensure consistent results.  This is a bit
	 * more painful because we must use PGOPTIONS, and we want to preserve the
	 * user's ability to set other variables through that.
	 */
	{
		const char *my_pgoptions = "-c intervalstyle=postgres_verbose";
		const char *old_pgoptions = getenv("PGOPTIONS");
		char	   *new_pgoptions;

		if (!old_pgoptions)
			old_pgoptions = "";
		new_pgoptions = malloc(strlen(old_pgoptions) + strlen(my_pgoptions) + 12);
		sprintf(new_pgoptions, "PGOPTIONS=%s %s", old_pgoptions, my_pgoptions);
		putenv(new_pgoptions);
	}

	{
		const char *pghost;
		const char *pgport;

		/*
		 * When testing an existing install, we honor existing environment
		 * variables, except if they're overridden by command line options.
		 */
		if (hostname != NULL)
		{
			doputenv("PGHOST", hostname);
			unsetenv("PGHOSTADDR");
		}
		if (port != -1)
		{
			char		s[16];

			sprintf(s, "%d", port);
			doputenv("PGPORT", s);
		}
		if (user != NULL)
			doputenv("PGUSER", user);

		/*
		 * Report what we're connecting to
		 */
		pghost = getenv("PGHOST");
		pgport = getenv("PGPORT");
#ifndef HAVE_UNIX_SOCKETS
		if (!pghost)
			pghost = "localhost";
#endif

		if (pghost && pgport)
			printf(_("(using postmaster on %s, port %s)\n"), pghost, pgport);
		if (pghost && !pgport)
			printf(_("(using postmaster on %s, default port)\n"), pghost);
		if (!pghost && pgport)
			printf(_("(using postmaster on Unix socket, port %s)\n"), pgport);
		if (!pghost && !pgport)
			printf(_("(using postmaster on Unix socket, default port)\n"));
	}

	convert_sourcefiles();
	load_resultmap();
}

/*
 * Issue a command via psql, connecting to the specified database
 *
 * Since we use system(), this doesn't return until the operation finishes
 */
static void
psql_command(const char *database, const char *query,...)
{
	char		query_formatted[1024];
	char		query_escaped[2048];
	char		psql_cmd[MAXPGPATH + 2048];
	va_list		args;
	char	   *s;
	char	   *d;

	/* Generate the query with insertion of sprintf arguments */
	va_start(args, query);
	vsnprintf(query_formatted, sizeof(query_formatted), query, args);
	va_end(args);

	/* Now escape any shell double-quote metacharacters */
	d = query_escaped;
	for (s = query_formatted; *s; s++)
	{
		if (strchr("\\\"$`", *s))
			*d++ = '\\';
		*d++ = *s;
	}
	*d = '\0';

	/* And now we can build and execute the shell command */
	snprintf(psql_cmd, sizeof(psql_cmd),
			 SYSTEMQUOTE "\"%s%spsql\" -X -c \"%s\" \"%s\"" SYSTEMQUOTE,
			 psqldir ? psqldir : "",
			 psqldir ? "/" : "",
			 query_escaped,
			 database);

	if (system(psql_cmd) != 0)
	{
		/* psql probably already reported the error */
		fprintf(stderr, _("command failed: %s\n"), psql_cmd);
		exit_nicely(2);
	}
}

/*
 * Spawn a process to execute the given shell command; don't wait for it
 *
 * Returns the process ID (or HANDLE) so we can wait for it later
 */
PID_TYPE
spawn_process(const char *cmdline)
{
#ifndef WIN32
	pid_t		pid;

	/*
	 * Must flush I/O buffers before fork.	Ideally we'd use fflush(NULL) here
	 * ... does anyone still care about systems where that doesn't work?
	 */
	fflush(stdout);
	fflush(stderr);
	if (logfile)
		fflush(logfile);

	pid = fork();
	if (pid == -1)
	{
		fprintf(stderr, _("%s: could not fork: %s\n"),
				progname, strerror(errno));
		exit_nicely(2);
	}
	if (pid == 0)
	{
		/*
		 * In child
		 *
		 * Instead of using system(), exec the shell directly, and tell it to
		 * "exec" the command too.	This saves two useless processes per
		 * parallel test case.
		 */
		char	   *cmdline2 = malloc(strlen(cmdline) + 6);

		sprintf(cmdline2, "exec %s", cmdline);
		execl(shellprog, shellprog, "-c", cmdline2, (char *) NULL);
		fprintf(stderr, _("%s: could not exec \"%s\": %s\n"),
				progname, shellprog, strerror(errno));
		exit(1);				/* not exit_nicely here... */
	}
	/* in parent */
	return pid;
#else
	char	   *cmdline2;
	BOOL		b;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	HANDLE		origToken;
	HANDLE		restrictedToken;
	SID_IDENTIFIER_AUTHORITY NtAuthority = {SECURITY_NT_AUTHORITY};
	SID_AND_ATTRIBUTES dropSids[2];
	__CreateRestrictedToken _CreateRestrictedToken = NULL;
	HANDLE		Advapi32Handle;

	ZeroMemory(&si, sizeof(si));
	si.cb = sizeof(si);

	Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
	if (Advapi32Handle != NULL)
	{
		_CreateRestrictedToken = (__CreateRestrictedToken) GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
	}

	if (_CreateRestrictedToken == NULL)
	{
		if (Advapi32Handle != NULL)
			FreeLibrary(Advapi32Handle);
		fprintf(stderr, _("%s: cannot create restricted tokens on this platform\n"),
				progname);
		exit_nicely(2);
	}

	/* Open the current token to use as base for the restricted one */
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken))
	{
		fprintf(stderr, _("could not open process token: %lu\n"),
				GetLastError());
		exit_nicely(2);
	}

	/* Allocate list of SIDs to remove */
	ZeroMemory(&dropSids, sizeof(dropSids));
	if (!AllocateAndInitializeSid(&NtAuthority, 2,
								  SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0, 0, &dropSids[0].Sid) ||
		!AllocateAndInitializeSid(&NtAuthority, 2,
								  SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS, 0, 0, 0, 0, 0, 0, &dropSids[1].Sid))
	{
		fprintf(stderr, _("could not allocate SIDs: %lu\n"), GetLastError());
		exit_nicely(2);
	}

	b = _CreateRestrictedToken(origToken,
							   DISABLE_MAX_PRIVILEGE,
							   sizeof(dropSids) / sizeof(dropSids[0]),
							   dropSids,
							   0, NULL,
							   0, NULL,
							   &restrictedToken);

	FreeSid(dropSids[1].Sid);
	FreeSid(dropSids[0].Sid);
	CloseHandle(origToken);
	FreeLibrary(Advapi32Handle);

	if (!b)
	{
		fprintf(stderr, _("could not create restricted token: %lu\n"),
				GetLastError());
		exit_nicely(2);
	}

	cmdline2 = malloc(strlen(cmdline) + 8);
	sprintf(cmdline2, "cmd /c %s", cmdline);

#ifndef __CYGWIN__
	AddUserToTokenDacl(restrictedToken);
#endif

	if (!CreateProcessAsUser(restrictedToken,
							 NULL,
							 cmdline2,
							 NULL,
							 NULL,
							 TRUE,
							 CREATE_SUSPENDED,
							 NULL,
							 NULL,
							 &si,
							 &pi))
	{
		fprintf(stderr, _("could not start process for \"%s\": %lu\n"),
				cmdline2, GetLastError());
		exit_nicely(2);
	}

	free(cmdline2);

	ResumeThread(pi.hThread);
	CloseHandle(pi.hThread);
	return pi.hProcess;
#endif
}

/*
 * Count bytes in file
 */
static long
file_size(const char *file)
{
	long		r;
	FILE	   *f = fopen(file, "r");

	if (!f)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, file, strerror(errno));
		return -1;
	}
	fseek(f, 0, SEEK_END);
	r = ftell(f);
	fclose(f);
	return r;
}

/*
 * Count lines in file
 */
static int
file_line_count(const char *file)
{
	int			c;
	int			l = 0;
	FILE	   *f = fopen(file, "r");

	if (!f)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, file, strerror(errno));
		return -1;
	}
	while ((c = fgetc(f)) != EOF)
	{
		if (c == '\n')
			l++;
	}
	fclose(f);
	return l;
}

bool
file_exists(const char *file)
{
	FILE	   *f = fopen(file, "r");

	if (!f)
		return false;
	fclose(f);
	return true;
}

static bool
directory_exists(const char *dir)
{
	struct stat st;

	if (stat(dir, &st) != 0)
		return false;
	if (S_ISDIR(st.st_mode))
		return true;
	return false;
}

/* Create a directory */
static void
make_directory(const char *dir)
{
	if (mkdir(dir, S_IRWXU | S_IRWXG | S_IRWXO) < 0)
	{
		fprintf(stderr, _("%s: could not create directory \"%s\": %s\n"),
				progname, dir, strerror(errno));
		exit_nicely(2);
	}
}

/*
 * In: filename.ext, Return: filename_i.ext, where 0 < i <= 9
 */
static char *
get_alternative_expectfile(const char *expectfile, int i)
{
	char	   *last_dot;
	int			ssize = strlen(expectfile) + 2 + 1;
	char	   *tmp = (char *) malloc(ssize);
	char	   *s = (char *) malloc(ssize);

	strcpy(tmp, expectfile);
	last_dot = strrchr(tmp, '.');
	if (!last_dot)
	{
		free(tmp);
		free(s);
		return NULL;
	}
	*last_dot = '\0';
	snprintf(s, ssize, "%s_%d.%s", tmp, i, last_dot + 1);
	free(tmp);
	return s;
}

/*
 * Run a "diff" command and also check that it didn't crash
 */
static int
run_diff(const char *cmd, const char *filename)
{
	int			r;

	r = system(cmd);
	if (!WIFEXITED(r) || WEXITSTATUS(r) > 1)
	{
		fprintf(stderr, _("diff command failed with status %d: %s\n"), r, cmd);
		exit_nicely(2);
	}
#ifdef WIN32

	/*
	 * On WIN32, if the 'diff' command cannot be found, system() returns 1,
	 * but produces nothing to stdout, so we check for that here.
	 */
	if (WEXITSTATUS(r) == 1 && file_size(filename) <= 0)
	{
		fprintf(stderr, _("diff command not found: %s\n"), cmd);
		exit_nicely(2);
	}
#else
	UnusedArg(filename);
#endif

	return WEXITSTATUS(r);
}

/*
 * Check the actual result file for the given test against expected results
 *
 * Returns true if different (failure), false if correct match found.
 * In the true case, the diff is appended to the diffs file.
 */
static bool
results_differ(const char *testname, const char *resultsfile, const char *default_expectfile)
{
	char		expectfile[MAXPGPATH];
	char		diff[MAXPGPATH];
	char		cmd[MAXPGPATH * 3];
	char		best_expect_file[MAXPGPATH];
    char        diff_opts[MAXPGPATH];
    char        m_pretty_diff_opts[MAXPGPATH];
	FILE	   *difffile;
	int			best_line_count;
	int			i;
	int			l;
	const char *platform_expectfile;

	/*
	 * We can pass either the resultsfile or the expectfile, they should have
	 * the same type (filename.type) anyway.
	 */
	platform_expectfile = get_expectfile(testname, resultsfile);

	strlcpy(expectfile, default_expectfile, sizeof(expectfile));
	if (platform_expectfile)
	{
		/*
		 * Replace everything afer the last slash in expectfile with what the
		 * platform_expectfile contains.
		 */
		char	   *p = strrchr(expectfile, '/');

		if (p)
			strcpy(++p, platform_expectfile);
	}

	/* Name to use for temporary diff file */
	snprintf(diff, sizeof(diff), "%s.diff", resultsfile);
    
	/* Add init file arguments if provided via commandline */
        if (initfile)
        {
	  snprintf(diff_opts, sizeof(diff_opts),
			   "%s --gpd_init %s", basic_diff_opts, initfile);

	  snprintf(m_pretty_diff_opts, sizeof(m_pretty_diff_opts),
			   "%s --gpd_init %s", pretty_diff_opts, initfile);
	}
	else
	{
		snprintf(diff_opts, sizeof(diff_opts),
			   "%s", basic_diff_opts);

		snprintf(m_pretty_diff_opts, sizeof(m_pretty_diff_opts),
                 "%s", pretty_diff_opts);
	}

	/* OK, run the diff */
	snprintf(cmd, sizeof(cmd),
			 SYSTEMQUOTE "gpdiff.pl %s \"%s\" \"%s\" > \"%s\"" SYSTEMQUOTE,
			 diff_opts, expectfile, resultsfile, diff);

	/* Is the diff file empty? */
	if (run_diff(cmd, diff) == 0)
	{
		unlink(diff);
		return false;
	}

	/* There may be secondary comparison files that match better */
	best_line_count = file_line_count(diff);
	strcpy(best_expect_file, expectfile);

	for (i = 0; i <= 9; i++)
	{
		char	   *alt_expectfile;

		alt_expectfile = get_alternative_expectfile(expectfile, i);
		if (!file_exists(alt_expectfile))
			continue;

		snprintf(cmd, sizeof(cmd),
				 SYSTEMQUOTE "gpdiff.pl %s \"%s\" \"%s\" > \"%s\"" SYSTEMQUOTE,
				 diff_opts, alt_expectfile, resultsfile, diff);

		if (run_diff(cmd, diff) == 0)
		{
			unlink(diff);
			return false;
		}

		l = file_line_count(diff);
		if (l < best_line_count)
		{
			/* This diff was a better match than the last one */
			best_line_count = l;
			strlcpy(best_expect_file, alt_expectfile, sizeof(best_expect_file));
		}
		free(alt_expectfile);
	}

	/*
	 * fall back on the canonical results file if we haven't tried it yet and
	 * haven't found a complete match yet.
	 */

	if (platform_expectfile)
	{
		snprintf(cmd, sizeof(cmd),
				 SYSTEMQUOTE "gpdiff.pl %s \"%s\" \"%s\" > \"%s\"" SYSTEMQUOTE,
				 diff_opts, default_expectfile, resultsfile, diff);

		if (run_diff(cmd, diff) == 0)
		{
			/* No diff = no changes = good */
			unlink(diff);
			return false;
		}

		l = file_line_count(diff);
		if (l < best_line_count)
		{
			/* This diff was a better match than the last one */
			best_line_count = l;
			strlcpy(best_expect_file, default_expectfile, sizeof(best_expect_file));
		}
	}

	/*
	 * Use the best comparison file to generate the "pretty" diff, which we
	 * append to the diffs summary file.
	 */
	snprintf(cmd, sizeof(cmd),
			 SYSTEMQUOTE "gpdiff.pl %s \"%s\" \"%s\" >> \"%s\"" SYSTEMQUOTE,
			 m_pretty_diff_opts, best_expect_file, resultsfile, difffilename);
	run_diff(cmd, difffilename);

	/* And append a separator */
	difffile = fopen(difffilename, "a");
	if (difffile)
	{
		fprintf(difffile,
				"\n======================================================================\n\n");
		fclose(difffile);
	}

	unlink(diff);
	return true;
}

/*
 * Wait for specified subprocesses to finish, and return their exit
 * statuses into statuses[]
 *
 * If names isn't NULL, print each subprocess's name as it finishes
 *
 * Note: it's OK to scribble on the pids array, but not on the names array
 */
static void
wait_for_tests(PID_TYPE * pids, int *statuses, char **names, int num_tests)
{
	int			tests_left;
	int			i;

#ifdef WIN32
	PID_TYPE   *active_pids = malloc(num_tests * sizeof(PID_TYPE));

	memcpy(active_pids, pids, num_tests * sizeof(PID_TYPE));
#endif

	tests_left = num_tests;
	while (tests_left > 0)
	{
		PID_TYPE	p;

#ifndef WIN32
		int			exit_status;

		p = wait(&exit_status);

		if (p == INVALID_PID)
		{
			fprintf(stderr, _("failed to wait for subprocesses: %s\n"),
					strerror(errno));
			exit_nicely(2);
		}
#else
		DWORD		exit_status;
		int			r;

		r = WaitForMultipleObjects(tests_left, active_pids, FALSE, INFINITE);
		if (r < WAIT_OBJECT_0 || r >= WAIT_OBJECT_0 + tests_left)
		{
			fprintf(stderr, _("failed to wait for subprocesses: %lu\n"),
					GetLastError());
			exit_nicely(2);
		}
		p = active_pids[r - WAIT_OBJECT_0];
		/* compact the active_pids array */
		active_pids[r - WAIT_OBJECT_0] = active_pids[tests_left - 1];
#endif   /* WIN32 */

		for (i = 0; i < num_tests; i++)
		{
			if (p == pids[i])
			{
#ifdef WIN32
				GetExitCodeProcess(pids[i], &exit_status);
				CloseHandle(pids[i]);
#endif
				pids[i] = INVALID_PID;
				statuses[i] = (int) exit_status;
				if (names)
					status(" %s", names[i]);
				tests_left--;
				break;
			}
		}
	}

#ifdef WIN32
	free(active_pids);
#endif
}

/*
 * report nonzero exit code from a test process
 */
static void
log_child_failure(int exitstatus)
{
	if (WIFEXITED(exitstatus))
		status(_(" (test process exited with exit code %d)"),
			   WEXITSTATUS(exitstatus));
	else if (WIFSIGNALED(exitstatus))
	{
#if defined(WIN32)
		status(_(" (test process was terminated by exception 0x%X)"),
			   WTERMSIG(exitstatus));
#elif defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
		status(_(" (test process was terminated by signal %d: %s)"),
			   WTERMSIG(exitstatus),
			   WTERMSIG(exitstatus) < NSIG ?
			   sys_siglist[WTERMSIG(exitstatus)] : "(unknown))");
#else
		status(_(" (test process was terminated by signal %d)"),
			   WTERMSIG(exitstatus));
#endif
	}
	else
		status(_(" (test process exited with unrecognized status %d)"),
			   exitstatus);
}

/*
 * Run all the tests specified in one schedule file
 */
static void
run_schedule(const char *schedule, test_function tfunc)
{
#define MAX_PARALLEL_TESTS 100
	char	   *tests[MAX_PARALLEL_TESTS];
	_stringlist *resultfiles[MAX_PARALLEL_TESTS];
	_stringlist *expectfiles[MAX_PARALLEL_TESTS];
	_stringlist *tags[MAX_PARALLEL_TESTS];
	PID_TYPE	pids[MAX_PARALLEL_TESTS];
	int			statuses[MAX_PARALLEL_TESTS];
	_stringlist *ignorelist = NULL;
	char		scbuf[1024];
	FILE	   *scf;
	int			line_num = 0;

	memset(resultfiles, 0, sizeof(_stringlist *) * MAX_PARALLEL_TESTS);
	memset(expectfiles, 0, sizeof(_stringlist *) * MAX_PARALLEL_TESTS);
	memset(tags, 0, sizeof(_stringlist *) * MAX_PARALLEL_TESTS);

	scf = fopen(schedule, "r");
	if (!scf)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, schedule, strerror(errno));
		exit_nicely(2);
	}

	while (fgets(scbuf, sizeof(scbuf), scf))
	{
		char	   *test = NULL;
		char	   *c;
		int			num_tests;
		bool		inword;
		int			i;
		struct timeval start_time, end_time;
		double		diff_secs;

		line_num++;

		for (i = 0; i < MAX_PARALLEL_TESTS; i++)
		{
			if (resultfiles[i] == NULL)
				break;
			free_stringlist(&resultfiles[i]);
			free_stringlist(&expectfiles[i]);
			free_stringlist(&tags[i]);
		}

		/* strip trailing whitespace, especially the newline */
		i = strlen(scbuf);
		while (i > 0 && isspace((unsigned char) scbuf[i - 1]))
			scbuf[--i] = '\0';

		if (scbuf[0] == '\0' || scbuf[0] == '#')
			continue;
		if (strncmp(scbuf, "test: ", 6) == 0)
		{
			char		cmd[MAXPGPATH * 3];

			test = scbuf + 6;

			/* MPP-9643: allow ability to disable tests per platform */
			snprintf(cmd, sizeof(cmd),
					 SYSTEMQUOTE "gpexclude.pl --test %s --exclude %s.EXCLUDE  --quiet  " SYSTEMQUOTE, test, schedule);

			if (run_diff(cmd, "") != 0)
			{
				c = test;
				while (*c && isspace((unsigned char) *c))
						c++;
				add_stringlist_item(&ignorelist, c);

				/*
				 * Note: ignore: lines do not run the test, they just
				 * say that failure of this test when run later on is
				 * to be ignored. A bit odd but that's how the
				 * shell-script version did it.
				 */
				continue;

			}
		}
		else if (strncmp(scbuf, "ignore: ", 8) == 0)
		{
			c = scbuf + 8;
			while (*c && isspace((unsigned char) *c))
				c++;
			add_stringlist_item(&ignorelist, c);

			/*
			 * Note: ignore: lines do not run the test, they just say that
			 * failure of this test when run later on is to be ignored. A bit
			 * odd but that's how the shell-script version did it.
			 */
			continue;
		}
		else
		{
			fprintf(stderr, _("syntax error in schedule file \"%s\" line %d: %s\n"),
					schedule, line_num, scbuf);
			exit_nicely(2);
		}

		num_tests = 0;
		inword = false;
		for (c = test; *c; c++)
		{
			if (isspace((unsigned char) *c))
			{
				*c = '\0';
				inword = false;
			}
			else if (!inword)
			{
				if (num_tests >= MAX_PARALLEL_TESTS)
				{
					/* can't print scbuf here, it's already been trashed */
					fprintf(stderr, _("too many parallel tests in schedule file \"%s\", line %d\n"),
							schedule, line_num);
					exit_nicely(2);
				}
				tests[num_tests] = c;
				num_tests++;
				inword = true;
			}
		}

		if (num_tests == 0)
		{
			fprintf(stderr, _("syntax error in schedule file \"%s\" line %d: %s\n"),
					schedule, line_num, scbuf);
			exit_nicely(2);
		}

		gettimeofday(&start_time, NULL);
		if (num_tests == 1)
		{
#ifdef TEST_EACH_SCRIPT_IN_ITS_OWN_DB
		    _stringlist *strList;
            for (strList = dblist; strList; strList = strList->next)
        	    drop_database_if_exists(strList->str);
            for (strList = dblist; strList; strList = strList->next)
        		create_database(strList->str);
#endif
			status(_("test %-20s ... "), tests[0]);
			pids[0] = (tfunc) (tests[0], &resultfiles[0], &expectfiles[0], &tags[0]);
			wait_for_tests(pids, statuses, NULL, 1);
			/* status line is finished below */
		}
		else if (max_connections > 0 && max_connections < num_tests)
		{
			int			oldest = 0;

			status(_("parallel group (%d tests, in groups of %d): "),
				   num_tests, max_connections);
			for (i = 0; i < num_tests; i++)
			{
				if (i - oldest >= max_connections)
				{
					wait_for_tests(pids + oldest, statuses + oldest,
								   tests + oldest, i - oldest);
					oldest = i;
				}
				pids[i] = (tfunc) (tests[i], &resultfiles[i], &expectfiles[i], &tags[i]);
			}
			wait_for_tests(pids + oldest, statuses + oldest,
						   tests + oldest, i - oldest);
			status_end();
		}
		else
		{
			status(_("parallel group (%d tests): "), num_tests);
			for (i = 0; i < num_tests; i++)
			{
				pids[i] = (tfunc) (tests[i], &resultfiles[i], &expectfiles[i], &tags[i]);
			}
			wait_for_tests(pids, statuses, tests, num_tests);
			status_end();
		}
		gettimeofday(&end_time, NULL);

		diff_secs = (end_time.tv_usec - start_time.tv_usec);
		diff_secs /= 1000000;
		diff_secs += end_time.tv_sec - start_time.tv_sec;

		/* Check results for all tests */
		for (i = 0; i < num_tests; i++)
		{
			_stringlist *rl,
					   *el,
					   *tl;
			bool		differ = false;

			if (num_tests > 1)
				status(_("     %-20s ... "), tests[i]);

			/*
			 * Advance over all three lists simultaneously.
			 *
			 * Compare resultfiles[j] with expectfiles[j] always. Tags are
			 * optional but if there are tags, the tag list has the same
			 * length as the other two lists.
			 */
			for (rl = resultfiles[i], el = expectfiles[i], tl = tags[i];
				 rl != NULL;	/* rl and el have the same length */
				 rl = rl->next, el = el->next)
			{
				bool		newdiff;

				if (tl)
					tl = tl->next;		/* tl has the same length as rl and el
										 * if it exists */

				newdiff = results_differ(tests[i], rl->str, el->str);
				if (newdiff && tl)
				{
					printf("%s ", tl->str);
				}
				differ |= newdiff;
			}

			if (differ)
			{
				bool		ignore = false;
				_stringlist *sl;

				for (sl = ignorelist; sl != NULL; sl = sl->next)
				{
					if (strcmp(tests[i], sl->str) == 0)
					{
						ignore = true;
						break;
					}
				}
				if (ignore)
				{
					status(_("failed (ignored)"));
					fail_ignore_count++;
				}
				else
				{
					status(_("FAILED"));
    				status(_(" (%.2f sec)"), diff_secs);
					fail_count++;
				}
			}
			else
			{
				status(_("ok"));
				status(_(" (%.2f sec)"), diff_secs);
				success_count++;
			}

			if (statuses[i] != 0)
				log_child_failure(statuses[i]);

			status_end();
		}
	}

	fclose(scf);
}

/*
 * Run a single test
 */
static void
run_single_test(const char *test, test_function tfunc)
{
	PID_TYPE	pid;
	int			exit_status;
	_stringlist *resultfiles = NULL;
	_stringlist *expectfiles = NULL;
	_stringlist *tags = NULL;
	_stringlist *rl,
			   *el,
			   *tl;
	bool		differ = false;

	status(_("test %-20s ... "), test);
	pid = (tfunc) (test, &resultfiles, &expectfiles, &tags);
	wait_for_tests(&pid, &exit_status, NULL, 1);

	/*
	 * Advance over all three lists simultaneously.
	 *
	 * Compare resultfiles[j] with expectfiles[j] always. Tags are optional
	 * but if there are tags, the tag list has the same length as the other
	 * two lists.
	 */
	for (rl = resultfiles, el = expectfiles, tl = tags;
		 rl != NULL;			/* rl and el have the same length */
		 rl = rl->next, el = el->next)
	{
		bool		newdiff;

		if (tl)
			tl = tl->next;		/* tl has the same length as rl and el if it
								 * exists */

		newdiff = results_differ(test, rl->str, el->str);
		if (newdiff && tl)
		{
			printf("%s ", tl->str);
		}
		differ |= newdiff;
	}

	if (differ)
	{
		status(_("FAILED"));
		fail_count++;
	}
	else
	{
		status(_("ok"));
		success_count++;
	}

	if (exit_status != 0)
		log_child_failure(exit_status);

	status_end();
}

/*
 * Create the summary-output files (making them empty if already existing)
 */
static void
open_result_files(void)
{
	char		file[MAXPGPATH];
	FILE	   *difffile;

	/* create the log file (copy of running status output) */
	snprintf(file, sizeof(file), "%s/regression.out", outputdir);
	logfilename = strdup(file);
	logfile = fopen(logfilename, "w");
	if (!logfile)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"),
				progname, logfilename, strerror(errno));
		exit_nicely(2);
	}

	/* create the diffs file as empty */
	snprintf(file, sizeof(file), "%s/regression.diffs", outputdir);
	difffilename = strdup(file);
	difffile = fopen(difffilename, "w");
	if (!difffile)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for writing: %s\n"),
				progname, difffilename, strerror(errno));
		exit_nicely(2);
	}
	/* we don't keep the diffs file open continuously */
	fclose(difffile);

	/* also create the output directory if not present */
	snprintf(file, sizeof(file), "%s/results", outputdir);
	if (!directory_exists(file))
		make_directory(file);
}

static void
drop_database_if_exists(const char *dbname)
{
	header(_("dropping database \"%s\""), dbname);
	psql_command("postgres", "DROP DATABASE IF EXISTS \"%s\"", dbname);
}

/*
 * TODO: change this function back after "alter database" and "create language" are supported
 */
static void
create_database(const char *dbname)
{

	_stringlist *sl;

	/*
	 * We use template0 so that any installation-local cruft in template1 will
	 * not mess up the tests.
	 */
	header(_("creating database \"%s\""), dbname);
	if (encoding && strlen(encoding) > 0)
		psql_command("postgres", "CREATE DATABASE \"%s\" TEMPLATE=template0 TABLESPACE=dfs_default ENCODING='%s'", dbname, encoding);
	else
		psql_command("postgres", "CREATE DATABASE \"%s\" TEMPLATE=template0 TABLESPACE=dfs_default", dbname);
	psql_command(dbname,
				 "ALTER DATABASE \"%s\" SET lc_messages TO 'C';"
				 "ALTER DATABASE \"%s\" SET lc_monetary TO 'C';"
				 "ALTER DATABASE \"%s\" SET lc_numeric TO 'C';"
				 "ALTER DATABASE \"%s\" SET lc_time TO 'C';"
			"ALTER DATABASE \"%s\" SET timezone_abbreviations TO 'Default';",
				 dbname, dbname, dbname, dbname, dbname);
	/*
	 * Install any requested procedural languages
	 */
	for (sl = loadlanguage; sl != NULL; sl = sl->next)
	{
		header(_("installing %s"), sl->str);
		psql_command(dbname, "CREATE LANGUAGE \"%s\"", sl->str);
	}
}

static void
drop_role_if_exists(const char *rolename)
{
	header(_("dropping role \"%s\""), rolename);
	psql_command("postgres", "DROP ROLE IF EXISTS \"%s\"", rolename);
}

static void
create_role(const char *rolename, const _stringlist * granted_dbs)
{
	header(_("creating role \"%s\""), rolename);
	psql_command("postgres", "CREATE ROLE \"%s\" WITH LOGIN", rolename);
	for (; granted_dbs != NULL; granted_dbs = granted_dbs->next)
	{
		psql_command("postgres", "GRANT ALL ON DATABASE \"%s\" TO \"%s\"",
					 granted_dbs->str, rolename);
	}
}

static char *
trim_white_space(char *str)
{
	char *end;
	while (isspace((unsigned char)*str))
	{
		str++;
	}

	if (*str == 0)
	{
		return str;
	}

	end = str + strlen(str) - 1;
	while (end > str && isspace((unsigned char)*end))
	{
		end--;
	}

	*(end+1) = 0;
	return str;
}

/*
 * Check whether the optimizer is on or off, and set the global
 * variable "optimizer_enabled" accordingly.
 */
static void
check_optimizer_status(void)
{
	char psql_cmd[MAXPGPATH];
	char statusfilename[MAXPGPATH];
	char line[1024];

	header(_("checking optimizer status"));

	snprintf(statusfilename, sizeof(statusfilename), SYSTEMQUOTE "%s/optimizer_status.out" SYSTEMQUOTE, outputdir);

	snprintf(psql_cmd, sizeof(psql_cmd),
			 SYSTEMQUOTE "\"%s%spsql\" -X -c \"show optimizer;\" -o \"%s\" -d \"postgres\"" SYSTEMQUOTE,
			 psqldir ? psqldir : "",
			 psqldir ? "/" : "",
			 statusfilename);

	if (system(psql_cmd) != 0)
	{
		exit_nicely(2);
	}

	FILE *statusfile = fopen(statusfilename, "r");
	if (!statusfile)
	{
		fprintf(stderr, _("%s: could not open file \"%s\" for reading: %s\n"),
				progname, statusfilename, strerror(errno));
		exit_nicely(2);
	}

	while (fgets(line, sizeof(line), statusfile))
	{
		char *trimmed = trim_white_space(line);
		if (strncmp(trimmed, "on", 2) == 0)
		{
			optimizer_enabled = true;
			status(_("Optimizer enabled. Using optimizer answer files whenever possible"));
			break;
		}

		if (strncmp(trimmed, "off", 3) == 0)
		{
			optimizer_enabled = false;
			status(_("Optimizer disabled. Using planner answer files"));
			break;
		}
	}
	status_end();
	fclose(statusfile);
}

static void
help(void)
{
	printf(_("PostgreSQL regression test driver\n"));
	printf(_("\n"));
	printf(_("Usage: %s [options...] [extra tests...]\n"), progname);
	printf(_("\n"));
	printf(_("Options:\n"));
	printf(_("  --dbname=DB               use database DB (default \"regression\")\n"));
	printf(_("  --debug                   turn on debug mode in programs that are run\n"));
	printf(_("  --inputdir=DIR            take input files from DIR (default \".\")\n"));
	printf(_("  --load-language=lang      load the named language before running the\n"));
	printf(_("                            tests; can appear multiple times\n"));
	printf(_("  --create-role=ROLE        create the specified role before testing\n"));
	printf(_("  --max-connections=N       maximum number of concurrent connections\n"));
	printf(_("                            (default is 0 meaning unlimited)\n"));
	printf(_("  --multibyte=ENCODING      use ENCODING as the multibyte encoding\n"));
	printf(_("  --outputdir=DIR           place output files in DIR (default \".\")\n"));
	printf(_("  --schedule=FILE           use test ordering schedule from FILE\n"));
	printf(_("                            (can be used multiple times to concatenate)\n"));
	printf(_("  --srcdir=DIR              absolute path to source directory (for VPATH builds)\n"));
    printf(_(" --init-file=GPD_INIT_FILE  init file to be used for gpdiff\n"));
	printf(_("\n"));
	printf(_("Options for using an existing installation:\n"));
	printf(_("  --host=HOST               use postmaster running on HOST\n"));
	printf(_("  --port=PORT               use postmaster running at PORT\n"));
	printf(_("  --user=USER               connect as USER\n"));
	printf(_("  --psqldir=DIR             use psql in DIR (default: find in PATH)\n"));
	printf(_("\n"));
	printf(_("The exit status is 0 if all tests passed, 1 if some tests failed, and 2\n"));
	printf(_("if the tests could not be run for some reason.\n"));
	printf(_("\n"));
	printf(_("Report bugs to <pgsql-bugs@postgresql.org>.\n"));
}

int
regression_main(int argc, char *argv[], init_function ifunc, test_function tfunc)
{
	_stringlist *sl;
	int			c;
	int			i;
	int			option_index;
	char		buf[MAXPGPATH * 4];

	static struct option long_options[] = {
		{"help", no_argument, NULL, 'h'},
		{"version", no_argument, NULL, 'V'},
		{"dbname", required_argument, NULL, 1},
		{"debug", no_argument, NULL, 2},
		{"inputdir", required_argument, NULL, 3},
		{"load-language", required_argument, NULL, 4},
		{"max-connections", required_argument, NULL, 5},
		{"multibyte", required_argument, NULL, 6},
		{"outputdir", required_argument, NULL, 7},
		{"schedule", required_argument, NULL, 8},
		{"no-locale", no_argument, NULL, 10},
		{"top-builddir", required_argument, NULL, 11},
		{"host", required_argument, NULL, 13},
		{"port", required_argument, NULL, 14},
		{"user", required_argument, NULL, 15},
		{"psqldir", required_argument, NULL, 16},
		{"srcdir", required_argument, NULL, 17},
		{"create-role", required_argument, NULL, 18},
        	{"init-file", required_argument, NULL, 19},
		{"tablespace", required_argument, NULL, 19},
		{NULL, 0, NULL, 0}
	};

	progname = get_progname(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_regress"));

#ifndef HAVE_UNIX_SOCKETS
	/* no unix domain sockets available, so change default */
	hostname = "localhost";
#endif

	/*
	 * We call the initialization function here because that way we can set
	 * default parameters and let them be overwritten by the commandline.
	 */
	ifunc();

	while ((c = getopt_long(argc, argv, "hV", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'h':
				help();
				exit_nicely(0);
			case 'V':
				puts("pg_regress (PostgreSQL) " PG_VERSION);
				exit_nicely(0);
			case 1:

				/*
				 * If a default database was specified, we need to remove it
				 * before we add the specified one.
				 */
				free_stringlist(&dblist);
				split_to_stringlist(strdup(optarg), ", ", &dblist);
				break;
			case 2:
				debug = true;
				break;
			case 3:
				inputdir = strdup(optarg);
				break;
			case 4:
				add_stringlist_item(&loadlanguage, optarg);
				break;
			case 5:
				max_connections = atoi(optarg);
				break;
			case 6:
				encoding = strdup(optarg);
				break;
			case 7:
				outputdir = strdup(optarg);
				break;
			case 8:
				add_stringlist_item(&schedulelist, optarg);
				break;
			case 10:
				nolocale = true;
				break;
			case 11:
				top_builddir = strdup(optarg);
				break;
			case 12:
				{
					int			p = atoi(optarg);

					/* Since Makefile isn't very bright, check port range */
					if (p >= 1024 && p <= 65535)
						temp_port = p;
				}
				break;
			case 13:
				hostname = strdup(optarg);
				break;
			case 14:
				port = atoi(optarg);
				break;
			case 15:
				user = strdup(optarg);
				break;
			case 16:
				/* "--psqldir=" should mean to use PATH */
				if (strlen(optarg))
					psqldir = strdup(optarg);
				break;
			case 17:
				srcdir = strdup(optarg);
				break;
			case 18:
				split_to_stringlist(strdup(optarg), ", ", &extraroles);
				break;
                        case 19:
                	        initfile = strdup(optarg);
                        break;
                        case 20:
			        tablespace = malloc(11 + strlen(optarg) + 1);
			        if (!tablespace)
			        {
			        	fprintf(stderr, _("out of memory.\n"));
			        	exit_nicely(2);
			        }
			        snprintf(tablespace, 11 + strlen(optarg) + 1,
						"TABLESPACE %s", optarg);
		        break;
            		default:
				/* getopt_long already emitted a complaint */
				fprintf(stderr, _("\nTry \"%s -h\" for more information.\n"),
						progname);
				exit_nicely(2);
		}
	}

	/*
	 * if we still have arguments, they are extra tests to run
	 */
	while (argc - optind >= 1)
	{
		add_stringlist_item(&extra_tests, argv[optind]);
		optind++;
	}

	/*
	 * Initialization
	 */
	open_result_files();

	initialize_environment();

#if defined(HAVE_GETRLIMIT) && defined(RLIMIT_CORE)
	unlimit_core_size();
#endif

	{
		/*
		 * Using an existing installation, so may need to get rid of
		 * pre-existing database(s) and role(s)
		 */
		for (sl = dblist; sl; sl = sl->next)
			drop_database_if_exists(sl->str);
		for (sl = extraroles; sl; sl = sl->next)
			drop_role_if_exists(sl->str);
	}

	/*
	 * Create the test database(s) and role(s)
	 */
	for (sl = dblist; sl; sl = sl->next)
		create_database(sl->str);
	for (sl = extraroles; sl; sl = sl->next)
		create_role(sl->str, dblist);

	/*
	 * Find out if optimizer is on or off
	 */
	check_optimizer_status();

	/*
	 * Ready to run the tests
	 */
	header(_("running regression test queries"));

	for (sl = schedulelist; sl != NULL; sl = sl->next)
	{
		run_schedule(sl->str, tfunc);
	}

	for (sl = extra_tests; sl != NULL; sl = sl->next)
	{
		run_single_test(sl->str, tfunc);
	}

	fclose(logfile);

	/*
	 * Emit nice-looking summary message
	 */
	if (fail_count == 0 && fail_ignore_count == 0)
		snprintf(buf, sizeof(buf),
				 _(" All %d tests passed. "),
				 success_count);
	else if (fail_count == 0)	/* fail_count=0, fail_ignore_count>0 */
		snprintf(buf, sizeof(buf),
				 _(" %d of %d tests passed, %d failed test(s) ignored. "),
				 success_count,
				 success_count + fail_ignore_count,
				 fail_ignore_count);
	else if (fail_ignore_count == 0)	/* fail_count>0 && fail_ignore_count=0 */
		snprintf(buf, sizeof(buf),
				 _(" %d of %d tests failed. "),
				 fail_count,
				 success_count + fail_count);
	else
		/* fail_count>0 && fail_ignore_count>0 */
		snprintf(buf, sizeof(buf),
				 _(" %d of %d tests failed, %d of these failures ignored. "),
				 fail_count + fail_ignore_count,
				 success_count + fail_count + fail_ignore_count,
				 fail_ignore_count);

	putchar('\n');
	for (i = strlen(buf); i > 0; i--)
		putchar('=');
	printf("\n%s\n", buf);
	for (i = strlen(buf); i > 0; i--)
		putchar('=');
	putchar('\n');
	putchar('\n');

	if (file_size(difffilename) > 0)
	{
		printf(_("The differences that caused some tests to fail can be viewed in the\n"
				 "file \"%s\".  A copy of the test summary that you see\n"
				 "above is saved in the file \"%s\".\n\n"),
			   difffilename, logfilename);
	}
	else
	{
		unlink(difffilename);
		unlink(logfilename);
	}

	if (fail_count != 0)
		exit_nicely(1);

	return 0;
}
