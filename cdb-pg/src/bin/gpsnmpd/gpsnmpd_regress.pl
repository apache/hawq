#! /usr/bin/perl

# PGSNMPD REGRESSION TEST -- pgsnmpd_regress.pl
# $Id: pgsnmpd_regress.pl,v 1.11 2007/08/07 02:33:22 eggyknap Exp $

use File::Temp qw( tempfile );
use Test::More tests => 7;
use POSIX;
use strict;

my $tmp;
my @tmp;
my @psqlToolCmds;
my %opt;

sub usage() {
	print <<EOF
DESCRIPTION
	pgsnmpd_regress.pl creates a new database on the specified host, popu-
	lates a few tables (including pgsnmpd-specific tables), starts pgsnmpd,
	and queries a bunch of stuff from it. It compares those results with
	various expected results to see that pgsnmpd works as expected. Note
	that it *will* drop the database after it is finished testing, unless
	explicitly told not to.

	pgsnmpd_regress.pl assumes it is running from a directory with
	pgsnmpd.sql and the pgsnmpd binary in it.

USAGE:
	pgsnmpd_regress.pl [ -U <user> ] [ -W <password> ] [ -h <host> ]
	    [ -p <port> ] [ -d <dbname> ] [ -c <createdb_path ]
	    [ -P <psql_path> ] [ -D <dropdb_path> ] [ -n ] [ -? ]

OPTIONS
	-U	Username to connect to an existing database cluster. This user
		should have permissions to create and drop databases on the
		cluster. This will default to the current user name if
		unspecified.

	-W	Password for PostgreSQL account. Note: pgsnmpd_regress.pl will
		prompt for this password a few times if this option is set,
		even though the specified password is correct, because it uses
		the createdb and psql binaries to run the regression test, and
		they won't take passwords automatically. pgsnmpd_regress.pl
		doesn't really want to create a password file and mess with any
		the user might already have set up. Sorry for the extra typing.

	-h	Hostname or IP address of the server to test; defaults to
		localhost. If this begins with a slash character, it will be
		interpreted as the directory of the Unix domain socket for this
		server

	-p	Port of the server to test; defaults to 5432. If the -h para-
		meter begins with a slash, this will be interpreted as the
		filename of the Unix domain socket
	
	-d	Name of the database to create; defaults to pgsnmpd_regress

	-c	Path to createdb, if required

	-P	Path to psql, if required

	-D	Path to dropdb, if required

	-n	Do not drop the test database after creation

	-v	Verbose mode

	-?	Show this help

EOF
;
	exit;
}

{
	use Getopt::Std;
	getopts( "U:W:h:p:d:c:P:D:n?v", \%opt ) or usage();
	usage() if $opt{'?'};
}

my $createdb = $opt{c} ? $opt{c} : 'createdb';
my $dbname   = $opt{d} ? $opt{d} : 'pgsnmpd_regress' ;
my $psql     = $opt{P} ? $opt{P} : 'psql';
my $dropdb   = $opt{D} ? $opt{D} : 'dropdb';

#print "c: $createdb\nu: $user\np: $pwd\nh: $host\np: $port\nd: $dbname\np: $psql\n";


sub TesterProc() {
	use SNMP;

	my $dbOID;

	SNMP::addMibDirs('.');
	SNMP::addMibFiles('RDBMS-MIB');

	sleep 5; 	# give pgsnmpd a chance to start up. There are better ways of doing this (e.g. wait for the port to start listening, perhaps), but this should do
	my $sess = new SNMP::Session(Version => '2c', DestHost => 'localhost', Community => 'public', RemotePort => '10161');
	my $rdbmsDbTable = $sess->gettable('RDBMS-MIB::rdbmsDbTable');

	# Get Database OID (rdbmsDbIndex value for this database from rdbmsDbTable)
	foreach my $key (keys %{$rdbmsDbTable}) {
		$tmp = $rdbmsDbTable->{$key};
		if ($tmp->{'rdbmsDbName'} eq $dbname) { 
			$dbOID = $key;
			last;
		}
	}

	# Test that OID exists
	ok( defined $dbOID, 'Found OID for this database');

	print "   ** Getting DbInfoTable\n";
	my $rdbmsDbInfoTable 		= $sess->gettable('RDBMS-MIB::rdbmsDbInfoTable');
	print "   ** Getting rdbmsDbParamTable\n";
	my $rdbmsDbParamTable		= $sess->gettable('RDBMS-MIB::rdbmsDbParamTable');
	print "   ** Getting rdbmsDbLimitedResourceTable\n";
	my $rdbmsDbLimitedResourceTable = $sess->gettable('RDBMS-MIB::rdbmsDbLimitedResourceTable');
	print "   ** Getting rdbmsSrvTable\n";
	my $rdbmsSrvTable 		= $sess->gettable('RDBMS-MIB::rdbmsSrvTable');
	print "   ** Getting rdbmsSrvInfoTable\n";
	my $rdbmsSrvInfoTable 		= $sess->gettable('RDBMS-MIB::rdbmsSrvInfoTable');
	print "   ** Getting rdbmsSrvParamTable\n";
	my $rdbmsSrvParamTable 		= $sess->gettable('RDBMS-MIB::rdbmsSrvParamTable');

	SKIP: {
		skip ("Database OID is undefined -- other tests are meaningless, because they'll all fail", 6) if (!defined $dbOID);

		# Test rdbmsDbInfoTable -- Make sure a row is present for this database OID
		ok( defined $rdbmsDbInfoTable->{$dbOID}, 'rdbmsDbInfoTable has a row for this database');

		# rdbmsDbParamTable -- geqo should be off in this database
		is(${$rdbmsDbParamTable->{$dbOID.".4.103.101.113.111.0"}}{"rdbmsDbParamCurrValue"}, 'off', 'geqo should be off');

		# rdbmsDbLimitedResourceTable -- row should exist for XID
		ok( defined ${$rdbmsDbLimitedResourceTable->{$dbOID.".3.88.73.68"}}{"rdbmsDbLimitedResourceCurrent"}, 'XID should be reported in rdbmsDbLimitedResourceTable');

		# rdbmsSrvTable -- contact name should be pgsnmpd_regress
		is ( ${$rdbmsSrvTable->{"1"}}{"rdbmsSrvContact"}, "pgsnmpd_regress", "Contact name should be set to pgsnmpd_regress");

		# rdbmsSrvInfoTable -- finished transactions should be reported
		ok ( defined ${$rdbmsSrvInfoTable->{$dbOID}}{"rdbmsSrvInfoFinishedTransactions"}, "Finished transactions should be reported");

		# rdbmsSrvParamTable -- lots of parameters should exist
		my @paramKeys = keys %{$rdbmsSrvParamTable};
		cmp_ok( $#paramKeys, '>', 0, 'Lots of server parameters should be defined');
	}

	# Kill pgsnmpd now that we're done
	{
		print "   ** Killing pgsnmpd\n";
		local $SIG{TERM} = 'IGNORE';
		kill -(POSIX::SIGTERM) => $$;
	}
}

# Create the database
@psqlToolCmds = ();
push @psqlToolCmds, ('-U', $opt{U}) if $opt{U};
push @psqlToolCmds, ('-h', $opt{h}) if $opt{h};
push @psqlToolCmds, ('-p', $opt{p}) if $opt{p};

{
	if (!defined($opt{v})) {
		open OLDOUT, ">&STDOUT";
		open STDOUT, "/dev/null" || warn "STDOUT can't be redirected -- pgsnmpd_regress.pl might be a bit verbose...\n";
	}
	my @tmpCmds = @psqlToolCmds;
	push @tmpCmds, $dbname;
	print "   ** Creating database $dbname\n";
	system $createdb, @tmpCmds;

	# Populate tables
	open INFILE, 'pgsnmpd.sql';
	$tmp = <<EOF
	CREATE SCHEMA pgsnmpd;
	SET SEARCH_PATH TO pgsnmpd, public;
EOF
;
	while (<INFILE>) {
		$tmp .= $_;
	}
	close INFILE;
	$tmp .= <<EOF
	INSERT INTO pgsnmpd_rdbmsSrvTable (contact_name) VALUES ('pgsnmpd_regress');
	INSERT INTO pgsnmpd_rdbmsDbTable (database_oid, vendor_name, contact_name, last_backup)
		SELECT oid, 'PostgreSQL Global Development Group', 'pgsnmpd_regress', CURRENT_TIMESTAMP - (INTERVAL '1 second' * oid::INTEGER)
			FROM pg_database;
	ALTER DATABASE $dbname SET geqo TO off;
EOF
;

	@tmpCmds = @psqlToolCmds;
	push @tmpCmds, ("-c", $tmp, $dbname);
	system $psql, @tmpCmds;
	if (!defined($opt{v})) {
		close STDOUT;
		open STDOUT, ">&OLDOUT";
	}
}

# Create config file and fire up pgsnmpd
my ($tmpHandle, $tmpName) = tempfile('pgsnmpd_regress_tmp_XXXX', UNLINK => 1);
while (<DATA>) {
	print $tmpHandle $_;
}
close DATA;

$| = 1;		# Make sure stuff flushes properly when we fork. This really shouldn't matter much, but this is just to be safe

my $pid;

if ($pid = fork()) {
	# Parent process

	TesterProc();
}
else {
	# Child process

	use Cwd;

	if (!defined($opt{v})) {
		print "   ** Redirecting pgsnmpd child process STDOUT and STDERR to /dev/null\n";
		open STDOUT, "> /dev/null" || warn "Can't direct STDOUT of child process -- pgsnmpd_regress.pl might be a bit verbose...\n";
		open STDERR, "> /dev/null" || warn "Can't direct STDERR of child process -- pgsnmpd_regress.pl might be a bit verbose...\n";
	}

	@tmp = ();
	$tmp = "dbname=$dbname";
	$tmp .= " user=$opt{U}" if ($opt{U} ne '');
	$tmp .= " password=$opt{W}" if ($opt{W} ne '');
	$tmp .= " host=$opt{h}" if ($opt{h} ne '');
	$tmp .= " port=$opt{p}" if ($opt{p} ne '');
	push @tmp, ('-c', $tmpName, '-C', $tmp);
	print "Running pgsnmpd with command line $tmp\n";

	$tmp = getcwd . "/pgsnmpd";
	print "Child process running $tmp\n";
	system $tmp, @tmp;
	if ($? == -1) {
		print "failed to execute: $!\n";
	}
	elsif ($? & 127) {
		printf "child died with signal %d, %s coredump\n",
		($? & 127),  ($? & 128) ? 'with' : 'without';
	}
	else {
		printf "child exited with value %d\n", $? >> 8;
	}

	die "Child pgsnmpd process killed\n";
}

if (! $opt{'n'}) {
	print "   ** Dropping test database $dbname\n";
	if (!defined($opt{v})) {
		open OLDOUT, ">&STDOUT";
		open STDOUT, "> /dev/null" || warn "Can't direct STDOUT of child process -- pgsnmpd_regress.pl might be a bit verbose...\n";
	}
	my @tmpCmds = @psqlToolCmds;
	push @tmpCmds, $dbname;
	system $dropdb, @tmpCmds;
	if (!defined($opt{v})) {
		close STDOUT;
		open STDOUT, ">&OLDOUT";
	}
}

__DATA__
com2sec 	readwrite	default		public
group 		MyRWGroup 	v2c		readwrite
view 		all    		included	.1		80
access 		MyRWGroup 	""		any		noauth	exact	all	all	none
agentaddress 	localhost:10161
