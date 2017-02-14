#!/usr/bin/env perl
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
use Pod::Usage;
use Getopt::Long;
use Data::Dumper;
use strict;
use warnings;

=head1 NAME

B<gpexclude.pl> - gpexclude

=head1 SYNOPSIS

B<gpexclude.pl> [options] 

Options:

    -help            brief help message
    -man             full documentation
    -connect         psql connect parameters
    -testname        test name
    -exclude_file    file containing exclusion rules
    -verbose         be very chatty
    -quiet           be very quiet

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-connect>

    psql connect string, e.g:

    -connect '-p 11000 -d template1'

    If the connect string is not defined gpexclude calls gpstringsubs
    which uses PGPORT and template1.  If the PGPORT is undefined then
    the port number defaults to 11000.

=item B<-testname>

    Name of the test

=item B<-exclude_file>

    File containing exclusion rules

=item B<-verbose>

    Print rule matching and evaluation status to stdout.  Useful for
    debugging.

=item B<-quiet>

    Return successfully even if the exclude file is not specified or
    does not exist.  This setting is useful for automated testing,
    since the assumption is that if the test schedule does not have an
    associated set of exclusions, then all the tests in the schedule
    are legal.


=back

=head1 DESCRIPTION

gpexclude.pl takes as arguments the name of a test and a file of
exclusion rules, and it returns 0 (TRUE) if the test should be
executed, else it returns 1 (FALSE).  pg_regress invokes gpexclude.pl
(supplying the test name and <schedule_name>.EXCLUDE) in order to
determine if it should run or ignore that test.  gpexclude.pl calls
gpstringsubs.pl to construct a hash of platform and configuration
information.  It evaluates rules in the exclude file that reference
this hash and returns FALSE if the test rule does not succeed.

=head2 EXCLUDE file format

pg_regress invokes gpexclude.pl with the assumption that each schedule
has an associated exclude file with a ".EXCLUDE" suffix.  For example,
for known_good_schedule, the exclude file is known_good_schedule.EXCLUDE.

The exclude file can contain single-line comments, where the first
non-blank character of the line is a pound sign (#).  If the line is
not blank or a comment, it must be a rule, which has the following
format:

<test name regex> ::: <expression>

That is, the first portion of the rule is a regular expression for a
test name in perl regex format (see "man perlre").  gpexclude.pl
automatically wraps the supplied expression with "^" and "$" to match
the beginning and of the line, so a simple regex like "foo" only
matches a test with the exact name of "foo".  If you want to use the
"foo" as a prefix match, you must specify "foo.*".  If you want a rule
that matches every test name specify ".*".  A test name can match
multiple rules as long as the expression evaluates to TRUE.
gpexclude.pl returns FALSE for the first rule that fails.

The regex and expression are separated by three colons (:::), mainly
to avoid the problem that a single colon is often a component of a
perl regex.  Leading and trailing whitespace is ignored.

The expression must be a perl boolean expression which evaluates to
TRUE or FALSE.  This expression can reference the "$gpx" hash, which
is defined at the start of gpexclude.pl with various platform and
configuration values.  For example, to enforce that the test
"sql_mojo" only runs on configurations with 5 segments, you can define
a rule:

  sql_mojo ::: $gpx->{number_of_segs} == 5

If the expression is TRUE, gpexclude will continue 

=head1 AUTHORS

Apache HAWQ

Address bug reports and comments to: dev@hawq.incubator.apache.org

=cut

my $glob_id = "";
my $glob_h1;
my $glob_tname;
my $glob_exclude;
my $glob_verbose;

sub stringsubs
{
    use IO::File;
    use File::Temp;

    my ($conn_str, $verbose) = @_;

    my ($tmpnam, $tmpfh);

    for (;;) {
        $tmpnam = tmpnam();
        sysopen($tmpfh, $tmpnam, O_RDWR | O_CREAT | O_EXCL) && last;
    }

    # write to a temporary file                                         

    close $tmpfh;

    my $gpx = {};

    open ($tmpfh, "> $tmpnam") or die "Could not open file $tmpnam for writing : $! \n";

    my $bigstr = <<'BIG_END';
$gpx->{hostname} = "@hostname@";
$gpx->{generic_processor} = "@gpuname_p@";
$gpx->{isainfo} = "@gpisainfo@";
$gpx->{osname} = "@perl_osname@";
$gpx->{number_of_segs} = "@number_of_segs@";
BIG_END

    print $tmpfh $bigstr;

    close $tmpfh;

    my $gpstringsubs = "gpstringsubs.pl";

    {

        my $plname = $0;

        my @foo = File::Spec->rel2abs(File::Spec->splitpath($plname));

        if (scalar(@foo))
        {
            pop @foo;

            $gpstringsubs =
                File::Spec->rel2abs(
                                    File::Spec->catfile(
                                                        @foo,
                                                        "gpstringsubs.pl"
                                                        ));
        }
    }
    
    system "$gpstringsubs $conn_str $tmpnam";
    
    open ($tmpfh, "< $tmpnam") or die "Could not open file $tmpnam for reading : $! \n";

    {
        # undefine input record separator (\n")
        # and slurp entire file into variable
        local $/;
        undef $/;

        my $whole_file = <$tmpfh>;

        eval "$whole_file";
        
        if ($@)
        {
            die "failed to evaluate file $tmpnam";
        }
    }

    if ($verbose)
    {
        print "gpx hash values are:\n";
        print Data::Dumper->Dump([$gpx]), "\n";
    }


    close $tmpfh;
    
    unlink $tmpnam;

    return $gpx;
}

BEGIN {
    my $man  = 0;
    my $help = 0;
    my $conn;
    my $tname;
    my $exclude;
    my $verbose;
    my $quiet;

    GetOptions(
               'help|?' => \$help, man => \$man, 
               "connect=s" => \$conn,
               "testname=s" => \$tname,
               "exclude_file=s" => \$exclude,
               "verbose" => \$verbose,
               "quiet" => \$quiet
               )
        or pod2usage(2);

    
    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    if ($quiet)
    {
        # quiet mode just exits if no exclude file
        exit(0)
            unless (defined($exclude) && length($exclude) && (-f $exclude));
    }



    unless ((defined($tname) && length($tname)) &&
            (defined($exclude) && length($exclude)))
    {
        my $got_tname = (defined($tname) && length($tname));
        my $got_exclu = (defined($exclude) && length($exclude));
        

        if ($got_tname)
        {
            $glob_id = "Missing required --exclude_file option"
        }
        else
        {
            if ($got_exclu)
            {
                $glob_id = "Missing required --testname option"
            }
            else
            {
                $glob_id = "Missing required --exclude_file and --testname options"
            }
        }

        pod2usage(-msg => $glob_id, -exitstatus => 2);
    }

    my $conn_str = "";

    $conn_str = "-c \'$conn\'" 
        if (defined($conn) && length($conn));

    $glob_verbose = $verbose;

    $glob_h1 = stringsubs($conn_str, $verbose);

    $glob_exclude = $exclude;
    $glob_tname = $tname;

}


if (1)
{
    my $fh;
    my $infile   = $glob_exclude;
    my $testname = $glob_tname;

    # hash of platform/os/configuration values
    my $gpx = $glob_h1;

    open ($fh, "< $infile ") 
        or die "Could not open file $infile for reading : $! \n";

    print "reading from file $infile\n"
        if ($glob_verbose);

    my $linecount = 0;

    while (<$fh>) # big while
    {
        my $ini = $_;

#        print $ini;

        $linecount++;

        next
            if (($ini =~ m/^\s*\#/) || # ignore comments
                ($ini =~ m/^\s*$/));   # or blanks

        chomp $ini;

        my @foo = split(/\:\:\:/, $ini, 2);

        die "invalid format for file $infile, line $linecount: $ini"
            unless (2 == scalar(@foo));

        # trim leading and trailing spaces
        $foo[0] =~ s/^\s+//gm;
        $foo[1] =~ s/^\s+//gm;
        $foo[0] =~ s/\s+$//gm;
        $foo[1] =~ s/\s+$//gm;

        my $regex1 = '^(' . $foo[0] . ')$';

        if ($testname =~ m/$regex1/)
        {
            print "line $linecount: $testname matches $foo[0]\n"
                if ($glob_verbose);

            my $stat;

            my $stat_str = '$stat = (' . $foo[1] . ')';

            eval "$stat_str";
            if ($@)
            {
                die "invalid condition in file $infile at line $linecount:\n$stat_str";
            }

#            print $ini;

            if ($stat)
            {
                print "\tTRUE:  ($foo[1])\n"
                    if ($glob_verbose);
            }
            else
            {
                print "\tFALSE: ($foo[1])\n"
                    if ($glob_verbose);

                exit(1);
            }

        }
        
    } # end big while

    exit(0);

}

exit(0);
