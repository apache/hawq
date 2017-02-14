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
use strict;
use warnings;
use POSIX;
use File::Spec;
use Env;
use Data::Dumper;

=head1 NAME

B<gpstringsubs.pl> - GreenPlum string substitution 

=head1 SYNOPSIS

B<gpstringsubs.pl> filename

Options:

    -help            brief help message
    -man             full documentation
    -connect         psql connect parameters

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-connect>

    psql connect string, e.g:

    -connect '-p 11000 template1'

    If the connect string is not defined gpstringsubs uses 
    PGPORT and template1.  If the PGPORT is undefined then 
    the port number defaults to 11000.
        
=back

=head1 DESCRIPTION

gpstringsubs find tokens in an input file and replaces them (in place)
with specified or environmental values, supplementing the pg_regress
"convert_sourcefiles_in" function.

The tokens are:

=over 8

=item hostname

 gpstringsubs finds the hostname for segment 0 
 from the gp_configuration table and replaces all 
 instances of the token @hostname@.

=item gpfilespace_(filespacename)

 filespace segment list for current configuration

=item gp_glob_connect

 a compound psql connect string, equivalent to the "-connect" option
 for this tool.

=item gphome

 location of the greenplum installation

=item ps_pidfirst

 generate a long ps listing with the pid first, equivalent to 
 ps -axww on osx

=item gpwhich_(executable)

 Find the full path for the executable and substitute.  For example, 
 @gpwhich_gpfdist@ is replaced with the full path for "gpfdist".

=item gpuname_(flag)

 Run "uname" with the specified flag and substitute the result.  For
 example, @gpuname_p@ is replaced with the output of "uname -p", which
 finds the generic processor type:

=item gpcurusername

 Replace @gpcurusername@ with the username of the user executing the script.

=item gpupgradeschemaname

 Replace @gpupgradeschemaname@ with upg_catalog.

=item gpupgradedatadir

 Replace @gpupgradedatadir@ with /path/to/regression/data/

=over 12

=item Mac OSX intel: i386
=item Mac OSX ppc: powerpc
=item Linux 64: x86_64
=item Linux 32: i686
=item Sun Sparc: sparc
=item Sun Solaris 64: i386

=back

=item gpisainfo

  If on Solaris, run "isainfo" and get the real architecture, eg sparcv9
  or amd64, else just use "uname -p".

=item number_of_segs

  Number of segments.

=item perl_osname

  Use what perl thinks the osname is.  

=back


=head1 AUTHORS

Apache HAWQ

Address bug reports and comments to: dev@hawq.incubator.apache.org

=cut

    my $glob_id = "";
    my $glob_connect;


BEGIN {
    my $man  = 0;
    my $help = 0;
    my $conn;

    GetOptions(
               'help|?' => \$help, man => \$man,
               "connect=s" => \$conn
               )
        or pod2usage(2);


    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    $glob_connect = $conn;

    my $porti = $ENV{PGPORT} if (exists($ENV{PGPORT}));

    $porti = 5432                 # 11000
        unless (defined($porti));

    $glob_connect = "-p $porti -d template1"
        unless (defined($glob_connect));

#    print "loading...\n" ;      

}


# convert a postgresql psql formatted table into an array of hashes
sub tablelizer
{
    my ($ini, $got_line1) = @_;

    # first, split into separate lines, the find all the column headings

    my @lines = split(/\n/, $ini);

    return undef
        unless (scalar(@lines));

    # if the first line is supplied, then it has the column headers,
    # so don't try to find them (or the ---+---- separator) in
    # "lines"
    my $line1 = $got_line1;
    $line1 = shift @lines
        unless (defined($got_line1));

    # look for <space>|<space>
    my @colheads = split(/\s+\|\s+/, $line1);

    # fixup first, last column head (remove leading,trailing spaces)

    $colheads[0] =~ s/^\s+//;
    $colheads[0] =~ s/\s+$//;
    $colheads[-1] =~ s/^\s+//;
    $colheads[-1] =~ s/\s+$//;

    return undef
        unless (scalar(@lines));
    
    shift @lines # skip dashed separator (unless it was skipped already)
        unless (defined($got_line1));
    
    my @rows;

    for my $lin (@lines)
    {
        my @cols = split(/\|/, $lin, scalar(@colheads));
        last 
            unless (scalar(@cols) == scalar(@colheads));

        my $rowh = {};

        for my $colhdcnt (0..(scalar(@colheads)-1))
        {
            my $rawcol = shift @cols;

            $rawcol =~ s/^\s+//;
            $rawcol =~ s/\s+$//;

            my $colhd = $colheads[$colhdcnt];
            $rowh->{($colhdcnt+1)} = $rawcol;
        }
        push @rows, $rowh;
    }

    return \@rows;
}

# get_filespace_seglist
#
# build a seglist (basically as list of dbid : directory name)
# for the gpfilespace_<fsname> token.  
# 
# TODO: options for missing/duplicate/pre-existing dirs, etc.
#
sub get_filespace_seglist
{
	my $fsname = shift;

	$fsname = "regressionfsx" unless (defined($fsname));

    my $psql_str = "psql ";
    
    $psql_str .= $glob_connect
        if (defined($glob_connect));

    $psql_str .= " -c \'" .
"select gscp.registration_order, " .
"gscp.hostname as hostname, gscp.address as address, " .
"fep.fselocation as loc, " .
"pfs.oid fsoid, " .
"pfs.fsname, " .
"gscp.status " .
"from " .
"gp_segment_configuration gscp, pg_filespace_entry fep, " .
"pg_filespace pfs " .
"where " .
'fsname = $q$pg_system$q$  ' .
"and pfs.oid = fep.fsefsoid " .
"order by 1,2  \' " ;

#    print $psql_str, "\n";

    my $tabdef = `$psql_str`;

#    print $tabdef;

    my $seg_config_table = tablelizer($tabdef);

#	print Data::Dumper->Dump([$seg_config_table]);
	
	my @seg_list;

    for my $rowh (@{$seg_config_table})	
	{
		my $fsseg;

		$fsseg = $rowh->{1} . ": ";

		my $dir = $rowh->{5};

		my @foo = File::Spec->splitdir($dir);		

		pop @foo;

		$dir = File::Spec->catdir(@foo, 
								  "gpregr_" . 
								  $fsname . "_" . $rowh->{10} . $rowh->{1});

###		my $outi = `gpssh -h $rowh->{4} rmdir -rf $dir`;
		
		$fsseg .= "\'" . $dir . "\'";
#		$fsseg .= '$q$'. $dir . '$q$';

		$seg_list[$rowh->{1}] = $fsseg;
	}

	shift @seg_list unless (defined($seg_list[0]));

#	print Data::Dumper->Dump(\@seg_list);

#	return quotemeta("( " . join(", ", @seg_list) . " )");
	return "( " . join(", ", @seg_list) . " )";
} # end get_filespace_seglist

if (0)
{
	print get_filespace_seglist(), "\n";
	print get_filespace_seglist("foo"), "\n";

}

if (1)
{
    exit(-1)
        unless (scalar(@ARGV));

    my $filnam = $ARGV[0];

    unless (-f $filnam)
    {
        die "invalid filename: $filnam";
        exit(-1);
    }


    my $psql_str = "psql ";
    
    $psql_str .= $glob_connect
        if (defined($glob_connect));

#    $psql_str .= " -c \'\\d $glob_tab \'";

    $psql_str .= " -c \'select registration_order, role, status, hostname from gp_segment_configuration \'";

#    print $psql_str, "\n";

    my $tabdef = `$psql_str`;

#    print $tabdef;

    my $mpp_config_table = tablelizer($tabdef);

#    print Data::Dumper->Dump([$mpp_config_table]);

    my $hostname= "localhost";

    for my $rowh (@{$mpp_config_table})
    {
        if (($rowh->{1} == 0) &&     # content (seg 0)
            ($rowh->{2} =~ m/p/) &&  # role = primary
            ($rowh->{3} =~ m/u/))    # status = up
        {
            $hostname = $rowh->{4};  # hostname
            last;
        }
        
    }

	my $username = getpwuid($>);

    $psql_str = "psql ";
    
    $psql_str .= $glob_connect
        if (defined($glob_connect));

    $psql_str .= " -c \"select count(*) from gp_segment_configuration where role = 'p' \"";

    $tabdef = `$psql_str`;

    $mpp_config_table = tablelizer($tabdef);

    my $numsegs = 0;

#   print Data::Dumper->Dump([$mpp_config_table]);

    for my $rowh (@{$mpp_config_table})
    {
        $numsegs = $rowh->{1};
        last;
    }

    my $numsegexp = '\\@number_of_segs\\@';

    my $hostexp = '\\@hostname\\@';
    my $psexp   = '\\@ps_pidfirst\\@';
	my $unexp = '\\@gpcurusername\\@';
	my $schema = '\\@gpupgradeschemaname\\@';
	my $upgtoolkit = '\\@gpupgradetoolkitschema\\@';
	my $infoschema = '\\@gpinfoschemaname\\@';
	my $ugdir = '\\@gpupgradedatadir\\@';
	my $gphomeexp = '\\@gphome\\@';
	my $gpglobconn = '\\@gp_glob_connect\\@';

	my $gphome = $ENV{GPHOME};

    my $locps   = 'ps axww';
    if ($^O !~ /darwin|mac|osx/i)
    {
        $locps   = 'ps -elf';
    }


    # get the perl osname
    my $perlosexp = '\\@perl_osname\\@';
    my $locperlos = $^O;

    # Sun Solaris only -- use uname -p otherwise
    my $isainfoexp = '\\@gpisainfo\\@';
    
    my $devn = '2>/dev/null';
    my $isainfoloc = `which isainfo $devn`;
    my $locisainfo = `uname -p`;
    chomp $locisainfo;

    if (length($isainfoloc) && ($isainfoloc !~ m/no isainfo/)) # on solaris
    {
        my $realinfo = `isainfo`;
        chomp $realinfo;

        my @foo = split(/\s+/, $realinfo);

        $locisainfo = $foo[0]
            if (scalar(@foo));
    }

    if ($^O !~ /darwin|mac|osx|linux/i)
    {
        # error!
    }

    my $gpfspace_all = `grep gpfilespace_ $filnam`;
    my $gpwhich_all  = `grep gpwhich_ $filnam`;
    my $gpuname_all  = `grep uname_ $filnam`;
	my $curdir = `pwd`;
	chomp $curdir;

#    print "$filnam\n";
    system "perl -i -ple \' s/$hostexp/$hostname/gm; s,$gphomeexp,$gphome,gm; s/$psexp/$locps/gm; s/$isainfoexp/$locisainfo/gm; s/$perlosexp/$locperlos/gm; s/$numsegexp/$numsegs/gm; s/$unexp/$username/gm; s/$schema/upg_catalog/gm; s/$infoschema/upg_information/gm; s,$ugdir,$curdir/data/upgrade42,gm; s/$gpglobconn/$glob_connect/gm; s/$upgtoolkit/upg_toolkit/gm;   \' $filnam\n";

    # replace filespace
    if (defined($gpfspace_all) && length($gpfspace_all))
    {
        my @foo = split(/\@/, $gpfspace_all);

#        print Data::Dumper->Dump(\@foo);

        my @gpfspace_list;

        if (scalar(@foo))
        {
            for my $gpfs (@foo)
            {
                my @exe_thing;

                next
                    if (($gpfs =~ m/is\_transformed\_to/));

                next
                    unless (($gpfs =~ m/gpfilespace\_\w/));

                my @fsname_thing = ($gpfs =~ m/gpfilespace\_(.*)/);

                next
                    unless (scalar(@fsname_thing));

                my $fsname = $fsname_thing[0];

                next
                    unless (length($fsname));

                chomp $fsname;

				my $fspaceseglist = get_filespace_seglist($fsname);

                my $fspaceexp = '\\@gpfilespace_' . $fsname_thing[0] . '\\@';

				{
					local $/;
					undef $/;
 
					my $ifh;
					open ($ifh, "<", $filnam);

					my $whole_file = <$ifh>;

					close $ifh;

					$whole_file =~ s|$fspaceexp|$fspaceseglist|gm;

					my $ofh;

					open ($ofh, ">", $filnam);
					print $ofh $whole_file;
					close $ofh;
				}
            } # end for
        } # end if scalar foo
    } # end if gpfilespace
	
    # replace all "which" expressions with binary
    if (defined($gpwhich_all) && length($gpwhich_all))
    {
        my @foo = split(/\@/, $gpwhich_all);

#        print Data::Dumper->Dump(\@foo);

        my @gpwhich_list;

        if (scalar(@foo))
        {
            for my $gpw (@foo)
            {
                my @exe_thing;

                next
                    if (($gpw =~ m/is\_transformed\_to/));

                next
                    unless (($gpw =~ m/gpwhich\_\w/));

                @exe_thing = ($gpw =~ m/gpwhich\_(.*)/);

                next
                    unless (scalar(@exe_thing));

                my $binloc = `which $exe_thing[0]`;

                next
                    unless (length($binloc));

                chomp $binloc;
                $binloc = quotemeta($binloc);

                my $whichexp = '\\@gpwhich_' . $exe_thing[0] . '\\@';

                system "perl -i -ple \' s/$whichexp/$binloc/gm;\' $filnam\n";

            } # end for
        } # end if scalar foo
    } # end if which all

    # replace all "gpuname" expressions with "uname -<flag>" output
    if (defined($gpuname_all) && length($gpuname_all))
    {
        my @foo = split(/\@/, $gpuname_all);

#        print Data::Dumper->Dump(\@foo);

        my @gpuname_list;

        if (scalar(@foo))
        {
            for my $gpw (@foo)
            {
                my @flag_thing;

                next
                    if (($gpw =~ m/is\_transformed\_to/));

                next
                    unless (($gpw =~ m/gpuname\_\w/));

                @flag_thing = ($gpw =~ m/gpuname\_(.*)/);

                next
                    unless (scalar(@flag_thing));

                my $flag = $flag_thing[0];
                my $binloc = `uname -$flag`;

                next
                    unless (length($binloc));

                chomp $binloc;
                $binloc = quotemeta($binloc);

                my $unameexp = '\\@gpuname_' . $flag . '\\@';

                system "perl -i -ple \' s/$unameexp/$binloc/gm;\' $filnam\n";

            } # end for
        }
    } # end gpuname


    exit(0);
}
