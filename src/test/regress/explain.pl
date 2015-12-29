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
use Pod::Usage;
use Getopt::Long;
use Data::Dumper;
use strict;
use warnings;

=head1 NAME

B<explain.pl> - parse and reformat Postgres EXPLAIN output

=head1 SYNOPSIS

B<explain> [options] filename

Options:

    -help            brief help message
    -man             full documentation
    -option          formatting option: perl, yaml, dot, query, jpg, json
    -querylist       list of queries
    -direction       direction of query plan graph: LR, RL, TB or BT.
    -colorscheme     graph color scheme
    -timeline        rank nodes by start offset time (experimental)
    -prune           prune tree attributes
    -output          output filename (else output to STDOUT).
    -statcolor       statistics coloring (experimental)
    -edge            edge decorations

=head1 OPTIONS

=over 8

=item B<-help>

    Print a brief help message and exits.

=item B<-man>
    
    Prints the manual page and exits.

=item B<-option>

    Choose the output format option.  Several formats are supported.
    
=over 12

=item perl:  output in perl L<Data::Dumper> format.

=item yaml:  output in L<yaml.org> machine and human-readable format

=item dot:   output in dot graphical language for L<graphiz.org> graphing tool.

=item querytext: output the text of the query (only useful for TPC-H)

=item jpg:   pipe the dot output thru the dot formatter (if it is installed) to get jpg output directly.  (May also support bmp, ps, pdf, png)
    
=item json:  output in L<json.org> machine and human-readable format

=back


=item B<-querylist>

   A list of queries to process.  The query numbering is 1-based. Some
   valid forms are:

   -querylist 1 
   -querylist=2 
   --ql=3,4,5
   --ql=6-9

    or some combination. By default, all queries are processed.


=item B<-direction>

   Direction of data flow in query plan graph.  Valid entries are:

=over 12

=item BT (default): bottom to top

=item TB: top to bottom

=item LR: left to right

=item RL: right to left
    
=back

=item B<-colorscheme>

   One of the supported ColorBrewer(TM) color schemes.  Use 
   -color ? 
   to get a list of the valid schemes, and
   -color dump
   to output a dot file displaying all the valid schemes.

    Colors from www.ColorBrewer.org by Cynthia A. Brewer, Geography, 
    Pennsylvania State University.
    
=item B<-prune>

   Prune tree attributes.  The only supported option is "stats" to
   remove the to_end and to_first timing information.

=item B<-output>

   Output file name.  If multiple queries are processed, the filename
   is used as a template to generate multiple files.  If the filename
   has an extension, it is preserved, else an extension is supplied
   based upon the formatting option.  The filename template inserts
   the query number before the "dot" (.) if more than one query was
   processed.

=item B<-statcolor>

    For an EXPLAIN ANALYZE plan, color according to the time spent in
    node. Red is greatest, and blue is least.  For statcolor=ts (default), 
    the node edge is colored by time, and the node interior is filled 
    by slice color.  For statcolor=st, the color scheme is reversed.  
    For statcolor=t (timing only), the entire node is colored according 
    to the time spent.

=item B<-edge>

    Decorate graph edges with row count if available. Valid entries are:

=over 12

=item long   - print average rows and number of workers

=item medium - print average rows and number of workers compactly

=item short  - print total row counts

=back


=back

=head1 DESCRIPTION

explain.pl reads EXPLAIN output from a text file (or standard
input) and formats it in several ways.  The text file must contain
output in one of the the following formats.  The first is a regular
EXPLAIN format, starting the the QUERY PLAN header and ending with the
number of rows in parentheses.  Indenting must be on:

                                                QUERY PLAN                                                
 ----------------------------------------------------------------------------------------------------------
  Gather Motion 64:1  (slice2)  (cost=6007722.78..6007722.79 rows=6 width=51)
    Merge Key: partial_aggregation.l_returnflag, partial_aggregation.l_linestatus
    ->  Sort  (cost=6007722.78..6007722.79 rows=6 width=51)
          Sort Key: partial_aggregation.l_returnflag, partial_aggregation.l_linestatus
          ->  HashAggregate  (cost=6007722.52..6007722.70 rows=6 width=51)
                Group By: lineitem.l_returnflag, lineitem.l_linestatus
                ->  Redistribute Motion 64:64  (slice1)  (cost=6007721.92..6007722.31 rows=6 width=51)
                      Hash Key: lineitem.l_returnflag, lineitem.l_linestatus
                      ->  HashAggregate  (cost=6007721.92..6007722.19 rows=6 width=51)
                            Group By: lineitem.l_returnflag, lineitem.l_linestatus
                            ->  Seq Scan on lineitem  (cost=0.00..3693046.50 rows=92587017 width=51)
                                  Filter: l_shipdate <= '1998-09-08 00:00:00'::timestamp without time zone
 (12 rows)
 

The second acceptable format is the TPC-H EXPLAIN ANALYZE, listing
each query followed by the EXPLAIN output delineated by vertical bars
('|', e.g. |QUERY PLAN| ):

 EXPLAIN ANALYZE 
 
 
 select
     l_returnflag,
     l_linestatus,
     sum(l_quantity) as sum_qty,
     sum(l_extendedprice) as sum_base_price,
     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
     avg(l_quantity) as avg_qty,
     avg(l_extendedprice) as avg_price,
     avg(l_discount) as avg_disc,
     count(*) as count_order
 from
     lineitem
 where
     l_shipdate <= date '1998-12-01' - interval '106 day'
 group by
     l_returnflag,
     l_linestatus
 order by
     l_returnflag,
     l_linestatus;
 
 
  Query 1 complete, 19 rows returned
 
 
 |QUERY PLAN|
 |Gather Motion 64:1  (slice2)  (cost=5990545.19..5990545.21 rows=6 width=51)|
 |  recv:  Total 4 rows with 1294937 ms to end.|
 |  Merge Key: partial_aggregation.junk_attr_1, partial_aggregation.junk_attr_2|
 |  ->  Sort  (cost=5990545.19..5990545.21 rows=6 width=51)|
 |        Avg 1.00 rows x 4 workers.  Max 1 rows (seg49) with 1294938 ms to end.|
 |        Sort Key: partial_aggregation.junk_attr_1, partial_aggregation.junk_attr_2|
 |        ->  HashAggregate  (cost=5990544.94..5990545.12 rows=6 width=51)|
 |              Avg 1.00 rows x 4 workers.  Max 1 rows (seg49) with 1294933 ms to end.|
 |              Group By: lineitem.l_returnflag, lineitem.l_linestatus|
 |              ->  Redistribute Motion 64:64  (slice1)  (cost=5990544.34..5990544.73 rows=6 width=51)|
 |                    recv:  Avg 64.00 rows x 4 workers.  Max 64 rows (seg49) with 1277197 ms to first row, 1294424 ms to end.|
 |                    Hash Key: lineitem.l_returnflag, lineitem.l_linestatus|
 |                    ->  HashAggregate  (cost=5990544.34..5990544.61 rows=6 width=51)|
 |                          Avg 4.00 rows x 64 workers.  Max 4 rows (seg44) with 1292222 ms to end.|
 |                          Group By: lineitem.l_returnflag, lineitem.l_linestatus|
 |                          ->  Seq Scan on lineitem  (cost=0.00..3693046.50 rows=91899913 width=51)|
 |                                Avg 91914578.95 rows x 64 workers.  Max 91914598 rows (seg13) with 14.694 ms to first row, 258614 ms to end.|
 |                                Filter: l_shipdate <= '1998-08-17 00:00:00'::timestamp without time zone|
 |1295317.560 ms elapsed|
 
 Time was 1295.33 seconds.  Query ended at Thu Oct 12 12:09:27 2006
 
=head1 CAVEATS/LIMITATIONS

If explain.pl uses Graphviz to graph the query plan, it may flip the
left and right children of a join to obtain a more balanced pictorial
representation.  Use the -edge option to label graph edges to
correctly identify the left and right children.

=head1 AUTHORS

Apache HAWQ

Address bug reports and comments to: dev@hawq.incubator.apache.org

=cut

# IMPLEMENTATION NOTES:
#
# EXPLAIN ANALYZE final statistics in analyze_node:
# 
# The final statistics look like this:
# 
#  Slice statistics:
#    (slice0)    Executor memory: 472K bytes.
#    (slice1)    Executor memory: 464K bytes avg x 2 workers, 464K bytes max (seg0).
#  Settings:
# 
#  Total runtime: 52347.493 ms
# 
# The "Settings" entry is optional (ie, it only exists if you change the
# settings in your session).  If the "Settings" entry is missing explain.pl
# adds a dummy entry to the statistics.  This technique is a bit easier
# than changing the parser to handle both cases.
#
# Parse_node:
#   InitPlan entries in greenplum are in separate slices, so explain.pl 
#   prefixes them with an arrow (and adds a fake cost) to make them
#   look like a top-level execution node.  Again, this technique was 
#   easier than modifying the parser to special case InitPlan.
#
#  Plan parsing in general:
#  The original code only dealt with the TPCH formatted output:
# |QUERY PLAN|
# |Gather Motion 64:1  (slice2)  (cost=5990545.19..5990545.21 rows=6 width=51)|
# |  recv:  Total 4 rows with 1294937 ms to end.|
# |  Merge Key: partial_aggr.junk_attr_1, partial_aggr.junk_attr_2|
# |  ->  Sort  (cost=5990545.19..5990545.21 rows=6 width=51)|
# |        Avg 1.00 rows x 4 workers.  Max 1 rows (seg49) with 1294 ms to end.|
#
# It was easier to modify the parser to wrap the input with missing bars
# than handle two cases (are you sensing a pattern here?).
#
# "Magic" Mode:
#    This mode just adds an output filename option and constructs jpgs
#
# Output File Name:
#  The guts of the formatting code always write to STDOUT, so this code
#  resets STDOUT to the filename of choice.
#
# treemap:
#  This routine applies a function over the entire parse tree
#
# OLAP fixups:
#  OLAP queries have duplicate Shared Scan and Multi Slice Motion nodes.
#  explain.pl only fixes them up for dot output, but not for yaml, perl, etc.
#  The rationale is that dot handle digraphs nicely, but yaml and perl are
#  more suitable for tree output.
# 

my $glob_id = "";

my $glob_optn;
my $glob_qlist;
my $glob_direction;
my $glob_timeline;
my $glob_prune;
my $glob_outi;
my $glob_statcolor;
my $glob_edge;

my $GV_formats; # graphviz output formats

my %glob_coltab;

my %glob_divcol;

my $glob_colorscheme;
BEGIN {

    $GV_formats = '^(jpg|bmp|ps|pdf|png)$';

    # table of valid "qualitative" color schemes

 %glob_coltab = (set312 => [
                              '#8DD3C7',
                              '#FFFFB3',
                              '#BEBADA',
                              '#FB8072',
                              '#80B1D3',
                              '#FDB462',
                              '#B3DE69',
                              '#FCCDE5',
                              '#D9D9D9',
                              '#BC80BD',
                              '#CCEBC5',
                              '#FFED6F'
                              ],
                   paired12 => [
                                '#a6cee3',
                                '#1f78b4',
                                '#b2df8a',
                                '#33a02c',
                                '#fb9a99',
                                '#e31a1c',
                                '#fdbf6f',
                                '#ff7f00',
                                '#cab2d6',
                                '#6a3d9a',
                                '#ffff99',
                                '#b15928'
                                ],
                   pastel19 => [
                                '#fbb4ae',
                                '#b3cde3',
                                '#ccebc5',
                                '#decbe4',
                                '#fed9a6',
                                '#ffffcc',
                                '#e5d8bd',
                                '#fddaec',
                                '#f2f2f2'
                                ],
                   pastel24 => [
                                '#b3e2cd',
                                '#fdcdac',
                                '#cbd5e8',
                                '#f4cae4',
                                '#e6f5c9',
                                '#fff2ae',
                                '#f1e2cc',
                                '#cccccc'
                                ],
                   set19 => [
                             '#e41a1c',
                             '#377eb8',
                             '#4daf4a',
                             '#984ea3',
                             '#ff7f00',
                             '#ffff33',
                             '#a65628',
                             '#f781bf',
                             '#999999'
                             ],
                   set28 => [
                             '#66c2a5',
                             '#fc8d62',
                             '#8da0cb',
                             '#e78ac3',
                             '#a6d854',
                             '#ffd92f',
                             '#e5c494',
                             '#b3b3b3'
                             ],
                   original => [
                                'azure',
                                'cornsilk',
                                'lavender',
                                'mintcream',
                                'mistyrose',
                                'lightgray',
                                'salmon',
                                'goldenrod',
                                'cyan'
                                ],
                   );

    # diverging color schemes

    %glob_divcol = (
                 rdbu11 => [
                            '#67001f',
                            '#b2182b',
                            '#d6604d',
                            '#f4a582',
                            '#fddac7',
                            '#f6f6f6',
                            '#d1e5f0',
                            '#92c5de',
                            '#4393c3',
                            '#2166ac',
                            '#053061'
                            ],
                    );

}

sub qlist_fixup
{
    my $qlist = shift;

    my @outi;

    for my $qnum (@{$qlist})
    {
        if ($qnum =~ m/^\d+$/)
        {
            push @outi, $qnum;
        }
        else
        {
            if ($qnum =~ m/^\d+\-\d+$/)
            {
                my $expr = $qnum;
                $expr =~ s/\-/\.\./;

                eval "for my \$val ($expr) { push \@outi, \$val; }";
            }
            else
            {
                die("Invalid format for querylist: \'$qnum\'\n");
                exit(1);
            }

        }

    }

    return \@outi;

}


# dump a nice graph listing all of the color schemes (neato is preferred)
sub dodumpcolor
{
    my $coltab = shift;
    my $fh = shift;

    my @ggg = sort(keys(%{$coltab}));

    # centered, with lines (not arrows)
    print $fh "digraph plan1 {  graph [center=\"root\",root=\"root\"] ;\n edge [dir=none]\n";

    # adjust the lengths to avoid overlap
    for my $ii (0..(scalar(@ggg)-1))
    {
        print $fh '"root" -> "' . $ii. '"' . " [len=2];\n";

        my $jj = 0;

        for my $cc (@{$coltab->{$ggg[$ii]}})
        {
            print $fh '"' . $ii. '" -> "' . $ii. "." . $jj. '"' 
                . " [len=1];\n";
            $jj++;
        }
    }
    
    print $fh '"root" [label="color schemes"]' . ";\n";

    for my $ii (0..(scalar(@ggg)-1))
    {
        print $fh '"' . $ii . '" [label="' . $ggg[$ii] . '"]' . ";\n";

        my $jj = 0;

        for my $cc (@{$coltab->{$ggg[$ii]}}) 
        {
            print $fh '"' . $ii . "." . $jj . 
                '" [label="", style=filled, ' .
                'fillcolor="' . $cc . '"]' . ";\n";
            $jj++;
        }
    }
    print $fh "\n}\n";

}


BEGIN {
    my $man  = 0;
    my $help = 0;
    my $optn = "YAML";
    my $dir  = "BT";
    my $DEFAULT_COLOR = "set28";
    my $colorscheme = $DEFAULT_COLOR;
    my $timeline = '';
    my $prune;
    my $outfile;
    my $statcol;
    my $edgescheme; 

    my @qlst;

    GetOptions(
               'help|?' => \$help, man => \$man, 
               "querylist|ql|list:s" => \@qlst,
               "option|operation=s" => \$optn,
               "direction:s" => \$dir,
               "colorscheme:s" => \$colorscheme,
               "timeline" => \$timeline,
               "prune:s" => \$prune,
               "output:s" => \$outfile,
               "statcolor:s" => \$statcol,
               "edge:s" => \$edgescheme)
        or pod2usage(2);

    
    pod2usage(-msg => $glob_id, -exitstatus => 1) if $help;
    pod2usage(-msg => $glob_id, -exitstatus => 0, -verbose => 2) if $man;

    $glob_optn = $optn;
    $glob_optn = "jpg" if ($glob_optn =~ m/^jpeg/i);

    $glob_timeline  = $timeline;
    $glob_prune     = $prune;
    $glob_outi      = $outfile;
    $glob_statcolor = $statcol;

    $glob_edge  = $edgescheme;

    if ($dir !~ m/^(TB|BT|LR|RL)$/i)
    {
        $glob_direction = "BT";
    }
    else
    {
        $glob_direction = uc($dir);
    }

    $colorscheme = lc($colorscheme);

#    print "color: $colorscheme\n";

    if (exists($glob_coltab{$colorscheme}))
    {
        $glob_colorscheme = $colorscheme;
    }
    else
    {
        if ($colorscheme =~ m/list|dump/i)
        {
            use IO::File;
            use POSIX qw(tmpnam);

            my ($tmpnam, $tmpfh);

            for (;;) {
                $tmpnam = tmpnam();
                sysopen($tmpfh, $tmpnam, O_RDWR | O_CREAT | O_EXCL) && last;
            }

            # write to a temporary file
            dodumpcolor(\%glob_coltab, $tmpfh);

            close $tmpfh;
            
            my $catcmd = "cat $tmpnam";

            # format with neato if jpg was specified
            if ($glob_optn =~ m/$GV_formats/i)
            {
                my $dotapp = "/Applications/Graphviz.app/Contents/MacOS/neato";

                if ($^O !~ m/darwin/)
                {
                    $dotapp = `which neato`;
                    chomp($dotapp);
                }
                if (defined($dotapp) && length($dotapp) && (-e $dotapp))
                {
                    $catcmd .= " | $dotapp -T$glob_optn";
                }
            }

            system($catcmd);
            
            unlink $tmpnam;

            exit(0);
        }
        else
        {
            my $colorschemelist = join("\n", sort(keys(%glob_coltab))) . "\n";

            # identify the default color
            $colorschemelist =~ 
                s/$DEFAULT_COLOR/$DEFAULT_COLOR  \(default\)/gm;

            print "\nvalid color schemes are:\n";
            print $colorschemelist;
            print "\nUse: \"explain.pl -color dump -opt jpg > graph.jpg\"\n";
            print "to construct a JPEG showing all the valid color schemes.\n";
            print "\nColors from www.ColorBrewer.org by Cynthia A. Brewer, Geography,\nPennsylvania State University.\n\n";

            exit(0);
        }
    }

    @qlst = split(/,/,join(',', @qlst));

    $glob_qlist = qlist_fixup(\@qlst);

#    print "loading...\n" ;
}

sub analyze_node
{
    my ($node, $parse_ctx) = @_;

    if (defined($node) && exists($node->{txt}))
    {

        # gather analyze statistics if it exists in this node...
        if ($node->{txt} =~
            m/Slice\s+statistics.*(Settings.*)*Total\s+runtime/s)
        {
            my $t1 = $node->{txt};

            # NOTE: the final statistics look something like this:
            
            # Slice statistics:
            #   (slice0)    Executor memory: 472K bytes.
            #   (slice1)    Executor memory: 464K bytes avg x 2 workers, 464K bytes max (seg0).
            # Settings:
            # Total runtime: 52347.493 ms

            # (we've actually added some vertical bars so it might look 
            # like this):
            # || Slice statistics:
            # ||   (slice0)    Executor memory: 472K bytes.

            # NB: the "Settings" entry is optional, so
            # add Settings if they are missing
            unless ($t1 =~ 
                    m/Slice\s+statistics.*Settings.*Total\s+runtime/s)
            {
                $t1 =~
                    s/\n.*\s+Total\s+runtime/\n Settings\:   \n Total runtime/;
            }

            my @foo = ($t1 =~ m/Slice\s+statistics\:\s+(.*)\s+Settings\:\s+(.*)\s+Total\s+runtime:\s+(.*)\s+ms/s);
            
            if (scalar(@foo) == 3)
            {
                my $mem  = shift @foo;
                my $sett = shift @foo;
                my $runt = shift @foo;

                $mem  =~ s/\|\|//gm; # remove '||'...
                $sett =~ s/\|\|//gm;

                my $statstuff = {};

                my @baz = split(/\n/, $mem);
                my $sliceh = {};
                for my $elt (@baz)
                {
                    my @ztesch = ($elt =~ m/(slice\d+)/);
                    next unless (scalar(@ztesch));
                    $elt =~ s/\s*\(slice\d+\)\s*//;
                    my $val = shift @ztesch;
                    $sliceh->{$val} = $elt;
                }

                $statstuff->{memory}   = $sliceh;
                $statstuff->{settings} = $sett;
                $statstuff->{runtime}  = $runt;
                $parse_ctx->{explain_analyze_stats} = $statstuff;
                $node->{statistics} = $statstuff;
            }
        }

        my @short = $node->{txt} =~ m/\-\>\s*(.*)\s*\(cost\=/;
        $node->{short} = shift @short;

        unless(exists($node->{id}))
        {
            print Data::Dumper->Dump([$node]), "\n";
        }

        if ($node->{id} == 1)
        {
            @short = $node->{txt} =~ m/^\s*\|\s*(.*)\s*\(cost\=/;
            $node->{short} = shift @short;

            # handle case where dashed line might have wrapped...
            unless (defined($node->{short}) && length($node->{short}))
            {
                # might not be first line...
                @short = $node->{txt} =~ m/\s*\|\s*(.*)\s*\(cost\=/;
                $node->{short} = shift @short;
            }


        }

        # handle case of "cost-free" txt (including a double ||
        # and not first line, or screwed-up parse of short as a single bar
        #
        # example: weird initplan like:
        # ||                       ->  InitPlan  (slice49)
        if (defined($node->{short}) && length($node->{short})
            && ($node->{short} =~ m/\s*\|\s*/))
        {
            $node->{short} = "";
        }

        unless (defined($node->{short}) && length($node->{short}))
        {
            @short = $node->{txt} =~ m/\s*\|(\|)?\s*(\w*)\s*/;
            $node->{short} = shift @short;

            if (defined($node->{short}) && length($node->{short})
                && ($node->{short} =~ m/\s*\|\s*/))
            {
                $node->{short} = "";
            }
        
            # last try!!
            unless (defined($node->{short}) && length($node->{short}))
            {
                my $foo = $node->{txt};
                $foo =~ s/\-\>//gm;
                $foo =~ s/\|//gm;
                $foo =~ s/^\s+//gm;
                $foo =~ s/\s+$//gm;
                $node->{short} = $foo;
            }

#            print "long: $node->{txt}\n";
#            print "short: $node->{short}\n";
        }

        $node->{short} =~ s/\s*$//;

        # remove quotes which mess up dot file
        $node->{short} =~ s/\"//gm;

#            print "long: $node->{txt}\n";
#            print "short: $node->{short}\n";

        # XXX XXX XXX XXX: FINAL "short" fixups
        while (defined($node->{short}) && length($node->{short})
               && ($node->{short} =~ m/(\n)|^\s+|\s+$|(\(cost\=)/m))
        {
            # remove leading and trailing spaces...
            $node->{short} =~ s/^\s*//;
            $node->{short} =~ s/\s*$//;

            # remove newlines
            $node->{short} =~ s/(\n).*//gm;

            # remove cost=...
            $node->{short} =~ s/\(cost\=.*//gm;

#            print "short fixup: $node->{short}\n\n\n";
        }

        {
            if ($node->{txt} =~ m/(\d+(\.\d*)?)(\s*ms\s*to\s*end)/i)
            {

                my @ggg = 
                    ($node->{txt} =~ m/(\d+(\.\d*)?)(\s*ms\s*to\s*end)/i);
            
#                print join('*', @ggg), "\n";

                my $tt = $ggg[0];

                $node->{to_end} = $tt; 

                $parse_ctx->{alltimes}->{$tt} = 1;

                if (exists($parse_ctx->{h_to_end}->{$tt}))
                {
                    push @{$parse_ctx->{h_to_end}->{$tt}}, '"'. $node->{id} .'"';
                }
                else
                {
                    $parse_ctx->{h_to_end}->{$tt} = ['"'. $node->{id} . '"'];
                }

                    

            }
            if ($node->{txt} =~ m/(\d+(\.\d*)?)(\s*ms\s*to\s*first\s*row)/i)
            {

                my @ggg = 
                    ($node->{txt} =~ m/(\d+(\.\d*)?)(\s*ms\s*to\s*first\s*row)/i);
            
#                print join('*', @ggg), "\n";

                my $tt = $ggg[0];

                $node->{to_first} = $tt;

                $parse_ctx->{alltimes}->{$tt} = 1;

                if (exists($parse_ctx->{h_to_first}->{$tt}))
                {
                    push @{$parse_ctx->{h_to_first}->{$tt}}, '"' . $node->{id} . '"' ;
                }
                else
                {
                    $parse_ctx->{h_to_first}->{$tt} = [ '"' . $node->{id} . '"'];
                }

            }

            if ($node->{txt} =~ m/start offset by (\d+(\.\d*)?)(\s*ms)/i)
            {

                my @ggg = 
                    ($node->{txt} =~ m/start offset by (\d+(\.\d*)?)(\s*ms)/i);
            
#                print join('*', @ggg), "\n";

                my $tt = $ggg[0];

                $node->{to_startoff} = $tt; 

                $parse_ctx->{allstarttimes}->{$tt} = 1;

                if (exists($parse_ctx->{h_to_startoff}->{$tt}))
                {
                    push @{$parse_ctx->{h_to_startoff}->{$tt}}, '"'. $node->{id} .'"';
                }
                else
                {
                    $parse_ctx->{h_to_startoff}->{$tt} = ['"'. $node->{id} . '"'];
                }
            }

            if (exists($node->{to_end}))
            {
                $node->{total_time} = 
                    (exists($node->{to_first})) ?
                    ($node->{to_end} - $node->{to_first}) :
                    $node->{to_end};
            }


        }

        if (1)
        {
            if (exists($node->{child}))
            {
                delete $node->{child}
                  unless (defined($node->{child}) 
                          && scalar(@{$node->{child}}));
            }
        }


    }

}

sub parse_node
{

    my ($ref_id, $parse_ctx, $depth, $plan_rows, $parent) = @_;

#    print "depth: $depth\n";
#    print "row: ",$plan_rows->[0],"\n"      if (scalar(@{$plan_rows}));

#    print "first: $first\n" if defined ($first);

    my $spclen = undef;
    my $node = undef;

    my $no_more_text = 0;

    while (scalar(@{$plan_rows}))
    {
        my $row = $plan_rows->[0];

        unless (defined($node))
        {
            $node = {};

            $node->{child} = [];

            $node->{txt} = "";
    
            $node->{parent} = $parent
                if (defined($parent));

            my $id = $$ref_id;
            $id++;
            $$ref_id= $id;
            $node->{id} = $id;
        }

        # XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX 
        # make initplan into a fake node so the graphs look nicer (eg
        # tpch query 15).  Prefix it with an arrow and add a fake cost.
        if ($row =~ m/\|(\s)*InitPlan(.*)slice/)
        {
            $row =~ s/InitPlan/\-\>  InitPlan/;
            if ($row !~ m/\(cost=/)
            {
                $row =~ s/\|$/\(cost=\?\)|/;
            }
        }

        if ($row !~ m/\|(\s)*\-\>/)
        {
            # add text to existing node

            if ($no_more_text)
            {
                print "error: no more text for ". $node->{id}, "\n";
            }

            $node->{txt} .= "\n" . $row;

#            print "txt: $node->{txt}\n";

            shift @{$plan_rows};
            next;
        }
        else
        {
            # new node
            unless ($no_more_text)
            {
                unless (length($node->{txt}))
                {
                    $node->{txt} .= $row;
                    shift @{$plan_rows};
                    next;
                }
            }

            # match the leading spaces before the '->', eg:
            # "|  ->  Sort  (cost=5990545.19..599..."

            my @spc = ($row =~ m/\|(\s*)\-\>/);

#            print "space match:", Data::Dumper->Dump(\@spc), "\n";

            $spclen = scalar(@spc) ?  length($spc[0]) : 0;

#            print "space len: $spclen, depth: $depth\n";

            if ($spclen > $depth)
            {
                # found a child
                push @{$node->{child}}, parse_node($ref_id, $parse_ctx,
                                                   $spclen, $plan_rows, 
                                                   $node->{id});
            }

        }

        if (defined($spclen))
        {
            if ($spclen <= $depth)
            { # found a sibling or parent
                # need to put the row back on the head of the list

                if (defined($node) && exists($node->{txt}))
                {
                    analyze_node($node, $parse_ctx);
                    
                    return $node;
                }
            }
        }
        else
        {
            die ("what the heck?");
        }

        $spclen = undef;
        $no_more_text = 1;

    } # end while
    
    if (defined($node))
    {
        analyze_node($node, $parse_ctx);
    }

    return $node;

}


if (1)
{
    
    my @bigarr;
    
    my $state = "INIT";
    
    my $pair = undef;
    
    my ($query, $plan);
    
    my $tpch_format=0;
    my $bigdash = '-' x 40; # make big dash smaller

    my $magic;

    for (<>)
    {
        my $ini = $_;
        
        if ($state =~ m/INIT/)
        {
            if ($ini !~ m/(^EXPLAIN ANALYZE)|(QUERY PLAN)/)
            {
                next;
            }

            $query = "";
            $plan  = "";
            $pair = {};

            if ($ini =~ m/^EXPLAIN ANALYZE/)
            {
                $tpch_format = 1;
                $state = "GETQUERY";
                next;
            }

            if ($ini =~ m/QUERY PLAN/)
            {
                $tpch_format = 0;
                $plan  = "";
                $state = "GETPLAN";
                next;
            }

        }

        if ($state !~ m/GETPLAN/)
        {
            # should be START or GETQUERY only...
            if ($tpch_format)
            {
                if ($ini =~ m/^EXPLAIN ANALYZE/)
                {
                    if (defined($pair))
                    {
                        $pair->{plan} = $plan;
                        $pair->{query} = $query;
                        push @bigarr, $pair;
                    }
                    $pair = {};
                    $query = "";
                    $plan  = "";
                    $state = "GETQUERY";
                    next;
                }
            }
            else
            {
                # not tpch analyze
                if ($ini =~ m/QUERY PLAN/)
                {
                    if (defined($pair))
                    {
                        $pair->{plan} = $plan;
                        $pair->{query} = $query;
                        push @bigarr, $pair;
                    }
                    $pair = {};
                    $query = "";
                    $plan  = "";
                    $state = "GETPLAN";
                    next;
                }

            }
            if ($state =~ m/GETQUERY/)
            {
                if ($ini =~ m/QUERY PLAN/)
                {
                    if (!($tpch_format))
                    {
                        if (defined($pair))
                        {
                            $pair->{plan} = $plan;
                            $pair->{query} = $query;
                            push @bigarr, $pair;
                        }
                        $pair = {};
                        $query = "";
                    }

                    $plan  = "";
                    $state = "GETPLAN";
                    next;
                }

                $query .= $ini;
            }
            
        } # end not getplan
        
        if ($state =~ m/GETPLAN/)
        {

            if ($tpch_format)
            {
                if ($ini !~ m/\|(.*)\|/)
                {
                    $state = "START";
                    next;
                }
            }
            else
            {
                if ($ini =~ m/(\(\d+\s+rows\))|(Time\s+was.*seconds\.\s+Query\s+ended)/)
                {
                    $state = "START";
                    next;
                }
            }
			# a bit weird here -- just ignore the separator.  But
			# maybe we should invest some effort to determine that the
			# separator is the next line after the header (and only
			# ignore it once) ?
            next
                if ($ini =~ m/$bigdash/);

            # add the missing bars
            if (!($tpch_format))
            {
                if ($ini !~ m/\|(.*)\|/)
                {
                    $ini = '|' . $ini . '|';
                }
            }

            $plan .= $ini;
        }
        
    } # end big for
    if (defined($pair))
    {
        $pair->{plan} = $plan;
        $pair->{query} = $query;
        push @bigarr, $pair;
    }
    
#print scalar(@bigarr), "\n";
    
    
#print Data::Dumper->Dump(\@bigarr);
    
#print $bigarr[0]->{plan};

    unless(scalar(@{$glob_qlist}))
    {
        # build a 1-based list of queries 
        for (my $ii =1; $ii <= scalar(@bigarr); $ii++)
        {
            push @{$glob_qlist}, $ii;
        }
    }

    my $realSTDOUT;

    for my $qqq (@{$glob_qlist})
    {
        my $qnum = $qqq - 1; # 0 based vs 1 based

        if ($qnum >  scalar(@bigarr))
        {
            warn("specified query $qqq is out-of-range -- skipping...\n");
            next;
        }
        
        if ($glob_optn =~ m/query|text|txt/i)
        {
            doquery($bigarr[$qnum]->{query});
            next;
        }

        my $plantxt = $bigarr[$qnum]->{plan};

        unless (defined($plantxt) && length($plantxt))
        {
            warn("invalid plan for query $qqq -- skipping...\n");   
            next;
        }
        
#print $plantxt, "\n";
        
        my @plan_r = split(/\n/, $plantxt);
        
        my $pr = \@plan_r;

        my $parse_ctx = {};

        my $id = 0;

        $parse_ctx->{alltimes} = {};
        $parse_ctx->{h_to_end} = {};
        $parse_ctx->{h_to_first} = {};

        $parse_ctx->{allstarttimes} = {};
        $parse_ctx->{h_to_startoff} = {};
        $parse_ctx->{explain_analyze_stats} = {};

        my $plantree = parse_node(\$id, $parse_ctx, 0, $pr);
        
#        my @timelist = sort {$a <=> $b} keys (%{$parse_ctx->{alltimes}});
        my @timelist = sort {$a <=> $b} keys (%{$parse_ctx->{allstarttimes}});
        

        if (defined($glob_prune))
        {
            if ($glob_prune =~ m/stat|heavy|heavily/i)
            {
                my $map_expr = 'delete $node->{to_end};';
                treeMap($plantree, undef, $map_expr);
                $map_expr = 'delete $node->{to_first};';
                treeMap($plantree, undef, $map_expr);

                # additional statistics
                $map_expr = 'delete $node->{to_startoff};';
                treeMap($plantree, undef, $map_expr);
                $map_expr = 'delete $node->{total_time};';
                treeMap($plantree, undef, $map_expr);
                $map_expr = 'delete $node->{statistics};';
                treeMap($plantree, undef, $map_expr);
            }
            if ($glob_prune =~ m/heavy|heavily/i)
            {
                treeMap($plantree, 'prune_heavily($node);');
            }
        }


        # magic mode : display everything magically
        #
        # NOTE: only set to magic on the first iteration, then reset
        # to jpg, so performs correctly with multiple queries
        if ($glob_optn =~ m/magic/i)
        {
            $glob_optn = "jpg";
            
            use IO::File;
            use POSIX qw(tmpnam);

            my $tmpnam;

            for (;;) {
                my $tmpfh;

                $tmpnam = tmpnam();
                sysopen($tmpfh, $tmpnam, O_RDWR | O_CREAT | O_EXCL) && last;
            }

            # create a temporary directory name -- just append ".dir"
            # to the new tempfile name and mkdir
            my $tmpdir = $tmpnam . ".dir";

            mkdir($tmpdir) or            die "magic failed" ;

            unlink $tmpnam;  # we didn't need this tempfile anyhow

            # reset output file name to create files in the new
            # temporary directory
            $glob_outi = File::Spec->catfile($tmpdir, "query_");
            
            $magic = $glob_outi;    
        }

        if ($glob_outi)
        {
            unless (defined($realSTDOUT))
            {
                open $realSTDOUT, ">&STDOUT"  or die "Can't dup STDOUT: $!";
            }

            my $outfilename = $glob_outi;

            # only need numbering if processed more than one query
            my $neednum = (scalar(@bigarr) > 1);

            # check if name has an extension like ".foo"
            if ($outfilename =~ m/\.(.){1,5}$/)
            {
                # qqq is query num (1 based)

                my $formatq = sprintf("%03d", $qqq);

                $outfilename =~ s/\.(.*)$/$formatq\.$1/
                    if ($neednum);
            }
            else
            {
                # qqq is query num (1 based)
                my $formatq = sprintf("%03d", $qqq);

                $outfilename .= $formatq
                    if ($neednum);
                if ($glob_optn =~ m/yaml/i)
                {
                    $outfilename .= ".yml";
                }
                if ($glob_optn =~ m/json/i)
                {
                    $outfilename .= ".json";
                }
                if ($glob_optn =~ m/perl|dump/i)
                {
                    $outfilename .= ".perl";
                }
                if ($glob_optn =~ m/dot|graph/i)
                {
                    $outfilename .= ".dot";
                }
                if ($glob_optn =~ m/$GV_formats/i)
                {
                    $outfilename .= ".$glob_optn";
                }
            }

            close STDOUT;

            open (STDOUT, ">$outfilename" ) or die "can't open STDOUT: $!";

#            print $outfilename, "\n";

        }

        
        if ($glob_optn =~ m/yaml/i)
        {
            doyaml($plantree);
        }
        if ($glob_optn =~ m/json/i)
        {
            doyaml($plantree, "json");
        }
        if ($glob_optn =~ m/perl|dump/i)
        {
            doDataDump($plantree);
        }
        if ($glob_optn =~ m/dot|graph/i)
        {
            dodotfile($plantree, \@timelist, $qqq, $parse_ctx, 
                      $glob_direction);
        }
        if ($glob_optn =~ m/operator/i)
        {
            doOperatorDump($plantree);
        }

        if ($glob_optn =~ m/$GV_formats/i)
        {
            my $dotapp = "/Applications/Graphviz.app/Contents/MacOS/dot";

            if ($^O !~ m/darwin/)
            {
                $dotapp = `which dot`;
                chomp($dotapp);
            }
            die "could not find dot app: $dotapp"
                unless (defined($dotapp) && length($dotapp) && (-e $dotapp));

            # should have been able to redirect STDOUT thru a pipe
            # directly to dotapp, but didn't work.  Use a tmpfile
            # instead.

            use IO::File;
            use POSIX qw(tmpnam);

            my $tmpnam;

            for (;;) {
                my $tmpfh;

                $tmpnam = tmpnam();
                sysopen($tmpfh, $tmpnam, O_RDWR | O_CREAT | O_EXCL) && last;
            }
            open my $oldout, ">&STDOUT"     or die "Can't dup STDOUT: $!";

            close STDOUT;
            open (STDOUT, ">$tmpnam" ) or die "can't open STDOUT: $!";
 
            select STDOUT; $| = 1;      # make unbuffered

            dodotfile($plantree, \@timelist, $qqq, $parse_ctx, 
                      $glob_direction);

            close STDOUT;
            open STDOUT, ">&", $oldout or die "Can't dup \$oldout: $!";

            system("cat $tmpnam | $dotapp -T$glob_optn");

            unlink $tmpnam;

            
        }
    } #end for querynum

    if (defined($realSTDOUT))
    {
        close STDOUT;
        open STDOUT, ">&", $realSTDOUT or die "Can't dup \$oldout: $!";

    }

    # magically display all files
    if (defined($magic))
    {
        # only need numbering if processed more than one query
        my $neednum = (scalar(@{$glob_qlist}) > 1);

        if ($^O =~ m/darwin/)
        {
            # use ImageMagick montage
            my $ggg = $magic . '*';
            my $montage = `which montage`;
            chomp($montage);

            # only perform a montage if more than one query
            if ($neednum && defined($montage) && ( -e $montage))
            {
                my $dir = $magic;
                # get the directory name (remove "query_" prefix)
                $dir =~ s/query_$//;
                system("cd $dir; montage -label \'%f\' $ggg -title \"$dir\n`date`\" -shadow INDEX.html; open INDEX.html");
            }
            else
            {
                system("open $ggg");
            }
        }
    }
    
} 
#print "\nmax id: $id\n\n";

#

sub treeMap
{
    my ($node, $pre_map, $post_map, $ctx) = @_;

    eval "$pre_map"
        if (defined($pre_map));

    if (exists($node->{child}))
    {
        for my $kid (@{$node->{child}})
        {
            treeMap($kid, $pre_map, $post_map, $ctx);
        }
    }
    eval "$post_map"
        if (defined($post_map));
}

sub doDataDump
{
    my $plantree = shift;

    
    local $Data::Dumper::Indent   = 1;
    local $Data::Dumper::Terse    = 1;
    local $Data::Dumper::Sortkeys = 1;

    my $map_expr = 'delete $node->{txt};';
#    my $map_expr = 'print "foo\n"';
    treeMap($plantree, undef, $map_expr);
    
    print Data::Dumper->Dump([$plantree]);
}

sub doOperatorDump
{
    my $plantree = shift;
    
	print $plantree->{short}, "\n" if (exists($plantree->{short}));

	return
		unless (exists($plantree->{child}));

	for my $kid (@{$plantree->{child}})
	{
		doOperatorDump($kid);
	}
}

# add slice info to node
# and gather explain analyze stats
sub addslice
{
    my ($node, $ctx) = @_;

    # AUTO-6: find nodes with "(slice1)" info where the slice numbers aren't
    # part of the "Slice statistics" 

    my $txt1 = $node->{txt};
    $txt1 =~ s/Slice statistics.*//gs;

    if ($txt1 =~ /(slice(\d+))/) 
    { 
        my @ggg = ($txt1 =~ m/(slice(\d+))/) ; 
        $node->{slice} = shift @ggg; 

        # check if we have explain analyze stats for the slice
        if (exists($ctx->{explain_analyze_stats})
            && exists($ctx->{explain_analyze_stats}->{memory})
            && exists($ctx->{explain_analyze_stats}->{memory}->{$node->{slice}}))
        {
            $node->{memory} = 
                $ctx->{explain_analyze_stats}->{memory}->{$node->{slice}};
        }

    }
}


sub doquery
{
    my $qtxt = shift;

    print $qtxt, "\n";

}

sub doyaml
{
    my ($plantree, $opti) = @_;

    $opti = "yaml" unless (defined($opti));

    if ($opti =~ m/json/i)
    {
        # JSON might not be installed, so test for it.

        if (eval "require JSON")
        {
            my $map_expr = 'delete $node->{txt};';

            treeMap($plantree, undef, $map_expr);
    
            # because JSON is REQUIREd, not USEd, the symbols are not
            # imported into the environment.
            print JSON::objToJson($plantree, {pretty => 1, indent => 2});
        }
        else
        {
            die("Fatal Error: The required package JSON is not installed -- please download it from www.cpan.org\n");
            exit(1);
        }

    }
    else
    {
        # YAML might not be installed, so test for it.

        if (eval "require YAML")
        {
            my $map_expr = 'delete $node->{txt};';

            treeMap($plantree, undef, $map_expr);
    
            # because YAML is REQUIREd, not USEd, the symbols are not
            # imported into the environment.
            print YAML::Dump($plantree);
        }
        else
        {
            die("Fatal Error: The required package YAML is not installed -- please download it from www.cpan.org\n");
            exit(1);
        }
        
    }

}

# remove slice numbering information to construct even more generic plans
sub prune_heavily
{
    my $node = shift;

    return 
        unless (exists($node->{short}));

	if ($node->{short} =~ m/Delete\s*\(slice.*segment.*\)\s*\(row.*width.*\)/)
	{
		# QA-1309: fix strange DELETE operator formatting
		$node->{short} = "Delete";
	}
	elsif ($node->{short} =~ m/Update\s*\(slice.*segment.*\)\s*\(row.*width.*\)/)
        {
                # QA-1309: fix strange UPDATE operator formatting
                $node->{short} = "Update";
        }
	elsif ($node->{short} =~ m/\d+\:\d+/)
	{

		# example: Gather Motion 8:1 (slice4);

		# strip the number of nodes and slice information
		$node->{short} =~ s/\s+\d+\:\d+.*//;

		# Note: don't worry about removing "(slice1)" info from the
		# "short" because addslice processes node->{text}
	}
}

# identify the slice for each node
# and find Shared Scan "Primary"
# and find MultiSliceMotion
sub pre_slice
{
    my ($node, $ctx) = @_;

    {
        if (scalar(@{$ctx->{a1}}))
        {
            my $parent = $ctx->{a1}->[-1];

            unless (exists($node->{slice}))
            {
                if (exists($parent->{slice}))
                {
                    $node->{slice} = $parent->{slice};
                }
            }
        }

        # olap stuff

        if ($node->{short} =~ m/^Shared Scan/)
        {
            # if the Shared Scan has a child it is the "primary"
            if (exists($node->{child}))
            {
				my $share_short_fixup = $node->{short};

				# remove the slice number from the "short"
				$share_short_fixup =~ s/(\d+)\:/\:/g;

                if (!exists($ctx->{share_input_h}->{$share_short_fixup}))
                {
                    $ctx->{share_input_h}->{$share_short_fixup} = $node; 
                }
            }
            else # not the primary, mark as a duplicate node
            {
                $node->{SharedScanDuplicate} = 1;
            }
        }
        if ($node->{short} =~ m/^Multi Slice Motion/)
        {
            # choose first Multi Slice Motion node as primary
            if (!exists($ctx->{multi_slice_h}->{$node->{short}}))
            {
                $ctx->{multi_slice_h}->{$node->{short}} = $node; 
            }
            else # not the primary, mark as a duplicate node
            {
                $node->{MultiSliceMotionDuplicate} = 1;
            }
        }

        if (exists($node->{total_time}))
        {
            my $tt = $node->{total_time};
            my $tt2 = $tt * $tt;
            $ctx->{time_stats_h}->{cnt} += 1;
            $ctx->{time_stats_h}->{sum} += $tt;
            $ctx->{time_stats_h}->{sumsq} += $tt2;

            if (exists($ctx->{time_stats_h}->{tt_h}->{$tt}))
            {
                push @{$ctx->{time_stats_h}->{tt_h}->{$tt}}, $node;
            }
            else
            {
                $ctx->{time_stats_h}->{tt_h}->{$tt} = [$node];
            }
        }

    }

    push @{$ctx->{a1}}, $node;

}

sub post_slice
{
    my ($node, $ctx) = @_;

    pop @{$ctx->{a1}};

}

# make all duplicate sharedscan nodes point back to primary
sub sharedscan_fixup
{
    my ($node, $ctx) = @_;

    if (exists($node->{SharedScanDuplicate}))
    {
		my $share_short_fixup = $node->{short};

		# remove the slice number from the "short"
		$share_short_fixup =~ s/(\d+)\:/\:/g;

        $node->{SharedScanDuplicate} =
            $ctx->{share_input_h}->{$share_short_fixup};
#        $node->{id} = 
#            $node->{SharedScanDuplicate}->{id};
    }

    if (exists($node->{MultiSliceMotionDuplicate}))
    {
        $node->{MultiSliceMotionDuplicate} =
            $ctx->{multi_slice_h}->{$node->{short}};
        # XXX XXX: for this case the node is really the same
        $node->{id} = 
            $node->{MultiSliceMotionDuplicate}->{id};
    }

}

sub human_num
{
    my $esti = shift;

    my @suffix = qw(K M G T P E Z Y);
    my $suff = "";

    # try to shorten estimate specification
    while (length(POSIX::ceil($esti)) > 3)
    {
        $suff = shift @suffix;

        $esti = $esti/1000;
    }

    if (length($suff))
    {
        $esti *= 100;
        $esti = POSIX::floor($esti+0.5);
        $esti = $esti/100;

        $esti .= $suff;
    }

    return $esti;
}

# label left and right for nest loops
sub nestedloop_fixup
{
    my ($node, $ctx) = @_;

    return
        unless (exists($node->{short}) &&
                ($node->{short} =~ m/Nested Loop/));

    my @kidlist;

    if (exists($node->{child}))
    {
        for my $kid (@{$node->{child}})
        {
            push @kidlist, $kid;
        }
    }

    return 
        unless (2 == scalar(@kidlist));

    if ($kidlist[0]->{id} < $kidlist[1]->{id})
    {
        $kidlist[0]->{nested_loop_position} = "left";
        $kidlist[1]->{nested_loop_position} = "right";
    }
    else
    {
        $kidlist[1]->{nested_loop_position} = "left";
        $kidlist[0]->{nested_loop_position} = "right";
    }

}

# find rows out information
sub get_rows_out
{
    my ($node, $ctx, $edge) = @_;

    return 
        unless ($node->{txt} =~ m/(Rows out\:)|(\(cost\=.*\s+rows=.*\s+width\=.*\))/);

    my $long = ($edge =~ m/long|med/i);

    if ($node->{txt} =~ m/Rows out\:\s+Avg.*\s+rows\s+x\s+.*\s+workers/)
    {
        if (!$long)
        {
            # short result
            my @foo = 
                ($node->{txt} =~ 
                 m/Rows out\:\s+Avg\s+(.*)\s+rows\s+x\s+(.*)\s+workers/);
        
            goto L_get_est unless (2 == scalar(@foo));

            # calculate row count as avg x num workers
            $node->{rows_out} = $foo[0] * $foo[1];
        }
        else
        {
            my @foo = 
                ($node->{txt} =~ 
                 m/Rows out\:\s+(Avg.*workers)/);
        
            goto L_get_est unless (1 == scalar(@foo));

            # just print the string
            $node->{rows_out} = $foo[0];

            if ($edge =~ m/med/i)
            {
                $node->{rows_out} =~ s/Avg\s+//;
                $node->{rows_out} =~ s/rows\s+//;
                $node->{rows_out} =~ s/\s*workers\s*//;
            }

        }
    }
    elsif ($node->{txt} =~ m/Rows out\:\s+.*\s+rows/)
    {
        my @foo = 
            ($node->{txt} =~ 
             m/Rows out\:\s+(.*)\s+rows/);
        
        goto L_get_est unless (1 == scalar(@foo));

        $node->{rows_out} = $foo[0];
    }

    if (
        exists($node->{rows_out}) &&
        length($node->{rows_out})
        )
    {
        if (
            ($node->{rows_out} !~ m/avg/i) &&
            ($node->{rows_out} =~ m/x/))
        {
            my @foo = ($node->{rows_out} =~ m/(x.*)$/);

            my $tail = $foo[0];
        
            @foo = ($node->{rows_out} =~ m/(.*)\s+x.*/);

            my $head = $foo[0];

            if (defined($tail) && defined($head))
            {
                $head = human_num($head);

                $node->{rows_out} = $head . " " . $tail;
            }

        }
        elsif ($node->{rows_out} =~ m/^\d+$/)
        {
            $node->{rows_out} = human_num($node->{rows_out});
        }
    }

L_get_est:

    # add row estimates
    if ($long && 
        ($node->{txt} =~ m/\(cost\=.*\s+rows=.*\s+width\=.*\)/))
    {
        my @foo = ($node->{txt} =~ m/cost\=.*\s+rows=(\d+)\s+width\=.*/);

        if (scalar(@foo))
        {
            use POSIX;

            my $esti = $foo[0];

            $esti = human_num($esti);

            unless (exists($node->{rows_out}) &&
                    length($node->{rows_out}))
            {
                $node->{rows_out} = "";
            }

            $node->{rows_out} .= " (est $esti)";
        }
    }

} # end get_rows_out

sub calc_color_rank
{
    my $ctx = shift;

    return
        unless (defined($glob_statcolor));

    if ($ctx->{time_stats_h}->{cnt} > 1)
    {
        # population variance = 
        # (sum of the squares)/n - (square of the sums)/n*n
        my $sum   = $ctx->{time_stats_h}->{sum};
        my $sumsq = $ctx->{time_stats_h}->{sumsq};
        my $enn   = $ctx->{time_stats_h}->{cnt};
        
        my $pop_var = ($sumsq/$enn) - (($sum*$sum)/($enn*$enn));
        my $std_dev = sqrt($pop_var);
        my $mean    = $sum/$enn;
        my $half    = $std_dev/2;

        # calculate a stanine (9 buckets, each 1/2 of stddev).  The
        # middle bucket (5, which is 4 if we start at zero) is
        # centered on the mean, so it starts on mean - (1/4 stddev),
        # and ends at mean + (1/4 stddev).
        my @bucket;
        my $buckstart = ($mean-($half/2))-(3*$half);

        push @bucket, 0;

        for my $ii (1..7)
        {
            push @bucket, $buckstart;
            $buckstart += $half;
        }
        push @bucket, 2**40; # "infinity"

        my @tlist = sort {$a <=> $b} (keys %{$ctx->{time_stats_h}->{tt_h}});

        # must have at least two
        my $firstt = shift @tlist;
        my $lastt  = pop @tlist;
#        print "f,l: $firstt, $lastt\n";

        for my $nod (@{$ctx->{time_stats_h}->{tt_h}->{$firstt}})
        {
#            print "first ", $nod->{id}, ": ", $nod->{short}, " - ", 0, "\n";
            $nod->{color_rank} = 10;
        }
        for my $nod (@{$ctx->{time_stats_h}->{tt_h}->{$lastt}})
        {
#            print "last ", $nod->{id}, ": ", $nod->{short}, " - ", 10, "\n";
            $nod->{color_rank} = 1;
        }

#        print "bucket: ", Data::Dumper->Dump(\@bucket);
#        print "tlist: ", Data::Dumper->Dump(\@tlist);
#        print Data::Dumper->Dump([$ctx->{time_stats_h}]);

        my $bucknum = 1;
        for my $tt (@tlist)
        {
#            print "tt: $tt\n";
#            print "bk: $bucket[$bucknum]\n";

            while ($tt > $bucket[$bucknum])
            {
#                print "$tt > $bucket[$bucknum]\n";
#                last if ($bucknum >= 11);
                $bucknum++;
            }
            for my $nod (@{$ctx->{time_stats_h}->{tt_h}->{$tt}})
            {
#                print "node ", $nod->{id}, ": ", $nod->{short}, " - ", $bucknum, "\n";
#                $nod->{color_rank} = ($bucknum-1);
                $nod->{color_rank} = (10 - $bucknum);
            }
        }

    }

}

sub dodotfile
{
    my ($plantree, $time_list, $plan_num, $parse_ctx, $direction) = @_;

    {
        my $map_expr = 'addslice($node, $ctx); ';
        
        treeMap($plantree, $map_expr, undef, $parse_ctx);
    }


#    $map_expr = 'propslice($node, $ctx);';
    my $ctx = {level => 0, a1 => [], 
               share_input_h => {}, multi_slice_h => {}, 
               time_stats_h => { cnt=>0, sum=>0, sumsq=>0, tt_h => {} }  };

#    my $map_expr = 'print "foo\n"';
    treeMap($plantree, 
            'pre_slice($node, $ctx); ',
            'post_slice($node, $ctx); ',
            $ctx);

    calc_color_rank($ctx);

    treeMap($plantree, 
            'sharedscan_fixup($node, $ctx); ',
            undef,
            $ctx);

    # always label the left/right sides of nested loop
    treeMap($plantree, 
            'nestedloop_fixup($node, $ctx); ',
            undef,
            $ctx);

    if (defined($glob_edge) && length($glob_edge))
    {
        treeMap($plantree, 
                'get_rows_out($node, $ctx, $glob_edge); ',
                undef,
                $ctx);
    }

    my $dotimeline = $glob_timeline;

    makedotfile($plantree, $time_list, $dotimeline, $plan_num, $parse_ctx,
                $direction);
}


sub dotkid
{
    my $node = shift;

    # XXX XXX: olap fixup - don't label duplicate multi slice motion nodes
    return
        if (exists($node->{MultiSliceMotionDuplicate}));

    # XXX XXX: olap fixup - have children of primary sharedscan
    # point to this node
    if (exists($node->{SharedScanDuplicate}))
    {
        for my $kid (@{$node->{SharedScanDuplicate}->{child}})
        {
            print '"' . $kid->{id} . '" -> "' . $node->{id} . '"' . ";\n";
        }
    }

    my $docrunch = 2;

    if (exists($node->{child}))
    {
        if (($docrunch != 0 ) && (scalar(@{$node->{child}} > 10)))
        {
            my $maxi = scalar(@{$node->{child}});
            
            $maxi -= 2;

            for my $ii (2..$maxi)
            {
                $node->{child}->[$ii]->{crunchme} = 1;
            }

            if ($docrunch == 2)
            {
                splice(@{$node->{child}}, 3, ($maxi-2));

                $node->{child}->[2]->{short} = "... removed " . ($maxi - 3) . " nodes ...";
            }


        }

        for my $kid (@{$node->{child}})
        {
            my $edge_label = "";

            print '"' . $kid->{id} . '" -> "' . $node->{id} . '"';

            if (exists($kid->{nested_loop_position}))
            {
                $edge_label .= $kid->{nested_loop_position};
            }

            if (exists($kid->{rows_out}))
            {
                $edge_label .= " ";
                $edge_label .= " "
                    if (length($edge_label));
                $edge_label .= $kid->{rows_out};
            }

            if (length($edge_label))
            {
                print ' [label="' . $edge_label . '" ] ';
            }

            print ";\n";
        }

        for my $kid (@{$node->{child}})
        {
            dotkid($kid);
        }

    }

}

sub dotlabel_detail
{
    my $node = shift;

#    return     $node->{short} ;

    my $outi = $node->{short};

    my ($frst, $last) = (" ", " ");

    if (exists($node->{to_end}))
    {
        $last = "end: " . $node->{to_end};
    }
    if (exists($node->{to_first}))
    {
        $frst = "first row: " . $node->{to_first};
    }
    
    my $slice = $node->{slice};
    $slice = " "
        unless (defined($slice));


    if ((length($frst) > 1) || (length($last) > 1))
    {
        my $memstuff = "";

        # add memory statistics if have them...
        if (exists($node->{memory}))
        {
            $memstuff = " | { {" . $node->{memory} . "} } ";
            # make multiline - split on comma and "Work_mem" 
            # (using the vertical bar formatting character)
            $memstuff =~ s/\,/\,\| /gm;
            $memstuff =~ s/Work\_mem/\| Work\_mem/gm;
        }

#        $outi .= " | { " . join(" | " , $frst, $last) . " } ";
        $outi .= " | { " . join(" | " , $slice, $frst, $last) . " } " . $memstuff;

        # wrapping with braces changes record organization to vertical
        $outi = "{ " . $outi . " } ";
    }


    return $outi;
}


sub dotlabel
{
    my $node = shift;

    # XXX XXX: olap fixup - don't label duplicate multi slice motion nodes
    return
        if (exists($node->{MultiSliceMotionDuplicate}));

    my $colortable = $glob_coltab{$glob_colorscheme};

    my $color = scalar(@{$colortable});
    $color = $node->{slice}  if (exists($node->{slice}));
    $color =~ s/slice//;
    
    $color = ($color) % (scalar(@{$colortable}));
    
    # build list of node attributes
    my @attrlist;
    push @attrlist, "shape=record";
#    push @attrlist, "shape=polygon";
#    push @attrlist, "peripheries=2";

#    push @attrlist, 'fontcolor=white';

    push @attrlist, 'label="' . dotlabel_detail($node) .'"';
    push @attrlist, 'style=filled';
#    push @attrlist, 'style="filled,bold"';
#    push @attrlist, "color=" . $colortable->[$color];
#    push @attrlist, "fillcolor=" . $colortable->[$color];

    if (exists($node->{color_rank})) # color by statistical ranking
    {
        my $edgecol = $glob_divcol{rdbu11}->[$node->{color_rank}];
        my $fillcol = $colortable->[$color];

        if (defined($glob_statcolor)) 
        {
            if ($glob_statcolor =~ m/^t$/i)
            {
                # show timing color only
                $fillcol = $edgecol;
            }
            if ($glob_statcolor =~ m/^st/i)
            {
                # edge is slice color, fill is time stats
                # invert the selection
                ($edgecol, $fillcol) = ($fillcol, $edgecol);
            }
        }

        push @attrlist, 'style="filled,setlinewidth(6)"';
        push @attrlist, "color=\"" . $edgecol . '"';

        push @attrlist, "fillcolor=\"" . $fillcol . '"';
    }
    else
    {
        push @attrlist, "color=\"" . $colortable->[$color] . '"';
        push @attrlist, "fillcolor=\"" . $colortable->[$color] . '"';
    }

    if (exists($node->{crunchme}))
    {
        @attrlist = ();

#    push @attrlist, 'style=filled';
    push @attrlist, 'style=filled';
    push @attrlist, "color=\"" . $colortable->[$color] . '"';
    push @attrlist, "fillcolor=\"" . $colortable->[$color] . '"';
#    push @attrlist, "shape=circle";
        push @attrlist, "label=\"" . $node->{short} . '"';
#    push @attrlist, "fontsize=1";
#    push @attrlist, "height=0.01";
#    push @attrlist, "width=0.01";
#    push @attrlist, "height=0.12";
#    push @attrlist, "width=0.12";

        print '"' . $node->{id} . '" [' . join(", ", @attrlist) . '];' . "\n" ;
    }
    else
    {
        print '"' . $node->{id} . '" [' . join(", ", @attrlist) . '];' . "\n" ;
    }

    if (exists($node->{child}))
    {
        for my $kid (@{$node->{child}})
        {
            dotlabel($kid);
        }

    }

}


sub makedotfile
{
    my ($plantree, $time_list, $do_timeline, $plan_num, $parse_ctx,
        $direction) = @_;

#    print "\n\ndigraph plan1 { ranksep=.75; size = \"7.5,7.5\";\n\n \n";
    print "\n\ndigraph plan$plan_num {  \n";

#    print "graph [bgcolor=black];\n edge [style=bold, color=white];\n";
#    print "graph [bgcolor=black];\n edge [style=dashed, color=white];\n";
#    print "graph [bgcolor=black];\n edge [style=dotted, color=white];\n";

    if ($do_timeline && scalar(@{$time_list}))
    {
        print "   ranksep=.75; size = \"7.5,7.5\";\n\n \n";
        print "   {\n node [shape=plaintext, fontsize=16];\n";
        print "/* the time-line graph */\n";
        
        print join(' -> ', @{$time_list} ), ";\n";
        print "}\n";
        
        print "node [shape=box];\n";
        
        while ( my ($kk, $vv) = each(%{$parse_ctx->{h_to_startoff}}))
        {
            print '{ rank = same; ' . $kk . '; ' . join("; ", @{$vv}) . "; }\n";
        }
        
    }

    print "rankdir=$direction;\n";

    dotkid($plantree);
    
    dotlabel($plantree);
    
    print "\n}\n";
    
}



exit();
