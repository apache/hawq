#!/usr/bin/perl

# MIB WRITER -- takes a description of a table (or a sequence thereof) in the
# following format and turns it into an SNMP MIB

# -- Lines beginning with -- are comments
# -- Fields are tab-delimited
# -- All fields can be double-quote-delimited, entering "" means the field is empty, and will default to whatever its default is, if it has one
# tablename	"description, can be double-quote-delimited"	parentObject	tableOID	indexField1[,indexField2,...indexFieldN]	max-access	status
# -- since I'm expecting these tables to come from PostgreSQL, I'll make this script translate pg_class into pgClass, for instance
# -- max-access and status default to "not-accessible" and "current" if not supplied
# fieldName1	fieldType1	"description1"	OID1	max-access1	status1
# fieldName2	fieldType2	"description2"	OID2	max-access2	status2
# fieldName3	fieldType3	"description3"	OID3	max-access3	status3
# ...
# fieldNameN	fieldTypeN	"descriptionN"	OIDN	max-accessN	statusN
# -- max-access defaults to read-only; status defaults as for a table. OID defaults to the next avaliable OID starting with 1
# 
# -- multiple tables may be defined at once, if separated by a blank line

# The definition file should be piped into this script as STDIN

use strict;

my $tableDef = 1;
my $lineNum = 0;
my ($finalTableName, $tableName, $tableDesc, $rowDesc, $tableParent, $tableOID, $tableIndexList, $tableMaxAccess, $tableStatus);
my ($fieldName, $fieldType, $fieldDesc, $fieldOID, $fieldMaxAccess, $fieldStatus);
my @fields;

sub printTable {
# Convert _[a-z] to [A-Z], remove all underscores that are left somehow, and
# capitalize the first letter
	my $entryType = "pgsql".$tableName."Entry";
	my $singleEntryType = "Pgsql".$tableName."Entry";
# TODO : Should pgsql<name>MaxAccess and status be the same as the table's entries?
	print <<EOF
$finalTableName		OBJECT-TYPE
	SYNTAX		SEQUENCE OF $entryType
	MAX-ACCESS	$tableMaxAccess
	STATUS		$tableStatus
	DESCRIPTION
		"$tableDesc"
	::= { $tableParent $tableOID }

$entryType		OBJECT-TYPE
	SYNTAX		$singleEntryType
	MAX-ACCESS	$tableMaxAccess
	STATUS		$tableStatus
	DESCRIPTION
		"$rowDesc"
	INDEX  { $tableIndexList }
	::= { $finalTableName 1 }

$singleEntryType ::=
	SEQUENCE {
EOF
;
	my $tmp = 0;
	foreach my $i (@fields) {
		print "\t\t". $i->{NAME}. "\t\t". $i->{TYPE};
		if ($tmp == $#fields) { print "\n"; }
		else { print ",\n"; }
		$tmp++;
	}
	print "\t}\n\n";

	$tmp = 1;
	foreach my $i (@fields) {
		my ($fname, $fma, $fsyn, $fstat, $fdesc, $foid) =
			($i->{NAME}, $i->{MA}, $i->{TYPE}, $i->{STATUS}, $i->{DESC}, $i->{OID});
		print <<EOF2
$fname			OBJECT-TYPE
	SYNTAX		$fsyn
	MAX-ACCESS	$fma
	STATUS		$fstat
	DESCRIPTION
		"$fdesc"
	::= { $entryType $foid }

EOF2
;
	}

	print "\n---------------------------------------\n";
}

while (<STDIN>) {
	chomp;
	$lineNum++;
	if (/^$/) {
		&printTable if ($tableName ne "");
		$tableDef = 1;
		undef $tableName;
		undef @fields;
		next;
	}
	if (/^\s*--/) { print $_."\n"; next; }
	if ($tableDef) {
		die "Invalid table definition at line $lineNum\n"
			if (!m/^"?([^"\t]+)"?\t"?([^"\t]+)"?\t"?([^"\t]+)"?\t"?([^"\t]+)"?\t"?([^"\t]+)"?(\t"?([^"\t]+)"?)?(\t"?([^"\t]+)"?)?/);
		($tableName, $tableDesc, $rowDesc, $tableParent, $tableOID, $tableIndexList, $tableMaxAccess, $tableStatus) =
			($1, $2, $3, $4, $5, $7, $9, $11);

		while ($tableName =~ /_(\w)/) {
			my $tmp = uc $1;
			$tableName =~ s/_$1/$tmp/;
		}
		$tableName =~ s/_//g;
		$tableName =~ /^(\w)/;
		my $tmp = uc $1;
		$tableName =~ s/^\w/$tmp/;

		$finalTableName = "pgsql".$tableName."Table";
		$tableMaxAccess = "not-accessible" if ($tableMaxAccess eq "");
		$tableStatus = "current" if ($tableStatus eq "");
		$tableDef = 0;

		$tmp = 'pgsql' . $tableName . 'EntryOID';
		$tableIndexList =~ s/entryOID/$tmp/;
		next;
	}
	die "Invalid field definition at line $lineNum\n"
		if (!m/^"?([^"\t]+)"?\t"?([^"\t]+)"?\t"?([^"\t]+)"?(\t"?([^"\t]+)"?)?(\t"?([^"\t]+)"?)?(\t"?([^"\t]+)"?)?/);
	($fieldName, $fieldType, $fieldDesc, $fieldOID, $fieldMaxAccess, $fieldStatus) =
		($1, $2, $3, $5, $7, $9);
	$fieldMaxAccess = "read-only" if ($fieldMaxAccess eq "");
	$fieldStatus = "current" if ($fieldStatus eq "");
	if ($fieldOID eq "") {
		$fieldOID = $#fields + 2 if ($fieldOID eq "");
		if ($fieldOID < 1) {
			$fieldOID = 1;
		}
	}
	while ($fieldName =~ /_(\w)/) {
		my $tmp = uc $1;
		$fieldName =~ s/_$1/$tmp/;
	}
	$fieldName =~ /^(\w)/;
	my $tmp = "pgsql" . $tableName . uc $1;
	$fieldName =~ s/^\w/$tmp/;

	push @fields,
		{
			NAME => $fieldName,
			TYPE => $fieldType,
			DESC => $fieldDesc,
			OID => $fieldOID,
			MA => $fieldMaxAccess,
			STATUS => $fieldStatus
		};
}

&printTable if ($tableName ne "");
