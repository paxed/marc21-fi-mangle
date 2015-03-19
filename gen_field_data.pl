#!/usr/bin/perl

# perl gen_field_data.pl > /tmp/kohadb.dat
# mysql -u root --local-infile -p koha
# CREATE TABLE tempkw (bibnum int unsigned not null, idx int unsigned not null, tag varchar(3) not null, subfield varchar(1), ind1 varchar(1), ind2 varchar(2), value varchar(1024), KEY valueidx (value), KEY tagidx (tag), KEY fieldidx (tag, subfield));
# LOAD DATA LOCAL INFILE '/tmp/kohadb.dat' INTO TABLE tempkw;


use strict;
use warnings;
use DBI;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use Getopt::Long;
use Pod::Usage;

use utf8;
use open ':utf8';

binmode(STDOUT, ":utf8");


my %dbdata = (
    'hostname' => 'localhost',
    'username' => 'kohaadmin',
    'password' => 'katikoan',
    'dbname' => 'koha',
    'driver' => 'mysql'
    );

my $help = 0;
my $man = 0;
my %ignore_fields;
my $nocache = 0;
my $insert_table = undef;
my $create_table = 0;

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (defined($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting."); } },
    'ignore=s' => sub { my ($onam, $oval) = @_; foreach my $tmp (split/,/, $oval) { $ignore_fields{$tmp} = 1; } },
    'nocache' => \$nocache,
    'insert=s' => \$insert_table,
    'create' => \$create_table,
    'help|h|?' => \$help,
    'man' => \$man
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

die("Illegal characters in insert table name.") if (defined($insert_table) && ($insert_table !~ /^[a-zA-Z_0-9]+$/));

MARC::Charset->assume_unicode(1);


sub db_connect {
    my $dbh = DBI->connect("DBI:" . $dbdata{'driver'} . ":dbname=" . $dbdata{'dbname'} . ";host=" . $dbdata{'hostname'}, $dbdata{'username'}, $dbdata{'password'}, {'RaiseError' => 1, mysql_enable_utf8 => 1});
    if (!$dbh) {
	print "DB Error.";
	exit;
    }
    return $dbh;
}

sub db_disconnect {
    my $dbh = shift || die("Error.");
    $dbh->disconnect();
}

my $dbh = db_connect();
my $sth;
my $ins_sth;


$dbh->do("SET NAMES 'utf8';");

if (defined($insert_table)) {
    my @tmparr = ($insert_table);
    if ($create_table) {
	$dbh->do("DROP TABLE IF EXISTS $insert_table");
	$dbh->do("CREATE TABLE $insert_table (bibnum int unsigned not null, idx int unsigned not null, tag varchar(3) not null, subfield varchar(1), ind1 varchar(1), ind2 varchar(2), value varchar(1024), KEY valueidx (value), KEY tagidx (tag), KEY fieldidx (tag, subfield))");
    }
    $ins_sth = $dbh->prepare("INSERT INTO $insert_table (bibnum, idx, tag, subfield, ind1, ind2, value) values (?, ?, ?, ?, ?, ?, ?)");
}

my %marcdata;

sub output_marcdata {
    foreach my $k (keys(%marcdata)) {
	foreach my $v (@{$marcdata{$k}}) {
	    if (defined($insert_table)) {
		my @arr = split(/\t/, $v);
		#unshift(@arr, $insert_table);
		$ins_sth->execute(@arr);
	    } else {
		print $v."\n";
	    }
	}
    }
    undef %marcdata;
}


my $sql = "select biblioitemnumber, marcxml from biblioitems order by biblioitemnumber asc";
$sth = $dbh->prepare($sql);
$sth->execute();
while (my $ref = $sth->fetchrow_hashref()) {

    my $bn = $ref->{'biblioitemnumber'};
    my $record;
    my $idx = 0;

    eval {
        $record = MARC::Record->new_from_xml($ref->{'marcxml'});
    };

    push(@{$marcdata{'LDR'}}, "$bn\t".($idx++)."\tLDR\t\t\t\t".$record->leader()) if (!defined($ignore_fields{'ldr'}));

    foreach my $luri ($record->field('...')) {
	my $tag = $luri->tag();
	next if (defined($ignore_fields{$tag}));
        if (scalar($tag) < 10) {
	    push(@{$marcdata{$tag}}, "$bn\t".($idx++)."\t$tag\t\t\t\t".$luri->data());
	    next;
	}

        my $ind1 = $luri->indicator(1);
        my $ind2 = $luri->indicator(2);

	foreach my $sf ($luri->subfields()) {
            my $code = $sf->[0];
            my $data = $sf->[1];
	    my $ctag = $tag . $code;
	    next if (defined($ignore_fields{$ctag}));
	    push(@{$marcdata{$ctag}}, "$bn\t".($idx++)."\t$tag\t$code\t$ind1\t$ind2\t$data");

	}
    }
    output_marcdata() if ($nocache);
}

db_disconnect($dbh);

output_marcdata();


__END__

=head1 NAME

gen_field_data.pl - Create a data dump out of Koha biblioitems marcxml

=head1 OPTIONS

=over 8

=item B<-help>

Print this help.

=item B<-man>

Print this help as a man page.

=item B<-nocache>

Output the data immediately, instead of keeping it all in memory
and dumping it out at end of script.

=item B<-insert=tablename>

Insert data into table `tablename` instead of printing it to
stdout. Drops and recreates the table automatically. Uses
the same database settings as reading the data.

=item B<-create>

Drop and create the table defined with -insert before inserting
data into it.

=item B<-ignore=fieldspecs>

Ignore certain fields or subfields. For example:
  C<-field=590,028a>
would ignore the fields 590 and subfields 028a.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will query MARC21 XML field contents and output them, one per line.

=cut
