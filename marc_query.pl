use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use DBI;


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
my $sqlquery = 'select biblionumber as id, marcxml as marc from biblioitems order by id asc';
my $marcfield = '600c';
my $showid = 0;

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (defined($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting."); } },
    'sql=s' => \$sqlquery,
    'field=s' => \$marcfield,
    'showid' => \$showid,
    'help|h|?' => \$help,
    'man' => \$man
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

die("No SQL query") if (!defined($sqlquery));
die("No XPath") if (!defined($marcfield));

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


MARC::Charset->assume_unicode(1);

my %marc_fields;

foreach (split(/,/, $marcfield)) {
    my $fnum = substr($_, 0, 3);
    if (!defined($marc_fields{$fnum})) {
	$marc_fields{$fnum} = substr($_, 3);
    } else {
	$marc_fields{$fnum} = $marc_fields{$fnum} . substr($_, 3);
    }
}

my $sth = $dbh->prepare($sqlquery);

$sth->execute();

my $i = 1;
while (my $ref = $sth->fetchrow_hashref()) {
    if ($ref->{'id'}) {
	my $record;
	eval {
	    $record = MARC::Record->new_from_xml($ref->{'marc'});
	};
	foreach my $f ($record->field('...')) {
	    my $fi = $f->tag();
	    next if (!defined($marc_fields{$fi}));
	    foreach (split(//, $marc_fields{$fi})) {
		if (defined($f->subfield($_))) {
		    print $ref->{'id'}."\t" if ($showid);
		    print $f->subfield($_)."\n";
		}
	    }
	}
	$i++;
    }
}

db_disconnect($dbh);




__END__

=head1 NAME

marc_query.pl - List MARC21 fields contents, one per line

=head1 OPTIONS

=over 8

=item B<-help>

Print this help.

=item B<-man>

Print this help as a man page.

=item B<-showid>

Also print the record id number for the data.

=item B<-sql=text>

Set the SQL query to perform. Must return two fields (id and marcxml). Default depends
on the preset values used. For example, if you want the id this program reports to be the
contents of the 001 field, use -sql='select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc'

=item B<-field=fieldspecs>

Query certain fields or subfields. For example:
  C<-field=590,028a>
would list the fields 590 and subfields 028a.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will query MARC21 XML field contents and output them, one per line.

=cut
