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

my %usefields = (
    '650' => '150',
    '100' => '100',
    '700' => '100',
    '600' => '100',
    '710' => '110',
    '651' => '151',
    '610' => '110',
    '110' => '110',
    '630' => '150',
    '830' => '150',
    '655' => '150',
    '130' => '150',
    '810' => '110'
    );

my %ignore_fields;
my $help = 0;
my $man = 0;
my $verbose = 0;
my $dry_run = 0;
my $sqlquery = 'select biblionumber as id, marcxml as marc from biblioitems order by id asc';

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (defined($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting."); } },
    'sql=s' => \$sqlquery,
    'v|verbose+' => \$verbose,
    'ignore=s' => sub { my ($onam, $oval) = @_; foreach my $tmp (split/,/, $oval) { $ignore_fields{$tmp} = 1; } },
    'dry-run|test' => \$dry_run,
    'help|h|?' => \$help,
    'man' => \$man
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

die("No SQL query") if (!defined($sqlquery));

sub db_connect {
    my $dbh = DBI->connect("DBI:" . $dbdata{'driver'} . ":dbname=" . $dbdata{'dbname'} . ";host=" . $dbdata{'hostname'}, $dbdata{'username'}, $dbdata{'password'}, {'RaiseError' => 1, mysql_enable_utf8 => 1});
    if (!$dbh) {
	print "DB Error.";
	footer();
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

sub get_auth {
    my ($authid, $authfield) = @_;

    my $sth = $dbh->prepare('select ExtractValue(marcxml, "//datafield[@tag='.substr($authfield, 0, 3).']/subfield[@code=\''.substr($authfield, 3, 1).'\']") as fielddata from auth_header where authid = ?');
    $sth->execute($authid);
    my $ref = $sth->fetchrow_hashref();
    return $ref->{'fielddata'};
}



my %cache_auths;

sub check_marc {
    my $id = shift;
    my $marc = shift;

    my $update = 0;

    my $record;
    eval {
	$record = MARC::Record->new_from_xml($marc);
    };

    foreach my $f ($record->field('...')) {
	my $fi = $f->{'_tag'};

	next if ((scalar($fi) < 10) || (lc($fi) eq 'ldr'));
	next if (defined($ignore_fields{$fi}));
	next if (!defined($usefields{$fi}));
	next if (!defined($f->subfield('9')));
	next if (!defined($f->subfield('a')));

	my $authid = $f->subfield('9');

	next if (!($authid =~ /^\d+$/));

	my $authdata = $cache_auths{$authid} || get_auth($authid, $usefields{$fi}.'a');

	if ($authdata =~ /^(.+) ?([:;,.])$/) {
	    $authdata =~ $1;
	}
	$cache_auths{$authid} = $authdata;

	my $origdata = $f->subfield('a');
	my $tmpdata = $origdata;

	my $aacr2punct = '';
	if ($tmpdata =~ /^(.+) ?([:;,.])$/) {
	    $aacr2punct = $2;
	    $tmpdata = $1;
	}

	if ((lc($tmpdata) eq lc($authdata)) && ($tmpdata ne $authdata)) {
	    my $newdata = $authdata . $aacr2punct;
	    $f->update('a' => $newdata);
	    print "UPDATE: $id, ".$fi."a: $origdata => $newdata ($authid)\n";
	    $update = 1;
	} else {
	    #print "XXX: $fi, $origdata | $tmpdata | $authdata\n";
	}

    }
    return $record->as_xml_record();
}

sub print_x {
    my ($stra, $strb, $width) = @_;
    my @arra = split(/\n/, $stra);
    my @arrb = split(/\n/, $strb);
    $width = $width || 80;

    my ($a, $b);
    my $fmt = "%s%-".$width."s%s|%s%-".$width."s%s\n";

    print "\n".("="x((2*$width)+1))."\n";
    for (my $i = 0; $i < scalar(@arra); $i++) {
	if ($arra[$i] ne $arrb[$i]) {
	    $a = "\e[31m";
	    $b = "\e[0m";
	} else {
	    $a = $b = '';
	}
	printf($fmt, $a, substr($arra[$i],0,$width), $b, $a, substr($arrb[$i],0,$width), $b);
    }
    print "".("="x((2*$width)+1))."\n";
}

my $sth = $dbh->prepare($sqlquery);

$sth->execute();

my $sth_upd = $dbh->prepare('update biblioitems set marcxml=? where biblionumber=?');

#print "OK, Ready to go! --more--\n";
#<STDIN>;

my $i = 1;
while (my $ref = $sth->fetchrow_hashref()) {
    if ($ref->{'id'}) {
	if ($verbose) {
	    print "\n$i" if (!($i % 100));
	    print ".";
	}
	my $newmarc = check_marc($ref->{'id'}, $ref->{'marc'});
	if ($ref->{'marc'} ne $newmarc) {
	    print_x($ref->{'marc'}, $newmarc) if ($verbose > 1);
	    $sth_upd->execute($newmarc, $ref->{'id'}) if (!$dry_run);
	}
	$i++;
    }
}
print "\n" if ($verbose > 1);

db_disconnect($dbh);


__END__

=head1 NAME

marc_bibs2authcase.pl - Change MARC21 biblio records data to match the authority data

=head1 OPTIONS

=over 8

=item B<-help>

Print this help.

=item B<-man>

Print this help as a man page.

=item B<-v|-verbose>

Print progress bars.

=item B<-dry-run|-test>

Do not actually change anything.

=item B<-sql=text>

Set the SQL query to perform. Must return two fields (id and marcxml). Default depends
on the preset values used. For example, if you want the id this program reports to be the
contents of the 001 field, use -sql='select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc'

=item B<-ignore=fieldspecs>

Ignore certain fields, subfields or indicators. For example:
  C<-ignore=590,028a,655.ind2>
would ignore the field 590, subfield 028a, and indicator 2 of field 655.

=item B<-nodata>

Do not test fixed field lengths or data validity.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will set the text of authorized fields (as per subfield 9), so the field data
matches the authority data.

=cut
