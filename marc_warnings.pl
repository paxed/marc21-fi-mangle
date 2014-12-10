use strict;
use warnings;
#use diagnostics;

use Getopt::Long;
use Pod::Usage;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use Encode;
use Unicode::Normalize;
use DBI;
use Data::Dumper;
use XML::TreePP;


# Biblios
use constant MARC_XML => './marcedit-tooltips.xml';
# Evergreen
#use constant DB_QUERY => 'select id, marc from biblio.record_entry order by id asc';
# Koha
use constant DB_QUERY => 'select biblionumber as id, marcxml as marc from biblioitems order by id asc';

# Koha authorities
#use constant MARC_XML => './aukt.xml';
#use constant DB_QUERY => 'select authid as id, marcxml as marc from auth_header order by id asc';
# or possibly something like
#use constant DB_QUERY => 'select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc';

my %dbdata = (
    'hostname' => 'localhost',
    'username' => 'kohaadmin',
    'password' => 'katikoan',
    'dbname' => 'koha',
    'driver' => 'mysql'
    );

my %not_repeatable;

my %allow_indicators;

my %ignore_fields;
my $help = 0;
my $man = 0;

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (defined($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting."); } },
    'ignore=s' => sub { my ($onam, $oval) = @_; foreach my $tmp (split/,/, $oval) { $ignore_fields{$tmp} = 1; } },
    'help|h|?' => \$help,
    'man' => \$man
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;


my $tpp = XML::TreePP->new();
$tpp->set( utf8_flag => 1 );
my $tree = $tpp->parsefile(MARC_XML);

my @treefields = $tree->{'fields'}{'field'};

foreach my $tf (@treefields) {
    foreach my $tfx ($tf) {
	my @arr = @{$tfx};
	foreach my $tmph (@arr) {
	    my %dat = %{$tmph};
	    my $dsf = $dat{'subfield'};
	    my $ind = $dat{'indicator'};
	    my @dsfar;
	    $not_repeatable{$dat{'-tag'}} = 1 if ($dat{'-repeatable'} eq 'false');
	    if (defined($dsf)) {
		if (ref($dat{'subfield'}) eq 'ARRAY') {
		    @dsfar = @{$dsf};
		} else {
		    @dsfar = $dsf;
		}
		foreach my $subfield (@dsfar) {
		    my %datsf = %{$subfield};
		    $not_repeatable{$dat{'-tag'}.$datsf{'-code'}} = 1 if ($datsf{'-repeatable'} eq 'false');
		}
	    }

	    if (defined($ind)) {
		my @indar;
		if (ref($dat{'indicator'}) eq 'ARRAY') {
		    @indar = @{$ind};
		} else {
		    @indar = $ind;
		}
		foreach my $indicator (@indar) {
		    my %datind = %{$indicator};
		    my $ipos = $datind{'-position'};
		    my $ival = $datind{'-value'};
		    $ival =~ s/#/ /;
		    $allow_indicators{$dat{'-tag'}.$ipos} = '' if (!defined($allow_indicators{$dat{'-tag'}.$ipos}));
		    $allow_indicators{$dat{'-tag'}.$ipos} .= $ival;
		}
	    }

	}
    }
}

# indicators are listed as sets of allowed chars. eg. ' ab' or '1-9'
foreach my $tmp (keys(%allow_indicators)) {
    $allow_indicators{$tmp} = '[' . $allow_indicators{$tmp} . ']';
}


MARC::Charset->assume_unicode(1);

sub check_marc {
    my $id = shift;
    my $marc = shift;

    my $record;
    eval {
	$record = MARC::Record->new_from_xml($marc);
    };

    my %mainf;
    my %inderrs;

    my @errors;

    foreach my $f ($record->field('...')) {
	my $fi = $f->{'_tag'};
	next if ((scalar($fi) < 10) || (lc($fi) eq 'ldr'));

	next if (defined($ignore_fields{$fi}));

	$mainf{$fi} = 0 if (!defined($mainf{$fi}));
	$mainf{$fi}++;

	my @subf = @{$f->{'_subfields'}};
	my %subff;

	while ($#subf > 0) {
	    my $key = shift @subf;
	    my $val = shift @subf;
	    my $fikey = $fi.$key;

	    next if (defined($ignore_fields{$fikey}));

	    $subff{$fikey} = 0 if (!defined($subff{$fikey}));
	    $subff{$fikey}++;
	}

	foreach my $k (keys(%subff)) {
	    push(@errors, (($subff{$k} > 1) ? $subff{$k}.'x' : '').$k) if (($subff{$k} > 1) && defined($not_repeatable{$k}));
	}

	foreach my $ind ((1, 2)) {
	    my $indv = $f->indicator($ind);
	    my $tmp = $allow_indicators{$fi.$ind};
	    my $key = $fi.'.ind'.$ind;

	    next if (defined($ignore_fields{$key}));

	    $inderrs{$key} = (defined($inderrs{$key}) ? $inderrs{$key} : 0) + 1 if (defined($tmp) && ($indv !~ /$tmp/));
	}
    }

    foreach my $k (keys(%inderrs)) {
	push(@errors, (($inderrs{$k} > 1) ? $inderrs{$k}.'x' : '').$k);
    }

    foreach my $k (keys(%mainf)) {
	push(@errors, (($mainf{$k} > 1) ? $mainf{$k}.'x' : '').$k) if (($mainf{$k} > 1) && defined($not_repeatable{$k}));
    }

    print "$id (".join(', ', @errors).")\n" if (@errors);
}

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

my $sql = "";
my $sth;
my $dbh = db_connect();

$sql = DB_QUERY;
$sth = $dbh->prepare($sql);

$sth->execute();


my $i = 1;
while (my $ref = $sth->fetchrow_hashref()) {
    if ($ref->{'id'}) {
	check_marc($ref->{'id'}, $ref->{'marc'});
	$i++;
    }
}


db_disconnect($dbh);


__END__

=head1 NAME

marc_warnings.pl - Report MARC errors against MARC21 format

=head1 OPTIONS

=over 8

=item B<-help>

Print this help.

=item B<-man>

Print this help as a man page.

=item B<-ignore>

Ignore certain fields, subfields or indicators. For example:
  C<-ignore=590,028a,655.ind2>
would ignore the field 590, subfield 028a, and indicator 2 of field 655.

=item B<-db hostname=localhost>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), and driver ("mysql").

=back

=cut
