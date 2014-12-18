use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use DBI;
use XML::TreePP;


my %dbdata = (
    'hostname' => 'localhost',
    'username' => 'kohaadmin',
    'password' => 'katikoan',
    'dbname' => 'koha',
    'driver' => 'mysql'
    );

my %presets = (
    'koha-auth' => {
	'sql' => 'select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc',
	'xml' => './data/aukt.xml'
    },
    'koha-bibs' => {
	'sql' => 'select biblionumber as id, marcxml as marc from biblioitems order by id asc',
	'xml' => './data/bibs.xml'
    },
    'eg-auth' => {
	'sql' => 'select id, marc from authority.record_entry order by id asc',
	'xml' => './data/aukt.xml'
    },
    'eg-bibs' => {
	'sql' => 'select id, marc from biblio.record_entry order by id asc',
	'xml' => './data/bibs.xml'
    }
    );

# Could we grab these from the format description somehow?
my %field_data = (
    'bibs' => {
	'fixed_length' => {
	    'ldr' => 24,
	    '005' => 16,
	    '006' => 18,
	    '008' => 40
	},
	'regex' => {
	    'ldr' => {
		'0' => qr/\d/,
		'1' => qr/\d/,
		'2' => qr/\d/,
		'3' => qr/\d/,
		'4' => qr/\d/,
		'5' => qr/[acdnp]/,
		'6' => qr/[acdefgijkmoprt]/,
		'7' => qr/[abcdims]/,
		'8' => qr/[ a]/,
		'9' => qr/[ a]/,
		'10' => qr/[2]/,
		'11' => qr/[2]/,
		'12' => qr/\d/,
		'13' => qr/\d/,
		'14' => qr/\d/,
		'15' => qr/\d/,
		'16' => qr/\d/,
		'17' => qr/[ 1234578uz]/,
		'18' => qr/[ aciu]/,
		'19' => qr/[ abc]/,
		'20' => qr/[4]/,
		'21' => qr/[5]/,
		'22' => qr/[0]/,
		'23' => qr/[0]/,
	    },
	    '005' => {
		'x' => qr/^\d{14}\.\d$/
	    },
	}
    },
    'auth' => {
	'fixed_length' => {
	    'ldr' => 24,
	    '005' => 16,
	    '008' => 40
	},
	'regex' => {
	    'ldr' => {
		'0' => qr/\d/,
		'1' => qr/\d/,
		'2' => qr/\d/,
		'3' => qr/\d/,
		'4' => qr/\d/,
		'5' => qr/[acdnosx]/,
		'6' => qr/[z]/,
		'7' => qr/[ ]/,
		'8' => qr/[ ]/,
		'9' => qr/[ a]/,
		'10' => qr/[2]/,
		'11' => qr/[2]/,
		'12' => qr/\d/,
		'13' => qr/\d/,
		'14' => qr/\d/,
		'15' => qr/\d/,
		'16' => qr/\d/,
		'17' => qr/[no]/,
		'18' => qr/[ ]/,
		'19' => qr/[ ]/,
		'20' => qr/[4]/,
		'21' => qr/[5]/,
		'22' => qr/[0]/,
		'23' => qr/[0]/,
	    },
	    '005' => {
		'x' => qr/^\d{14}\.\d$/
	    },
	    '008' => {
		'0' => qr/\d/,
		'1' => qr/\d/,
		'2' => qr/\d/,
		'3' => qr/\d/,
		'4' => qr/\d/,
		'5' => qr/\d/,
		'6' => qr/[ din|]/,
		'7' => qr/[ abcdefgn|]/,
		'8' => qr/[ bef|]/,
		'9' => qr/[ abcdefg|]/,
		'10' => qr/[abcdnz|]/,
		'11' => qr/[abcdknrsvz|]/,
		'12' => qr/[abcnz|]/,
		'13' => qr/[abcn|]/,
		'14' => qr/[ab|]/,
		'15' => qr/[ab|]/,
		'16' => qr/[ab|]/,
		'17' => qr/[abcden|]/,
		'18' => qr/[ |]/,
		'19' => qr/[ |]/,
		'20' => qr/[ |]/,
		'21' => qr/[ |]/,
		'22' => qr/[ |]/,
		'23' => qr/[ |]/,
		'24' => qr/[ |]/,
		'25' => qr/[ |]/,
		'26' => qr/[ |]/,
		'27' => qr/[ |]/,
		'28' => qr/[ acfilmosuz|]/,
		'29' => qr/[abn|]/,
		'30' => qr/[ |]/,
		'31' => qr/[ab|]/,
		'32' => qr/[abn|]/,
		'33' => qr/[abcdn|]/,
		'34' => qr/[ |]/,
		'35' => qr/[ |]/,
		'36' => qr/[ |]/,
		'37' => qr/[ |]/,
		'38' => qr/[ sx|]/,
		'39' => qr/[ cdu|]/,
	    }
    }
    }
    );


my %not_repeatable;

my %allow_indicators;

my %ignore_fields;
my $help = 0;
my $man = 0;
my $verbose = 0;
my $test_field_data = 1;
my $marc_xml;
my $sqlquery;

my $auth_or_bibs = 'bibs';
my $koha_or_eg = 'koha';

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (defined($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting."); } },
    'xml=s' => \$marc_xml,
    'sql=s' => \$sqlquery,
    'v|verbose' => \$verbose,
    'a|auth|authority|authorities' => sub { $auth_or_bibs = 'auth'; },
    'b|bib|bibs|biblios' => sub { $auth_or_bibs = 'bibs'; },
    'koha' => sub { $koha_or_eg = 'koha'; },
    'nodata' => sub { $test_field_data = 0; },
    'eg|evergreen' => sub { $koha_or_eg = 'eg'; },
    'ignore=s' => sub { my ($onam, $oval) = @_; foreach my $tmp (split/,/, $oval) { $ignore_fields{$tmp} = 1; } },
    'help|h|?' => \$help,
    'man' => \$man
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

if (defined($koha_or_eg) && defined($auth_or_bibs)) {
    my $tmp = $koha_or_eg.'-'.$auth_or_bibs;
    if (defined($presets{$tmp})) {
	$marc_xml = $presets{$tmp}{'xml'} if (!defined($marc_xml));
	$sqlquery = $presets{$tmp}{'sql'} if (!defined($sqlquery));
    } else {
	die("Unknown preset combination $tmp");
    }
}

die("No SQL query") if (!defined($sqlquery));
die("No MARC21 format XML file") if (!defined($marc_xml));

my $tpp = XML::TreePP->new();
$tpp->set( utf8_flag => 1 );
my $tree = $tpp->parsefile($marc_xml);

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

	next if (defined($ignore_fields{$fi}));

	if ($test_field_data) {
	    my $key = $fi.'.length';
	    if (defined($field_data{$auth_or_bibs}{'fixed_length'}{$fi}) && !defined($ignore_fields{$key})) {
		my $tmp = $field_data{$auth_or_bibs}{'fixed_length'}{$fi};
		if ($tmp != length($f->data())) {
		    push(@errors, "$key=".length($f->data())."/$tmp");
		    next;
		}
	    }

	    if (defined($field_data{$auth_or_bibs}{'regex'}{$fi})) {
		my $data = $f->data();
		foreach my $k (sort(keys($field_data{$auth_or_bibs}{'regex'}{$fi}))) {
		    my $s;
		    if ($k =~ /^\d+$/) {
			$s = substr($data, $k, 1);
			if ($s !~ /$field_data{$auth_or_bibs}{'regex'}{$fi}{$k}/) {
			    push(@errors, "field $fi position $k value at pos: \"$s\"");
			    next;
			}
		    } else {
			$s = $data;
			if ($s !~ /$field_data{$auth_or_bibs}{'regex'}{$fi}{$k}/) {
			    push(@errors, "field $fi illegal value \"$s\" does not match $field_data{$auth_or_bibs}{'regex'}{$fi}{$k}");
			    next;
			}
		    }
		}
	    }
	}

	next if ((scalar($fi) < 10) || (lc($fi) eq 'ldr'));

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

    print STDERR "ERROR: id=$id (".join(', ', @errors).")\n" if (@errors);
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

my $sth;
my $dbh = db_connect();

$sth = $dbh->prepare($sqlquery);

$sth->execute();


my $i = 1;
while (my $ref = $sth->fetchrow_hashref()) {
    if ($ref->{'id'}) {
	if ($verbose) {
	    print "\n$i" if (!($i % 100));
	    print ".";
	}
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

=item B<-v|-verbose>

Print progress bars.

=item B<-auth>

=item B<-bibs>

=item B<-koha>

=item B<-evergreen>

Use preset values. Requires one of B<-auth> or B<-bibs> and one of B<-koha> or B<-evergreen>.
Defaults to B<-koha> and B<-bibs>.

=item B<-xml=filename>

Set the XML file where MARC21 format rules are read from. Default depends on B<-auth> or
B<-bibs> option: data/aukt.xml or data/bibs.xml, respectively.

=item B<-sql=text>

Set the SQL query to perform. Must return two fields (id and marcxml). Default depends
on the preset values used. For example, if you want the id this program reports to be the
contents of the 001 field, use -sql='select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc'

=item B<-ignore=fieldspecs>

Ignore certain fields, subfields or indicators. For example:
  C<-ignore=590,028a,655.ind2,008.length>
would ignore the field 590, subfield 028a, indicator 2 of field 655, and
length checking for field 008.

=item B<-nodata>

Do not test fixed field lengths or data validity.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will report format errors in your MARC21 XML data, either in your Koha or Evergreen
system. Output will list a record ID and the fields where errors occurred. This will list
repeated fields and subfields which are not repeatable, invalid indicator values, and
some fixed-length controlfield data errors.

=cut
