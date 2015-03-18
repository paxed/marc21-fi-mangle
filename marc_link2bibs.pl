use strict;
use warnings;

use Getopt::Long;
use Pod::Usage;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use DBI;
use Unicode::Normalize;

use Data::Dumper;


binmode(STDOUT, ":utf8");

my %dbdata = (
    'hostname' => 'localhost',
    'username' => 'kohaadmin',
    'password' => 'katikoan',
    'dbname' => 'koha',
    'driver' => 'mysql'
    );

my %usefields = (
    '100' => '100a',
##    '110' => '110ab',
    '130' => '150axyz',
    '600' => '100a',
##    '610' => '110ab',
    '630' => '150axyz',
    '830' => '150axyz',
    '650' => '150axyz',
##    '651' => '151ab',
    '655' => '150axyz',
    '700' => '100a',
##    '710' => '110ab',
##    '810' => '110ab'
    );


# MARC21
my %auth_typecode = (
        '100' => 'PERSO_NAME',
        '110' => 'CORPO_NAME',
        '111' => 'MEETI_NAME',
        '130' => 'UNIF_TITLE',
        '148' => 'CHRON_TERM',
        '150' => 'TOPIC_TERM',
        '151' => 'GEOGR_NAME',
        '155' => 'GENRE/FORM',
        '180' => 'GEN_SUBDIV',
        '181' => 'GEO_SUBDIV',
        '182' => 'CHRON_SUBD',
        '185' => 'FORM_SUBD'
    );



my %ignore_fields;
my $help = 0;
my $man = 0;
my $verbose = 0;
my $dry_run = 0;
my $skip_existing = 0;
my $remove_nonexist = 0;
my $no_new = 0;
my $sqlquery = 'select biblionumber as id, marcxml as marc from biblioitems order by id asc';

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (defined($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting."); } },
    'sql=s' => \$sqlquery,
    'v|verbose+' => \$verbose,
    'ignore=s' => sub { my ($onam, $oval) = @_; foreach my $tmp (split/,/, $oval) { $ignore_fields{$tmp} = 1; } },
    'dry-run|test' => \$dry_run,
    'skip-existing' => \$skip_existing,
    'remove-nonexisting' => \$remove_nonexist,
    'no-new' => \$no_new,
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



sub trim_aacr2 {
    my $str = shift;
    my $aacr2punct = '';
    if ($str =~ /^(.+) ?([:;,.])$/) {
	$aacr2punct = $2;
	$str = $1;
    }
    return ($str, $aacr2punct);
}

my %cache_auths;

sub get_auth_cache {
    my ($authtypecode, $val) = @_;
    return $cache_auths{$authtypecode . $val} if (defined($cache_auths{$authtypecode . $val}));
    return $cache_auths{$authtypecode . lc($val)} if (defined($cache_auths{$authtypecode . lc($val)}));
    my $decom = NFD($val);
    return $cache_auths{$authtypecode . $decom} if (defined($cache_auths{$authtypecode . $decom}));
    return $cache_auths{$authtypecode . lc($decom)} if (defined($cache_auths{$authtypecode . lc($decom)}));
    my $detri = $decom;
    $detri =~ s/[^a-zA-Z0-9]//g;
    return $cache_auths{$authtypecode . $detri} if (defined($cache_auths{$authtypecode . $detri}));
    return $cache_auths{$authtypecode . lc($detri)} if (defined($cache_auths{$authtypecode . lc($detri)}));
    my $trimmed = $val;
    $trimmed =~ s/[^a-zA-Z0-9]//g;
    return $cache_auths{$authtypecode . $trimmed} if (defined($cache_auths{$authtypecode . $trimmed}));
    return $cache_auths{$authtypecode . lc($trimmed)} if (defined($cache_auths{$authtypecode . lc($trimmed)}));
    return [0, ''];
}

sub add_auth_cache {
    my ($authtypecode, $val, $field, $authid, $isauthoritative) = @_;
    my $s = $authtypecode . $val;
    if (defined($isauthoritative) && $isauthoritative) {
	if (defined($cache_auths{$s}) && ($cache_auths{$s}[0] != $authid) && ($cache_auths{$s}[2])) {
	    $cache_auths{$s} = [-1, $field, $isauthoritative];
	    $authid = -1;
	} else {
	    #print "CACHED:($authid, $field) (AUTH)\n";
	    $cache_auths{$s} = [$authid, $field, $isauthoritative];
	}
    } else {
	if (defined($cache_auths{$s}) && ($cache_auths{$s}[0] != $authid)) {
	    $cache_auths{$s} = [-1, $field, $isauthoritative] if (!$cache_auths{$s}[2]);
	    $authid = -1;
	    return;
	    #print "ERROR: \"$s\" already cached (old:".$cache_auths{$s}[0].", new:".$authid.")\n";
	} else {
	    #print "CACHED:($authid, $field)\n";
	    $cache_auths{$s} = [$authid, $field, $isauthoritative];
	}
    }
    my $trimmed = $val;
    $trimmed =~ s/[^a-zA-Z0-9]//g;
    add_auth_cache($authtypecode, $trimmed, $field, $authid, 0) if ($trimmed ne $val);
    add_auth_cache($authtypecode, lc($val), $field, $authid, 0) if (lc($val) ne $val);
    my $decom = NFD($val);
    #print $decom."\n";
    if ($decom ne $val) {
	$val = $decom;
	add_auth_cache($authtypecode, $val, $field, $authid, 0);
	add_auth_cache($authtypecode, lc($val), $field, $authid, 0) if (lc($val) ne $val);
    }
}

sub do_cache_auths {

    my $sth = $dbh->prepare('select authid, authtypecode, marcxml from auth_header');
    $sth->execute();
    my $i = 0;
    while (my $ref = $sth->fetchrow_hashref()) {

	my $atc = $ref->{'authtypecode'};
	my $aid = $ref->{'authid'};

	if ($verbose) {
	    print "\n$i" if (!((++$i) % 100));
	    print ".";
	}

	my $record;
	eval {
	    $record = MARC::Record->new_from_xml($ref->{'marcxml'});
	};

	foreach my $f ($record->field('...')) {
	    my $fi = $f->tag();

	    if ($atc eq 'PERSO_NAME') {
		next if (($fi ne '100') && ($fi ne '400') && ($fi ne '500'));
		next if (!defined($f->subfield('a')));

		# ohita fiktiiviset henkilöt
		next if (defined($f->subfield('c')) && $f->subfield('c') =~ /fikt/i);

		my $val = $f->subfield('a');

		# Pakko ohittaa nämä auktoriteetit
		next if ($val =~ /^Manu$/);
		next if ($val =~ /^Mikko$/);
		next if ($val =~ /^Nemo$/);
		next if ($val =~ /^Bon$/i);

		my ($tmp, $aacr2) = trim_aacr2($val);

		add_auth_cache($atc, $tmp, $fi, $aid, ($fi eq '100'));

		if (($f->indicator(1) eq '1') && ($val =~ /^(.+), (.+)$/)) {
		    my $lastname = $1;
		    my $firstname = $2;
		    add_auth_cache($atc, $firstname." ".$lastname, $fi, $aid, ($fi eq '100'));
		}
	    } elsif ($atc eq 'TOPIC_TERM') {
		next if (($fi ne '150') && ($fi ne '450'));
		next if (!defined($f->subfield('a')));
		my $val = $f->subfield('a');
		$val .= " " . $f->subfield('x') if (defined($f->subfield('x')));

		add_auth_cache($atc, $val, $fi, $aid, ($fi eq '150'));
	    } elsif ($atc eq 'CORPO_NAME') {
		# TODO
	    } elsif ($atc eq 'GEOGR_NAME') {
		# TODO
	    }

	}
    }
}

sub auth_exists {
    my $authid = shift;
    my $sth = $dbh->prepare('select count(authid) as cnt from auth_header where authid=?');
    $sth->execute($authid);
    my $ref = $sth->fetchrow_hashref();
    return 1 if (defined($ref->{'cnt'}) && $ref->{'cnt'} == 1);
    return 0;
}

sub print_cached_auths {
    $Data::Dumper::Sortkeys = 1;
    print Dumper(\%cache_auths);
}


sub check_marc {
    my $id = shift;
    my $marc = shift;

    my $update = 0;

    my $record;
    eval {
	$record = MARC::Record->new_from_xml($marc);
    };

    foreach my $f ($record->field('...')) {
	my $fi = $f->tag();
	my $nineval;

	next if ((scalar($fi) < 10) || (lc($fi) eq 'ldr'));
	next if (defined($ignore_fields{$fi}));
	next if (!defined($usefields{$fi}));

	if (defined($f->subfield('9'))) {
	    if ($remove_nonexist) {
		my $authexist = auth_exists($f->subfield('9'));
		if (!$authexist) {
		    print "REMOVED: $id, $fi\$9=".$f->subfield('9')." (nonexisting auth)\n";
		    $f->delete_subfield(code => '9');
		}
	    }
	    next if (defined($f->subfield('9')) && $skip_existing);
	}

	next if ($no_new);

	next if (!defined($f->subfield(substr($usefields{$fi}, 3, 1))));

	$nineval = $f->subfield('9');
	my $typecode = $auth_typecode{substr($usefields{$fi}, 0, 3)};

	# Älä auktorisoi fiktiivisiä henkilöitä
	next if ($typecode eq 'PERSO_NAME' && defined($f->subfield('c')) && $f->subfield('c') =~ /fikt/i);


	next if (!defined($typecode));

	my $val = $f->subfield(substr($usefields{$fi}, 3, 1));
	my $i = 4;
	while ((substr($usefields{$fi}, $i, 1) ne '' && defined($f->subfield(substr($usefields{$fi}, $i, 1))))) {
	    $val .= " " . $f->subfield(substr($usefields{$fi}, $i, 1));
	    $i++;
	}
	#$val .= " " . $f->subfield(substr($usefields{$fi}, 4, 1)) if ((substr($usefields{$fi}, 4, 1) ne '') && (defined($f->subfield(substr($usefields{$fi}, 4, 1)))));
	#$val .= " " . $f->subfield(substr($usefields{$fi}, 4, 1)) if ((substr($usefields{$fi}, 4, 1) ne '') && (defined($f->subfield(substr($usefields{$fi}, 4, 1)))));

	my $authed = get_auth_cache($typecode, $val);

	#print Dumper(\$authed);

	if ($authed->[0] == 0) {
	    print "NOT FOUND: $id, ".$usefields{$fi}.", ".$typecode.", \"".$val."\"\n" if ($verbose);
	} elsif ($authed->[0] < 0) {
	    print "AMBIGUOUS: $id, ".$usefields{$fi}.", ".$typecode.", \"".$val."\"\n" if ($verbose);
	} else {
	    print "FOUND: $id, ".$authed->[0].", ".$usefields{$fi}.", ".$typecode.", \"".$val."\"\n" if ($verbose);
	    $f->update('9' => $authed->[0]) if (!defined($nineval) || ($nineval ne $authed->[0]));
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


print "Caching auths\n" if ($verbose);
do_cache_auths() if (!$no_new);
print "Forward!\n" if ($verbose);

#print_cached_auths();

#exit;

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

marc_link2bibs.pl - Link MARC21 biblio records to authority data

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

=item B<-skip-existing>

Do not change existing $9 subfields.

=item B<-remove-nonexisting>

Remove $9 subfield if the authority it links to does not exist.

=item B<-no-new>

Do not create new $9 subfields.

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
