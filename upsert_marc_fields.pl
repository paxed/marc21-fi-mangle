use strict;
use warnings;

#
# Update and/or insert MARC21 field information from the Finnish
# National Library's translated machine-readable MARC21 format into Koha
#

use Getopt::Long;
use Pod::Usage;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use DBI;
use XML::Simple;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

my %dbdata = (
    'hostname' => 'localhost',
    'username' => 'kohaadmin',
    'password' => 'katikoan',
    'dbname' => 'koha',
    'mysql_socket' => undef,
    'driver' => 'mysql'
    );

my %xml_globs = (
    'marc' => './data/bib-*.xml',
    'auth' => './data/aukt-*.xml',
    );

# These are hardcoded in Koha
# https://wiki.koha-community.org/wiki/Hidden_values
# https://bugs.koha-community.org/bugzilla3/show_bug.cgi?id=22123
my %koha_hidden_flags = (
    -8 => 'flagged',
    -7 => 'collapsed,opac',
    -6 => 'intranet,opac',
    -5 => 'collapsed,intranet,opac',
    -4 => 'opac',
    -3 => 'collapsed,editor,opac',
    -2 => 'editor,opac',
    -1 => 'collapsed,editor,intranet,opac',
    0  => 'editor,intranet,opac',
    1  => 'collapsed,editor,intranet',
    2  => 'editor',
    3  => 'collapsed,editor',
    4  => 'editor,intranet',
    5  => 'collapsed',
    6  => 'intranet',
    7  => 'collapsed,intranet',
    8  => '',
    );

my $xml_glob = '';
my $ignore_fields_param = '';
my $only_fields_param = '';
my $help = 0;
my $man = 0;
my $debug = 0;
my $insert = 0;
my $update = 0;
my $bib_or_auth = 'marc';
my $frameworkcode = '';
my $hidden_value = -6; # intranet,opac
my $set_existing_hidden = 0;
my $dryrun = 0;

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (exists($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { warn "Unknown db setting '".$onam."'."; } },
    'ignore=s' => \$ignore_fields_param,
    'onlyfields=s' => \$only_fields_param,
    'help|h|?' => \$help,
    'man' => \$man,
    'dryrun|dry-run' => \$dryrun,
    'debug' => \$debug,
    'xml=s' => \$xml_glob,
    'insert' => \$insert,
    'update' => \$update,
    'setflags' => \$set_existing_hidden,
    'auth' => sub { $bib_or_auth = 'auth'; },
    'bib|bibs' => sub { $bib_or_auth = 'marc'; },
    'framework=s' => \$frameworkcode,
    'flags=s' => \&handle_koha_flags,
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

if ($xml_glob eq '') {
    $xml_glob = $xml_globs{$bib_or_auth};
    die "XML glob files missing." if (!$xml_glob);
}



my %ignore_fields = map { ($_ => 1) } generate_tag_sequence($ignore_fields_param);
my %only_fields = map { ($_ => 1) } generate_tag_sequence($only_fields_param);
my %field_data = ( ); # marc format data from xml
my %tags = ( );  # tag/subfield data from the database

#################################################################
#################################################################

sub handle_koha_flags {
    my ($name, $value) = @_;

    $value =~ s/ //g;
    my @tmp = split(/,/, lc($value));
    $value = join(',', sort @tmp);

    my $rev = ($value =~ /[a-z]/) ? 1 : 0;

    my %flaglut = %koha_hidden_flags;
    %flaglut = reverse(%koha_hidden_flags) if ($rev);

    die("Cannot represent flags $value with koha hidden integer.") if (!defined($flaglut{$value}));

    my $sval = $rev ? $value : $flaglut{$value};
    print "Using flags $sval for fields\n" if ($debug || !$rev);

    $hidden_value = (!$rev) ? $value : $flaglut{$value};
}

sub fwcname {
    my $s = shift || "default";
    return "[$s] ";
}

sub generate_tag_sequence {
    my ($tag) = @_;

    my @fields;

    if ($tag =~ /,/) {
	foreach my $tmp (split(/,/, $tag)) {
	    push(@fields, generate_tag_sequence($tmp));
	}
	return @fields;
    }

    if (defined($tag) && $tag =~ /x/i && $tag =~ /^([0-9x])([0-9x])([0-9x])(.*)$/i) {
        my ($p1, $p2, $p3, $p4) = ($1, $2, $3, $4);
        my @c1 = (($p1 =~ /x/i) ? 0..9 : $p1);
        my @c2 = (($p2 =~ /x/i) ? 0..9 : $p2);
        my @c3 = (($p3 =~ /x/i) ? 0..9 : $p3);
	my @c4 = (($p4 =~ /x/i) ? (0..9, "a".."z") : $p4);

        foreach my $a1 (@c1) {
            foreach my $a2 (@c2) {
                foreach my $a3 (@c3) {
		    foreach my $a4 (@c4) {
			my $fld = $a1.$a2.$a3.$a4;
			push @fields, $fld;
		    }
                }
            }
        }
    } else {
        push @fields, $tag;
    }

    return @fields;
}

sub db_connect {
    my $s = "DBI:" . $dbdata{'driver'} . ":dbname=" . $dbdata{'dbname'};

    if (defined($dbdata{'mysql_socket'})) {
        $s .= ";mysql_socket=" . $dbdata{'mysql_socket'};
    } else {
        $s .= ";host=" . $dbdata{'hostname'};
    }

    my $dbh = DBI->connect($s, $dbdata{'username'}, $dbdata{'password'}, {'RaiseError' => 1, mysql_enable_utf8 => 1});

    die("DB Error") if (!$dbh);

    return $dbh;
}

sub db_disconnect {
    my $dbh = shift || die("Error.");
    $dbh->disconnect();
}

sub tablename {
    my ($issubfield) = @_;
    # auth_subfield_structure
    # marc_tag_structure
    return $bib_or_auth . '_' . (($issubfield) ? 'subfield' : 'tag') . '_structure';
}

sub db_query_tags {
    my ($issubfield) = @_;

    my $sql;
    my $tablename;

    my $sth;
    my $dbh = db_connect();

    $tablename = tablename($issubfield);
    $sql = 'select * from '.$tablename;

    $sth = $dbh->prepare($sql);
    $sth->execute();

    while (my $ref = $sth->fetchrow_hashref()) {
	my $fwc = ($bib_or_auth eq 'marc') ? $ref->{'frameworkcode'} : '';
        if (exists($ref->{'tagsubfield'})) {
            $tags{$tablename}{$fwc}{$ref->{'tagfield'}}{$ref->{'tagsubfield'}} = $ref;
        } else {
            $tags{$tablename}{$fwc}{$ref->{'tagfield'}} = $ref;
        }
    }
    db_disconnect($dbh);
}

sub db_query_alltags {
    db_query_tags(0);
    db_query_tags(1);
}

sub  trim { my $s = shift; $s =~ s/^\s+|\s+$//g; return $s };

sub handle_code {
    my ($tag, $name, $repeatable) = @_;

    my $sf = "".substr($tag, 3, 1);
    $sf = '@' if ($sf eq '');

    print "Ignored: $tag\n" if ($debug && $ignore_fields{$tag});
    return if ($ignore_fields{$tag});
    return if ($only_fields_param && !$only_fields{$tag});
    return if ($repeatable eq '' && $field_data{$tag});

    $repeatable = uc($repeatable);
    $name = '' if (!$name);

    print "$tag\t$name\trepeatable=$repeatable\n" if ($debug);
    my %tmphash = (
	'tagfield' => substr($tag, 0, 3),
	'name' => trim($name),
	'repeatable' => ($repeatable eq 'Y') ? 1 : (($repeatable eq 'N') ? 0 : -1),
	);
    $tmphash{'tagsubfield'} = $sf if ($sf ne '@');

    $field_data{$tag} = \%tmphash;
}

sub parse_single_field {
    my ($field, $data) = @_;

    my $name = $field->{'name'};
    my $tag = $field->{'tag'};
    my $type = $field->{'type'} || '';
    my $repeatable = $field->{'repeatable'} || '';

    if ($tag =~ /x/i) {
        my @tags = generate_tag_sequence($tag);
        foreach my $tmptag (@tags) {
            $field->{'tag'} = $tmptag;
            parse_single_field($field, $data);
        }
        return;
    }

    handle_code($tag, $name, $repeatable);

    if (defined($field->{'subfields'}{'subfield'})) {
        my $subfields = $field->{'subfields'}{'subfield'};
        my @subfieldarr;

        if (ref($subfields) eq 'ARRAY') {
            @subfieldarr = @{$subfields};
        } else {
            @subfieldarr = $subfields;
        }

        foreach my $sf (@subfieldarr) {
            my $sf_code = $sf->{'code'};
            my $sf_repeatable = $sf->{'repeatable'};
            my $sf_name = $sf->{'name'};

            if ($sf_code =~ /^.-.$/) {
                my ($code_s, $code_e) = split(/-/, $sf_code);
                foreach my $sfc ($code_s..$code_e) {
                    handle_code($tag.$sfc, $sf_name, $sf_repeatable);
                }
            } elsif ($sf_code =~ /^.$/) {
                handle_code($tag.$sf_code, $sf_name, $sf_repeatable);
            } else {
                warn "Unhandled subfield \"$sf_code\" for \"$tag\"";
            }
        }
    }
}


sub parse_multiple_fields {
    my ($fieldsref, $data) = @_;

    my @fieldarr;

    if (ref($fieldsref) eq 'ARRAY') {
        @fieldarr = @{$fieldsref};
    } else {
        @fieldarr = $fieldsref;
    }

    foreach my $field (@fieldarr) {
        parse_single_field($field, $data);
    }
}

sub parse_xml_data {
    my ($filename, $data) = @_;

    my $tpp = XML::Simple->new();
    my $tree = $tpp->XMLin($filename, KeyAttr => []);

    if (defined($tree->{'leader-directory'})) {
        $tree->{'leader-directory'}{'leader'}{'tag'} = '000';
        parse_multiple_fields($tree->{'leader-directory'}{'leader'}, $data);
    } elsif (defined($tree->{'controlfields'})) {
        parse_multiple_fields($tree->{'controlfields'}{'controlfield'}, $data);
    } elsif (defined($tree->{'datafields'})) {
        parse_multiple_fields($tree->{'datafields'}{'datafield'}, $data);
    } else {
        warn "parse_xml_data: unhandled file $filename" if ($debug);
    }
}

sub read_xml {
    my ($glob) = @_;

    my @xmlfiles = glob($xml_glob);
    die("No XML files found with $xml_glob") if (scalar(@xmlfiles) < 1);

    foreach my $file (@xmlfiles) {
	print "\nParsing: $file\n" if ($debug);
        parse_xml_data($file, \%field_data);
    }
}

sub uniq {
    my %seen;
    return grep { !$seen{$_}++ } @_;
}

sub yesno {
    my $i = shift || 0;
    return $i ? "YES" : "no ";
}

sub pfld {
    my $f = shift;
    return sprintf("%-4s", $f);
}

# parameters: tag, frameworkcode, hashref to %tags, hashref to %field_data
# returns hashref
sub check_need_update {
    my ($ftag, $fwc, $ct, $cfd) = @_;
    my %updatedata = ();

    $ftag = pfld($ftag);

    my $desc = 0;

    if (trim($ct->{'liblibrarian'}) ne $cfd->{'name'} && $cfd->{'name'} ne '') {
	$desc = 1;
	$updatedata{'liblibrarian'} = $cfd->{'name'};
    }
    if (trim($ct->{'libopac'}) ne $cfd->{'name'} && $cfd->{'name'} ne '') {
	$desc |= 2;
	$updatedata{'libopac'} = $cfd->{'name'};
    }

    if ($desc) {
	print fwcname($fwc)."$ftag description: ";
	if ($desc == 3 && $ct->{'liblibrarian'} eq $ct->{'libopac'}) {
	    print "Koha:'".$ct->{'libopac'}."', ";
	} else {
	    print "Intra:'".$ct->{'liblibrarian'}."', " if ($desc & 1);
	    print "OPAC:'".$ct->{'libopac'}."', " if ($desc & 2);
	}
	print "Format:'".$cfd->{'name'}."'\n";
    }

    if ($ct->{'repeatable'} != $cfd->{'repeatable'} && $cfd->{'repeatable'} > -1) {
	print fwcname($fwc)."$ftag repeatable: Koha:".yesno($ct->{'repeatable'}).", Format:".yesno($cfd->{'repeatable'})."\n";
	$updatedata{'repeatable'} = $cfd->{'repeatable'};
    }

    if ($set_existing_hidden && $ct->{'tagsubfield'} && $ct->{'tagsubfield'} ne '@' && $hidden_value != $ct->{'hidden'}) {
	print fwcname($fwc)."$ftag set flags=".$koha_hidden_flags{$hidden_value}." (was:".$koha_hidden_flags{$ct->{'hidden'}}.")\n";
	$updatedata{'hidden'} = $hidden_value;
    }

    return \%updatedata;
}

sub mk_sql_update {
    my ($dbh, $updatedata, $tablename, $fwc, $tag, $subfield) = @_;

    if (($update || $debug) && scalar(keys(%{$updatedata}))) {
	my $sql = "UPDATE ".$tablename." SET ";
	my (@u_fields, @u_datas);
        foreach my $k (sort keys(%{$updatedata})) {
            push(@u_fields, $k);
            push(@u_datas, $updatedata->{$k});
        }
	$sql = $sql . join("=?, ", @u_fields) . "=?";
	$sql = $sql . " WHERE tagfield=? AND frameworkcode=?";
	push(@u_datas, $tag);
	push(@u_datas, $fwc);
	if (defined($subfield)) {
	    $sql = $sql . " AND tagsubfield=?";
	    push(@u_datas, $subfield);
	}
	print $sql."\n" if ($debug);
	print Dumper(\@u_datas) if ($debug);
	if ($update && $dbh) {
	    my $sth = $dbh->prepare($sql);
	    $sth->execute(@u_datas);
	}
    }
}

sub mk_sql_insert {
    my ($dbh, $tablename, $fwc, $ftag, $tag, $subfield) = @_;
    if ($insert || $debug) {
	my %datas = (
	    'tagfield' => $tag,
	    'liblibrarian' => $field_data{$ftag}{'name'},
	    'libopac' => $field_data{$ftag}{'name'},
	    'repeatable' => $field_data{$ftag}{'repeatable'},
	    'frameworkcode' => $fwc,
	    );
	if (defined($subfield)) {
	    $datas{'tagsubfield'} = "".$subfield;
	    $datas{'tab'} = substr($tag, 0, 1);
	    $datas{'hidden'} = $hidden_value;
	}

	my (@u_fields, @u_datas);
        foreach my $k (sort keys(%datas)) {
            push(@u_fields, $k);
            push(@u_datas, $datas{$k});
        }

	my $qmarks = ("?," x scalar(@u_fields));
	$qmarks =~ s/,$//;
	my $sql = "INSERT INTO $tablename (".join(',',@u_fields).") VALUES (".$qmarks.")";

	print $sql."\n".Dumper(\@u_datas) if ($debug);

	if ($insert) {
	    my $sth = $dbh->prepare($sql);
	    $sth->execute(@u_datas);
	    print fwcname($fwc)."Added new field: ".pfld($ftag)." (".$field_data{$ftag}{'name'}.")\n";
	}
    } else {
	print fwcname($fwc)."Missing: ".pfld($ftag)." (".$field_data{$ftag}{'name'}.")\n";
    }
}

#
# Update tags in the db
sub update_db_tags {

    my $dbh = db_connect();

    my @frameworks = sort(uniq(keys(%{$tags{tablename(0)}}), keys(%{$tags{tablename(1)}})));

    if ($frameworkcode !~ /^\*/) {
        my @tmpfw = split(/,/, $frameworkcode, -1);
        @tmpfw = ('') if (scalar(@tmpfw) < 1);

        my @usefw = grep { my $f = $_; grep $_ eq $f, @tmpfw } @frameworks;

        @frameworks = @usefw;
    } elsif ($frameworkcode =~ /^\*-/) {
        $frameworkcode =~ s/^\*-//;

        my @tmpfw = split(/,/, $frameworkcode, -1);
        @tmpfw = ('') if (scalar(@tmpfw) < 1);

        my @usefw = grep { my $f = $_; !grep $_ eq $f, @tmpfw } @frameworks;
        @frameworks = @usefw;
    } else {
        # nothing, use default value
    }
    die("No valid frameworks") if (scalar(@frameworks) < 1);

    foreach my $ftag (sort keys(%field_data)) {
	my $tag = substr($ftag, 0, 3);
	my $subfield = "".substr($ftag, 3, 1);
	my $tablename;

	$subfield = '@' if ($subfield eq '');
	$tablename = tablename($subfield ne '@');

	foreach my $fwc (@frameworks) {
	    my %updatedata = ();
	    my $ct;
	    my $cfd;

	    if ($subfield eq '@') {
		if (not exists($tags{$tablename}{$fwc}{$tag})) {
		    mk_sql_insert($dbh, $tablename, $fwc, $ftag, $tag);
		} else {
		    $ct = \%{$tags{$tablename}{$fwc}{$tag}};
		    $cfd = \%{$field_data{$tag}};
		    my $updatedata = check_need_update($ftag, $fwc, $ct, $cfd);
		    mk_sql_update($dbh, $updatedata, $tablename, $fwc, $tag);
		}
	    } else {
		if (not exists($tags{$tablename}{$fwc}{$tag}{$subfield})) {
		    mk_sql_insert($dbh, $tablename, $fwc, $ftag, $tag, $subfield);
		} else {
		    $ct = \%{$tags{$tablename}{$fwc}{$tag}{$subfield}};
		    $cfd = \%{$field_data{$ftag}};
		    my $updatedata = check_need_update($ftag, $fwc, $ct, $cfd);
		    mk_sql_update($dbh, $updatedata, $tablename, $fwc, $tag, $subfield);
		}
	    }
	}
    }
    db_disconnect($dbh);
}

read_xml($xml_glob);
db_query_alltags() if (!$dryrun);

if ($debug) {
    print "Data from XML:\n".Dumper(\%field_data);
    print "Data from Koha database:\n".Dumper(\%tags);
}

update_db_tags() if (!$dryrun);



__END__

=head1 NAME

upsert_marc_fields.pl - Insert or update Koha MARC21 frameworks from FNL's translated MARC format.

=head1 OPTIONS

=over 8

=item B<-help>

Print this help.

=item B<-man>

Print this help as a man page.

=item B<-debug>

Output a lot of debugging data.

=item B<-insert>

Insert missing field data into the Koha database.

=item B<-update>

Update field data in the Koha database to match the format spec.
Updates the field descriptions and repeatability.

=item B<-setflags>

If updating existing fields (with -update), also update the field flags.

=item B<-auth>

Use the authority XML data (data/aukt-*.xml), and check the authority
frameworks.

=item B<-bibs>

Use the bibliographic XML data (data/bib-*.xml), and check the bibliographic
frameworks. This is the default mode.

=item B<-xml=globstring>

Set the XML files where MARC21 format rules are read from. Default depends on
whether -auth or -bibs is given.

=item B<-framework=string>

Only check the listed frameworks, separated by commas.
By default only the default framework is checked.
Asterisk '*' means all frameworks. To do all frameworks except the listed ones,
use '*-X,Y', which would do every framework except X and Y.
Asterisk, if used, must be first.
 For example:
  C<-framework=ACQ,VR>

=item B<-ignore=fieldspecs>

Ignore certain fields or subfields. For example:
  C<-ignore=590,028a,655.ind2,008.length,9xx>
would ignore the field 590, subfield 028a, indicator 2 of field 655,
length checking for field 008, and all 9XX fields.

=item B<-onlyfields=fieldspecs>

Only handle these fields or subfields. Same format as -ignore.

=item B<-flags=string>

Create new fields with these flags set. List flags separated by commas.
Allowed flags are collapsed,editor,flagged,intranet,opac.
Default value is intranet,opac. Not all combinations are valid.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), mysql_socket (no default value), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will read in the Finnish National Library's translated machine-readable MARC21 format
and update or insert the fields in Koha frameworks.

=cut
