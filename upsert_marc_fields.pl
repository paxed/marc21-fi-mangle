use strict;
use warnings;

#
# Update and/or insert MARC21 field information from the Finnish
# National Library's translated machine-readable MARC21 format into Koha
#
#
#
#
#
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

my $xml_glob = '';
my $ignore_fields_param = '';
my $help = 0;
my $man = 0;
my $verbose = 0;
my $debug = 0;
my $print = 0;
my $insert = 0;
my $missing = 0;
my $bib_or_auth = 'marc';
my $frameworkcode = ' ';

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (exists($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting '".$onam."'."); } },
    'v|verbose' => \$verbose,
    'ignore=s' => \$ignore_fields_param,
    'help|h|?' => \$help,
    'man' => \$man,
    'debug' => \$debug,
    'xml=s' => \$xml_glob,
    'print' => \$print,
    'insert' => \$insert,
    'missing' => \$missing,
    'auth' => sub { $bib_or_auth = 'auth'; },
    'framework=s' => \$frameworkcode,
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

if ($xml_glob eq '') {
    $xml_glob = $xml_globs{$bib_or_auth};
    die "XML glob files missing." if (!$xml_glob);
}
    


my %ignore_fields = map { ($_ => 1) } generate_tag_sequence($ignore_fields_param);
my %field_data = ( ); # marc format data from xml
my %tags = ( );  # tag/subfield data from the database


#################################################################
#################################################################

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
        if ($ref->{'tagsubfield'}) {
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

sub handle_code {
    my ($tag, $name, $repeatable) = @_;

    my $sf = substr($tag, 3, 1) || '@';

    print "Ignored: $tag\n" if ($debug && $ignore_fields{$tag});
    return if ($ignore_fields{$tag});
    
    print "$tag\t$name\trepeatable=$repeatable\n" if ($print);
    my %tmphash = (
	'tagfield' => substr($tag, 0, 3),
	'name' => $name,
	'repeatable' => ($repeatable eq 'Y') ? 1 : 0,
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

    return if ($repeatable eq '');

    #print Dumper($field);

    if ($tag =~ /x/i) {
        my @tags = generate_tag_sequence($tag);
        foreach my $tmptag (@tags) {
            $field->{'tag'} = $tmptag;
            parse_single_field($field, $data);
        }
        return;
    }

    handle_code($tag, $name, $repeatable);

    #my %valid_fields = %{$data->{'valid_fields'}};
    #my %not_repeatable = %{$data->{'not_repeatable'}};
    #my %allow_indicators = %{$data->{'allow_indicators'}};
    #my %typed_field = %{$data->{'typed'}};
    #my %regex_field = %{$data->{'regex'}};
    #my %allow_regex = %{$data->{'allow_regex'}};

    #$type = '' if ($type eq 'yleista');
    #$type = "-".$type if ($type ne '');
    #$typed_field{$tag} = 1 if ($type ne '');

    #$valid_fields{$tag} = 1;
    #$not_repeatable{$tag . $type} = 1 if ($repeatable eq 'N');

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
                    handle_code($tag.$sfc, $sf_name, $repeatable);
                }
            } elsif ($sf_code =~ /^.$/) {
                handle_code($tag.$sf_code, $sf_name, $repeatable);
            } else {
                die "Unhandled subfield \"$sf_code\" for \"$tag\"";
            }

            #$valid_fields{$tag . $sf_code} = 1;
            #$not_repeatable{$tag . $sf_code . $type} = 1 if ($sf_repeatable eq 'N');
        }
    }

    #$data->{'valid_fields'} = \%valid_fields;
    #$data->{'not_repeatable'} = \%not_repeatable;
    #$data->{'allow_indicators'} = \%allow_indicators;
    #$data->{'typed'} = \%typed_field;
    #$data->{'regex'} = \%regex_field;
    #$data->{'allow_regex'} = \%allow_regex;
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
    foreach my $file (@xmlfiles) {
	print "\nParsing: $file\n" if ($debug);
        parse_xml_data($file, \%field_data);
    }
}

sub uniq {
    my %seen;
    return grep { !$seen{$_}++ } @_;
}

#
# Lists tags that are missing from the db
sub find_missing_tags {
    my @frameworks = sort(uniq(keys(%{$tags{tablename(0)}}), keys(%{$tags{tablename(1)}})));

    if ($frameworkcode ne '*') {
	my @tmpfw = split(/,/, $frameworkcode);
	my @usefw = grep { my $f = $_; grep $_ eq $f, @tmpfw } @frameworks;

	@frameworks = @usefw;
	@frameworks = ('') if (scalar(@frameworks) < 1);
    }
    
    foreach my $ftag (sort keys(%field_data)) {
	my $tag = substr($ftag, 0, 3);
	my $subfield = substr($ftag, 3, 1) || '@';
	my $tablename = tablename($subfield ne '@');

	next if ($subfield eq '@');

	foreach my $fwc (@frameworks) {
	    print $fwc."\t".$tag." ".$subfield."\n" if (not exists($tags{$tablename}{$fwc}{$tag}{$subfield}));
	}
	
    }
}

read_xml($xml_glob);
db_query_alltags();

find_missing_tags();

if ($debug) {
    print Dumper(\%field_data);
    print Dumper(\%tags);
    exit;
}





__END__

=head1 NAME

upsert_marc_fields.pl - Insert or update Koha MARC21 frameworks from FNL's translated MARC format.

=head1 OPTIONS

=over 8

=item B<-help>

Print this help.

=item B<-man>

Print this help as a man page.

=item B<-print>

Print output to STDOUT.

=item B<-missing>

Print field data missing from the Koha database to STDOUT.

=item B<-insert>

Insert missing field data into the Koha database.

=item B<-xml=globstring>

Set the XML files where MARC21 format rules are read from. Default is data/bib-*.xml

=item B<-framework=fwcodespecs>

Only check the listed frameworks, separated by commas. Default value is '', which means the
default framework. Asterisk '*' means all frameworks. For example:
  C<-framework=ACQ,VR>

=item B<-ignore=fieldspecs>

Ignore certain fields, subfields or indicators. For example:
  C<-ignore=590,028a,655.ind2,008.length,9xx>
would ignore the field 590, subfield 028a, indicator 2 of field 655,
length checking for field 008, and all 9XX fields.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), mysql_socket (no default value), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will read in the Finnish National Library's translated machine-readable MARC21 format
and update or insert  the fields in Koha frameworks.

=cut
