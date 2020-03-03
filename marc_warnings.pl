use strict;
use warnings;

# Copyright (c) 2020 Pasi Kallinen
# Licensed under the MIT License, the "Expat" variant.

use Getopt::Long;
use Pod::Usage;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use DBI;
use XML::LibXML;

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

my %presets = (
    'koha-auth' => {
	'sql' => 'select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc',
    },
    'koha-bibs' => {
	'sql' => 'select biblionumber as id, metadata as marc from biblio_metadata order by id asc',
    },
    'koha-hold' => {
	'sql' => 'select holding_id as id, metadata as marc from holdings_metadata order by id asc',
    },
    'eg-auth' => {
	'sql' => 'select id, marc from authority.record_entry order by id asc',
    },
    'eg-bibs' => {
	'sql' => 'select id, marc from biblio.record_entry order by id asc',
    }
    );

my %xml_globs = (
    'bibs' => './data/bib-*.xml',
    'auth' => './data/aukt-*.xml',
    'hold' => './data/hold-*.xml'
    );

# Could we grab these from the format description somehow?
my %field_data = (
    'bibs' => {
	'valid_fields' => {},
	'not_repeatable' => {},
	'allow_indicators' => {},
	'typed' => {},
	'fixed_length' => {
	    '000' => 24,
	    '005' => 16,
	    '006' => 18,
	    '008' => 40
	},
	'regex' => {},
	'allow_regex' => {
	    '000' => {
		'00' => '[0-9]',
		'01' => '[0-9]',
		'02' => '[0-9]',
		'03' => '[0-9]',
		'04' => '[0-9]',
		'12' => '[0-9]',
		'13' => '[0-9]',
		'14' => '[0-9]',
		'15' => '[0-9]',
		'16' => '[0-9]',
	    },
	    '005' => {
		'x' => '^[0-9]{14}\.[0-9]$',
	    },
	},
    },
    'auth' => {
	'valid_fields' => {},
	'not_repeatable' => {},
	'allow_indicators' => {},
	'typed' => {},
	'fixed_length' => {
	    '000' => 24,
	    '005' => 16,
	    '008' => 40
	},
	'regex' => {},
        'allow_regex' => {
	    '000' => {
		'00' => '[0-9]',
		'01' => '[0-9]',
		'02' => '[0-9]',
		'03' => '[0-9]',
		'04' => '[0-9]',
		'12' => '[0-9]',
		'13' => '[0-9]',
		'14' => '[0-9]',
		'15' => '[0-9]',
		'16' => '[0-9]',
	    },
	    '005' => {
		'x' => '^[0-9]{14}\.[0-9]$'
	    },
        },
    },
    'hold' => {
	'valid_fields' => {},
	'not_repeatable' => {},
	'allow_indicators' => {},
	'typed' => {},
	'fixed_length' => {},
	'regex' => {},
	'allow_regex' => {},
    },
    );

# convert 006/00 to material code
my %convert_006_material = (
    'a' => 'BK',
    't' => 'BK',
    'm' => 'CF',
    's' => 'CR',
    'e' => 'MP',
    'f' => 'MP',
    'c' => 'MU',
    'd' => 'MU',
    'i' => 'MU',
    'j' => 'MU',
    'p' => 'MX',
    'g' => 'VM',
    'k' => 'VM',
    'o' => 'VM',
    'r' => 'VM',
    );

my $ignore_fields_param = '';
my $help = 0;
my $man = 0;
my $verbose = 0;
my $test_field_data = 1;
my $xml_glob;
my $sqlquery;
my $biburl;
my $skip_enclevels = ''; # Encoding levels (ldr/17) values to skip the record
my $debug = 0;
my $marcxml = '';

my $auth_or_bibs = 'bibs';
my $koha_or_eg = 'koha';

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (exists($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting '".$onam."'."); } },
    'xml=s' => \$xml_glob,
    'marcxml=s' => \$marcxml,
    'sql=s' => \$sqlquery,
    'v|verbose' => \$verbose,
    'a|auth|authority|authorities' => sub { $auth_or_bibs = 'auth'; },
    'b|bib|bibs|biblios' => sub { $auth_or_bibs = 'bibs'; },
    'hold|holds|holdings' => sub { $auth_or_bibs = 'hold'; },
    'koha' => sub { $koha_or_eg = 'koha'; },
    'nodata' => sub { $test_field_data = 0; },
    'eg|evergreen' => sub { $koha_or_eg = 'eg'; },
    'ignore=s' => \$ignore_fields_param,
    'skip-enclevels=s' => \$skip_enclevels,
    'debug' => \$debug,
    'help|h|?' => \$help,
    'man' => \$man,
    'biburl|bibliourl=s' => \$biburl
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

if (defined($koha_or_eg) && defined($auth_or_bibs)) {
    my $tmp = $koha_or_eg.'-'.$auth_or_bibs;
    if (defined($presets{$tmp})) {
	$xml_glob = $xml_globs{$auth_or_bibs} if (!defined($xml_glob));
	$sqlquery = $presets{$tmp}{'sql'} if (!defined($sqlquery));
    } else {
	die("Unknown preset combination $tmp");
    }
}

die("No SQL query") if (!defined($sqlquery));
die("No MARC21 format XML file") if (!defined($xml_glob));


sub sort_by_number {
    my ( $anum ) = $a =~ /(\d+)/;
    my ( $bnum ) = $b =~ /(\d+)/;
    ( $anum || 0 ) <=> ( $bnum || 0 );
}


#################################################################
################################################################

sub get_field_tagntype {
    my ($f, $record) = @_;

    my $tag = $f->tag();

    if ($tag eq '006') {
        my $data = substr($f->data(), 0, 1) || '';
        return $tag.'-'.$convert_006_material{$data} if (defined($convert_006_material{$data}));
    } elsif ($tag eq '007') {
        my $data = substr($f->data(), 0, 1) || '';
        return $tag.'-'.$data if ($data ne '');
    } elsif ($tag eq '008') {
        my $ldr = $record->leader();
        my $l6 = substr($ldr, 6, 1);
        my $l7 = substr($ldr, 7, 1);
        my $data = '';
        # FIXME: Same as 006, but also checks ldr/07
        $data = 'BK' if (($l6 eq 'a' || $l6 eq 't') && !($l7 eq 'b' || $l7 eq 'i' || $l7 eq 's'));
        $data = 'CF' if ($l6 eq 'm');
        $data = 'CR' if (($l6 eq 'a' || $l6 eq 't') &&  ($l7 eq 'b' || $l7 eq 'i' || $l7 eq 's'));
        $data = 'MP' if ($l6 eq 'e' || $l6 eq 'f');
        $data = 'MU' if ($l6 eq 'c' || $l6 eq 'd' || $l6 eq 'i' || $l6 eq 'j');
        $data = 'MX' if ($l6 eq 'p');
        $data = 'VM' if ($l6 eq 'g' || $l6 eq 'k' || $l6 eq 'o' || $l6 eq 'r');
        return $tag.'-'.$data if ($data ne '');
    }
    return $tag;
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

        foreach my $a1 (@c1) {
            foreach my $a2 (@c2) {
                foreach my $a3 (@c3) {
                    my $fld = $a1.$a2.$a3.$p4;
                    push @fields, $fld;
                }
            }
        }
    } else {
        push @fields, $tag;
    }

    return @fields;
}

sub parse_positions {
    my ($field, $data, $tag, $type, $subfield) = @_;

    my @posdom = $field->findnodes('positions/position');
    if (scalar(@posdom) > 0) {
        foreach my $p (@posdom) {
            my $pos = $p->getAttribute('pos');
            my @equals = $p->findnodes('equals');
            my @pvalues = $p->findnodes('alternatives/alternative/values/value|values/value');
            my @vals;

	    $pos =~ s/^\///;

            if (scalar(@pvalues) > 0) {
		my $fcode = $tag . $subfield . $type;
                foreach my $pv (@pvalues) {
                    my $pv_code = $pv->getAttribute('code');
                    $pv_code =~ s/#/ /g;
                    $data->{'regex'}{$fcode}{$pos} = [] if (!defined($data->{'regex'}{$fcode}{$pos}));
                    push @{$data->{'regex'}{$fcode}{$pos}}, $pv_code;

                    $data->{'allow_regex'}{$fcode}{$pos} = [] if (!defined($data->{'allow_regex'}{$fcode}{$pos}));
		    if (ref($data->{'allow_regex'}{$fcode}{$pos}) eq 'ARRAY') {
			push @{$data->{'allow_regex'}{$fcode}{$pos}}, $pv_code;
		    } else {
			print STDERR "allow_regex is not array for '$fcode/$pos' '$pv_code'";
		    }
                }

                if (scalar(@equals) > 0) {
                    foreach my $eq (@equals) {
                        my $eq_tag = $eq->getAttribute('tag');
                        my $eq_pos = $eq->getAttribute('positions');
			my $efcode =  $eq_tag . $type;
                        $data->{'regex'}{$efcode}{$eq_pos} = [] if (!defined($data->{'regex'}{$efcode}{$eq_pos}));
                        @{$data->{'regex'}{$efcode}{$eq_pos}} = @{$data->{'regex'}{$fcode}{$pos}};

                        $data->{'allow_regex'}{$efcode}{$eq_pos} = [] if (!defined($data->{'allow_regex'}{$efcode}{$eq_pos}));
                        if (ref($data->{'allow_regex'}{$efcode}{$eq_pos}) eq 'ARRAY') {
                            @{$data->{'allow_regex'}{$efcode}{$eq_pos}} = @{$data->{'allow_regex'}{$fcode}{$pos}};
                        } else {
                            print STDERR "allow_regex equals is not array for '$eq_tag' '$type' '$eq_pos'"
                        }
                    }
                }
            }
        }
    }
}

sub parse_single_field {
    my ($field, $data) = @_;

    #my $name = $field->findvalue('name');
    my $tag = $field->getAttribute('tag');
    my $type = $field->getAttribute('type') || '';
    my $repeatable = $field->getAttribute('repeatable') || '';

    if ($tag =~ /x/i) {
        my @tags = generate_tag_sequence($tag);
        foreach my $tmptag (@tags) {
            $field->setAttribute('tag', $tmptag);
            parse_single_field($field, $data);
        }
        return;
    }

    $type = '' if ($type eq 'yleista');
    $type = "-".$type if ($type ne '');
    $data->{'typed'}{$tag} = 1 if ($type ne '');

    $data->{'valid_fields'}{$tag} = 1;
    $data->{'not_repeatable'}{$tag . $type} = 1 if ($repeatable eq 'N');

    my @inddom = $field->findnodes('indicators/indicator');
    if (scalar(@inddom) > 0) {
        foreach my $ind (@inddom) {
            my $ind_num = $ind->getAttribute('num');
            my @ind_values = $ind->findnodes('values/value');
            my $allowed_ind_values = '';

            foreach my $indval (@ind_values) {
                my $ivcode = $indval->getAttribute('code');
                $ivcode =~ s/#/ /g;
                $allowed_ind_values .= $ivcode;
            }
            $data->{'allow_indicators'}{$tag . $ind_num} = $allowed_ind_values if ($allowed_ind_values ne '');
        }
    }


    my @sfdom = $field->findnodes('subfields/subfield');
    if (scalar(@sfdom) > 0) {
        foreach my $sf (@sfdom) {
            my $sf_code = $sf->getAttribute('code');
            my $sf_repeatable = $sf->getAttribute('repeatable');
            my $sf_name = $sf->findvalue('name');

            my $sf_a;
            my $sf_b;
            if ($sf_code =~ /^(.)-(.)$/) {
                $sf_a = $1;
                $sf_b = $2;
            } else {
                $sf_a = $sf_b = $sf_code;
            }

            for my $sfc ($sf_a .. $sf_b) {
                $data->{'valid_fields'}{$tag . $sfc} = 1;
                $data->{'not_repeatable'}{$tag . $sfc . $type} = 1 if ($sf_repeatable eq 'N');
                parse_positions($sf, $data, $tag, $type, $sfc);
            }
        }
    }

    parse_positions($field, $data, $tag, $type, '');
}

sub parse_xml_data {
    my ($filename, $data) = @_;

    my $dom = XML::LibXML->load_xml(location => $filename);

    my @ldr = $dom->findnodes('//fields/leader-directory/leader');
    if (scalar(@ldr) > 0) {
        foreach my $tag (@ldr) {
            $tag->setAttribute('tag', '000');
            parse_single_field($tag, $data);
        }
    }

    my @ctrls = $dom->findnodes('//fields/controlfields/controlfield');
    if (scalar(@ctrls) > 0) {
        foreach my $tag (@ctrls) {
            parse_single_field($tag, $data);
        }
    }

    my @datas = $dom->findnodes('//fields/datafields/datafield');
    if (scalar(@datas) > 0) {
        foreach my $tag (@datas) {
            parse_single_field($tag, $data);
        }
    }
}


sub fix_regex_data {
    my ($data) = @_;

    my %re = %{$data};

    foreach my $rekey (sort keys(%re)) {
        my %sr = %{$re{$rekey}};
        foreach my $srkey (sort keys(%sr)) {
            my $dat = $sr{$srkey};
            my $rdat = ref($sr{$srkey});
            next if ($rdat eq 'Regexp');

            my $srkeylen = 1;
            if ($srkey =~ /(\d+)-(\d+)/) {
                my ($startpos, $endpos) = ($1, $2);
                $srkeylen = ($endpos - $startpos) + 1;
            }

            if ($rdat eq 'ARRAY') {
                my @vals;
                for (my $idx = 0; $idx < scalar(@{$dat}); $idx++) {
                    my $val = @{$dat}[$idx];
                    if ($val =~ /^(\d+)-(\d+)$/) {
                        push(@vals, ($1 .. $2));
                        next;
                    }
                    push(@vals, $val);
                }

                my %reparts;
                foreach my $val (@vals) {
                    my $lval = length($val);
                    $val =~ s/\|/\\|/g;
                    $reparts{$lval} = () if (!defined($reparts{$lval}));
                    push(@{$reparts{$lval}}, $val);
                }

                my @restr;
                for my $key (sort keys(%reparts)) {
                    if (int($key) == $srkeylen) {
                        push(@restr, @{$reparts{$key}});
                    } else {
                        my $reps = ($srkeylen / int($key));
                        if ($reps == int($reps)) {
                            my $s = '(' . join('|', @{$reparts{$key}}) . '){'.int($reps).'}';
                            push(@restr, $s);
                        } else {
                            print STDERR "ERROR: Regexp repeat for $rekey/$srkey does not fit: (".join('|', @{$reparts{$key}}).")\n";
                        }
                    }
                }

                my $s = join('|', @restr);
                $re{$rekey}{$srkey} = qr/^($s)$/;

            } else {
                print STDERR "marc21 format regex is not array";
            }
        }
    }
    return $data;
}

sub quoted_str_list {
    my ($lst) = @_;
    my $ret = '';
    if (defined($lst)) {
        my @arr = do { my %seen; grep { !$seen{$_}++ } @{$lst} };

	my $haspipes = 0;
	my $len = 0;
	my %lens;
	foreach my $tmp (@arr) {
	    $haspipes = 1 if ($tmp =~ /\|/);
	    $len = length($tmp) if ($len == 0);
	    $len = -1 if ($len != length($tmp));
	}
	if (!$haspipes && $len != -1) {
	    $ret = join('', @arr) if ($len == 1);
	    $ret = join('|', @arr) if ($len > 1);
	} elsif ($len != -1) {
	    $ret = join('', @arr) if ($len == 1);
	    $ret = join(',', @arr) if ($len > 1);
	} else {
	    $ret = join('","', @arr);
	    $ret = '"'.$ret.'"' if ($ret ne '');
	}
    }
    return '['.$ret.']';
}

sub fix_allow_regex_data {
    my ($data) = @_;

    my %re = %{$data};

    foreach my $rekey (sort keys(%re)) {
        my %sr = %{$re{$rekey}};
        foreach my $srkey (sort keys(%sr)) {
            my $dat = $sr{$srkey};
	    if (ref($dat) eq 'ARRAY') {
		$re{$rekey}{$srkey} = quoted_str_list($dat);
	    }
        }
    }

    return $data;
}

sub copy_allow_to_regex {
    my ($allow, $regex) = @_;

    my %al = %{$allow};
    my %re = %{$regex};

    foreach my $alkey (keys (%al)) {
        my @arr = sort do { my %seen; grep { !$seen{$_}++ } keys (%{$al{$alkey}}) };
	foreach my $xlkey (@arr) {
	    $re{$alkey} = {} if (!defined($re{$alkey}));
	    $re{$alkey}{$xlkey} = qr/$al{$alkey}{$xlkey}/ if (!defined($re{$alkey}{$xlkey}));
	}
    }
    return \%re;
}

$field_data{$auth_or_bibs}{'regex'} = copy_allow_to_regex($field_data{$auth_or_bibs}{'allow_regex'}, $field_data{$auth_or_bibs}{'regex'});

my @xmlfiles = glob($xml_glob);
foreach my $file (@xmlfiles) {
    parse_xml_data($file, \%{$field_data{$auth_or_bibs}});
}

$field_data{$auth_or_bibs}{'regex'} = fix_regex_data($field_data{$auth_or_bibs}{'regex'});
$field_data{$auth_or_bibs}{'allow_regex'} = fix_allow_regex_data($field_data{$auth_or_bibs}{'allow_regex'});

# indicators are listed as sets of allowed chars. eg. ' ab' or '1-9'
foreach my $tmp (keys(%{$field_data{$auth_or_bibs}{'allow_indicators'}})) {
    $field_data{$auth_or_bibs}{'allow_indicators'}{$tmp} = '[' . $field_data{$auth_or_bibs}{'allow_indicators'}{$tmp} . ']';
}


#################################################################
################################################################

my %ignore_fields = map { ($_ => 1) } generate_tag_sequence($ignore_fields_param);

if ($debug) {
    print "ignore_fields:" . Dumper(\%ignore_fields);
    print "field_data:" . Dumper(\%field_data);
    exit;
}

MARC::Charset->assume_unicode(1);

sub output_err {
    my ($id, $urllink, $errs) = @_;
    my @errors = sort @{$errs};

    if (defined($biburl)) {
        print "<li><a href='".sprintf($biburl, $id)."'>$urllink</a>: (".join(', ', @errors).")\n" if (@errors);
    } else {
        print "id=$id (".join(', ', @errors).")\n" if (@errors);
    }
}

sub check_regexkeys {
    my ($f, $data, $tag, $tagntype, $subfield) = @_;

    my @errors;
    my @regexkeys;
    push(@regexkeys, $tag.$subfield) if (defined($field_data{$auth_or_bibs}{'regex'}{$tag.$subfield}));
    push(@regexkeys, $tagntype.$subfield) if ($tag ne $tagntype && defined($field_data{$auth_or_bibs}{'regex'}{$tagntype.$subfield}));
    push(@regexkeys, $tag.'-kaikki'.$subfield) if (defined($field_data{$auth_or_bibs}{'regex'}{$tag.'-kaikki'.$subfield}));
    if (scalar(@regexkeys)) {
	foreach my $rk (sort @regexkeys ) {
	    my $s;
	    my $showrk = $rk;
	    if ($tag eq $tagntype && $rk =~ /^(...)(.)$/) {
		$showrk = $1 . '$' . $2;
	    }

	    my $zf = $field_data{$auth_or_bibs}{'regex'}{$rk};
	    my %ff = %{$zf};
	    foreach my $ffk (sort(sort_by_number keys(%ff))) {

		my $allow_vals = $field_data{$auth_or_bibs}{'allow_regex'}{$rk}{$ffk};

		if ($ffk =~ /^\d+$/) {
		    $s = length($data) < int($ffk) ? '' : substr($data, int($ffk), 1);

		    if ($s !~ /$ff{$ffk}/) {
			push(@errors, "$showrk/$ffk illegal value \"$s\", should be $allow_vals");
			next;
		    }
		} elsif ($ffk =~ /^(\d+)-(\d+)$/) {
		    my ($kstart, $kend) = (int($1), int($2));
		    $s = length($data) < $kend ? '' : substr($data, $kstart, $kend - $kstart + 1);

		    if ($s !~ /$ff{$ffk}/) {
			push(@errors, "$showrk/$ffk illegal value \"$s\", should be $allow_vals");
			next;
		    }
		} else {
		    $s = $data || "";

		    if ($s !~ /$ff{$ffk}/) {
			push(@errors, "$showrk illegal value \"$s\", does not match $allow_vals");
			next;
		    }
		}
	    }
	}
    }
    return \@errors;
}

sub check_marc {
    my ($id, $marc, $urllink) = @_;

    my $record;
    eval {
	$record = MARC::Record->new_from_xml($marc);
    };
    if ($@) {
        my @err = ("MARC record error");
        output_err($id, $urllink, \@err);
        return;
    }

    return if (index($skip_enclevels, substr($record->leader(),17,1)) != -1);

    my %mainf;
    my %inderrs;
    my %undeffs;

    my %numfields;

    my @errors;

    my %valid_fields = %{$field_data{$auth_or_bibs}{'valid_fields'}};
    my %not_repeatable = %{$field_data{$auth_or_bibs}{'not_repeatable'}};
    my %allow_indicators = %{$field_data{$auth_or_bibs}{'allow_indicators'}};
    my %typed_field = %{$field_data{$auth_or_bibs}{'typed'}};

    $record->append_fields(MARC::Field->new('000', $record->leader()));

    foreach my $f ($record->field('...')) {
	my $fi = $f->tag();
	my $fityp = get_field_tagntype($f, $record);

	next if (defined($ignore_fields{$fi}) || defined($ignore_fields{$fityp}));

        if (!defined($valid_fields{$fi})) {
            $undeffs{$fi} = 1;
            #push(@errors, "field $fi not defined by format");
            next;
        }

	if ($test_field_data) {
	    my $key = $fi.'.length';
	    if (!defined($ignore_fields{$key}) && defined($field_data{$auth_or_bibs}{'fixed_length'}{$fi})) {
		my $tmp = $field_data{$auth_or_bibs}{'fixed_length'}{$fi};
		if ($tmp != length($f->data())) {
		    push(@errors, "$key=".length($f->data())."/$tmp");
		    next;
		}
	    }

	    if ($f->is_control_field()) {
		push(@errors, @{check_regexkeys($f, $f->data(), $fi, $fityp, '')});
	    }
	}

	if ($typed_field{$fi}) {

	    next if (defined($ignore_fields{$fityp}));

	    if ($fityp ne $fi) {
		$mainf{$fityp} = 0 if (!defined($mainf{$fityp}));
		$mainf{$fityp}++;
	    }
	}

	next if (scalar($fi) < 10);

	$mainf{$fi} = 0 if (!defined($mainf{$fi}));
	$mainf{$fi}++;

	my @subf = $f->subfields();
	my %subff;
	foreach my $sf (@subf) {
	    my $key = $sf->[0];
	    my $val = $sf->[1];
	    my $fikey = $fi.$key;

	    next if (defined($ignore_fields{$fikey}));

	    if (!defined($valid_fields{$fikey})) {
		$undeffs{$fi . '$' . $key} = 1;
		#push(@errors, "field $fikey not defined by format");
		next;
	    }

	    push(@errors, @{check_regexkeys($sf, $val, $fi, $fityp, $key)});

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

    if (scalar(keys(%undeffs)) > 0) {
        push(@errors, "field".(scalar(keys(%undeffs)) > 1 ? "s" : "")." ".join(',', sort(keys(%undeffs)))." not in format");
    }

    foreach my $k (keys(%inderrs)) {
	push(@errors, (($inderrs{$k} > 1) ? $inderrs{$k}.'x' : '').$k);
    }

    foreach my $k (keys(%mainf)) {
	push(@errors, (($mainf{$k} > 1) ? $mainf{$k}.'x' : '').$k) if (($mainf{$k} > 1) && defined($not_repeatable{$k}));
    }

    output_err($id, $urllink, \@errors);
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

if ($marcxml ne '') {
    open my $fh, '<', $marcxml or die "Can't open file $!";
    my $file_content = do { local $/; <$fh> };
    check_marc(0, $file_content, 0);
} else {

    my $sth;
    my $dbh = db_connect();

    $sth = $dbh->prepare($sqlquery);

    $sth->execute();

    print "<html><head></head><body><ol>" if (defined($biburl));

    my $i = 1;
    while (my $ref = $sth->fetchrow_hashref()) {
        if ($ref->{'id'}) {
            if ($verbose) {
                print "\n$i" if (!($i % 100));
                print ".";
            }
            $ref->{'urllink'} = $ref->{'id'} if (!defined($ref->{'urllink'}));
            check_marc($ref->{'id'}, $ref->{'marc'}, $ref->{'urllink'});
            $i++;
        }
    }

    print "</ol></body></html>" if (defined($biburl));

    db_disconnect($dbh);
}


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

=item B<-holds>

=item B<-koha>

=item B<-evergreen>

Use preset values. Requires one of B<-auth>, B<-bibs>, or B<-holds>, and one of B<-koha>, or B<-evergreen>.
Defaults to B<-koha> and B<-bibs>.

=item B<-biburl=urlformat>

Output HTML, with urlformat string as the href link to the biblio item.
Use %s as the biblio id number placeholder in the urlformat string.

=item B<-xml=filename>

Set the filename glob where MARC21 format rules are read from. Default depends on B<-auth>, or
B<-bibs>, or B<-holds> option: data/aukt-*.xml, data/bibs-*.xml, or data/hold-*.xml, respectively.

=item B<-marcxml=filename>

Read a file containing single marcxml entry, and show warnings for it instead.

=item B<-sql=text>

Set the SQL query to perform. Must return two fields (id and marcxml), and optionally third (urllink).
Default depends on the preset values used. For example, if you want the id this program reports to be the
contents of the 001 field, use -sql='select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc'
The urllink is used as the biblio url link when outputting HTML, and if it doesn't exist,
then the id is used instead.

=item B<-ignore=fieldspecs>

Ignore certain fields, subfields or indicators. For example:
  C<-ignore=590,028a,655.ind2,008.length,9xx>
would ignore the field 590, subfield 028a, indicator 2 of field 655,
length checking for field 008, and all 9XX fields.

=item B<-skip-enclevels=str>

Set the record encoding levels (000/17) which you want to skip. For example:
  C<-skip-enclevels=78>
would skip records with encoding level 7 or 8.

=item B<-debug>

Parse the XML files, output the internal state, and exit.

=item B<-nodata>

Do not test fixed field lengths or data validity.

=item B<-db setting=value>

Set database settings. Available settings and default values are hostname ("localhost"),
username ("kohaadmin"), password ("katikoan"), dbname ("koha"), mysql_socket (no default value), and driver ("mysql").

=back

=head1 DESCRIPTION

This program will report format errors in your MARC21 XML data, either in your Koha or Evergreen
system. Output will list a record ID and the fields where errors occurred. This will list
repeated fields and subfields which are not repeatable, invalid indicator values, and
some fixed-length controlfield data errors.

=cut
