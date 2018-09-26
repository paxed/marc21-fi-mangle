use strict;
use warnings;

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

my %presets = (
    'koha-auth' => {
	'sql' => 'select ExtractValue(marcxml, "//controlfield[@tag=001]") as id, marcxml as marc from auth_header order by id asc',
    },
    'koha-bibs' => {
	'sql' => 'select biblionumber as id, metadata as marc from biblio_metadata order by id asc',
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
    'auth' => './data/aukt-*.xml'
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
        'allow_regex' => {},
	'regex' => {
	    '000' => {
		'00' => qr/[0-9]/,
		'01' => qr/[0-9]/,
		'02' => qr/[0-9]/,
		'03' => qr/[0-9]/,
		'04' => qr/[0-9]/,
		'12' => qr/[0-9]/,
		'13' => qr/[0-9]/,
		'14' => qr/[0-9]/,
		'15' => qr/[0-9]/,
		'16' => qr/[0-9]/,
	    },
	    '005' => {
		'x' => qr/^[0-9]{14}\.[0-9]$/
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
        'allow_regex' => {},
	'regex' => {
	    '000' => {
		'0' => qr/\d/,
		'1' => qr/\d/,
		'2' => qr/\d/,
		'3' => qr/\d/,
		'4' => qr/\d/,
		'7' => qr/[ ]/,
		'8' => qr/[ ]/,
		'12' => qr/\d/,
		'13' => qr/\d/,
		'14' => qr/\d/,
		'15' => qr/\d/,
		'16' => qr/\d/,
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

my $auth_or_bibs = 'bibs';
my $koha_or_eg = 'koha';

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (exists($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { die("Unknown db setting '".$onam."'."); } },
    'xml=s' => \$xml_glob,
    'sql=s' => \$sqlquery,
    'v|verbose' => \$verbose,
    'a|auth|authority|authorities' => sub { $auth_or_bibs = 'auth'; },
    'b|bib|bibs|biblios' => sub { $auth_or_bibs = 'bibs'; },
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
    my ($tag, $record) = @_;

    if ($tag eq '006') {
        my $f = $record->field('006');
        if ($f) {
            my $data = substr($f->data(), 0, 1) || '';
            return $tag.'-'.$convert_006_material{$data} if (defined($convert_006_material{$data}));
        }
    } elsif ($tag eq '007') {
        my $f = $record->field($tag);
        if ($f) {
            my $data = substr($f->data(), 0, 1) || '';
            return $tag.'-'.$data if ($data ne '');
        }
    } elsif ($tag eq '008') {
        my $ldr = $record->leader();
        my $l6 = substr($ldr, 6, 1);
        my $l7 = substr($ldr, 7, 1);
        my $data = '';
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

sub parse_single_field {
    my ($field, $data) = @_;

    #my $name = $field->{'name'};
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

    my %valid_fields = %{$data->{'valid_fields'}};
    my %not_repeatable = %{$data->{'not_repeatable'}};
    my %allow_indicators = %{$data->{'allow_indicators'}};
    my %typed_field = %{$data->{'typed'}};
    my %regex_field = %{$data->{'regex'}};
    my %allow_regex = %{$data->{'allow_regex'}};

    $type = '' if ($type eq 'yleista');
    $type = "-".$type if ($type ne '');
    $typed_field{$tag} = 1 if ($type ne '');

    $valid_fields{$tag} = 1;
    $not_repeatable{$tag . $type} = 1 if ($repeatable eq 'N');

    if (defined($field->{'indicators'}{'indicator'})) {
        my $indicators = $field->{'indicators'}{'indicator'};
        my @indicatorarr;

        if (ref($indicators) eq 'ARRAY') {
            @indicatorarr = @{$indicators};
        } else {
            @indicatorarr = $indicators;
        }

        foreach my $ind (@indicatorarr) {
            my $ind_num = $ind->{'num'};
            my $ind_values = $ind->{'values'}{'value'};
            my @ind_valuearr;
            my $allowed_ind_values = '';

            next if (!defined($ind_values));

            if (ref($ind_values) eq 'ARRAY') {
                @ind_valuearr = @{$ind_values};
            } else {
                @ind_valuearr = $ind_values;
            }
            foreach my $indval (@ind_valuearr) {
                my $ivcode = $indval->{'code'};
                $ivcode =~ s/#/ /g;
                $allowed_ind_values .= $ivcode;
            }
            $allow_indicators{$tag . $ind_num} = $allowed_ind_values;
        }
    }


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

            $valid_fields{$tag . $sf_code} = 1;
            $not_repeatable{$tag . $sf_code . $type} = 1 if ($sf_repeatable eq 'N');
        }
    }

    if (defined($field->{'positions'}{'position'})) {
        my $positions = $field->{'positions'}{'position'};
        my @positionarr;

        if (ref($positions) eq 'ARRAY') {
            @positionarr = @{$positions};
        } else {
            @positionarr = $positions;
        }

        foreach my $p (@positionarr) {
            my $pos = $p->{'pos'};
            my $equals = $p->{'equals'};
            my @vals;

            if (defined($p->{'values'}{'value'})) {
                my $pvalues = $p->{'values'}{'value'};
                my @pvaluearr;
                if (ref($pvalues) eq 'ARRAY') {
                    @pvaluearr = @{$pvalues};
                } else {
                    @pvaluearr = $pvalues;
                }
                foreach my $pv (@pvaluearr) {
                    my $pv_code = $pv->{'code'};
                    $pv_code =~ s/#/ /g;
                    $regex_field{$tag . $type}{$pos} = [] if (!defined($regex_field{$tag . $type}{$pos}));
                    push @{$regex_field{$tag . $type}{$pos}}, $pv_code;

                    $allow_regex{$tag . $type}{$pos} = [] if (!defined($allow_regex{$tag . $type}{$pos}));
                    push @{$allow_regex{$tag . $type}{$pos}}, $pv_code;
                }

                if (defined($equals)) {
                    my $eq_tag = $equals->{'tag'};
                    my $eq_pos = $equals->{'positions'};
                    $regex_field{$eq_tag . $type}{$eq_pos} = [] if (!defined($regex_field{$eq_tag . $type}{$eq_pos}));
                    @{$regex_field{$eq_tag . $type}{$eq_pos}} = @{$regex_field{$tag . $type}{$pos}};

                    $allow_regex{$eq_tag . $type}{$eq_pos} = [] if (!defined($allow_regex{$eq_tag . $type}{$eq_pos}));
                    @{$allow_regex{$eq_tag . $type}{$eq_pos}} = @{$allow_regex{$tag . $type}{$pos}};
                }
            }
        }
    }

    $data->{'valid_fields'} = \%valid_fields;
    $data->{'not_repeatable'} = \%not_repeatable;
    $data->{'allow_indicators'} = \%allow_indicators;
    $data->{'typed'} = \%typed_field;
    $data->{'regex'} = \%regex_field;
    $data->{'allow_regex'} = \%allow_regex;
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
        #warn "parse_marc21_format_xml: unhandled file $filename";
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
                            print STDERR "Regexp repeat not an int: (".join('|', @{$reparts{$key}})."){".$reps."}";
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
	my @arr = @{$lst};
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
            $re{$rekey}{$srkey} = quoted_str_list($dat);
        }
    }

    return $data;
}


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
	my $fi = $f->{'_tag'};
	my $fityp = get_field_tagntype($fi, $record);

	next if (defined($ignore_fields{$fi}) || defined($ignore_fields{$fityp}));

        if (!defined($valid_fields{$fi})) {
            $undeffs{$fi} = 1;
            #push(@errors, "field $fi not defined by format");
            next;
        }

	if ($test_field_data) {
	    my $key = $fi.'.length';
	    next if (defined($ignore_fields{$key}));
	    if (defined($field_data{$auth_or_bibs}{'fixed_length'}{$fi})) {
		my $tmp = $field_data{$auth_or_bibs}{'fixed_length'}{$fi};
		if ($tmp != length($f->data())) {
		    push(@errors, "$key=".length($f->data())."/$tmp");
		    next;
		}
	    }

	    my @regexkeys;
	    push(@regexkeys, $fi) if (defined($field_data{$auth_or_bibs}{'regex'}{$fi}));
	    push(@regexkeys, $fityp) if ($fi ne $fityp && defined($field_data{$auth_or_bibs}{'regex'}{$fityp}));
	    push(@regexkeys, $fi.'-kaikki') if (defined($field_data{$auth_or_bibs}{'regex'}{$fi.'-kaikki'}));
	    if (scalar(@regexkeys)) {
		my $data = $f->data();

		foreach my $rk (sort @regexkeys ) {
		    my $s;

		    my $zf = $field_data{$auth_or_bibs}{'regex'}{$rk};
		    my %ff = %{$zf};
		    foreach my $ffk (sort(sort_by_number keys(%ff))) {

                        my $allow_vals = $field_data{$auth_or_bibs}{'allow_regex'}{$rk}{$ffk};

			if ($ffk =~ /^\d+$/) {
			    $s = length($data) < int($ffk) ? '' : substr($data, int($ffk), 1);

			    if ($s !~ /$ff{$ffk}/) {
				push(@errors, "$rk/$ffk illegal value \"$s\", should be $allow_vals");
				next;
			    }
			} elsif ($ffk =~ /^(\d+)-(\d+)$/) {
			    my ($kstart, $kend) = (int($1), int($2));
			    $s = length($data) < $kend ? '' : substr($data, $kstart, $kend - $kstart + 1);

			    if ($s !~ /$ff{$ffk}/) {
				push(@errors, "$rk/$ffk illegal value \"$s\", should be $allow_vals");
				next;
			    }
			} else {
			    $s = $data || "";

			    if ($s !~ /$ff{$ffk}/) {
				push(@errors, "$rk illegal value \"$s\", does not match $allow_vals");
				next;
			    }
			}
		    }
		}
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

	my @subf = @{$f->{'_subfields'}};
	my %subff;

	while ($#subf > 0) {
	    my $key = shift @subf;
	    my $val = shift @subf;
	    my $fikey = $fi.$key;

	    next if (defined($ignore_fields{$fikey}));

	    if (!defined($valid_fields{$fikey})) {
		$undeffs{$fi . '$' . $key} = 1;
		#push(@errors, "field $fikey not defined by format");
		next;
	    }

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

=item B<-biburl=urlformat>

Output HTML, with urlformat string as the href link to the biblio item.
Use %s as the biblio id number placeholder in the urlformat string.

=item B<-xml=filename>

Set the XML file where MARC21 format rules are read from. Default depends on B<-auth> or
B<-bibs> option: data/aukt.xml or data/bibs.xml, respectively.

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
