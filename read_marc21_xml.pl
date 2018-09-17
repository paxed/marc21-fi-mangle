use strict;
use warnings;

use Data::Dumper;
$Data::Dumper::Sortkeys = 1;

use XML::Simple;

sub generate_tag_sequence {
    my ($tag) = @_;

    my @fields;

    if ($tag =~ /x/i && $tag =~ /^([0-9x])([0-9x])([0-9x])(.*)$/i) {
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
    my $repeatable = $field->{'repeatable'} || 'N';

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
                }

                if (defined($equals)) {
                    my $eq_tag = $equals->{'tag'};
                    my $eq_pos = $equals->{'positions'};
                    $regex_field{$eq_tag . $type}{$eq_pos} = [] if (!defined($regex_field{$eq_tag . $type}{$eq_pos}));
                    @{$regex_field{$eq_tag . $type}{$eq_pos}} = @{$regex_field{$tag . $type}{$pos}};
                }
            }
        }
    }

    $data->{'valid_fields'} = \%valid_fields;
    $data->{'not_repeatable'} = \%not_repeatable;
    $data->{'allow_indicators'} = \%allow_indicators;
    $data->{'typed'} = \%typed_field;
    $data->{'regex'} = \%regex_field;
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
                if ($srkeylen == 1) {
                    my $okay_to_regex = 1;
                    for (my $idx = 0; $idx < scalar(@{$dat}); $idx++) {
                        $okay_to_regex = 0 if (length(@{$dat}[$idx]) != 1);
                    }
                    if ($okay_to_regex) {
                        my $s = join('', @{$dat});
                        $re{$rekey}{$srkey} = qr/[$s]/;
                        next;
                    }
                }
                for (my $idx = 0; $idx < scalar(@{$dat}); $idx++) {
                    my $val = @{$dat}[$idx];
                    next if (ref($val) eq 'Regexp');
                    next if (length($val) == $srkeylen);
                    next if (length($val) == 1);

                    if ($val =~ /^\d-\d$/) {
                        @{$dat}[$idx] = qr/^[$val]+$/;
                        next;
                    }

                    # Ugh!
                    if ($val eq '001-999') {
                        @{$dat}[$idx] = qr/[0-9][0-9][1-9]/;
                        next;
                    }

                    print "Unhandled: $rekey/$srkey [value:\"".$val."\"]\n";
                }
            } else {
                #warn "marc21 format regex is not array"
            }
        }
    }
    return $data;
}


my %field_data = (
    'valid_fields' => {},
    'not_repeatable' => {},
    'allow_indicators' => {},
    'typed' => {},
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
    'fixed_length' => {
        '000' => 24,
	'005' => 16,
	'006' => 18,
	'008' => 40
    }
    );

my @xmlfiles = glob("data/bib-*.xml");
foreach my $file (@xmlfiles) {
    parse_xml_data($file, \%field_data);
}


$field_data{'regex'} = fix_regex_data($field_data{'regex'});

print Dumper(\%field_data);

#print ref($field_data{'regex'}{'000'}{'00'});

