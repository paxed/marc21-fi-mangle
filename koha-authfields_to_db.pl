#!/usr/bin/perl

use strict;
use warnings;
use diagnostics;

use XML::TreePP;

use Data::Dumper;


my $tpp = XML::TreePP->new();
$tpp->set(force_array => ['subfield']);
my $tree = $tpp->parsefile("data/aukt.xml");

my @fields = @{$tree->{fields}->{field}};


print <<HEADERI;
UPDATE auth_tag_structure SET liblibrarian='Nimiö', libopac='Nimiö' WHERE tagfield='000';
UPDATE auth_subfield_structure SET liblibrarian='kiinteämittainen kontrollikenttä', libopac='kiinteämittainen kontrollikenttä' WHERE tagfield IN ('000', '008') AND tagsubfield='\@';
UPDATE auth_subfield_structure SET liblibrarian='kontrollikenttä', libopac='kontrollikenttä' WHERE tagfield IN ('001', '003', '005') AND tagsubfield='\@';

HEADERI

foreach my $nod (@fields) {

    my $f = $nod->{-tag};
    my $n = $nod->{name};

    $n =~ s/'/\'/g;

    print "UPDATE auth_tag_structure SET liblibrarian='".$n."', libopac='".$n."' WHERE tagfield='".$f."';\n" if ($n);

    foreach my $sf (@{$nod->{subfield}}) {
	my $c = $sf->{-code};
	my $d = $sf->{description};

	$d =~ s/'/\'/g;

	print "UPDATE auth_subfield_structure SET liblibrarian='".$d."', libopac='".$d."' WHERE tagfield='".$f."' AND tagsubfield='".$c."';\n" if ($d);

    }

}

