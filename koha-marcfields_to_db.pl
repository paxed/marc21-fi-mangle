#!/usr/bin/perl

use strict;
use warnings;
use diagnostics;

use XML::TreePP;

use Data::Dumper;


my $tpp = XML::TreePP->new();
$tpp->set(force_array => ['subfield']);
my $tree = $tpp->parsefile("data/bibs.xml");

my @fields = @{$tree->{fields}->{field}};


my $koha_field='952';
my %koha_subfields = (
    '0' => 'Pois kierrosta -tila',
    '1' => 'Kadonnut -tila',
    '2' => 'Luokitus',
    '3' => 'Liitteiden määrä',
    '4' => 'Vaurioitunut -tila',
    '5' => 'Käyttörajoitukset',
    '6' => 'Kohan normalisoitu luokitus',
    '7' => 'Ei lainattavissa',
    '8' => 'Kokoelmakoodi',
    '9' => 'Niteen numero',
    'a' => 'Omistajakirjasto',
    'b' => 'Sijaintikirjasto',
    'c' => 'Hyllypaikka',
    'd' => 'Hankintapvm',
    'e' => 'Hankintapaikka',
    'f' => 'Koodattu paikkamääre',
    'g' => 'Hankintahinta',
    'h' => 'Sarjanumero',
    'i' => 'Inventaarionumero',
    'j' => 'Hyllytyksen kontrollinumero',
#    'k' => '',
    'l' => 'Lainauksia yhteensä',
    'm' => 'Uusintoja yhteensä',
    'n' => 'Varauksia yhteensä',
    'o' => 'Kohan koko signum',
    'p' => 'Viivakoodi',
    'q' => 'Lainassa',
    'r' => 'Viimeeksi nähty pvm',
    's' => 'Viimeeksi lainattu pvm',
    't' => 'Nidenumero',
    'u' => 'URI',
    'v' => 'Korvaushinta',
    'w' => 'Hinta voimassa alkaen',
    'x' => 'Huomautus virkailijoille',
    'y' => 'Kohan aineistolaji',
    'z' => 'Yleinen huomautus'
);

print <<HEADERI;
UPDATE marc_tag_structure SET liblibrarian='Nimiö', libopac='Nimiö' WHERE tagfield='000';
UPDATE marc_subfield_structure SET liblibrarian='kiinteämittainen kontrollikenttä', libopac='kiinteämittainen kontrollikenttä' WHERE tagfield IN ('000', '006', '007', '008', '009') AND tagsubfield='\@';
UPDATE marc_subfield_structure SET liblibrarian='kontrollikenttä', libopac='kontrollikenttä' WHERE tagfield IN ('001', '003', '005') AND tagsubfield='\@';

HEADERI

foreach my $c (keys(%koha_subfields)) {
    my $d = $koha_subfields{$c};
    my $f = $koha_field;
    print "UPDATE marc_subfield_structure SET liblibrarian='".$d."', libopac='".$d."' WHERE tagfield='".$f."' AND tagsubfield='".$c."';\n" if ($d);
}


foreach my $nod (@fields) {

    my $f = $nod->{-tag};
    my $n = $nod->{name};

    print "UPDATE marc_tag_structure SET liblibrarian='".$n."', libopac='".$n."' WHERE tagfield='".$f."';\n" if ($n);

    foreach my $sf (@{$nod->{subfield}}) {
	my $c = $sf->{-code};
	my $d = $sf->{description};

	print "UPDATE marc_subfield_structure SET liblibrarian='".$d."', libopac='".$d."' WHERE tagfield='".$f."' AND tagsubfield='".$c."';\n" if ($d);

    }

}

