use strict;
use warnings;
#use diagnostics;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use Encode;
use Unicode::Normalize;
use DBI;
use Data::Dumper;
use XML::TreePP;


use constant DB_HOSTNAME => 'localhost';
use constant DB_USERNAME => 'kohaadmin';
use constant DB_PASSWORD => 'katikoan';
use constant DB_DBNAME   => 'koha';
use constant DB_DRIVER   => 'mysql';

use constant MARC_XML => './marcedit-tooltips.xml';
#use constant DB_QUERY => 'select id, marc from biblio.record_entry order by id asc';
use constant DB_QUERY => 'select biblionumber as id, marcxml as marc from biblioitems order by id asc';

my %not_repeatable;



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
	    my @dsfar;
	    $not_repeatable{$dat{'-tag'}} = 1 if ($dat{'-repeatable'} eq 'false');
#	    print $dat{'-tag'}."\n" if ($dat{'-repeatable'} eq 'false');
	    if (defined($dsf)) {
#		print "XXX: ".ref($dat{'subfield'})."\n";
		if (ref($dat{'subfield'}) eq 'ARRAY') {
		    @dsfar = @{$dsf};
		} else {
		    @dsfar = $dsf;
		}
		foreach my $subfield (@dsfar) {
		    my %datsf = %{$subfield};
		    $not_repeatable{$dat{'-tag'}.$datsf{'-code'}} = 1 if ($datsf{'-repeatable'} eq 'false');
#		    print $dat{'-tag'}.$datsf{'-code'}."\n" if ($datsf{'-repeatable'} eq 'false');
		}
	    }
	}
    }
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

    foreach my $f ($record->field('...')) {
	my $fi = $f->{'_tag'};
	next if ((scalar($fi) < 10) || (lc($fi) eq 'ldr'));

	print "$id ($fi)\n" if (defined($mainf{$fi}) && defined($not_repeatable{$fi}));
	$mainf{$fi} = 1;

	my @subf = @{$f->{'_subfields'}};
	my %subff;

	while ($#subf > 0) {
	    my $key = shift @subf;
	    my $val = shift @subf;
	    my $fikey = $fi.$key;
	    print "$id ($fikey)\n" if (defined($subff{$fikey}) && defined($not_repeatable{$fikey}));
	    $subff{$fikey} = 1;
	}

    }
}

sub db_connect {
    my $dbh = DBI->connect("DBI:" . DB_DRIVER . ":dbname=" . DB_DBNAME . ";host=" . DB_HOSTNAME, DB_USERNAME, DB_PASSWORD, {'RaiseError' => 1});
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
