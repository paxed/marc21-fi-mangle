use strict;
use warnings;

# Copyright (c) 2022 Pasi Kallinen
# Licensed under the MIT License, the "Expat" variant.

#
# Very quick-n-dirty script to add 942$b to Koha MARC frameworks
#




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

my $help = 0;
my $man = 0;
my $debug = 0;
my $insert = 1;
my $update = 0;
my $frameworkcode = '*';
my $hidden_value = 4; # editor,intranet
my $set_existing_hidden = 0;
my $dryrun = 0;
my $field = '942';
my $subfield = 'b';
my $field_desc = 'Estä valuminen';
my $auth_value = 'YES_NO';


my $db_filename = "/etc/koha/koha-conf.xml";

%dbdata = %{_read_db_settings_xml($db_filename, \%dbdata)} if (-f $db_filename && -r $db_filename);

my %tags = ( );

GetOptions(
    'db=s%' => sub { my $onam = $_[1]; my $oval = $_[2]; if (exists($dbdata{$onam})) { $dbdata{$onam} = $oval; } else { warn "Unknown db setting '".$onam."'."; } },
    'help|h|?' => \$help,
    'man' => \$man,
    'dryrun|dry-run' => \$dryrun,
    'debug' => \$debug,
    'insert' => \$insert,
    'update' => \$update,
    'setflags' => \$set_existing_hidden,
    'framework=s' => \$frameworkcode,
    'flags=s' => \&handle_koha_flags,
    'desc=s' => \$field_desc,
    'authvalue=s' => \$auth_value,
    ) or pod2usage(2);

pod2usage(1) if ($help);
pod2usage(-exitval => 0, -verbose => 2) if $man;

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

#################################################################
#################################################################

sub _read_db_settings_xml {
    my ($fname, $dbdata) = @_;
    my %data = %{$dbdata};

    my %matchxml = (
        '//config/db_scheme' => 'driver',
        '//config/database' => 'dbname',
        '//config/hostname' => 'hostname',
        '//config/user' => 'username',
        '//config/pass' => 'password',
        # TODO: port, socket
        );

    my $dom = XML::LibXML->load_xml(location => $fname);

    foreach my $k (keys(%matchxml)) {
        my $v = $matchxml{$k};
        $data{$v} = $dom->findvalue($k) || $data{$v};
    }

    return \%data;
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

sub db_query_frameworks {
    my $sth;
    my $dbh = db_connect();

    my $sql = 'select * from marc_subfield_structure';

    $sth = $dbh->prepare($sql);
    $sth->execute();

    while (my $ref = $sth->fetchrow_hashref()) {
	my $fwc = $ref->{'frameworkcode'} || '';
        if (exists($ref->{'tagsubfield'})) {
            $tags{$fwc}{$ref->{'tagfield'}}{$ref->{'tagsubfield'}} = $ref;
        } else {
            $tags{$fwc}{$ref->{'tagfield'}} = $ref;
        }
    }
    db_disconnect($dbh);
}

sub mk_sql_insert {
    my ($dbh, $fwc, $tag, $subfield, $fname, $authval) = @_;
    my $tablename = 'marc_subfield_structure';
    if ($insert || $debug) {
	my %datas = (
	    'tagfield' => $tag,
	    'liblibrarian' => $fname,
	    'libopac' => $fname,
#	    'repeatable' => $field_data{$ftag}{'repeatable'},
	    'frameworkcode' => $fwc,
            'authorised_value' => $authval,
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
            #print $sql."\n";
	}
    }
}

db_query_frameworks();

my @frameworks = sort(keys(%tags));
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
    # nothing, use all frameworks
}


my $dbh = db_connect();
foreach my $framework (@frameworks) {
    if (!defined($tags{$framework}{$field}{$subfield})) {
        mk_sql_insert($dbh, $framework, $field, $subfield, $field_desc, $auth_value);
    } else {
        print "$field$subfield exists in framework \"$framework\"\n";
    }
}
db_disconnect($dbh);
print "Added $field$subfield to all MARC frameworks.\n";
