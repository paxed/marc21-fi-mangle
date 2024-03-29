#!/bin/sh

test -z "$KOHA_CONF" && echo "No KOHA_CONF." && exit 1
getconf() { xmllint --xpath "yazgfs/config/$1/text()" $KOHA_CONF 2> /dev/null; }
DBNAME="$(getconf database)"
HOSTNAME="$(getconf hostname)"
DBPORT="$(getconf port)"
DBUSER="$(getconf user)"
DBPASS="$(getconf pass)"

OUTPATH=/var/koha

IGNORES=$(mysql -h $HOSTNAME -P $DBPORT -u $DBUSER -p$DBPASS --protocol=tcp $DBNAME -NBe "select plugin_value from plugin_data where plugin_class like '%::Nalkutin' and plugin_key = 'ignore_marc_fields';")

if [ "x$IGNORES" = "x" ]; then
  IGNORES="59x,790,8845,9xx,xxx9"
fi


perl marc_warnings.pl \
    -sql='select biblionumber as id, metadata as marc, ExtractValue(metadata, "//controlfield[@tag=003]") as f003, ExtractValue(metadata, "//datafield[@tag=040]/subfield[@code=\"a\"]") as f040a, ExtractValue(metadata, "//datafield[@tag=040]/subfield[@code=\"d\"]") as f040d, CONCAT(ExtractValue(metadata, "//controlfield[@tag=003]")," ",ExtractValue(metadata, "//datafield[@tag=040]/subfield[@code=\"a\"]")," ",ExtractValue(metadata, "//datafield[@tag=040]/subfield[@code=\"d\"]")," ",biblionumber) as urllink from biblio_metadata order by f003, f040a, f040d, biblionumber asc' \
    -biburl='/cgi-bin/koha/catalogue/MARCdetail.pl?biblionumber=%s' \
    -ignore="$IGNORES" -skip-enclevels=78 \
    > "$OUTPATH/marc_virheet.$DBNAME.html"

grep -a '<li>' "$OUTPATH/marc_virheet.$DBNAME.html" \
        | cut -d'(' -f 2 \
        | sed -e 's/, should be \[[^]]\+\]//g' \
        | sed -e 's/)$//g' \
        | sed -e 's/, /\n/g' -e 's/^ +//g' -e 's/ +$//g' \
        | cut -d'=' -f 1 \
        | sed -e 's/ illegal value "[^"]*"//g' \
        | sed -e 's/^[0-9]\+x//g' \
        | sed -e 's/^fields\? //g' \
        | sed -e 's/ not in format$//g' \
        | sed -e 's/,/\n/g' \
        | grep -v "should be" \
        | sort \
        | uniq -c \
        | sort -rn \
        > "$OUTPATH/marc_virhemaara.$DBNAME.txt"
