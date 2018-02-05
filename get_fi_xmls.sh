#!/bin/bash

mkdir -p data


BIBDIR="bib"
BIBFILES="000 001-006 007 008 01X-04X 05X-08X 1XX 20X-24X 250-270 3XX 4XX 50X-53X 53X-58X 6XX 70X-75X 76X-78X 80X-830 841-88X 9XX"
AUKDIR="aukt"
AUKFILES="000 00X 01X-09X 1XX 2XX-3XX 4XX 5XX 64X 663-666 667-68X 7XX 8XX"

function getfiles() {
 SDIR=$1
 shift
 FILES=$@
 for x in $FILES; do
    FNAME="data/$SDIR-${x}.xml"
    if [ ! -e "$FNAME" ]; then
	wget "http://www.kansalliskirjasto.fi/extra/marc21/$SDIR/${x}.xml" -O "$FNAME"
    fi
 done
}

getfiles $BIBDIR $BIBFILES
getfiles $AUKDIR $AUKFILES


xsltproc bib.xslt data/$BIBDIR-000.xml > data/bibs.xml
xsltproc aukt.xslt data/$AUKDIR-000.xml > data/aukt.xml

# Evergreen uses marcedit-tooltips.xml
cp data/bibs.xml marcedit-tooltips.xml

# Create SQL to update Koha marc and auth field descriptions to finnish
perl koha-marcfields_to_db.pl > koha_db_fi.sql
perl koha-authfields_to_db.pl >> koha_db_fi.sql

