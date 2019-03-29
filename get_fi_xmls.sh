#!/bin/bash

mkdir -p data


BIBDIR="bib"
BIBFILES="000 001-006 007 008 01X-04X 05X-08X 1XX 20X-24X 250-270 3XX 4XX 50X-53X 53X-58X 6XX 70X-75X 76X-78X 80X-830 841-88X 9XX"
AUKDIR="aukt"
AUKFILES="000 00X 01X-09X 1XX 2XX-3XX 4XX 5XX 64X 663-666 667-68X 7XX 8XX"
HLDDIR="hold"
HLDFILES="000 001-008 0XX 5XX-84X 852-856 853-855 863-865 866-868 876-878 88X"

function getfiles() {
 SDIR=$1
 shift
 FILES=$@
 for x in $FILES; do
    FNAME="data/$SDIR-${x}.xml"
    wget "http://marc21.kansalliskirjasto.fi/$SDIR/${x}.xml" -O "$FNAME"
 done
}

getfiles $BIBDIR $BIBFILES
getfiles $AUKDIR $AUKFILES
getfiles $HLDDIR $HLDFILES
