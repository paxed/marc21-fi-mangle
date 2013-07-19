#!/bin/sh

mkdir -p data

FILES="000 001-006 007 008 01X-04X 05X-08X 1XX 20X-24X 250-270 3XX 4XX 50X-53X 53X-58X 6XX 70X-75X 76X-78X 80X-830 841-88X 9XX"

for x in $FILES; do
    FNAME="data/${x}.xml"
    if [ ! -e "$FNAME" ]; then
	wget "http://www.kansalliskirjasto.fi/extra/marc21/bib/${x}.xml" -O "$FNAME"
    fi
done

xsltproc bib.xslt data/000.xml > marcedit-tooltips.xml
