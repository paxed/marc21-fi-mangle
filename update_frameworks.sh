#!/bin/bash

test -z "$KOHA_CONF" && echo "No KOHA_CONF." && exit 1
getconf() { xmllint --xpath "yazgfs/config/$1/text()" $KOHA_CONF 2> /dev/null; }
DBNAME="$(getconf database)"

OUTPATH=/var/koha/
OUTFILE=${DBNAME}_framework_updates.txt

PVM=$(date +%Y%m%d)
FILEA=/tmp/upsert_${DBNAME}_default.log
FILEB=/tmp/upsert_${DBNAME}_kaikki.log
FILEC=/tmp/${OUTFILE}
FILEE=/tmp/upsert_${DBNAME}_errors.log

if [ ! -e "$FILEC" ]; then
    # default framework
    eval perl upsert_marc_fields.pl --flags=intranet,opac,editor --insert --update > "$FILEA" 2> "$FILEE"

    # all other frameworks
    eval perl upsert_marc_fields.pl --framework='*-' --flags=intranet,opac --insert --update > "$FILEB" 2>> "$FILEE"

    cat "$FILEE" > "$FILEC"
    cat "$FILEA" >> "$FILEC"
    sort "$FILEB" >> "$FILEC"
fi

RIVIT=$(wc -l "$FILEC" | cut -d' ' -f 1)

if test $RIVIT -gt 0
then
    OFNAME="${OUTPATH}${OUTFILE}"
    echo "Updated: $PVM" > "${OFNAME}"
    cat "$FILEC" >> "${OFNAME}"
fi
