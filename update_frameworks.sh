#!/bin/bash

#10 03 * * *        cd /home/koha/marc21-fi-mangle && ./luo_virhelista.sh
#02 03 1 * *        cd /home/koha/marc21-fi-mangle && ./get_fi_xmls.sh && ./update_frameworks.sh

TUNNUS=VAARA
MAILI_TO=myemail@example.com
MAILI_FROM=fromemail@example.com
MAILI_SUBJ="Luettelointipohjien muutokset"
MAILI_TEXT=""
DBPARAMS="-db username=root -db password='' -db dbname=koha -db hostname=127.0.0.1"


PVM=$(date +%Y%m%d)
FILEA=/tmp/upsert_${TUNNUS}_default.log
FILEB=/tmp/upsert_${TUNNUS}_kaikki.log
FILEC=/tmp/${TUNNUS}_marc21_framework_updates_${PVM}.log
FILEE=/tmp/upsert_${TUNNUS}_errors.log

if [ ! -e "$FILEC" ]; then
    # default framework
    eval perl upsert_marc_fields.pl $DBPARAMS --flags=intranet,opac,editor --insert --update > "$FILEA" 2> "$FILEE"

    # all other frameworks
    eval perl upsert_marc_fields.pl $DBPARAMS --framework='*-' --flags=intranet,opac --insert --update > "$FILEB" 2>> "$FILEE"

    cat "$FILEE" > "$FILEC"
    cat "$FILEA" >> "$FILEC"
    sort "$FILEB" >> "$FILEC"
fi

RIVIT=$(wc -l "$FILEC" | cut -d' ' -f 1)

if test $RIVIT -gt 0
then
    echo "${MAILI_TEXT}" | mail -s "${MAILI_SUBJ}" -a "$FILEC" -r "${MAILI_FROM}" "${MAILI_TO}"
fi
