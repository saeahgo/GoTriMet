#!/bin/bash
set -e

# Load .env variables
export $(grep -v '^#' ~/.env | xargs)

DBHOST="$DB_HOST"
DBNAME="$DB_NAME"
DBUSER="$DB_USER"
export PGPASSWORD="$DB_PASSWORD"  # psql work without prompting

OUTDIR="data"
mkdir -p "$OUTDIR"

for i in {1..7}; do
    SQLFILE="queries/query${i}.sql"
    TSVFILE="${OUTDIR}/data${i}.tsv"
    GEOJSONFILE="${OUTDIR}/data${i}.geojson"
    if [ ! -f "$SQLFILE" ]; then
        echo "[WARN] Skipping query${i}.sql — file not found."
        continue
    fi
    echo "[INFO] Exporting data${i}.tsv from $SQLFILE..."
    psql -h "$DBHOST" -d "$DBNAME" -U "$DBUSER" -f "$SQLFILE" -A -F $'\t' -t > "$TSVFILE"

    #echo "[INFO] Removing header from data${i}.tsv..."
    #sed -i '1d' "$TSVFILE"

    echo "[INFO] Converting to GeoJSON..."
    python3 tsvscript.py "$TSVFILE" "$GEOJSONFILE"
done

echo "[INFO] Done building all geojson files."
