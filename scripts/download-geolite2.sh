#!/usr/bin/env bash
# Download GeoLite2-City database for geo enrichment
# Run once before starting enrichment-service

set -e

ACCOUNT_ID="${MAXMIND_ACCOUNT_ID}"
LICENSE_KEY="${MAXMIND_LICENSE_KEY}"

if [[ -z "$ACCOUNT_ID" ]] || [[ -z "$LICENSE_KEY" ]]; then
  echo "Error: MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY must be set"
  echo "Get free account at: https://www.maxmind.com/en/geolocation_landing"
  exit 1
fi

ARCHIVE_URL="https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=${LICENSE_KEY}&suffix=tar.gz"
DEST_DIR="$(dirname "$0")/../data"

mkdir -p "$DEST_DIR"
cd "$DEST_DIR"

curl -fsSL -o geolite2.tar.gz "$ARCHIVE_URL"
tar -xzf geolite2.tar.gz
mv GeoLite2-City_*/GeoLite2-City.mmdb "$DEST_DIR/geolite2-city.mmdb"
rm -rf GeoLite2-City_* geolite2.tar.gz

echo "GeoLite2-City.mmdb downloaded to $DEST_DIR"
