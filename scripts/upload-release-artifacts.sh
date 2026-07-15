#!/bin/sh
# Upload every file under ./artifacts to the GitLab Generic Package Registry.
#
# Inputs (env):
#   PKG_REGISTRY_URL  — base URL ending at .../packages/generic/<pkg>/<version>
#   CI_JOB_TOKEN      — provided by GitLab CI

set -eu

: "${PKG_REGISTRY_URL:?PKG_REGISTRY_URL not set}"
: "${CI_JOB_TOKEN:?CI_JOB_TOKEN not set}"

cd artifacts

for f in *; do
    [ -f "$f" ] || continue
    echo "Uploading $f to $PKG_REGISTRY_URL/$f"
    curl --fail --silent --show-error \
        --header "JOB-TOKEN: $CI_JOB_TOKEN" \
        --upload-file "$f" \
        "$PKG_REGISTRY_URL/$f"
done
