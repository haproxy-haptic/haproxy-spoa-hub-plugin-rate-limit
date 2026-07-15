#!/usr/bin/env bash
# Verify a published release end-to-end: download SHA256SUMS, every artifact
# named in it, and every artifact's cosign bundle; check sha256 sums; verify
# cosign signatures against this project's group identity and tag ref.
#
# Usage: scripts/verify-release.sh <vX.Y.Z>
#
# Requires: curl, sha256sum, cosign (>= 2.x).

set -euo pipefail

VERSION="${1:?usage: $0 <vX.Y.Z>}"

PROJECT_PATH="haproxy-haptic/haproxy-spoa-hub-plugin-rate-limit"
PKG_NAME="rate-limit"
PKG_BASE="https://gitlab.com/api/v4/projects/$(printf '%s' "${PROJECT_PATH}" | sed 's|/|%2F|g')/packages/generic/${PKG_NAME}/${VERSION}"

ISSUER="https://gitlab.com"
IDENTITY_REGEX="^https://gitlab\\.com/${PROJECT_PATH}//\\.gitlab-ci\\.yml@refs/tags/.*\$"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT
cd "${WORKDIR}"

echo "Downloading SHA256SUMS for ${VERSION}..."
curl --fail --silent --location --output SHA256SUMS "${PKG_BASE}/SHA256SUMS"

# Each line of SHA256SUMS is "<hash>  <filename>"
mapfile -t names < <(awk '{print $2}' SHA256SUMS)

echo "Downloading ${#names[@]} artifacts and signature bundles..."
for name in "${names[@]}"; do
    curl --fail --silent --location --output "${name}" "${PKG_BASE}/${name}"
    curl --fail --silent --location --output "${name}.cosign.bundle" \
        "${PKG_BASE}/${name}.cosign.bundle"
done

echo "Checking SHA256 sums..."
sha256sum --check --quiet SHA256SUMS

echo "Verifying cosign signatures..."
for name in "${names[@]}"; do
    cosign verify-blob "${name}" \
        --bundle "${name}.cosign.bundle" \
        --certificate-identity-regexp "${IDENTITY_REGEX}" \
        --certificate-oidc-issuer "${ISSUER}" \
        > /dev/null
done

echo "OK - ${#names[@]} artifacts verified for ${VERSION}"
