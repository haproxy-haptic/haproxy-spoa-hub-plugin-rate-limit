#!/bin/sh
# Cosign keyless-sign every release artifact under ./artifacts, writing
# <file>.cosign.bundle next to it. cosign picks up the CI OIDC token itself.
# Skips *.sha256, SHA256SUMS, and existing *.cosign.bundle files.

set -eu

cd artifacts

for f in *; do
    [ -f "$f" ] || continue
    case "$f" in
        *.sha256 | *.cosign.bundle | SHA256SUMS) continue ;;
    esac
    cosign sign-blob --yes "$f" --bundle "$f.cosign.bundle"
done
