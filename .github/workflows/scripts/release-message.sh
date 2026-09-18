#!/bin/sh
#
# Automatically generates a message for a new release of steve with some useful
# links and embedded release notes.
#
# Usage:
#   ./release-message.sh <prev steve release> <new steve release>
#
# Example:
# ./release-message.sh "v0.5.2" "v0.5.3"

PREV_STEVE_VERSION=$1   # e.g. v0.5.2
NEW_STEVE_VERSION=$2    # e.g. v0.5.3
GITHUB_TRIGGERING_ACTOR=${GITHUB_TRIGGERING_ACTOR:-}

usage() {
    cat <<EOF
Usage:
  $0 <prev steve release> <new steve release>
EOF
}

if [ -z "$PREV_STEVE_VERSION" ] || [ -z "$NEW_STEVE_VERSION" ]; then
    usage
    exit 1
fi

set -ue

# XXX: That's wasteful but doing it by caching the response in a var was giving
# me unicode error.
url=$(gh release view --repo rancher/steve --json url "${NEW_STEVE_VERSION}" --jq '.url')
body=$(gh release view --repo rancher/steve --json body "${NEW_STEVE_VERSION}" --jq '.body')

generated_by=""
if [ -n "$GITHUB_TRIGGERING_ACTOR" ]; then
    generated_by=$(cat <<EOF
# About this PR

The workflow was triggered by @$GITHUB_TRIGGERING_ACTOR.
EOF
)
fi

cat <<EOF
# Release note for [${NEW_STEVE_VERSION}]($url)

$body

# Useful links

- Commit comparison: https://github.com/rancher/steve/compare/${PREV_STEVE_VERSION}...${NEW_STEVE_VERSION}
- Release ${PREV_STEVE_VERSION}: https://github.com/rancher/steve/releases/tag/${PREV_STEVE_VERSION}

$generated_by
EOF
