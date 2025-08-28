#!/bin/sh

set -e

REPO=$1
PULL_REQUEST=$2
TARGET_BRANCH=$3
BRANCH=$4

if [ -z "$REPO" ] || [ -z "$PULL_REQUEST" ] || [ -z "$TARGET_BRANCH" ] || [ -z "$BRANCH" ]; then
    echo "Usage: $0 <owner/repo> <PR number> <target branch> <branch name to create>" 1>&2
    exit 1
fi

GITHUB_TRIGGERING_ACTOR=${GITHUB_TRIGGERING_ACTOR:-}
DRAFT=${DRAFT:-false}

repo_owner=$(echo "$REPO" | cut -d/ -f1)
repo_name=$(echo "$REPO" | cut -d/ -f2)

pr_link=https://github.com/$REPO/pull/$PULL_REQUEST
pr_number=$PULL_REQUEST

# Separate calls because otherwise it was creating trouble.. can probably be fixed
old_title=$(gh pr view "$pr_number" --json title --jq '.title')
old_body=$(gh pr view "$pr_number" --json body --jq '.body')

git checkout -b "$branch_name" "$TARGET_BRANCH"

committed_something=""

for commit in $(gh pr view "$pr_number" --json commits --jq '.commits[].oid'); do
    if git cherry-pick --allow-empty "$commit"; then
        committed_something="true"
    else
        echo "Cherry-pick failed, skipping" 1>&2
        git cherry-pick --abort
    fi
done

if [ -z "$committed_something" ]; then
    # Github won't allow us to create a PR without any changes so we're making an empty commit here
    git commit --allow-empty -m "Please amend this commit"
fi

git push origin "$branch_name"

generated_by=""
if [ -n "$GITHUB_TRIGGERING_ACTOR" ]; then
    generated_by=$(cat <<EOF

This PR was automatically created via [GHA workflow]($GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID) initiated by @${GITHUB_TRIGGERING_ACTOR}.

EOF
)
fi

title=$(echo "[$TARGET_BRANCH] $old_title")
body=$(cat <<EOF
**Backport**

Backport of $pr_link

You can make changes to this PR with the following command:

\`\`\`
git clone https://github.com/$REPO
cd $repo_name
git switch $branch_name
\`\`\`
$generated_by
---

$old_body
EOF
)

opts=""
if [ "$DRAFT" = "true" ]; then
    opts="$opts --draft"
fi

gh pr create \
    ${opts}
    --title "$title" \
    --body "$body" \
    --repo "$REPO" \
    --head "$repo_owner:$branch_name" \
    --base "$TARGET_BRANCH"
