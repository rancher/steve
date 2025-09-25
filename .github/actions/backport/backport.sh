#!/bin/sh

set -e

PULL_REQUEST=$1
TARGET_BRANCH=$2
REPO=$3

IS_DRAFT=${IS_DRAFT:-false}
if [ "$IS_DRAFT" = "true" ]; then
	DRAFT_FLAG="--draft"
fi

if [ -z "$PULL_REQUEST" ] || [ -z "$TARGET_BRANCH" ] || [ -z "$REPO" ]; then
	echo "Usage: $0 <PR number> <target branch> <owner/repo>" 1>&2
	exit 1
fi

GITHUB_TRIGGERING_ACTOR=${GITHUB_TRIGGERING_ACTOR:-}

repo_name=$(echo "$REPO" | cut -d/ -f2)
repo_owner=$(echo "$REPO" | cut -d/ -f1)

target_slug=$(echo "$TARGET_BRANCH" | sed "s|/|-|")
branch_name="backport-$PULL_REQUEST-$target_slug-$$"

pr_link=https://github.com/$REPO/pull/$PULL_REQUEST
pr_number=$PULL_REQUEST

# Separate calls because otherwise it was creating trouble.. can probably be fixed
old_title=$(gh pr view "$pr_number" --json title --jq '.title')
old_body=$(gh pr view "$pr_number" --json body --jq '.body')

git checkout -b "$branch_name" "origin/$TARGET_BRANCH"

committed_something=""

failed_commit=""
skipped_commits=""

for commit in $(GH_PAGER=  gh pr view "$pr_number" --json commits --jq '.commits[].oid'); do
	if [ -n "$failed_commit" ]; then
		skipped_commits="$skipped_commits
$commit"
		continue
	fi
	# Those commits might be orphaned so we attempt to fetch them
	git fetch origin "$commit:refs/remotes/origin/orphaned-commit"

	if git cherry-pick --allow-empty "$commit"; then
		committed_something="true"
	else
		echo "Cherry-pick failed, skipping" 1>&2
		git cherry-pick --abort
		failed_commit="$commit"
	fi
done

if [ -z "$committed_something" ]; then
	# Github won't allow us to create a PR without any changes so we're making an empty commit here
	git commit --allow-empty -m "Please amend this commit"
fi

git push -u origin "$branch_name"

generated_by=""
if [ -n "$GITHUB_TRIGGERING_ACTOR" ]; then
    generated_by=$(cat <<EOF
The workflow was triggered by @$GITHUB_TRIGGERING_ACTOR.
EOF
)
fi

failed=""
if [ -n "$failed_commit" ]; then
	failed=$(cat <<EOF
Cherry-picking failed, here are the list of commits that are missing:

\`\`\`
$failed_commit
$skipped_commits
\`\`\`
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

$failed

$generated_by

---

$old_body
EOF
)

PR_URL=$(gh pr create \
  --title "$title" \
  --body "$body" \
  --repo "$REPO" \
  --head "$(echo $REPO | cut -d/ -f1):$branch_name" \
  --base "$TARGET_BRANCH" \
  $DRAFT_FLAG)
echo "Created PR at $PR_URL"
echo "pr-url=$PR_URL" >> $GITHUB_OUTPUT
