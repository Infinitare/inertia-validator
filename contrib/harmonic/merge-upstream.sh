#!/usr/bin/env bash

# merge-upstream.sh pulls upstream (which still ships Frankendancer) into
# this Firedancer-only fork and re-applies the strip.
#
#   contrib/harmonic/merge-upstream.sh v26.09-harmonic
#   UPSTREAM_REMOTE=origin contrib/harmonic/merge-upstream.sh main
#
# What it does:
#
#   1. git fetch <remote>
#   2. git merge --no-commit <remote>/<branch>
#   3. resolve "deleted by us" conflicts under the Frankendancer paths by
#      keeping them deleted -- these are files upstream changed and we do
#      not carry
#   4. re-run strip-frankendancer.sh, which deletes any *new* Frankendancer
#      files the merge brought in (no conflict is raised for those, so
#      this step is the one that keeps the tree honest)
#   5. leave anything else for you to resolve, then commit yourself
#
# The merge is deliberately left uncommitted.  Read the diff before
# committing -- step 3 discards upstream changes by design, and you want
# to see what you dropped.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

REMOTE="${UPSTREAM_REMOTE:-upstream}"
BRANCH="${1:-}"

if [[ -z $BRANCH ]]; then
  echo "usage: $0 <upstream-branch>" >&2
  echo "  e.g. $0 v26.09-harmonic" >&2
  echo "  set UPSTREAM_REMOTE to use a remote other than '$REMOTE'" >&2
  exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is dirty; commit or stash first" >&2
  exit 1
fi

PATHS_FILE=contrib/harmonic/frankendancer-paths.txt
FRANKEN_PATHS=()
while IFS= read -r line; do
  line="${line%%#*}"
  line="$(echo "$line" | tr -d '[:space:]')"
  [[ -n $line ]] && FRANKEN_PATHS+=("$line")
done < "$PATHS_FILE"

echo "==> fetching $REMOTE"
git fetch "$REMOTE"

echo "==> merging $REMOTE/$BRANCH"
git merge --no-commit --no-ff "$REMOTE/$BRANCH" || true

# --- resolve delete/modify conflicts under Frankendancer paths --------

resolved=0
while IFS= read -r f; do
  [[ -n $f ]] || continue
  for p in "${FRANKEN_PATHS[@]}"; do
    if [[ $f == "$p" || $f == "$p"/* ]]; then
      git rm -q -f --ignore-unmatch -- "$f" >/dev/null 2>&1 || rm -f -- "$f"
      echo "  kept deleted: $f"
      resolved=$((resolved+1))
      break
    fi
  done
done < <(git diff --name-only --diff-filter=U 2>/dev/null || true)

[[ $resolved -gt 0 ]] && echo "==> auto-resolved $resolved Frankendancer conflict(s)"

# --- strip anything new the merge brought in -------------------------

echo "==> re-running strip"
set +e
contrib/harmonic/strip-frankendancer.sh
strip_status=$?
set -e

# --- report -----------------------------------------------------------

echo
remaining="$(git diff --name-only --diff-filter=U 2>/dev/null || true)"
if [[ -n $remaining ]]; then
  echo "Conflicts left for you to resolve:"
  echo "$remaining" | sed 's/^/  /'
  echo
  echo "Resolve them, 'git add' the results, then 'git commit'."
  exit 1
fi

if [[ $strip_status != 0 ]]; then
  echo "The strip found references needing review (see above)."
  echo "Fix them, then 'git commit'."
  exit 1
fi

echo "Merge is staged and clean.  Review with:"
echo "  git diff --cached --stat"
echo "then commit."
