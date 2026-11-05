#!/usr/bin/env bash

# strip-frankendancer.sh removes Frankendancer-only code from this
# Firedancer-only fork.
#
# The script is idempotent: on a tree that is already clean it does
# nothing and exits 0.  Run it after every upstream merge -- upstream
# still ships Frankendancer, so a merge will reintroduce those files.
# merge-upstream.sh does this for you.
#
#   contrib/harmonic/strip-frankendancer.sh            strip the tree
#   contrib/harmonic/strip-frankendancer.sh --check    report only, no writes
#
# Exits 0 if the tree is (now) clean, 1 if something needs a human.
#
# The path list lives in frankendancer-paths.txt.  The scan below is a
# backstop for Frankendancer code that arrives *outside* those paths --
# an upstream commit wiring an agave build rule into a shared Local.mk,
# say.  It reports rather than edits, because a blind rewrite of shared
# files goes wrong in ways that are hard to notice.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

CHECK_ONLY=0
case "${1:-}" in
  --check) CHECK_ONLY=1 ;;
  "")      ;;
  *)       echo "usage: $0 [--check]" >&2; exit 2 ;;
esac

PATHS_FILE=contrib/harmonic/frankendancer-paths.txt
if [[ ! -f $PATHS_FILE ]]; then
  echo "error: $PATHS_FILE not found" >&2
  exit 2
fi

# Read the path list, dropping comments and blanks.
FRANKEN_PATHS=()
while IFS= read -r line; do
  line="${line%%#*}"
  line="$(echo "$line" | tr -d '[:space:]')"
  [[ -n $line ]] && FRANKEN_PATHS+=("$line")
done < "$PATHS_FILE"

status=0
removed=0

# --- 1. Frankendancer-only paths -------------------------------------

for p in "${FRANKEN_PATHS[@]}"; do
  tracked=""
  if git rev-parse --git-dir >/dev/null 2>&1; then
    tracked="$(git ls-files -- "$p" | head -n1)"
  fi
  [[ -e $p || -n $tracked ]] || continue

  if [[ $CHECK_ONLY == 1 ]]; then
    echo "would remove: $p"
    status=1
    continue
  fi

  echo "removing: $p"
  if [[ -n $tracked ]]; then
    # -r for directories, --ignore-unmatch so a worktree-only leftover
    # (an uninitialised submodule, a stale build dir) is not an error.
    git rm -r -q -f --ignore-unmatch -- "$p" || true
  fi
  rm -rf -- "$p"
  removed=$((removed+1))
done

# Drop the agave submodule's leftover git metadata and config, if any.
if [[ $CHECK_ONLY == 0 ]]; then
  rm -rf .git/modules/agave 2>/dev/null || true
  git config --remove-section submodule.agave 2>/dev/null || true
fi

# --- 2. Backstop scan ------------------------------------------------

# Markers that should never appear in a Firedancer-only tree.  Kept
# deliberately narrow: fdctl_shared, fdctl_platform, fdctl_cfg_get_* and
# fdctl_default_config are shared code that Firedancer genuinely uses,
# and must not match here.
# NB: git grep's default engine has no \b, so these are plain
# substrings.  Each was checked not to match a shared name:
# "fdctl_shared" and "fdctl_platform" contain no "fd_fdctl".
MARKERS=(
  'app/fdctl'
  'app/fddev'
  'discoh/'
  'fd_discoh'
  'fd_fdctl'
  'fd_fddev'
  'agave_validator'
  'agave/Cargo.toml'
  'agave/target'
  '-p agave-validator'
)

# Paths excluded from the scan.  Three reasons appear here:
#
#   - contrib/harmonic names the markers on purpose;
#   - contrib/agave-cluster and contrib/test/setup_fd_cluster*.sh drive
#     an *external* Agave as a localnet peer, which Firedancer supports;
#   - config/everything.mk still defines the generic make-bin-rust
#     helper and RMDIRs agave/target on clean.  Both are inert with no
#     Rust targets left, and everything.mk changes often upstream, so
#     they are left alone to keep merges quiet.
#   - book/snippets holds captured terminal output from `fddev bench`.
#     It is historical console output, not code; re-capture it with
#     `firedancer-dev bench` rather than editing it by hand.
scan_hits=0
for m in "${MARKERS[@]}"; do
  hits="$(git grep -n -I -F -- "$m" -- \
            ':!contrib/harmonic' \
            ':!contrib/agave-cluster' \
            ':!contrib/test/setup_fd_cluster.sh' \
            ':!contrib/test/setup_fd_cluster_stakes.sh' \
            ':!config/everything.mk' \
            ':!book/snippets' \
            ':!contrib/codeql' \
            2>/dev/null || true)"
  if [[ -n $hits ]]; then
    if [[ $scan_hits == 0 ]]; then
      echo
      echo "Frankendancer references found outside the stripped paths:"
      echo "(review by hand -- an upstream merge may have wired new"
      echo " Frankendancer code into a shared file)"
      echo
    fi
    echo "$hits"
    scan_hits=1
  fi
done
[[ $scan_hits == 1 ]] && status=1

# --- 3. Report -------------------------------------------------------

echo
if [[ $CHECK_ONLY == 1 ]]; then
  [[ $status == 0 ]] && echo "tree is clean: no Frankendancer code found"
else
  if [[ $removed == 0 && $scan_hits == 0 ]]; then
    echo "tree is clean: nothing to strip"
  else
    echo "stripped $removed path(s)"
    [[ $scan_hits == 1 ]] && echo "review the references above, then commit"
  fi
fi

exit $status
