#!/usr/bin/env bash
# Move main to origin/main, rebuild, pre-download PEER's current snapshot pair into SNAPDIR,
# then restart Firedancer and follow the log.  Everything but the restart
# runs while the validator is up, and any failure aborts before it.
# Set PULL=0, BUILD=0 or SNAPSHOT=0 to skip a step.
set -euo pipefail

PEER=${PEER:-http://162.19.112.132:8899}
SNAPDIR=${SNAPDIR:-/mnt/ledger/snapshots}
LOG=${LOG:-/home/sol/logs/firedancer.log}
RESTART_CMD=${RESTART_CMD:-sudo systemctl restart firedancer}

# filename PEER currently serves at $1 (snapshot.tar.bz2 or incremental-snapshot.tar.bz2)
current() {
  local loc
  loc=$(curl -sI --max-time 10 "$PEER/$1" | awk 'tolower($1)=="location:"{print $2}' | tr -d '\r')
  [[ -n $loc ]] && basename "$loc"
}

# download $1 into SNAPDIR unless it is already there
fetch() {
  [[ -e $SNAPDIR/$1 ]] && return
  echo "downloading $1"
  curl -L --fail -o "$SNAPDIR/.$1.part" "$PEER/$1"
  mv "$SNAPDIR/.$1.part" "$SNAPDIR/$1"
}

main() {
  local repo
  repo=$(dirname "$0")
  if [[ ${PULL:-1} == 1 ]]; then
    git -C "$repo" diff --quiet HEAD || { echo "local changes in $repo, commit or stash them" >&2; exit 1; }
    git -C "$repo" fetch origin
    git -C "$repo" checkout -B main origin/main # releases rewrite main, a pull cannot fast-forward
  fi
  if [[ ${BUILD:-1} == 1 ]]; then nice -n 19 make -C "$repo" -j firedancer; fi

  if [[ ${SNAPSHOT:-1} == 1 ]]; then
    rm -f "$SNAPDIR"/.*.part
    full=$(current snapshot.tar.bz2)
    fetch "$full"
    incr=$(current incremental-snapshot.tar.bz2)
    [[ $incr == incremental-snapshot-$(cut -d- -f2 <<<"$full")-* ]] || { echo "peer rotated its full snapshot, rerun" >&2; exit 1; }
    fetch "$incr"
    [[ $(current snapshot.tar.bz2) == "$full" ]] || { echo "peer rotated its full snapshot, rerun" >&2; exit 1; }
  fi

  echo "restarting: $RESTART_CMD"
  $RESTART_CMD
  exec tail -n 50 -F "$LOG"
}

main "$@"