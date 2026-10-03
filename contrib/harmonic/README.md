# Keeping this Firedancer-only fork in sync with upstream

Upstream (`harmonic/samba`, itself a fork of `firedancer-io/firedancer`)
builds both Frankendancer and Firedancer from one tree.  This fork
carries only Firedancer.  That means every upstream merge tries to bring
Frankendancer back, and this directory holds the tooling that takes it
back out.

The design goal is that your own changes survive upstream merges.  Two
things make that work:

1. **Deletions, not rewrites.**  Frankendancer is removed by deleting
   whole paths.  Git merges deletions cleanly; it only stops to ask when
   upstream modified a file you deleted, and then the answer is always
   "stay deleted".  Rewriting shared files instead would conflict on
   every upstream touch.
2. **A short, reviewed edit list.**  A bounded set of shared files was
   edited (see below).  Those are the only places an upstream merge can
   raise a real content conflict caused by this fork.

## Moving to a new mainnet release

`main` is a firedancer-io mainnet tag plus **one** commit, "inertia:
harmonic patch + frankendancer strip + grpc streaming tile", which holds
everything this fork adds: harmonic's patch, the strip and our own
features.  Check which tags are mainnet-ready on
https://github.com/firedancer-io/firedancer/releases, then:

```console
$ git fetch firedancer --tags && git fetch upstream --tags
$ git checkout -b inertia/vNEW main
$ git rebase --onto vNEW vOLD           # replay the inertia commit
$ contrib/harmonic/strip-frankendancer.sh
```

Harmonic publishes a fresh patch per release (`vNEW-harmonic`).  Pull
their changes in by diffing `vOLD-harmonic..vNEW-harmonic` minus the
upstream delta (`git diff vNEW vNEW-harmonic` against `git diff vOLD
vOLD-harmonic`) and folding the result into the inertia commit with
`git commit --amend`.  If the gap is large, rebuilding on top of
`vNEW-harmonic` and squashing everything above `vNEW` is simpler -- that
is how the v26.09.5 commit was made.

After either route, recompute the `sizeof(fd_config_t)` assert in
`src/app/shared/fd_config_json.c`: the strip and the grpc config both
change the struct layout.

## Pulling upstream

```console
$ contrib/harmonic/merge-upstream.sh v26.09-harmonic
```

That will:

1. `git fetch upstream`
2. `git merge --no-commit upstream/<branch>`
3. resolve "deleted by us" conflicts under the Frankendancer paths by
   keeping them deleted
4. re-run `strip-frankendancer.sh`, which removes any *new* Frankendancer
   files the merge introduced — these arrive without conflict, so this
   step is the one that actually keeps the tree honest
5. leave anything else for you to resolve

The merge is deliberately left **uncommitted**.  Step 3 discards upstream
changes by design, so read the diff before committing:

```console
$ git diff --cached --stat
$ git commit
```

Use a different remote or branch with:

```console
$ UPSTREAM_REMOTE=origin contrib/harmonic/merge-upstream.sh main
```

## Checking the tree by hand

```console
$ contrib/harmonic/strip-frankendancer.sh --check   # report only
$ contrib/harmonic/strip-frankendancer.sh           # strip
```

Both are idempotent and exit 0 when the tree is clean.  Worth wiring into
CI or a pre-commit hook if you want the guarantee enforced.

## What gets removed

`frankendancer-paths.txt` is the source of truth — it is read by both
scripts.  When an upstream merge brings in a new Frankendancer-only
directory, add it there rather than deleting it by hand; that keeps the
next merge automatic.

Currently:

| Path | Why |
|---|---|
| `agave` + `.gitmodules` | The Agave (Rust) validator, linked into Frankendancer as a static library |
| `src/app/fdctl` | Frankendancer production CLI, topology, and all cargo build rules |
| `src/app/fddev` | Frankendancer development CLI |
| `src/discoh` | Frankendancer tiles: bank, pohh, resolh, store, plugin, guih |
| `.github/actions/submodule` | CI action that builds and caches the agave submodule |
| `.github/actions/submodule-init` | Submodule init action; this tree has no submodules |
| `book/api/cli.md` | `fdctl` CLI reference |
| `contrib/tag-release.py`, `contrib/tag-release-frankendancer.py` | Release tagging that packs the Agave version into Frankendancer's (renamed upstream in v26) |
| `contrib/tool/extract_abi.py` | Extracts Agave's `RuntimeTransaction` layout for `fd_bank_abi.c` |
| `src/disco/README.md` | Historical Frankendancer filtering diagram, ending at the Agave validator |
| `src/flamenco/gossip/differential_fuzzer.patch` | Patch against the agave submodule for the gossip differential fuzzer |

### Agave is not always Frankendancer

Worth knowing before you add to that list.  Agave appears in this tree in
two unrelated roles:

* **Linked into Frankendancer** as a Rust static library — that is the
  `agave` submodule, and it is gone.
* **As an external peer** for localnet testing, cloned separately and run
  as a neighbouring validator.  Firedancer supports and is tested this
  way, so `contrib/agave-cluster` and `contrib/test/setup_fd_cluster*.sh`
  stay.  (Upstream dropped the `test_firedancer_localnet.yml` workflow
  that drove them in v26.)

`contrib/bundle-test-server` and `contrib/ledger-gen` are Rust but are
not Frankendancer either — they are dev tools for the bundle client and
for generating test ledgers, both of which Firedancer uses.

## Shared files edited by this fork

These are the only places upstream can conflict with the strip.  If a
merge conflicts here, the fix is usually to take upstream's version and
re-apply the one-line change.

| File | Change |
|---|---|
| `src/app/shared/Local.mk` | Dropped `fd_fdctl` from the config test targets (`test_config_parse`, `_auto`, `_json`, `fuzz_fdctl_config`); added the default.toml prerequisite |
| `src/app/shared/test_config_parse.c` | `FD_IMPORT_BINARY` of Firedancer's `default.toml` instead of linking Frankendancer's symbol; the oversized-array case uses a Firedancer key, the aliased-array case (Frankendancer-only) is gone |
| `src/app/shared/test_config_json.c`, `src/app/firedancer-dev/tests/test_firedancer_dev.c` | `fd_config_load()` / `fd_dev_main()` calls without `is_firedancer` |
| `.github/workflows/*.yml` | Dropped `fdctl`/`fddev` targets, rust-toolchain steps, and `submodule-init`/`submodules: recursive`; deleted `cbmc`, `coverage_test_vectors` and the `on_*` dispatchers, which only run on firedancer-io's self-hosted runners |
| `.github/workflows/harmonic-draft-release.yml` | Not in harmonic's v26 branches; carried by this fork.  Firedancer-only; releases from `src/app/firedancer/version.mk` (branch `v26.09-harmonic` for `26.09.x`) |
| `.github/workflows/inertia-build.yml` | Fork-only compile and unit-test workflow for GitHub-hosted runners |
| `contrib/build.sh` | `RUST_TARGETS` → `APP_TARGETS`; dropped the rustup install and `--no-rust` flag |
| `contrib/test/ci_tests.sh` | Default `TARGETS` builds `firedancer`, not `fdctl` |
| `config/everything.mk` | Dropped the `rust` goal, `make-bin-rust`, and `agave/target` from the clean rules; `FRONTEND_CLIENT` accepts only `Firedancer` |
| `config/extra/with-blst.mk` | Comment reworded; upstream explains the trailing archive in terms of `fdctl` linking Agave |
| `deps.sh` | No rustup install; the C build needs no Rust toolchain |
| `book/.vitepress/config.mts` | Dropped the Frankendancer CLI sidebar entry |
| `.github/dependabot.yml` | Deleted; dependency bumps come from upstream merges, not this fork.  If a merge conflicts on it, keep it deleted |
| `book/guide/*.md`, `doc/*`, `contrib/containers/README.md`, `CLAUDE.md` | `fdctl` paths and targets point at the Firedancer equivalents |

### The Frankendancer config section

Upstream's `fd_config_t` is a union discriminated by `is_firedancer`:
`fd_configh_t frankendancer` for the hybrid, `fd_configf_t firedancer`
for the full client.  Both binaries in this tree pass `is_firedancer=1`,
so the whole `configh` half — the struct, `fd_config_extract_podh()`,
`fd_config_fillh()`, `fd_config_validateh()` and every `!is_firedancer`
branch — was unreachable, and is gone.  `is_firedancer` itself is gone
with it, including from the `fd_main()` / `fd_dev_main()` /
`fd_config_load()` signatures.

Two Agave-only bits of the topology went the same way: the
`reserve_agave_cores` parameter on `fd_topob_auto_layout{,_cpus}()` (it
reserved a second core per tile for the Agave side, and was always 0
here) and `fd_topo_t`'s `agave_affinity_*` fields, including the
`agave_affinity` key in the topology JSON dump.

Both shrink `fd_config_t`, which `fd_config_json.c` pins with a
`FD_STATIC_ASSERT` on its size.  Recompute it after a merge (the union
held the larger `fd_configh_t`, so dropping it is not a no-op), and keep
emitting `"is_firedancer": true` in the resolved config JSON so the
boot report schema does not change.

`fd_topo_tile_t.is_agave` is deliberately **kept** — removing it would
rewrite every column-aligned `fd_topob_tile()` row in `topology.c` and
conflict on every upstream topology change.  It is always 0.

Expect conflicts here on any upstream change to config parsing or
auto-layout.  The fix is to take upstream's version and delete the
Frankendancer half again.

Upstream keeps adding `if( config->is_firedancer )` guards to shared
code — the v0.11 merge brought three new ones, in `run.c` (twice) and
`genesis.c`, none of which conflicted; v26 added more in `run.c`,
`ethtool-channels.c`, `fd_dev_boot.c` and `fd_config_json.c`.  They compile-fail rather than
misbehave, but grep for `is_firedancer` after every merge rather than
waiting for the build.  If they keep accumulating, consider keeping the
field as a constant 1 for the same reason `is_agave` was kept.

### Harmonic re-applies its patch per release

`frankensamba patch` is not merged forward; each harmonic release branch
carries its own fresh copy.  Merging v0.10-harmonic into v0.11-harmonic
therefore conflicts on every file that patch touches — bundle, pack,
verify, gossip, resolv, config — because git sees two independent
introductions of similar lines.  Take upstream's copy: it is the same
patch rebased onto a newer base.  This accounted for 39 of the 67
conflicts in the v0.11 merge.

Watch for harmonic dropping parts of their own patch: v0.11 removed
`frontend_release_channel`, which v0.10 had added, because upstream
replaced per-channel frontend bundles with `FRONTEND_CLIENT`.

### The plugin header

Up to v0.11 this fork moved `fd_plugin.h` from `src/discoh/plugin/` to
`src/disco/plugin/`, because Firedancer's GUI and gossip tiles still
included it.  As of v26 nothing outside `src/discoh` includes it, so the
move is gone and `src/discoh` is deleted whole.
