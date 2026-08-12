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

## Pulling upstream

```console
$ contrib/harmonic/merge-upstream.sh v0.10-harmonic
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
| `src/discoh` | Frankendancer tiles: bank, pohh, resolh, store, plugin |
| `.github/actions/submodule` | CI action that builds and caches the agave submodule |
| `.github/actions/submodule-init` | Submodule init action; this tree has no submodules |
| `book/api/cli.md` | `fdctl` CLI reference |
| `contrib/tag-release.py` | Release tagging that packs the Agave version into Frankendancer's |
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
  way, so `contrib/agave-cluster`, `contrib/test/setup_fd_cluster*.sh`
  and `.github/workflows/test_firedancer_localnet.yml` all stay.

`contrib/bundle-test-server` and `contrib/ledger-gen` are Rust but are
not Frankendancer either — they are dev tools for the bundle client and
for generating test ledgers, both of which Firedancer uses.

## Shared files edited by this fork

These are the only places upstream can conflict with the strip.  If a
merge conflicts here, the fix is usually to take upstream's version and
re-apply the one-line change.

| File | Change |
|---|---|
| `src/app/shared/Local.mk` | Dropped `fd_fdctl` from the two config-parse test targets; added the default.toml prerequisite |
| `src/app/shared/test_config_parse.c` | `FD_IMPORT_BINARY` of Firedancer's `default.toml` instead of linking Frankendancer's symbol |
| `src/flamenco/gossip/Local.mk` | Dropped the Agave differential-fuzzer rules |
| `src/disco/gui/fd_gui.c`, `fd_gui_tile.c` | Include path follows `fd_plugin.h` to `src/disco/plugin/` |
| `.github/workflows/*.yml` | Dropped `fdctl`/`fddev` targets, rust-toolchain steps, and `submodule-init`/`submodules: recursive` |
| `.github/workflows/harmonic-draft-release.yml` | Firedancer-only; releases from `src/util/fd_version.c` |
| `contrib/build.sh` | `RUST_TARGETS` → `APP_TARGETS`; dropped the rustup install and `--no-rust` flag |
| `contrib/test/ci_tests.sh` | Default `TARGETS` builds `firedancer`, not `fdctl` |
| `config/everything.mk` | Dropped the `rust` goal, `make-bin-rust`, and `agave/target` from the clean rules; `FRONTEND_CLIENT` accepts only `Firedancer` |
| `config/extra/with-blst.mk` | Comment reworded; upstream explains the trailing archive in terms of `fdctl` linking Agave |
| `deps.sh` | No rustup install; the C build needs no Rust toolchain |
| `book/.vitepress/config.mts` | Dropped the Frankendancer CLI sidebar entry |

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
here) and `fd_topo_t`'s `agave_affinity_*` fields.

`fd_topo_tile_t.is_agave` is deliberately **kept** — removing it would
rewrite every column-aligned `fd_topob_tile()` row in `topology.c` and
conflict on every upstream topology change.  It is always 0.

Expect conflicts here on any upstream change to config parsing or
auto-layout.  The fix is to take upstream's version and delete the
Frankendancer half again.

Upstream keeps adding `if( config->is_firedancer )` guards to shared
code — the v0.11 merge brought three new ones, in `run.c` (twice) and
`genesis.c`, none of which conflicted.  They compile-fail rather than
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

`fd_plugin.h` only defines plugin message types.  Upstream moved it into
`src/discoh/plugin/` when the plugin tile became Frankendancer-only, but
Firedancer's GUI and gossip tiles still include it.  This fork moves it
to `src/disco/plugin/fd_plugin.h`, which is where it lived before that
move, so `src/discoh` can be deleted whole.

If upstream ever relocates it again, expect a conflict on those two GUI
includes.
