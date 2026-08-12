# Firedancer

This is a **Firedancer-only** fork.  Upstream ships two validator
clients; this tree carries only the first:

- **Firedancer** — A fully C-based Solana validator client.
- **Frankendancer** — Legacy Rust/C hybrid validator. **Stripped out of
  this repo**: no `agave` submodule, no `fdctl`, no `fddev`, no
  `src/discoh`, and no Rust in the build.

Do not reintroduce Frankendancer code.  Use the Firedancer equivalents
(`firedancer`, `firedancer-dev`, `src/discof`).

Merging from upstream re-introduces Frankendancer, so it is scripted —
see `contrib/harmonic/README.md`.  After any upstream merge,
`contrib/harmonic/strip-frankendancer.sh` must pass.

Firedancer only supports x86-64 on Linux, other targets are not correct
for example ARM due to TSO assumptions.

Topology: `src/app/firedancer/topology.c`.
Tiles: `src/disco`, `src/discof`.

## Building

`make -j` - builds everything
`make -j firedancer-dev` - builds dev validator
`make -j test_blake3` - builds a test

The default make parameters are:
- CC=gcc
- MACHINE=native
- EXTRAS=''

Always isolate build dirs when changing Make params, e.g.:
- `make -j BUILDDIR=clang-fuzz-asan CC=clang EXTRAS="fuzz asan"`
- `make -j BUILDDIR=clang-cov CC=clang EXTRAS=cov`

For Firedancer builds:
- keep a single flat name for BUILDDIR
- never pass arbitrary other make variables
- never invoke raw gcc

## Auto-generated Code

- **Metrics:** After changing `metrics.xml`, run:
  ```bash
  make -C src/disco/metrics metrics
  ```
  Regenerates all files in `src/disco/metrics/generated/` and `book/api/metrics-generated.md`.

- **Features:** After changing `feature_map.json`, run:
  ```bash
  cd src/flamenco/features && make generate
  ```
  Regenerates `fd_features_generated.h` and `fd_features_generated.c`.

- **Protobufs:** After protosol proto definitions change, run:
  ```bash
  make -C src/flamenco/runtime/tests protobufs
  ```
  Regenerates all files in `src/flamenco/runtime/tests/generated/`.

## Code Style

Follow the coding conventions in `CONTRIBUTING.md` when making code changes.
