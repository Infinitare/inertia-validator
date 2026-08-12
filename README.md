# [Firedancer](https://jumpcrypto.com/firedancer/) 🔥💃

Firedancer is a new validator client for Solana.

* **Fast** Designed from the ground up to be *fast*. The concurrency
model draws from experience in the low latency trading space, and the code
contains many novel high-performance reimplementations of core Solana
primitives.
* **Secure** The architecture of the validator allows it to run with a
highly restrictive sandbox and almost no system calls.
* **Independent** Firedancer is written from scratch. This brings client
diversity to the Solana network and helps it stay resilient to supply
chain attacks in build tooling or dependencies.

## About this fork

This is a **Firedancer-only** fork.  Upstream builds two validators from
one codebase:

* **Frankendancer** A hybrid validator using parts of Firedancer and
parts of Agave. Frankendancer uses the Firedancer networking stack and
block production components to perform better while leader. Other
functionality including execution and consensus is using the Agave
validator code.
* **Firedancer** A full from-scratch Firedancer with no Agave code.

This tree carries only the second.  The Agave submodule, `fdctl`,
`fddev` and the `src/discoh` tiles are stripped out, so there is no Rust
in the build and no submodule to initialise.

Upstream is still tracked, and merging from it is scripted.  See
[contrib/harmonic/README.md](contrib/harmonic/README.md) for how to pull
upstream changes and re-apply the strip.

## Documentation
If you are an operator or looking to run the validator, see the Getting
Started guide in the [Firedancer
docs](https://docs.firedancer.io/)

Note that much of the upstream operator documentation under `book/` is
written against Frankendancer's `fdctl` binary. It is kept as upstream
wrote it so that documentation merges stay clean; read it with the
Firedancer equivalents (`firedancer`, `firedancer-dev`) in mind.

## Developing
Firedancer only supports Linux on x86-64 and requires a relatively new
kernel, at least v4.18 to build.

```console
$ git clone <this-repo>
$ cd inertia-validator
$ ./deps.sh +dev
$ make -j firedancer-dev
```

Note there is no `--recurse-submodules`: this fork has no submodules.

To run a local development cluster:

```console
$ sudo ./build/native/gcc/bin/firedancer-dev dev
```

This development command will ensure your system is configured correctly
before creating a genesis block, some keys, a faucet, and then starting a
validator on the local machine. It makes privileged changes to system
configuration, so it needs `sudo` or root.

The production binary is built with `make -j firedancer`.

## License
Firedancer is available under the [Apache 2
license](https://www.apache.org/licenses/LICENSE-2.0). Firedancer also
includes external libraries that are available under a variety of
licenses. See [LICENSE](LICENSE) for the full license text.
