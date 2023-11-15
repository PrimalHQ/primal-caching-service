#!/usr/bin/env sh

LD_LIBRARY_PATH="$(ni secp256k1)/lib:$(dirname $0)" $(ni julia-bin)/bin/julia --project -t14 -L pkg.jl -- deduplicator.jl "$@"
