#!/usr/bin/env sh

ni () {
    nix build --no-link --print-out-paths github:NixOS/nixpkgs/nixos-23.11#$1
}

LD_LIBRARY_PATH="$(ni secp256k1)/lib:$(dirname $0)" $(ni julia-bin)/bin/julia --project -t14 -L pkg.jl -- deduplicator.jl "$@"
