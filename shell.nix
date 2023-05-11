{ pkgs ? import <nixpkgs> { } }:

with pkgs;

mkShell {
  buildInputs = [
    julia-bin
    secp256k1
    gcc
  ];

  start_julia = ''
    julia --project -t8 -L pkg.jl
  '';
  start_primal_caching_service = ''
    julia --project -t8 -L pkg.jl -L load.jl -L start.jl
  '';

  shellHook = ''
    export LD_LIBRARY_PATH=${secp256k1}/lib:.
    make
  '';
}
