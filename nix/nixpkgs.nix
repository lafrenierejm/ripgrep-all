# https://github.com/hercules-ci/flake-parts/issues/339#issuecomment-3989441384
#
# `rust-flake.flakeModules.nixpkgs` builds `pkgs` by importing nixpkgs' own
# `nixos/modules/misc/nixpkgs.nix` into `perSystem`, which triggers infinite
# recursion when this flake's `packages.<system>.*` outputs are consumed from
# deep within another flake's module tree (e.g. a home-manager config nested
# in a nix-darwin system). Build `pkgs` directly instead.
{ inputs, lib, ... }:
{
  perSystem =
    { config, system, ... }:
    {
      options.nixpkgs.overlays = lib.mkOption {
        type = lib.types.listOf lib.types.raw;
        default = [ ];
      };
      config._module.args.pkgs = import inputs.nixpkgs {
        inherit system;
        overlays = config.nixpkgs.overlays;
      };
    };
}
