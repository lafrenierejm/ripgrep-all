{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs";

  outputs = { self, nixpkgs }:
    let
      pkgs = import nixpkgs { system = "x86_64-linux"; };
    in
    {
      devShells.default = pkgs.mkShell {
        buildInputs = [ pkgs.hello pkgs.cowsay
        ];
      };
    };
}
