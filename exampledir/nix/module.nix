{ config, lib, pkgs, ... }:

{
  options.services.example = {
    enable = mkEnableOption "example";
  };

  config.systemd.services.example.description = "Example service";
  config.systemd.services.example.wantedBy = ["multi-user.target"];
}
