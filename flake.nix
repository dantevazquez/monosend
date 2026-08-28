{
  description = "A focused LocalSend CLI for sending and receiving files";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      cargoDetails = (nixpkgs.lib.importTOML ./Cargo.toml).package;
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs:
        let
          monosend = pkgs.rustPlatform.buildRustPackage {
            pname = cargoDetails.name;
            version = cargoDetails.version;

            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
            };

            nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
              pkgs.makeWrapper
            ];

            buildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin (
              if pkgs ? apple-sdk then
                [ pkgs.apple-sdk ]
              else
                with pkgs.darwin.apple_sdk.frameworks; [
                  Security
                  SystemConfiguration
                ]
            );

            postInstall = pkgs.lib.optionalString pkgs.stdenv.hostPlatform.isLinux ''
              wrapProgram $out/bin/monosend \
                --prefix PATH : ${pkgs.lib.makeBinPath (with pkgs; [ wl-clipboard xclip ])}
            '';

            meta = with pkgs.lib; {
              description = cargoDetails.description;
              homepage = cargoDetails.repository;
              license = licenses.mit;
              mainProgram = "monosend";
            };
          };
        in
        {
          inherit monosend;
          default = monosend;
        }
      );

      apps = forAllSystems (pkgs:
        let
          system = pkgs.stdenv.hostPlatform.system;
        in
        {
          monosend = {
            type = "app";
            program = "${self.packages.${system}.monosend}/bin/monosend";
            meta = {
              description = cargoDetails.description;
            };
          };
          default = self.apps.${system}.monosend;
        }
      );

      overlays = {
        monosend = final: prev: {
          monosend = self.packages.${prev.stdenv.hostPlatform.system}.monosend;
        };
        default = self.overlays.monosend;
      };

      nixosModules = {
        monosend = { config, lib, pkgs, ... }:
          let
            cfg = config.programs.monosend;
          in
          {
            options.programs.monosend = {
              enable = lib.mkEnableOption "monosend, a focused LocalSend CLI";
              package = lib.mkPackageOption self.packages.${pkgs.stdenv.hostPlatform.system} "monosend" { };
            };

            config = lib.mkIf cfg.enable {
              environment.systemPackages = [ cfg.package ];
            };
          };
        default = self.nixosModules.monosend;
      };

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.stdenv.hostPlatform.system}.monosend ];
          packages = with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
            rust-analyzer
          ];
        };
      });
    };
}
