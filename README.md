## Commands
```bash
monosend receive
```
```bash
monosend receive --port 53318
monosend receive --autoaccept
```
```bash
monosend share file.txt file2.txt
```
```bash
monosend share --clipboard
```
## Installation

### Cargo
```bash
cargo install --path .
```
Make sure to have `xclip` or `wl-clipboard` for clipboard features.

### Nix / NixOS

#### Run directly
```bash
nix run github:dantevazquez/monosend -- receive
```

#### Install with `nix profile`
```bash
nix profile install github:dantevazquez/monosend
```

#### In your NixOS configuration (Flakes)
Add `monosend` to your flake inputs:
```nix
inputs = {
  nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  monosend.url = "github:dantevazquez/monosend";
};
```

Then either add the package to `environment.systemPackages`:
```nix
environment.systemPackages = [
  inputs.monosend.packages.${pkgs.stdenv.hostPlatform.system}.default
];
```

Or enable the NixOS module:
```nix
{
  imports = [ inputs.monosend.nixosModules.default ];
  programs.monosend.enable = true;
}
```

Or use the overlay:
```nix
{
  nixpkgs.overlays = [ inputs.monosend.overlays.default ];
  environment.systemPackages = [ pkgs.monosend ];
}
```

## License
MIT
