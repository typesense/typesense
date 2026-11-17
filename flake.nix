{
  description = "Typesense native build shell, bazel via bazelisk";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        bazel = pkgs.writeShellScriptBin "bazel" ''exec ${pkgs.bazelisk}/bin/bazelisk "$@"'';
      in
      {
        # no cc in the shell, bazel has to pick up the xcode toolchain on macos
        devShells.default = pkgs.mkShellNoCC (
          {
            packages = [
              bazel
              pkgs.bazelisk
            ];

            shellHook = ''
              unset MACOSX_DEPLOYMENT_TARGET
              echo "Using $(bazel --version) via bazelisk"
            '';
          }
          // pkgs.lib.optionalAttrs pkgs.stdenv.isDarwin {
            # the wrapped_clang in bazel < 6.6 has no LC_UUID, which macos 26+ dyld rejects
            USE_BAZEL_VERSION = "6.6.0";
          }
        );
      }
    );
}
