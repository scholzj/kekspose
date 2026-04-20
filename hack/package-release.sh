#!/usr/bin/env bash

set -euo pipefail

VERSION="${1:?usage: package-release.sh <version> [dist-dir]}"
DIST_DIR="${2:-dist}"
PLATFORMS="${PLATFORMS:-darwin/arm64 darwin/amd64 linux/amd64 linux/arm64 windows/amd64 windows/arm64}"
VERSION_NO_V="${VERSION#v}"

mkdir -p "$DIST_DIR"
rm -f "$DIST_DIR"/*

for platform in $PLATFORMS; do
  IFS=/ read -r goos goarch <<< "$platform"

  archive_base="kekspose_${VERSION_NO_V}_${goos}_${goarch}"
  binary_name="kekspose"

  if [[ "$goos" == "windows" ]]; then
    binary_name="kekspose.exe"
  fi

  staging_dir="$(mktemp -d)"

  echo "Building ${platform}"
  env GOOS="$goos" GOARCH="$goarch" go build -o "$staging_dir/$binary_name" .

  cp README.md LICENSE "$staging_dir/"
  tar -C "$staging_dir" -czf "$DIST_DIR/${archive_base}.tar.gz" "$binary_name" README.md LICENSE

  rm -rf "$staging_dir"
done

(
  cd "$DIST_DIR"
  shasum -a 256 ./* > checksums.txt
)
