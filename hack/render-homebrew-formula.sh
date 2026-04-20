#!/usr/bin/env bash

set -euo pipefail

VERSION="${1:?usage: render-homebrew-formula.sh <version> <dist-dir> [formula-name]}"
DIST_DIR="${2:?usage: render-homebrew-formula.sh <version> <dist-dir> [formula-name]}"
FORMULA_NAME="${3:-kekspose}"
REPOSITORY="${GITHUB_REPOSITORY:-scholzj/kekspose}"
VERSION_NO_V="${VERSION#v}"

sha256() {
  shasum -a 256 "$DIST_DIR/$1" | awk '{print $1}'
}

darwin_arm64="kekspose_${VERSION_NO_V}_darwin_arm64.tar.gz"
darwin_amd64="kekspose_${VERSION_NO_V}_darwin_amd64.tar.gz"
linux_arm64="kekspose_${VERSION_NO_V}_linux_arm64.tar.gz"
linux_amd64="kekspose_${VERSION_NO_V}_linux_amd64.tar.gz"

formula_class() {
  printf '%s' "${1//@/ AT }" | awk '
    {
      n = split($0, parts, /[^[:alnum:]]+/)
      for (i = 1; i <= n; i++) {
        if (parts[i] == "") {
          continue
        }

        if (parts[i] == "AT") {
          printf "AT"
          continue
        }

        part = tolower(parts[i])
        printf toupper(substr(part, 1, 1)) substr(part, 2)
      }
      printf "\n"
    }
  '
}

FORMULA_CLASS="$(formula_class "$FORMULA_NAME")"

cat <<EOF
class ${FORMULA_CLASS} < Formula
  desc "Expose Strimzi Kafka clusters from local Kubernetes environments"
  homepage "https://github.com/${REPOSITORY}"
  version "${VERSION_NO_V}"
  license "Apache-2.0"

  if OS.mac?
    if Hardware::CPU.arm?
      url "https://github.com/${REPOSITORY}/releases/download/${VERSION}/${darwin_arm64}"
      sha256 "$(sha256 "$darwin_arm64")"
    else
      url "https://github.com/${REPOSITORY}/releases/download/${VERSION}/${darwin_amd64}"
      sha256 "$(sha256 "$darwin_amd64")"
    end
  elsif OS.linux?
    if Hardware::CPU.arm?
      url "https://github.com/${REPOSITORY}/releases/download/${VERSION}/${linux_arm64}"
      sha256 "$(sha256 "$linux_arm64")"
    else
      url "https://github.com/${REPOSITORY}/releases/download/${VERSION}/${linux_amd64}"
      sha256 "$(sha256 "$linux_amd64")"
    end
  end

  def install
    bin.install Dir["kekspose*"][0] => "kekspose"
    prefix.install "README.md", "LICENSE"
  end

  test do
    assert_match "Keksposé version:", shell_output("#{bin}/kekspose version")
  end
end
EOF
