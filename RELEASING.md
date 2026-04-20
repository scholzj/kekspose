# Releasing

## Homebrew

Tagged builds publish Homebrew-ready release assets:

- `kekspose_<version>_darwin_arm64.tar.gz`
- `kekspose_<version>_darwin_amd64.tar.gz`
- `kekspose_<version>_linux_arm64.tar.gz`
- `kekspose_<version>_linux_amd64.tar.gz`
- `checksums.txt`
- `kekspose.rb`
- `kekspose@<version>.rb`

The release workflow generates the archives, checksums, and formulas automatically for tags and uploads them to the GitHub release.

The generated Homebrew formulas are intended to be copied into a tap repository such as:

- `scholzj/homebrew-tap/Formula/kekspose.rb`
- `scholzj/homebrew-tap/Formula/kekspose@<version>.rb`

Use `kekspose.rb` for the latest release. Keep older tagged formulas in the tap as versioned files so users can install specific older versions with commands such as:

```bash
brew install scholzj/tap/kekspose@0.8.1
```

For a new release:

1. Tag the release in this repository.
2. Wait for the workflow to publish the release assets.
3. Copy the generated `kekspose.rb` into the tap as the new latest formula.
4. Copy the generated `kekspose@<version>.rb` into the tap to preserve that exact version for future installs.

## Local Reproduction

To generate the release artifacts locally:

```bash
bash ./hack/package-release.sh v0.0.0-test dist
bash ./hack/render-homebrew-formula.sh v0.0.0-test dist > dist/kekspose.rb
bash ./hack/render-homebrew-formula.sh v0.0.0-test dist kekspose@0.0.0-test > dist/kekspose@0.0.0-test.rb
```
