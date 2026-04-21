# AGENTS.md

## Workflow
- This repo is a single Go module. Use `go test ./...` for the full test suite and `go build` for the standard build; those are the only checks wired into PR CI.
- For focused verification, run `go test ./pkg/kekspose/...` or `go test ./pkg/kekspose/keks -run TestName`.
- Use `bash ./hack/package-release.sh <version> [dist-dir]` to reproduce the Homebrew/release archives locally.
- Keep command-line examples aligned with the real Cobra flags in `cmd/root.go`: `--kubeconfig`, `--context`, `-n/--namespace`, `-c/--cluster-name`, `-l/--listener-name`, `-p/--starting-port`, `-v`.

## Architecture
- `main.go` only calls `cmd.Execute()`.
- `cmd/` owns the CLI surface. `cmd/root.go` parses flags, sets `slog` verbosity from repeated `-v`, constructs `kekspose.Kekspose`, and calls `ExposeKafka()`.
- `pkg/kekspose/kekspose.go` is the main runtime entrypoint. It resolves kubeconfig from `--kubeconfig`, then `KUBECONFIG`, then `~/.kube/config`; `--context` overrides the kubeconfig current context for both client creation and namespace discovery; if `--namespace` is omitted it reads the selected context namespace and exits if none is set.
- `pkg/kekspose/keks/` discovers the Strimzi Kafka cluster, requires the Kafka resource to be `Ready`, picks either the named listener or the first non-TLS listener, and only exposes broker-role node-pool nodes.
- `pkg/kekspose/proksy/` is where Kafka protocol responses are rewritten to `localhost:<forwarded-port>`. If behavior changes around advertised broker addresses, coordinator lookup, or supported API keys, start there.
- `pkg/kekspose/proxiedforward/` is a forked/customized version of Kubernetes port-forwarding logic; preserve that intent when editing instead of swapping it back to stock client-go code.

## Gotchas
- Keksposé only supports listeners without TLS encryption. Tests in `pkg/kekspose/keks/keks_test.go` cover this and other cluster-discovery edge cases.
- Port mappings are intentionally sorted by broker node ID before assigning ports or printing bootstrap addresses. Preserve that stable ordering.
- The repo root may contain ignored build outputs like `kekspose` or `kekspose-*`; do not treat them as source files.
- Toolchain is aligned on Go `1.25`: `go.mod` declares `1.25.0`, and GitHub Actions uses `1.25.x`.
- Tagged builds generate Homebrew-ready `tar.gz` archives, `checksums.txt`, and both `kekspose.rb` and `kekspose@<version>.rb` formulas via `hack/package-release.sh` and `hack/render-homebrew-formula.sh`.
