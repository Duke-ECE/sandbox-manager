# AGENTS.md — sandbox-manager

Go 1.25 gRPC server (:50051) owning all sandbox pods in the k8s cluster.

**Read [Duke-ECE/standards](https://github.com/Duke-ECE/standards) first** —
its `AGENTS.md` holds the platform-wide rules (layout, testing, toolchain,
data access, secrets, CI). They apply in full here.

Repo-specific:

- Pool controller keeps `POOL_SIZE` warm sandboxes; each v2 `Execute` runs in
  a fresh single-use anonymous sandbox destroyed immediately after. Stateless
  itself — state lives on pod labels/annotations.
- Sandboxes run in namespace `pi-sandbox` (default-deny NetworkPolicy, no SA
  token); this manager runs in `pi-sandbox-system` and needs a kubeconfig
  locally.
- Contracts: `sandbox.v1.SandboxManagerService` (ops/debug) and
  `sandbox.v2.SandboxExecutorService` — protos pinned in go.mod.
- k8s.io deps are pinned (v0.34.x) — do not let `go mod tidy` bump them past
  the golang:1.25 Docker line.
- Layout: vertical slices + hexagonal ports. `internal/sandbox/` (lifecycle +
  pool slice: types, service, pool controller, `PodStore` port, Kind-typed
  domain errors), `internal/executor/` (v2 Execute slice; reaches the pool via
  a consumer-owned `Pool` interface, exec via the `ExecFunc` seam),
  `internal/infrastructure/k8s/` (clientset `PodStore` impl + `pods/exec`
  SPDY), `internal/transport/grpc/` (thin handlers + the single
  error→status mapper). Slices never import grpc/status|codes.
- Tests: `k8s.io/client-go` fake clientset + gRPC `bufconn`; the
  `pods/exec` SPDY call sits behind the `executor.ExecFunc` seam.
- Manual check: `grpcurl -plaintext localhost:50051 list`.
