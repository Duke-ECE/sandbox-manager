# sandbox-manager

gRPC service that owns all sandbox pods in our k8s cluster. Runtimes submit
**tool calls** and get results — they never see a sandbox. See the design
docs 《沙箱技术方案》(v1) and 《沙箱技术方案 v2》 for rationale and non-goals.

- Sandboxes live in the `pi-sandbox` namespace; the manager runs in `pi-sandbox-system`.
- A **pool controller** keeps `POOL_SIZE` ready sandboxes warm at all times.
- Every `Execute` runs in a **fresh, single-use sandbox** that is destroyed
  right after the call — sandboxes are anonymous and stateless.
- The manager is stateless: pool phase, sandbox ID, creation time and TTL are
  stored as labels/annotations on the Pod itself.
- A built-in reaper deletes v1 sandboxes that outlive their TTL (default 15 min).

## API

Defined in the [`Duke-ECE/protos`](https://github.com/Duke-ECE/protos) repo,
consumed as the Go module `github.com/Duke-ECE/protos`:

**`sandbox.v2.SandboxExecutorService`** — what runtimes call:

- `Execute(tool, input, timeout_seconds?) → (exit_code, stdout, stderr, truncated)`
  - built-in tools: `shell` (`/bin/sh -c`), `python` (`python3 -c`); extend via `TOOLS`

**`sandbox.v1.SandboxManagerService`** — ops/debug only (not for runtimes):

- `CreateSandbox(image?, ttl_seconds?) → sandbox_id`
- `DeleteSandbox(sandbox_id)`

Errors use standard gRPC status codes (`INVALID_ARGUMENT`, `RESOURCE_EXHAUSTED`,
`DEADLINE_EXCEEDED`, …). Server reflection is enabled for `grpcurl`.

## Configuration (env vars)

| Var | Default | Meaning |
|---|---|---|
| `SANDBOX_NAMESPACE` | `pi-sandbox` | where sandbox pods are created |
| `DEFAULT_IMAGE` | `busybox:1.36` | pool sandbox image (stand-in until a sandbox-worker image exists) |
| `MAX_SANDBOXES` | `10` | total pool cap (`RESOURCE_EXHAUSTED` beyond) |
| `DEFAULT_TTL_SECONDS` | `900` | v1 sandbox TTL when the request doesn't specify one |
| `POOL_SIZE` | `3` | ready sandboxes kept warm for Execute |
| `EXEC_TIMEOUT_SECONDS` | `60` | default per-call timeout (capped at 300) |
| `MAX_OUTPUT_BYTES` | `1048576` | per-stream output truncation |
| `TOOLS` | `{"shell":["/bin/sh","-c"],"python":["python3","-c"]}` | JSON tool → command-prefix map |

## Local development

Requires Go 1.25 and a kubeconfig with access to the cluster
(`~/.kube/config` or `$KUBECONFIG`). The proto contract lives in
[`Duke-ECE/protos`](https://github.com/Duke-ECE/protos) — to pick up API
changes, `go get github.com/Duke-ECE/protos@latest`.

```sh
go build ./...
go test ./...
go run ./cmd/server     # serves on :50051

grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 sandbox.v2.SandboxExecutorService/Execute \
  -d '{"tool":"shell","input":"echo hello"}'
```

## Deploy

CI builds and pushes `ghcr.io/duke-ece/sandbox-manager` on every push to
main, then applies `k8s.yaml` (requires the `KUBE_CONFIG` repo secret, same as
the other managed-agents repos). Manual deploy:

```sh
kubectl apply -f k8s.yaml
```
