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

**`sandbox.v3.SessionSandboxService`** — opt-in Cube development pilot (registered only when `CUBE_API_URL` is set):

- `EnsureSessionSandbox(session_id, profile_id?)` creates or reconnects a Cube workspace.
- `ExecuteTool` runs typed `read`, `write`, `edit`, and `bash` operations.
- `PutResource` and `GetResource` stream bounded files under `/workspace`.
- `CloseSessionSandbox` destroys the workspace; `GetSessionSandboxStatus` reports the manager state.

The pilot uses one manager replica. It recovers session bindings from Cube sandbox metadata at startup, but has no durable compare-and-swap store or service authentication yet. Do not enable it for general production traffic. See the [Cube integration design](https://github.com/Duke-ECE/managed-agents-design/tree/main/requirements/0005-cubesandbox-integration).

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
| `CUBE_API_URL` | unset | CubeAPI HTTPS URL; unset disables v3 |
| `CUBE_API_KEY` | unset | CubeAPI key, required by the current test cluster; load from a Kubernetes Secret, never a ConfigMap |
| `CUBE_TEMPLATE_ID` | unset | Cube template ID; required when `CUBE_API_URL` is set |
| `CUBE_PROXY_NODE_IP` | unset | Host to dial for CubeProxy data traffic (current pilot: `cube.weihao.duckdns.org`) |
| `CUBE_PROXY_PORT_HTTP` | `80` | CubeProxy dial port (current pilot: `10443`) |
| `CUBE_PROXY_SCHEME` | `http` for port 80, `https` for port 443 | Data-plane scheme (current pilot: `https`) |
| `CUBE_SANDBOX_DOMAIN` | `cube.app` | Domain used in generated sandbox hosts (current pilot: `weihao.duckdns.org`) |
| `CUBE_MAX_SESSIONS` | `10` | Maximum active v3 session sandboxes in this manager replica |
| `CUBE_TTL_SECONDS` | `1800` | Cube sandbox idle timeout |
| `CUBE_MAX_RESOURCE_BYTES` | `1048576` | Maximum upload or download per file |
| `CUBE_MAX_OUTPUT_BYTES` | `1048576` | Maximum stdout and stderr bytes returned per command |
| `CUBE_MAX_RUN_SECONDS` | `60` | Hard cap on a command timeout |

## Local development

Requires Go 1.25 and a kubeconfig with access to the cluster
(`~/.kube/config` or `$KUBECONFIG`). The proto contract lives in
[`Duke-ECE/protos`](https://github.com/Duke-ECE/protos) — to pick up API
changes, `go get github.com/Duke-ECE/protos@latest`.

```sh
make build test       # standard Makefile; `make gates` before every push
make run              # serves on :50051

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
