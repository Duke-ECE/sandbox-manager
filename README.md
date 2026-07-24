# sandbox-manager

gRPC service that manages the lifecycle of sandbox pods in our k8s cluster.
v1 scope: create a sandbox, delete a sandbox — nothing else. See the design
doc 《沙箱技术方案》 for the rationale and non-goals.

- Sandboxes live in the `pi-sandbox` namespace; the manager runs in `pi-sandbox-system`.
- The manager is stateless: sandbox ID, creation time and TTL are stored as
  labels/annotations on the Pod itself.
- A built-in reaper deletes sandboxes that outlive their TTL (default 15 min).

## API

Defined in the [`Duke-ECE/protos`](https://github.com/Duke-ECE/protos) repo
(`proto/sandbox/v1/sandbox_manager.proto`), consumed as the Go module
`github.com/Duke-ECE/protos`:

- `CreateSandbox(image?, ttl_seconds?) → sandbox_id`
- `DeleteSandbox(sandbox_id)`

Errors use standard gRPC status codes (`NOT_FOUND`, `RESOURCE_EXHAUSTED`, …).
Server reflection is enabled for debugging with `grpcurl`.

## Configuration (env vars)

| Var | Default | Meaning |
|---|---|---|
| `SANDBOX_NAMESPACE` | `pi-sandbox` | where sandbox pods are created |
| `DEFAULT_IMAGE` | `ghcr.io/duke-ece/sandbox-worker:latest` | sandbox image when the request doesn't specify one |
| `MAX_SANDBOXES` | `10` | concurrent sandbox limit (`RESOURCE_EXHAUSTED` beyond) |
| `DEFAULT_TTL_SECONDS` | `900` | TTL when the request doesn't specify one |

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
grpcurl -plaintext localhost:50051 sandbox.v1.SandboxManagerService/CreateSandbox -d '{}'
grpcurl -plaintext localhost:50051 sandbox.v1.SandboxManagerService/DeleteSandbox -d '{"sandbox_id":"sb-..."}'
```

## Deploy

CI builds and pushes `ghcr.io/duke-ece/sandbox-manager` on every push to
main, then applies `k8s.yaml` (requires the `KUBE_CONFIG` repo secret, same as
the other managed-agents repos). Manual deploy:

```sh
kubectl apply -f k8s.yaml
```
