package grpc

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	v3 "github.com/Duke-ECE/protos/gen/go/sandbox/v3"
	"github.com/Duke-ECE/sandbox-manager/internal/sessioncube"
)

type SessionHandler struct {
	v3.UnimplementedSessionSandboxServiceServer
	svc *sessioncube.Service
}

func NewSessionHandler(svc *sessioncube.Service) *SessionHandler { return &SessionHandler{svc: svc} }

func sessionState(state sessioncube.State) v3.SessionSandboxState {
	switch state {
	case sessioncube.StateCreating:
		return v3.SessionSandboxState_SESSION_SANDBOX_STATE_CREATING
	case sessioncube.StateReady:
		return v3.SessionSandboxState_SESSION_SANDBOX_STATE_READY
	case sessioncube.StateLost:
		return v3.SessionSandboxState_SESSION_SANDBOX_STATE_LOST
	case sessioncube.StateClosing:
		return v3.SessionSandboxState_SESSION_SANDBOX_STATE_CLOSING
	default:
		return v3.SessionSandboxState_SESSION_SANDBOX_STATE_ABSENT
	}
}

func (h *SessionHandler) EnsureSessionSandbox(ctx context.Context, req *v3.EnsureSessionSandboxRequest) (*v3.EnsureSessionSandboxResponse, error) {
	state, err := h.svc.Ensure(ctx, req.GetSessionId(), req.GetProfileId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v3.EnsureSessionSandboxResponse{State: sessionState(state)}, nil
}

func (h *SessionHandler) ExecuteTool(ctx context.Context, req *v3.ExecuteToolRequest) (*v3.ExecuteToolResponse, error) {
	var result sessioncube.CommandResult
	var truncated bool
	var err error
	switch tool := req.GetTool().(type) {
	case *v3.ExecuteToolRequest_Bash:
		result, truncated, err = h.svc.Run(ctx, req.GetSessionId(), tool.Bash.GetCommand(), time.Duration(req.GetTimeoutSeconds())*time.Second)
	case *v3.ExecuteToolRequest_Read:
		var data []byte
		data, err = h.svc.Read(ctx, req.GetSessionId(), tool.Read.GetPath())
		if err == nil {
			lines := strings.Split(string(data), "\n")
			offset := tool.Read.GetOffsetLines()
			limit := tool.Read.GetLimitLines()
			if offset < 0 || limit < 0 {
				return nil, toStatus(sessioncube.ErrInvalid)
			}
			if offset > int64(len(lines)) {
				offset = int64(len(lines))
			}
			end := int64(len(lines))
			if limit > 0 && limit < end-offset {
				end = offset + limit
			}
			result.Stdout = strings.Join(lines[offset:end], "\n")
		}
	case *v3.ExecuteToolRequest_Write:
		err = h.svc.Write(ctx, req.GetSessionId(), tool.Write.GetPath(), []byte(tool.Write.GetContent()))
	case *v3.ExecuteToolRequest_Edit:
		err = h.svc.Edit(ctx, req.GetSessionId(), tool.Edit.GetPath(), tool.Edit.GetOldText(), tool.Edit.GetNewText())
	default:
		return nil, toStatus(sessioncube.ErrInvalid)
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &v3.ExecuteToolResponse{ExitCode: int32(result.ExitCode), Stdout: result.Stdout, Stderr: result.Stderr, Truncated: truncated}, nil
}

func (h *SessionHandler) PutResource(stream v3.SessionSandboxService_PutResourceServer) error {
	var sessionID, filePath string
	var data bytes.Buffer
	first := true
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if first {
			sessionID, filePath = req.GetSessionId(), req.GetPath()
			first = false
		} else if (req.GetSessionId() != "" && req.GetSessionId() != sessionID) || (req.GetPath() != "" && req.GetPath() != filePath) {
			return toStatus(sessioncube.ErrInvalid)
		}
		if len(req.GetChunk()) > h.svc.MaxResourceBytes()-data.Len() {
			return toStatus(sessioncube.ErrLimit)
		}
		data.Write(req.GetChunk())
	}
	if first {
		return toStatus(sessioncube.ErrInvalid)
	}
	if err := h.svc.Write(stream.Context(), sessionID, filePath, data.Bytes()); err != nil {
		return toStatus(err)
	}
	return stream.SendAndClose(&v3.PutResourceResponse{BytesWritten: int64(data.Len())})
}

func (h *SessionHandler) GetResource(req *v3.GetResourceRequest, stream v3.SessionSandboxService_GetResourceServer) error {
	data, err := h.svc.Read(stream.Context(), req.GetSessionId(), req.GetPath())
	if err != nil {
		return toStatus(err)
	}
	const chunkSize = 64 << 10
	if len(data) == 0 {
		return stream.Send(&v3.GetResourceResponse{})
	}
	for len(data) > 0 {
		n := min(len(data), chunkSize)
		if err := stream.Send(&v3.GetResourceResponse{Chunk: data[:n]}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (h *SessionHandler) CloseSessionSandbox(ctx context.Context, req *v3.CloseSessionSandboxRequest) (*v3.CloseSessionSandboxResponse, error) {
	if err := h.svc.Close(ctx, req.GetSessionId()); err != nil {
		return nil, toStatus(err)
	}
	return &v3.CloseSessionSandboxResponse{}, nil
}

func (h *SessionHandler) GetSessionSandboxStatus(_ context.Context, req *v3.GetSessionSandboxStatusRequest) (*v3.GetSessionSandboxStatusResponse, error) {
	state, err := h.svc.Status(req.GetSessionId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &v3.GetSessionSandboxStatusResponse{State: sessionState(state)}, nil
}
