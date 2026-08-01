package executor

// Kind classifies a domain Error so the transport layer can pick a status
// code without re-parsing messages. The zero value is invalid.
type Kind int

const (
	// KindInternal is an unexpected failure inside the service.
	KindInternal Kind = iota + 1
	// KindInvalidArgument is a malformed or unknown request field.
	KindInvalidArgument
	// KindResourceExhausted is the sandbox pool being exhausted.
	KindResourceExhausted
	// KindDeadlineExceeded is an execution exceeding its timeout.
	KindDeadlineExceeded
	// KindCanceled is the caller going away mid-call.
	KindCanceled
)

// Error is a domain failure with a caller-facing message. The transport
// layer maps Kind to a gRPC status code and returns Message verbatim.
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return e.Message }

func internal(msg string) error          { return &Error{Kind: KindInternal, Message: msg} }
func invalidArgument(msg string) error   { return &Error{Kind: KindInvalidArgument, Message: msg} }
func resourceExhausted(msg string) error { return &Error{Kind: KindResourceExhausted, Message: msg} }
func deadlineExceeded(msg string) error  { return &Error{Kind: KindDeadlineExceeded, Message: msg} }
func canceled(msg string) error          { return &Error{Kind: KindCanceled, Message: msg} }
