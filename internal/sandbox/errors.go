package sandbox

// Kind classifies a domain Error so the transport layer can pick a status
// code without re-parsing messages. The zero value is invalid.
type Kind int

const (
	// KindInternal is an unexpected failure inside the service.
	KindInternal Kind = iota + 1
	// KindInvalidArgument is a malformed or missing request field.
	KindInvalidArgument
	// KindNotFound is a reference to a sandbox that does not exist.
	KindNotFound
	// KindResourceExhausted is a sandbox limit being reached.
	KindResourceExhausted
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
func notFound(msg string) error          { return &Error{Kind: KindNotFound, Message: msg} }
func resourceExhausted(msg string) error { return &Error{Kind: KindResourceExhausted, Message: msg} }
