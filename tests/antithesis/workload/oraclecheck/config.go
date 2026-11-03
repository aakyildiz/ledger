package oraclecheck

// Bounds of DESIGN §4.8 and §5.5.
const (
	// DefaultMaxInflight is the candidate search's hard cap (M/search.go:10).
	DefaultMaxInflight = 8
	// DefaultPendingCap bounds the pending successes (`backlog` above it).
	DefaultPendingCap = 1024
	// DefaultMaxTrackedRequests bounds the requests of one tracked bulk.
	DefaultMaxTrackedRequests = 64

	// ViewCap bounds every list a view returns.
	ViewCap = 64
	// StampsCap bounds each list of the stamps view.
	StampsCap = 32
	// ReplayRegistryCap bounds the replayable registry (M/config.go:141).
	ReplayRegistryCap = 2048
	// replayableViewNewest is how many newest replayable entries the view
	// always lists.
	replayableViewNewest = 32
	// CommittedResponseCap bounds the committed responses held by wid
	// (DESIGN §5.5); above it the oldest are dropped and ResyncNeeded is set.
	CommittedResponseCap = 100_000
	// deletedCheckpointHistoryCap bounds the deleted checkpoint snapshots kept
	// (M/checkpoints.go).
	deletedCheckpointHistoryCap = 10
)

// Config configures a Tracker.
type Config struct {
	RunID, TrackedPrefix string
	QueryCheckpointLimit uint64
	MaxInflight          int // 8
	PendingCap           int // 1024
	MaxTrackedRequests   int // 64
}

func (c Config) withDefaults() Config {
	if c.MaxInflight <= 0 {
		c.MaxInflight = DefaultMaxInflight
	}
	if c.PendingCap <= 0 {
		c.PendingCap = DefaultPendingCap
	}
	if c.MaxTrackedRequests <= 0 {
		c.MaxTrackedRequests = DefaultMaxTrackedRequests
	}

	return c
}

// Refusal is an error answer of the protocol (DESIGN §4.1).
type Refusal struct {
	HTTP    int // 400, 404 or 409
	Code    string
	Message string
	Details map[string]any
}

// Refusal codes (DESIGN §4.1).
const (
	CodeUnmodeled     = "unmodeled"
	CodeProtocol      = "protocol"
	CodeUnknownTicket = "unknown_ticket"
	CodeBusy          = "busy"
	CodeBacklog       = "backlog"
	CodeSerialized    = "serialized"
	CodeNotQuiet      = "not_quiet"
	CodeSetup         = "setup"
	CodePanic         = "panic"
)

func refuse(http int, code, msg string, d map[string]any) *Refusal {
	if d == nil {
		d = map[string]any{}
	}

	return &Refusal{HTTP: http, Code: code, Message: msg, Details: d}
}

// Protocol returns a 400 protocol refusal.
func Protocol(msg string, d map[string]any) *Refusal { return refuse(400, CodeProtocol, msg, d) }

// Serialized returns the 409 refusal a begin hook uses.
func Serialized(msg string, d map[string]any) *Refusal { return refuse(409, CodeSerialized, msg, d) }

// UnknownTicket returns the 404 answer for a ticket this boot never issued
// (or already finished, for reads).
func UnknownTicket(ticket uint64) *Refusal {
	return refuse(404, CodeUnknownTicket, "ticket is not known on this boot", map[string]any{"ticket": ticket})
}
