package oraclecheck

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Registries filled by families from init() (DESIGN §5.3). Every hook kind
// runs in sorted name order, so the service stays a pure function of the
// request sequence.

// ReadKind is a registered read kind.
type ReadKind struct {
	Name        string
	SendsWrites bool // §4.7 transient class
	Validate    func(*ReadCtx) []Event
}

// Route is how the driver sent a read.
type Route struct {
	Client      int    `json:"client"`
	Route       string `json:"route"`
	Node        string `json:"node"`
	Consistency string `json:"consistency"`
}

// ReadCtx is what a read validator sees.
type ReadCtx struct {
	T                          *Tracker
	Ticket, MaxTicket, Version uint64
	Snapshot                   oracle.GlobalState // taken at /read/begin with the ticket
	Views                      []json.RawMessage  // as returned at /read/begin
	Kind                       string
	Request                    json.RawMessage
	Outcome                    Outcome
	Route                      Route
	Hint, Params               json.RawMessage // Params = begin params merged with end params
}

// Matches reports whether some candidate base satisfies m.
func (c *ReadCtx) Matches(m func(oracle.GlobalState) bool) bool {
	return c.T.MatchesModel(c.MaxTicket, m)
}

// LedgerNotFoundExplained accepts a ledger-scoped NotFound only when some
// legal serialization has not created the ledger yet or already deleted it
// (M/validate.go validateLedgerNotFound).
func (c *ReadCtx) LedgerNotFoundExplained(ledger string) []Event {
	if c.Matches(func(base oracle.GlobalState) bool {
		lc, exists := base.Lifecycle(ledger)

		return !exists || lc.Deleted
	}) {
		return nil
	}

	return []Event{Violation(EvLedgerNotFoundUnexplained, map[string]any{
		"ledger":    ledger,
		"operation": c.Kind,
		"maxTicket": c.MaxTicket,
	})}
}

// Hook and registry function types (DESIGN §5.2).
type (
	ViewFunc      func(snap oracle.GlobalState, t *Tracker, params json.RawMessage) (any, error)
	BeginHook     func(t *Tracker, rec WriteRecord) (predicted map[string]any, ref *Refusal)
	DrainHook     func(t *Tracker, rec WriteRecord, resp *servicepb.ApplyResponse, before, after oracle.GlobalState) []Event
	FailureHook   func(t *Tracker, rec WriteRecord, res oracle.ApplyResult, reason string) []Event
	SettleHook    func(t *Tracker) []Event
	QuietSection  func(t *Tracker, epoch, seq uint64, body json.RawMessage) (next any, evs []Event, ref *Refusal)
	QuietProvider func(t *Tracker) any
)

type named[F any] struct {
	name string
	fn   F
}

type registry struct {
	mu        sync.Mutex
	readKinds map[string]ReadKind
	views     map[string]ViewFunc
	begin     []named[BeginHook]
	drain     []named[DrainHook]
	failure   []named[FailureHook]
	settle    []named[SettleHook]
	sections  map[string]QuietSection
	providers []named[QuietProvider]
	coverage  map[EventID]bool
}

var reg = &registry{
	readKinds: map[string]ReadKind{},
	views:     map[string]ViewFunc{},
	sections:  map[string]QuietSection{},
	coverage:  map[EventID]bool{},
}

func insertNamed[F any](list []named[F], name string, fn F) []named[F] {
	for _, n := range list {
		if n.name == name {
			panic(fmt.Sprintf("oraclecheck: hook %q registered twice", name))
		}
	}
	list = append(list, named[F]{name: name, fn: fn})
	slices.SortFunc(list, func(a, b named[F]) int {
		if a.name < b.name {
			return -1
		}
		if a.name > b.name {
			return 1
		}

		return 0
	})

	return list
}

// RegisterReadKind registers a read kind (panics on a duplicate name).
func RegisterReadKind(k ReadKind) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, dup := reg.readKinds[k.Name]; dup {
		panic(fmt.Sprintf("oraclecheck: read kind %q registered twice", k.Name))
	}
	reg.readKinds[k.Name] = k
}

// RegisterView registers a view. ledgerPick/owned resolution is done by the
// core before fn runs: params then carry the resolved "ledger".
func RegisterView(name string, fn ViewFunc) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, dup := reg.views[name]; dup {
		panic(fmt.Sprintf("oraclecheck: view %q registered twice", name))
	}
	reg.views[name] = fn
}

// RegisterBeginHook registers a /write/begin hook.
func RegisterBeginHook(name string, fn BeginHook) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.begin = insertNamed(reg.begin, name, fn)
}

// RegisterDrainHook registers a hook run after crossCheckCommit agreed.
func RegisterDrainHook(name string, fn DrainHook) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.drain = insertNamed(reg.drain, name, fn)
}

// RegisterFailureHook registers a hook run after a failure was explained.
func RegisterFailureHook(name string, fn FailureHook) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.failure = insertNamed(reg.failure, name, fn)
}

// RegisterSettleHook registers a hook run after every EndWrite, LateWrite,
// AbandonWrite, FinishRead and drain step.
func RegisterSettleHook(name string, fn SettleHook) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.settle = insertNamed(reg.settle, name, fn)
}

// RegisterQuietSection registers a /quiet/section handler.
func RegisterQuietSection(name string, fn QuietSection) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, dup := reg.sections[name]; dup {
		panic(fmt.Sprintf("oraclecheck: quiet section %q registered twice", name))
	}
	reg.sections[name] = fn
}

// RegisterQuietProvider registers a /quiet/begin provider.
func RegisterQuietProvider(name string, fn QuietProvider) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.providers = insertNamed(reg.providers, name, fn)
}

// RegisterCoverage declares data-driven probes; they are emitted as Raw
// events and catalogued by the service at startup.
func RegisterCoverage(ids ...EventID) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for _, id := range ids {
		reg.coverage[id] = true
	}
}

// LookupReadKind returns a registered read kind.
func LookupReadKind(name string) (ReadKind, bool) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	k, ok := reg.readKinds[name]

	return k, ok
}

// LookupQuietSection returns a registered quiet section.
func LookupQuietSection(name string) (QuietSection, bool) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	s, ok := reg.sections[name]

	return s, ok
}

func lookupView(name string) (ViewFunc, bool) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	v, ok := reg.views[name]

	return v, ok
}

// CoverageIDs returns the registered Raw probe ids, sorted.
func CoverageIDs() []EventID {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	out := make([]EventID, 0, len(reg.coverage))
	for id := range reg.coverage {
		out = append(out, id)
	}
	slices.Sort(out)

	return out
}

// IsCoverageID reports whether id was registered with RegisterCoverage.
func IsCoverageID(id EventID) bool {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	return reg.coverage[id]
}

func snapshotHooks[F any](list []named[F]) []named[F] {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	return append([]named[F](nil), list...)
}

// Providers runs every quiet provider in name order.
func (t *Tracker) Providers() map[string]any {
	out := map[string]any{}
	for _, p := range snapshotHooks(reg.providers) {
		out[p.name] = p.fn(t)
	}

	return out
}

func (t *Tracker) runSettle() []Event {
	var evs []Event
	for _, h := range snapshotHooks(reg.settle) {
		evs = append(evs, h.fn(t)...)
	}

	return evs
}

func (t *Tracker) runDrainHooks(rec WriteRecord, resp *servicepb.ApplyResponse, before, after oracle.GlobalState) []Event {
	var evs []Event
	for _, h := range snapshotHooks(reg.drain) {
		evs = append(evs, h.fn(t, rec, resp, before, after)...)
	}

	return evs
}

func (t *Tracker) runFailureHooks(rec WriteRecord, res oracle.ApplyResult, reason string) []Event {
	var evs []Event
	for _, h := range snapshotHooks(reg.failure) {
		evs = append(evs, h.fn(t, rec, res, reason)...)
	}

	return evs
}

func (t *Tracker) runBeginHooks(rec WriteRecord, predicted map[string]any) *Refusal {
	for _, h := range snapshotHooks(reg.begin) {
		p, ref := h.fn(t, rec)
		if ref != nil {
			if ref.Details == nil {
				ref.Details = map[string]any{}
			}

			return ref
		}
		for k, v := range p {
			predicted[k] = v
		}
	}

	return nil
}

// TestingUnregister removes test registrations. Test-only: call it from a
// t.Cleanup of a test that registered hooks.
func TestingUnregister(names ...string) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for _, n := range names {
		delete(reg.readKinds, n)
		delete(reg.views, n)
		delete(reg.sections, n)
		reg.begin = slices.DeleteFunc(reg.begin, func(h named[BeginHook]) bool { return h.name == n })
		reg.drain = slices.DeleteFunc(reg.drain, func(h named[DrainHook]) bool { return h.name == n })
		reg.failure = slices.DeleteFunc(reg.failure, func(h named[FailureHook]) bool { return h.name == n })
		reg.settle = slices.DeleteFunc(reg.settle, func(h named[SettleHook]) bool { return h.name == n })
		reg.providers = slices.DeleteFunc(reg.providers, func(h named[QuietProvider]) bool { return h.name == n })
	}
}
