package main

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
)

// Service-side mirror registry (DESIGN §5.3): every event id the library can
// return has exactly one asserter, registered from init() of an h_<fam>.go
// file, whose body calls assert.<Kind> with the literal message. Raw coverage
// ids (oc.RegisterCoverage) are emitted with assert.AssertRaw instead.

type asserterFunc func(cond bool, d map[string]any)

var asserters = map[oc.EventID]asserterFunc{}

func registerAsserter(id oc.EventID, fn asserterFunc) {
	if _, dup := asserters[id]; dup {
		panic(fmt.Sprintf("oracle: asserter for %q registered twice", id))
	}
	asserters[id] = fn
}

const (
	rawCoverageClass = "github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/oracle"
	rawCoverageFile  = "tests/antithesis/workload/bin/oracle/registry.go"
)

// emitRaw reports one data-driven coverage probe (M/coverage.go pattern).
func emitRaw(cond bool, id string, d map[string]any, hit bool) {
	assert.AssertRaw(cond, id, d, rawCoverageClass, "emitRaw", rawCoverageFile, 0, 0, hit, true, "sometimes", "Sometimes", id)
}

// catalogCoverage declares every registered Raw probe unhit at startup, so a
// probe never evaluated still shows as an unsatisfied property.
func catalogCoverage() {
	for _, id := range oc.CoverageIDs() {
		emitRaw(false, string(id), nil, false)
	}
}

// assertEvents asserts every event at its registered call site.
func assertEvents(evs []oc.Event) {
	for _, e := range evs {
		d := e.Details
		if d == nil {
			d = map[string]any{}
		}
		if e.Kind == oc.EvRaw {
			emitRaw(e.Cond, string(e.ID), d, true)

			continue
		}
		fn, ok := asserters[e.ID]
		if !ok {
			assert.Unreachable("oracle protocol: unmapped event", map[string]any{"id": string(e.ID), "kind": e.Kind.String()})

			continue
		}
		fn(e.Cond, d)
	}
}
