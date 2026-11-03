// Package oraclecheck is the assertion-free checker library behind the hegel
// oracle service (tests/antithesis/workload/bin/oracle). DESIGN §5 is
// normative.
//
// It wraps the forward model of tests/oracle (GlobalState) with the bookkeeping
// lifted from the singleton model driver (bin/cmds/model/singleton_driver_model):
// tickets, the in-flight / pending / read sets, the drain gate, the candidate
// search over serializations of in-flight bulks, the commit and failure checks,
// the learned side tables, and the views the driver aims with.
//
// The library never asserts. Every check returns Events; the service maps each
// event id to exactly one literal SDK assertion at its own call site (DESIGN
// §5.3). It imports no Antithesis SDK and nothing from workload/internal
// (enforced by bin/oracle/deps_test.go).
//
// Determinism: the Tracker is a pure function of the sequence of calls made on
// it. Every map iteration that can affect a result is sorted, hooks run in
// registration-name order, and nothing reads the clock.
//
// Concurrency: nothing here locks. The service holds one mutex around every
// call on a Tracker.
package oraclecheck
