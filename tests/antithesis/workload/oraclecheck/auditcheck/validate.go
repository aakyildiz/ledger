package auditcheck

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
)

// Finding is one model divergence: its id, the entry that produced it and why.
type Finding struct {
	ID      oc.EventID
	Entry   string
	Why     string
	Missing uint64 // the sequence the model lacked, for diagnostics
}

// Verdict is ValidatePage's outcome: the first finding, or the coverage the
// page earned when it holds.
type Verdict struct {
	Finding *Finding
	// Rejections counts the tracked failure entries a recorded rejection
	// explained.
	Rejections int
	// Unjudged counts tracked failure entries with a non-definitive
	// (retryable) reason under a key the oracle registered: the server audits
	// them without freezing them, the driver retries the key, and the model
	// records no rejection for them, so no verdict is possible.
	Unjudged int
	// FailureOwedChecked marks a key-filtered page that was held to the
	// bulk-atomicity "a rejected keyed bulk has a failure audit entry" rule.
	FailureOwedChecked bool
	// KnownCertified counts the remembered entries the filter certainly
	// selects; CursorCorroborated marks a page whose resume cursor one of them
	// proved was owed.
	KnownCertified     int
	CursorCorroborated bool
}

// Page is the request-side shape of a served page.
type Page struct {
	Entries  []match.AuditEntry
	Filter   *commonpb.QueryFilter
	Reverse  bool
	PageSize int    // effective
	AfterSeq uint64 // decoded cursor, 0 for the first page
	Next     string // the next cursor's key ("" when none)
	// ReadTicket is the read's ticket (0 when unknown: the rules that need
	// the read to have begun after an observation are skipped).
	ReadTicket uint64
}

// entryVerdict is checkEntry's classification of an entry that holds.
type entryVerdict int

const (
	entryHolds entryVerdict = iota
	entryExplained
	entryUnjudged
)

// checkEntry holds one entry to the per-entry rules (M/audit.go
// validateAuditPage's loop body and validateAuditEntry), with foreign
// tolerance: a foreign entry must only stay out of the tracked ledgers; a
// tracked one is the model's own record of a bulk. explained is set for a
// tracked failure a recorded rejection explained.
func checkEntry(m Model, e match.AuditEntry, filter *commonpb.QueryFilter) (*Finding, entryVerdict) {
	if !m.Tracked(e) {
		if l, touched := m.trackedLedger(e); touched {
			return &Finding{ID: EvAuditForeignTouchedTracked, Entry: match.DescribeAuditEntry(e),
				Why: "foreign key " + strconv.Quote(e.IdemKey) + " names tracked ledger " + l}, entryHolds
		}

		return nil, entryHolds
	}

	// System-scoped orders (query checkpoints, maintenance mode) touch no
	// ledger and are not judged here, as in the singleton.
	if len(e.Ledgers) == 0 {
		return nil, entryHolds
	}

	if e.Failed {
		// The server neither audits nor freezes an UncommittedFailure
		// (machine.go), so no tracked entry may carry one of those reasons.
		if oc.UncommittedReason(e.Reason) {
			return &Finding{ID: EvAuditFailureUnexplained, Entry: match.DescribeAuditEntry(e),
				Why: "the server never audits a failure with reason " + e.Reason}, entryHolds
		}
		if m.rejectionExplains(e) {
			return nil, entryExplained
		}
		if m.Outstanding(e.IdemKey) {
			return nil, entryHolds
		}
		// A retryable failure (reason outside D) is audited but not frozen
		// (machine.go recordIdempotencyFailure); the client saw it as
		// non-definitive and the driver retried the key, which may since have
		// committed or failed for another reason. The model records only
		// definitive rejections, so such an entry under a registered key is
		// left unjudged.
		if !oc.IsDefinitiveReason(e.Reason) && m.Registered != nil && m.Registered(e.IdemKey) {
			return nil, entryUnjudged
		}

		return &Finding{ID: EvAuditFailureUnexplained, Entry: match.DescribeAuditEntry(e),
			Why: "no recorded rejection with these ledgers, order count and reason"}, entryHolds
	}

	// Bulk atomicity (MD/parallel_driver_bulk_atomicity, audit half): the only
	// write under this key was rejected for good, so nothing under it commits.
	if m.FrozenRejected != nil && m.FrozenRejected(e.IdemKey) {
		return &Finding{ID: EvAuditRejectedBulkHasSuccess, Entry: match.DescribeAuditEntry(e),
			Why: "the only write under key " + e.IdemKey + " was definitively rejected at the FSM stage"}, entryHolds
	}

	if e.MinLog > m.CommittedMax {
		if m.Outstanding(e.IdemKey) {
			return nil, entryHolds
		}

		return &Finding{ID: EvAuditNamesUncommittedLogs, Entry: match.DescribeAuditEntry(e),
			Why: "committed frontier is " + u(m.CommittedMax), Missing: e.MinLog}, entryHolds
	}

	if why, missing := match.AuditSuccessMismatch(e, m.Logs, m.Bulks, m.CommittedMax); why != "" {
		return &Finding{ID: EvAuditOutsideModel, Entry: match.DescribeAuditEntry(e), Why: why, Missing: missing}, entryHolds
	}

	// The order types the model committed under this entry's logs are the only
	// ones the order-type index can hold for it, so with them the whole filter
	// is decided at once.
	if match.AuditFiltersOrderType(filter) {
		types := match.CommittedOrderTypes(e, m.Logs)
		if !match.AuditEntryMatches(filter, e, types) {
			return &Finding{ID: EvAuditOutsideOrderType, Entry: match.DescribeAuditEntry(e),
				Why: "model committed " + strings.Join(slices.Sorted(maps.Keys(types)), ",")}, entryHolds
		}
	}

	return nil, entryHolds
}

// ValidatePage checks a served page against the model and the remembered
// entries (M/audit.go validateAuditPage + auditKnownMatchViolation). known
// are the entries remembered before the read began, by ascending sequence.
// The singleton's "audit tail misses the newest committed bulk" is not ported:
// foreign writes can be newer than every tracked bulk.
func ValidatePage(m Model, known []match.AuditEntry, p Page) Verdict {
	var v Verdict
	for _, e := range p.Entries {
		f, ev := checkEntry(m, e, p.Filter)
		if f != nil {
			return Verdict{Finding: f}
		}
		switch ev {
		case entryExplained:
			v.Rejections++
		case entryUnjudged:
			v.Unjudged++
		}
	}

	// Bulk atomicity, failure half (MD/parallel_driver_bulk_atomicity
	// "failure audit entry observed for failed atomic bulk"): a whole page of
	// an exact key filter, read after the key's only write was rejected for
	// good at the FSM stage, holds at least one failure entry under it. The
	// FSM writes the failure entry in the batch that freezes the outcome.
	if key, ok := exactKeyFilter(p.Filter); ok && p.AfterSeq == 0 && len(p.Entries) < p.PageSize &&
		m.FrozenRejected != nil && m.FrozenRejected(key) && m.FrozenObservedAt != nil && p.ReadTicket != 0 {
		if at := m.FrozenObservedAt(key); at != 0 && at < p.ReadTicket {
			v.FailureOwedChecked = true
			if !slices.ContainsFunc(p.Entries, func(e match.AuditEntry) bool { return e.Failed && e.IdemKey == key }) {
				return Verdict{Finding: &Finding{ID: EvAuditRejectedBulkMissesFailure, Entry: "key " + key,
					Why: "the key's only write was rejected for good at the FSM stage (observed at ticket " + u(at) +
						") but a whole page of its key filter holds no failure entry"}}
			}
		}
	}

	// A page the size limit did not cut short holds every entry the filter
	// selects, so every committed tracked log the model learned inside a bare
	// log-sequence range must be under some served success entry. Foreign logs
	// interleave the sequences; only learned tracked sequences are iterated.
	if cond := match.BareAuditLogSeqRange(p.Filter); cond != nil && len(p.Entries) < p.PageSize && p.AfterSeq == 0 {
		for _, seq := range sortedLogSeqs(m.Logs) {
			if !match.MatchUintBounds(cond, seq) {
				continue
			}
			if !slices.ContainsFunc(p.Entries, func(e match.AuditEntry) bool { return !e.Failed && e.MinLog <= seq && seq <= e.MaxLog }) {
				return Verdict{Finding: &Finding{ID: EvAuditMissesCommittedLog, Entry: "log sequence " + u(seq),
					Why: "no served entry covers it", Missing: seq}}
			}
		}
	}

	k := knownMatchViolation(m, known, p)
	if k.Finding != nil {
		return k
	}
	v.KnownCertified, v.CursorCorroborated = k.KnownCertified, k.CursorCorroborated

	return v
}

// exactKeyFilter returns the key when filter is exactly one hardcoded
// AUDIT_FIELD_IDEMPOTENCY_KEY equality leaf.
func exactKeyFilter(filter *commonpb.QueryFilter) (string, bool) {
	a := filter.GetAudit()
	if a == nil || a.GetField() != commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY {
		return "", false
	}
	sc := a.GetStringCond()
	if sc == nil {
		return "", false
	}
	h, ok := sc.GetValue().(*commonpb.StringCondition_Hardcoded)
	if !ok {
		return "", false
	}

	return h.Hardcoded, true
}

// knownMatchViolation judges a page against the entries the trail already
// served (M/audit.go auditKnownMatchViolation). A page is a prefix of the
// server's result and the known set a subset of it; audit history is
// permanent and a linearizable read sees every entry committed before it
// began.
func knownMatchViolation(m Model, known []match.AuditEntry, p Page) Verdict {
	served := make(map[uint64]bool, len(p.Entries))
	for _, e := range p.Entries {
		served[e.Seq] = true
	}
	var last uint64
	if len(p.Entries) > 0 {
		last = p.Entries[len(p.Entries)-1].Seq
	}

	var out Verdict
	for _, k := range known {
		seq := k.Seq
		if p.AfterSeq != 0 && (!p.Reverse && seq <= p.AfterSeq || p.Reverse && seq >= p.AfterSeq) {
			continue // before the cursor: not this page's business
		}
		if !match.AuditEntryCertainlyMatches(p.Filter, k, m.Logs) {
			continue
		}
		out.KnownCertified++

		onPage := len(p.Entries) > 0 && (!p.Reverse && seq <= last || p.Reverse && seq >= last)
		switch {
		case onPage:
			if !served[seq] {
				return Verdict{Finding: &Finding{ID: EvAuditOmitsKnown, Entry: match.DescribeAuditEntry(k), Why: "inside the page's own range", Missing: seq}}
			}
		case len(p.Entries) < p.PageSize:
			return Verdict{Finding: &Finding{ID: EvAuditStoppedShortOfKnown, Entry: match.DescribeAuditEntry(k),
				Why: strconv.Itoa(len(p.Entries)) + " of " + strconv.Itoa(p.PageSize) + " rows used", Missing: seq}}
		case p.Next == "":
			return Verdict{Finding: &Finding{ID: EvAuditDroppedResumeCursor, Entry: match.DescribeAuditEntry(k),
				Why: "a full page left this entry unserved and named no next cursor", Missing: seq}}
		default:
			out.CursorCorroborated = true
		}
	}

	return out
}

// BatchKeyViolation names the first served entry whose key is not the one
// the driver sent for the replayable bulk that committed at its log range
// (M/audit.go auditIdempotencyViolation), with the number of entries checked.
func BatchKeyViolation(m Model, entries []match.AuditEntry) (f *Finding, checked int) {
	for _, e := range entries {
		if e.Failed || e.MinLog == 0 {
			continue
		}
		key, known := m.KeyByFirstSeq[e.MinLog]
		if !known {
			continue
		}
		checked++
		if e.IdemKey != key {
			return &Finding{ID: EvAuditWrongBatchKey, Entry: match.DescribeAuditEntry(e),
				Why: "entry carries key " + e.IdemKey + " but that log range was committed under " + key}, checked
		}
	}

	return nil, checked
}

// DuplicateProposalViolation checks, for every key whose only write was
// rejected for good, that its failure entries (on the page and remembered)
// carry distinct proposal ids: one proposal is audited once.
func DuplicateProposalViolation(m Model, known, entries []match.AuditEntry) *Finding {
	if m.FrozenRejected == nil {
		return nil
	}
	bySeq := map[uint64]match.AuditEntry{}
	for _, e := range known {
		bySeq[e.Seq] = e
	}
	keys := map[string]bool{}
	for _, e := range entries {
		bySeq[e.Seq] = e
		if e.Failed && m.Tracked(e) {
			keys[e.IdemKey] = true
		}
	}
	if len(keys) == 0 {
		return nil
	}
	seqs := slices.Sorted(maps.Keys(bySeq))
	qualifies := map[string]bool{}
	for k := range keys {
		qualifies[k] = m.FrozenRejected(k)
	}
	first := map[string]map[uint64]uint64{}
	for _, seq := range seqs {
		e := bySeq[seq]
		if !e.Failed || !qualifies[e.IdemKey] {
			continue
		}
		props := first[e.IdemKey]
		if props == nil {
			props = map[uint64]uint64{}
			first[e.IdemKey] = props
		}
		if prev, dup := props[e.ProposalID]; dup {
			return &Finding{ID: EvAuditFailuresUniquePerPropsl, Entry: match.DescribeAuditEntry(e),
				Why: "proposal " + u(e.ProposalID) + " under key " + e.IdemKey + " is also audited at seq " + u(prev)}
		}
		props[e.ProposalID] = seq
	}

	return nil
}
