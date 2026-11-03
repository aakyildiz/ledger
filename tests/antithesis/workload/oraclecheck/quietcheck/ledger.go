package quietcheck

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// LedgerBody is the body of a "ledger" section chunk.
type LedgerBody struct {
	Ledger string `json:"ledger"`
	// Scope is "linearizable" or "node:<address>" (stale reads pinned to that
	// node after it reached the barrier horizon).
	Scope string `json:"scope"`
	// Part is logs | transactions | accounts | stats | info.
	Part string `json:"part"`
	// Items are protojson rows of the part, in server order.
	Items []json.RawMessage `json:"items"`
	// Final marks the last chunk of this part.
	Final bool `json:"final"`
}

// Scopes and parts.
const (
	ScopeLinearizable = "linearizable"
	ScopeNodePrefix   = "node:"

	PartLogs         = "logs"
	PartTransactions = "transactions"
	PartAccounts     = "accounts"
	PartStats        = "stats"
	PartInfo         = "info"
)

// LinearizableParts are the parts the linearizable scope checks; NodeParts
// those a node scope checks (usage counters are asynchronous and the stats
// boundaries have no per-node literal, so a node posts no stats).
var (
	LinearizableParts = []string{PartLogs, PartTransactions, PartAccounts, PartStats, PartInfo}
	NodeParts         = []string{PartLogs, PartTransactions, PartAccounts, PartInfo}
)

// rowRef is one served log as the global stream check needs it.
type rowRef struct {
	ledger   string
	id, seq  uint64
	date     uint64
	hasDate  bool
	position int // position in the served listing
}

type partIDs struct{ logs, txs, accts, stats, info oc.EventID }

var (
	linearizableIDs = partIDs{EvQuietLogsDiffer, EvQuietTxsDiffer, EvQuietAcctsDiffer, EvQuietStatsDiffer, EvQuietInfoDiffers}
	nodeIDs         = partIDs{EvNodeLogsDiffer, EvNodeTxsDiffer, EvNodeAcctsDiffer, "", EvNodeInfoDiffers}
)

// ledgerSection accumulates one chunk and checks the part at final.
func ledgerSection(t *oc.Tracker, epoch, _ uint64, raw json.RawMessage) (any, []oc.Event, *oc.Refusal) {
	var body LedgerBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, nil, oc.Protocol("cannot decode the ledger section body", map[string]any{"error": err.Error()})
	}
	d := map[string]any{"ledger": body.Ledger, "scope": body.Scope, "part": body.Part}
	node := strings.HasPrefix(body.Scope, ScopeNodePrefix) && len(body.Scope) > len(ScopeNodePrefix)
	if body.Scope != ScopeLinearizable && !node {
		return nil, nil, oc.Protocol("unknown ledger section scope", d)
	}
	parts := LinearizableParts
	if node {
		parts = NodeParts
	}
	if !slices.Contains(parts, body.Part) {
		return nil, nil, oc.Protocol("unknown ledger section part for this scope", d)
	}
	model := t.Model()
	if lc, exists := model.Lifecycle(body.Ledger); !exists || lc.Deleted || !strings.HasPrefix(body.Ledger, t.Config().TrackedPrefix) {
		return nil, nil, oc.Protocol("the ledger is not a live tracked ledger of the model", d)
	}

	st := load(t, epoch)
	pk := partKey{body.Ledger, body.Scope, body.Part}
	st.parts[pk] = append(st.parts[pk], body.Items...)
	if !body.Final {
		return nil, nil, nil
	}
	items := st.parts[pk]
	delete(st.parts, pk)

	ids := linearizableIDs
	if node {
		ids = nodeIDs
	}
	ls := model.Ledger(body.Ledger)
	var evs []oc.Event
	var why string
	var id oc.EventID
	switch body.Part {
	case PartLogs:
		id = ids.logs
		var logs []*commonpb.Log
		logs, why = decodeAll(items, func() *commonpb.Log { return &commonpb.Log{} })
		if why == "" {
			why = logsDiffer(ls, body.Ledger, logs)
		}
		if why == "" && !node {
			st.servedLogs[body.Ledger] = refsOf(logs)
		}
	case PartTransactions:
		id = ids.txs
		var txs []*commonpb.Transaction
		txs, why = decodeAll(items, func() *commonpb.Transaction { return &commonpb.Transaction{} })
		if why == "" {
			why = txsDiffer(ls, txs)
		}
	case PartAccounts:
		id = ids.accts
		var accts []*commonpb.Account
		accts, why = decodeAll(items, func() *commonpb.Account { return &commonpb.Account{} })
		if why == "" {
			why = accountsDiffer(ls, accts)
		}
	case PartStats:
		id = ids.stats
		var stats []*commonpb.LedgerStats
		stats, why = decodeAll(items, func() *commonpb.LedgerStats { return &commonpb.LedgerStats{} })
		if why == "" && len(stats) != 1 {
			why = fmt.Sprintf("%d stats rows posted, want 1", len(stats))
		}
		if why == "" {
			why, evs = statsDiffer(ls, body.Ledger, stats[0])
		}
	case PartInfo:
		id = ids.info
		var infos []*commonpb.LedgerInfo
		infos, why = decodeAll(items, func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} })
		if why == "" && len(infos) != 1 {
			why = fmt.Sprintf("%d ledger info rows posted, want 1", len(infos))
		}
		if why == "" {
			why = infoDiffers(t, model, body.Ledger, infos[0])
		}
	}

	sk := scopeKey{body.Ledger, body.Scope}
	if why != "" {
		d["why"] = why
		d["items"] = len(items)
		evs = append(evs, oc.Violation(id, d))
	} else {
		if st.verified[sk] == nil {
			st.verified[sk] = map[string]bool{}
		}
		st.verified[sk][body.Part] = true
		if !st.reported[sk] && len(st.verified[sk]) == len(parts) {
			st.reported[sk] = true
			done := map[string]any{"ledger": body.Ledger, "scope": body.Scope, "txs": ls.Txs().Len(), "logs": len(ls.LogRows())}
			if node {
				evs = append(evs, oc.Reached(EvQuietNodeStateVerified, done))
			} else {
				evs = append(evs, oc.Reached(EvQuietExactStateVerified, done))
			}
		}
	}

	if !node && body.Part == PartLogs {
		evs = append(evs, st.maybeGlobal(t, model)...)
	}

	return nil, evs, nil
}

func decodeAll[M proto.Message](items []json.RawMessage, mk func() M) ([]M, string) {
	out := make([]M, 0, len(items))
	for i, r := range items {
		m := mk()
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(r, m); err != nil {
			return nil, fmt.Sprintf("row %d does not decode: %v", i, err)
		}
		out = append(out, m)
	}

	return out, ""
}

// logsDiffer compares the whole served log listing (ascending) with every
// committed log of the model: id, kind, payload, embedded transaction,
// sequence, date and volume annotations (LogRowsMatch, page size ∞).
func logsDiffer(ls oracle.LedgerState, ledger string, logs []*commonpb.Log) string {
	rows := match.LogWindowRows(ls, ledger, nil, 0, false)
	page := match.ServerLogRows(logs)
	if match.LogRowsMatch(ledger, rows, math.MaxInt, page) && len(rows) == len(page) {
		return ""
	}
	n := min(len(rows), len(page))
	for i := range n {
		if rows[i].ID != page[i].ID || !match.LogRowMatches(ledger, rows[i], page[i]) {
			return fmt.Sprintf("model has %d logs, served %d; first difference at position %d: model id %d kind %s seq %d, served id %d kind %s seq %d",
				len(rows), len(page), i, rows[i].ID, rows[i].Kind, rows[i].Sequence, page[i].ID, page[i].Kind, page[i].Sequence)
		}
	}

	return fmt.Sprintf("model has %d logs, served %d (the common prefix agrees)", len(rows), len(page))
}

// txsDiffer compares every served transaction with the model's record of it
// (TxRecordMatches), and the count with Txs().Len(). Order-insensitive.
func txsDiffer(ls oracle.LedgerState, txs []*commonpb.Transaction) string {
	n := ls.Txs().Len()
	seen := map[uint64]bool{}
	for _, tx := range txs {
		id := tx.GetId()
		switch {
		case id == 0 || id > uint64(n):
			return fmt.Sprintf("served transaction %d is not in the model (%d transactions)", id, n)
		case seen[id]:
			return fmt.Sprintf("transaction %d is served twice", id)
		}
		seen[id] = true
		if !match.TxRecordMatches(ls.Txs().Get(int(id-1)), tx) {
			return fmt.Sprintf("transaction %d differs from the model's record", id)
		}
	}
	if len(txs) != n {
		return fmt.Sprintf("model has %d transactions, served %d", n, len(txs))
	}

	return ""
}

// purgeable reports whether the model's address matches an ephemeral or
// transient account type, whose zero-balance cells the server purges.
func purgeable(ls oracle.LedgerState, addr string) bool {
	ty := ls.MatchAddress(addr)

	return ty != nil && (ty.Persistence == commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL ||
		ty.Persistence == commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT)
}

// modelCells is addr's volume cells; tolerated lists the zero-balance cells
// of a purgeable address, which the server may already have dropped.
func modelCells(ls oracle.LedgerState, addr string) (cells map[match.AssetColor]oracle.VolumePair, tolerated map[match.AssetColor]bool) {
	cells, tolerated = map[match.AssetColor]oracle.VolumePair{}, map[match.AssetColor]bool{}
	purge := purgeable(ls, addr)
	for k, vp := range ls.Volumes().From(oracle.VolumeKey{Address: addr}) {
		if k.Address != addr {
			break
		}
		key := match.AssetColor{Asset: k.Asset, Color: k.Color}
		cells[key] = vp
		if purge && vp.Input.Cmp(&vp.Output) == 0 {
			tolerated[key] = true
		}
	}

	return cells, tolerated
}

// accountMatchesTolerant is AccountMatches, except that purged zero cells of
// a purgeable address may be absent from the served account (model-reads
// §3.6, §5.13).
func accountMatchesTolerant(ls oracle.LedgerState, addr string, a *commonpb.Account) bool {
	if match.AccountMatches(ls, addr, a) {
		return true
	}
	if !match.MetadataMatches(ls, addr, a.GetMetadata()) || a.GetFirstUsage() != nil || a.GetInsertionDate() != nil || a.GetUpdatedAt() != nil {
		return false
	}
	got, ok := match.AccountVolumeSet(a)
	if !ok {
		return false
	}
	cells, tolerated := modelCells(ls, addr)
	for k, vp := range got {
		want, in := cells[k]
		if !in || want.Input.Cmp(&vp.Input) != 0 || want.Output.Cmp(&vp.Output) != 0 {
			return false
		}
	}
	for k := range cells {
		if _, served := got[k]; !served && !tolerated[k] {
			return false
		}
	}

	return true
}

// accountsDiffer requires the listed set to be AccountUniverse (an account
// whose only state is purged zero cells may be absent) and every listed
// account to match the model.
func accountsDiffer(ls oracle.LedgerState, accts []*commonpb.Account) string {
	universe := match.AccountUniverse(ls)
	inUniverse := make(map[string]bool, len(universe))
	for _, a := range universe {
		inUniverse[a] = true
	}
	served := map[string]bool{}
	for _, a := range accts {
		addr := a.GetAddress()
		switch {
		case served[addr]:
			return fmt.Sprintf("account %s is listed twice", addr)
		case !inUniverse[addr]:
			return fmt.Sprintf("account %s is listed but holds no state in the model", addr)
		case !accountMatchesTolerant(ls, addr, a):
			return fmt.Sprintf("account %s differs from the model", addr)
		}
		served[addr] = true
	}
	for _, addr := range universe {
		if served[addr] {
			continue
		}
		cells, tolerated := modelCells(ls, addr)
		if len(ls.AccountMetadata(addr)) > 0 || len(cells) != len(tolerated) {
			return fmt.Sprintf("account %s of the model is not listed (%d listed, %d in the model)", addr, len(accts), len(universe))
		}
	}

	return ""
}

// statsDiffer checks the exact boundaries and bounds the usage counters by
// the model (usage counters are asynchronous, model-reads §4.13).
func statsDiffer(ls oracle.LedgerState, ledger string, s *commonpb.LedgerStats) (string, []oc.Event) {
	if s.GetNumscriptExecutionCount() != 0 {
		return fmt.Sprintf("numscript executions counted: %d", s.GetNumscriptExecutionCount()), nil
	}
	if !match.StatsBoundsMatch(ls, s) {
		return fmt.Sprintf("served transactionCount %d logCount %d, model %d and %d",
			s.GetTransactionCount(), s.GetLogCount(), ls.Txs().Len(), len(ls.LogDates())), nil
	}
	served, model := match.ServedStatsUsage(s), match.ModelUsage(ls)
	d := map[string]any{"ledger": ledger, "served": fmt.Sprintf("%+v", served), "model": fmt.Sprintf("%+v", model)}
	if !match.StatsUsageWithin(ls, served) {
		return "", []oc.Event{oc.Violation(EvQuietStatsUsageAboveModel, d)}
	}

	return "", []oc.Event{oc.Sometimes(EvQuietStatsUsageCaughtUp, served == model, d)}
}

// infoDiffers compares GetLedger's answer with the model and the identity the
// ledger's creation log reported.
func infoDiffers(t *oc.Tracker, model oracle.GlobalState, ledger string, info *commonpb.LedgerInfo) string {
	if info.GetName() != ledger {
		return fmt.Sprintf("served ledger %q for %q", info.GetName(), ledger)
	}
	if v := match.LedgerInfoStructureViolation(info); v != "" {
		return v
	}
	if !match.LedgerInfoMatches(model, info) {
		return "the ledger info differs from the model (chart, metadata, schema, mode or enforcement)"
	}
	if id, known := t.LedgerIdentities()[ledger]; known && !id.MatchIdentity().Matches(info) {
		return fmt.Sprintf("identity id %d created %d, creation log said id %d created %d",
			info.GetId(), info.GetCreatedAt().GetData(), id.ID, id.CreatedAt.GetData())
	}

	return ""
}

func refsOf(logs []*commonpb.Log) []rowRef {
	out := make([]rowRef, 0, len(logs))
	for i, r := range match.ServerLogRows(logs) {
		out = append(out, rowRef{ledger: r.Ledger, id: r.ID, seq: r.Sequence, date: r.Date, hasDate: r.HasDate, position: i})
	}

	return out
}

// maybeGlobal runs the global stream check once every live tracked ledger's
// linearizable log listing matched the model in this epoch.
func (st *state) maybeGlobal(t *oc.Tracker, model oracle.GlobalState) []oc.Event {
	if st.globalDone {
		return nil
	}
	prefix := t.Config().TrackedPrefix
	for _, l := range model.LiveLedgers() {
		if _, ok := st.servedLogs[l]; !ok && strings.HasPrefix(l, prefix) {
			return nil
		}
	}
	st.globalDone = true

	return globalStream(st.servedLogs, model, t.CommittedBulks(), t.CommittedLogs())
}

const maxProblems = 8

// globalStream checks the union of the served log listings against the
// learned global stream: learned sequences map 1:1 to served logs, per-ledger
// sequences strictly increase with the log id, each committed bulk's logs
// occupy a contiguous sequence range, and log dates never decrease along the
// global sequence.
func globalStream(served map[string][]rowRef, model oracle.GlobalState, bulks map[uint64]oc.CommittedBulk,
	logs map[uint64]oc.CommittedLog,
) []oc.Event {
	var problems []string
	note := func(f string, a ...any) {
		if len(problems) < maxProblems {
			problems = append(problems, fmt.Sprintf(f, a...))
		}
	}

	bySeq := map[uint64]rowRef{}
	var all []rowRef
	ledgers := make([]string, 0, len(served))
	for l := range served {
		ledgers = append(ledgers, l)
	}
	slices.Sort(ledgers)
	for _, l := range ledgers {
		rows := slices.Clone(served[l])
		sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })
		for i, r := range rows {
			if i > 0 && r.seq <= rows[i-1].seq {
				note("ledger %s: log %d has sequence %d, not above log %d's %d", l, r.id, r.seq, rows[i-1].id, rows[i-1].seq)
			}
			if prev, dup := bySeq[r.seq]; dup {
				note("sequence %d is served by %s/%d and %s/%d", r.seq, prev.ledger, prev.id, r.ledger, r.id)

				continue
			}
			bySeq[r.seq] = r
			all = append(all, r)
			learned, ok := model.Log(r.seq)
			if !ok || learned.Ledger != r.ledger || learned.ID != r.id {
				note("served %s/%d at sequence %d, learned %s/%d (known %v)", r.ledger, r.id, r.seq, learned.Ledger, learned.ID, ok)
			}
		}
	}
	for seq, g := range model.Logs() {
		if _, isServed := served[g.Ledger]; !isServed {
			continue
		}
		if r, ok := bySeq[seq]; !ok || r.ledger != g.Ledger || r.id != g.ID {
			note("learned %s/%d at sequence %d is not served there", g.Ledger, g.ID, seq)
		}
	}
	mins := make([]uint64, 0, len(bulks))
	for m := range bulks {
		mins = append(mins, m)
	}
	slices.Sort(mins)
	for _, m := range mins {
		b := bulks[m]
		for s := b.MinSeq; s <= b.MaxSeq && s != 0; s++ {
			cl, ok := logs[s]
			if !ok {
				note("bulk %s [%d,%d]: sequence %d belongs to none of its logs", b.Wid, b.MinSeq, b.MaxSeq, s)

				break
			}
			if _, isServed := served[cl.Ledger]; !isServed {
				continue
			}
			if r, ok := bySeq[s]; !ok || r.ledger != cl.Ledger || r.id != cl.ID {
				note("bulk %s [%d,%d]: %s/%d at sequence %d is not served there", b.Wid, b.MinSeq, b.MaxSeq, cl.Ledger, cl.ID, s)
			}
		}
	}

	var evs []oc.Event
	if len(problems) > 0 {
		evs = append(evs, oc.Violation(EvQuietGlobalStreamDiffers, map[string]any{"problems": problems, "served": len(all)}))
	}

	sort.Slice(all, func(i, j int) bool { return all[i].seq < all[j].seq })
	var last *rowRef
	for i := range all {
		r := &all[i]
		if !r.hasDate {
			continue
		}
		if last != nil && r.date < last.date {
			evs = append(evs, oc.Violation(EvQuietLogDatesDecrease, map[string]any{
				"before": map[string]any{"ledger": last.ledger, "id": last.id, "sequence": last.seq, "date": last.date},
				"after":  map[string]any{"ledger": r.ledger, "id": r.id, "sequence": r.seq, "date": r.date},
			}))

			break
		}
		last = r
	}

	return evs
}
