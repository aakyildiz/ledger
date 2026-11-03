// Package matchtest renders the responses a correct server would return for a
// model state (oracle.GlobalState), so read-family tests can build "the
// model-consistent response passes, a one-field mutation fails" cases without
// each writing its own renderer. Every renderer is pure and deterministic, and
// its output passes the corresponding match predicate (see render_test.go).
//
// Values the model does not derive are filled deterministically: a ledger's
// id and created_at come from SyntheticIdentity unless given, and log dates or
// transaction timestamps the model has not learned are left nil.
package matchtest

import (
	"hash/fnv"
	"math/big"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ---- accounts ----

// RenderAccount is GetAccount's answer for address on ledger: its metadata and
// its volume cells (colours summed into the uncolored bucket when
// collapseColors), sorted by asset then colour, each with its balance. An
// account the model does not know renders with its address only.
func RenderAccount(gs oracle.GlobalState, ledger, address string, collapseColors bool) *commonpb.Account {
	ls := gs.Ledger(ledger)

	acct := &commonpb.Account{Address: address}

	if meta := ls.AccountMetadata(address); len(meta) > 0 {
		acct.Metadata = meta
	}

	sums := map[match.AssetColor]*oracle.VolumePair{}
	var keys []match.AssetColor

	for k, vp := range ls.Volumes().From(oracle.VolumeKey{Address: address}) {
		if k.Address != address {
			break
		}

		key := match.AssetColor{Asset: k.Asset, Color: k.Color}
		if collapseColors {
			key.Color = ""
		}

		p := sums[key]
		if p == nil {
			p = &oracle.VolumePair{}
			sums[key] = p
			keys = append(keys, key)
		}

		p.Input.Add(&p.Input, &vp.Input)
		p.Output.Add(&p.Output, &vp.Output)
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Asset != keys[j].Asset {
			return keys[i].Asset < keys[j].Asset
		}

		return keys[i].Color < keys[j].Color
	})

	for _, key := range keys {
		p := sums[key]
		acct.Volumes = append(acct.Volumes, &commonpb.AccountVolume{
			Asset: key.Asset,
			Color: key.Color,
			Volumes: &commonpb.VolumesWithBalance{
				Input:   commonpb.NewBigUintFromUint256(&p.Input),
				Output:  commonpb.NewBigUintFromUint256(&p.Output),
				Balance: balance(&p.Input, &p.Output),
			},
		})
	}

	return acct
}

func balance(in, out *uint256.Int) *commonpb.SignedBigInt {
	b := new(big.Int).Sub(in.ToBig(), out.ToBig())

	return commonpb.MustSignedBigIntFromDecimal(b.String())
}

// AccountPageReq is a ListAccounts request as the matchers read it: Cursor is
// the decoded key (an address; "" for the first page) and PageSize the
// requested size (before match.EffectivePageSize).
type AccountPageReq struct {
	Filter   *commonpb.QueryFilter
	Cursor   string
	PageSize uint32
	Reverse  bool
}

// RenderAccountPage is ListAccounts' page: the model's window
// (match.AccountWindow) at the effective page size, each row rendered by
// RenderAccount, and the next-cursor token (match.PageToken of the last
// address) when a further row is waiting, "" otherwise.
func RenderAccountPage(gs oracle.GlobalState, ledger string, req AccountPageReq) (page []*commonpb.Account, nextToken string) {
	ls := gs.Ledger(ledger)
	pageSize := match.EffectivePageSize(int(req.PageSize))

	window := match.AccountWindow(ls, req.Filter, req.Cursor, pageSize+1, req.Reverse)

	more := len(window) > pageSize
	if more {
		window = window[:pageSize]
	}

	for _, addr := range window {
		page = append(page, RenderAccount(gs, ledger, addr, false))
	}

	if more {
		nextToken = match.PageToken(match.LastAccountKey(page))
	}

	return page, nextToken
}

// ---- transactions ----

// RenderTxRecord renders one model transaction record the way the server
// sends it: every pinned field, updated_at equal to inserted_at, and the
// post-commit volumes sorted.
func RenderTxRecord(rec match.TxRecordView) *commonpb.Transaction {
	return &commonpb.Transaction{
		Id:                    rec.Id(),
		Reference:             rec.Reference(),
		Reverted:              rec.Reverted(),
		RevertedByTransaction: rec.RevertedBy(),
		RevertsTransaction:    rec.RevertsTransaction(),
		Timestamp:             rec.Timestamp(),
		InsertedAt:            rec.InsertedAt(),
		// Stamped from the same proposal date as inserted_at and never bumped.
		UpdatedAt:         rec.InsertedAt(),
		RevertedAt:        rec.RevertedAt(),
		Postings:          rec.Postings(),
		Metadata:          rec.Metadata(),
		PostCommitVolumes: renderPCV(rec.PostCommitVolumes()),
	}
}

func renderPCV(model map[oracle.VolumeKey]oracle.VolumePair) *commonpb.PostCommitVolumes {
	if len(model) == 0 {
		return nil
	}

	byAccount := map[string]*commonpb.VolumesByAssets{}

	for key, vp := range model {
		entry := byAccount[key.Address]
		if entry == nil {
			entry = &commonpb.VolumesByAssets{}
			byAccount[key.Address] = entry
		}

		entry.Volumes = append(entry.Volumes, &commonpb.VolumeEntry{
			Asset: key.Asset,
			Color: key.Color,
			Volumes: &commonpb.Volumes{
				Input:  commonpb.NewBigUintFromUint256(&vp.Input),
				Output: commonpb.NewBigUintFromUint256(&vp.Output),
			},
		})
	}

	out := &commonpb.PostCommitVolumes{VolumesByAccount: byAccount}
	out.SortVolumes()

	return out
}

// RenderTx is GetTransaction's transaction for id on ledger, nil when the
// model holds no such transaction.
func RenderTx(gs oracle.GlobalState, ledger string, id uint64) *commonpb.Transaction {
	txs := gs.Ledger(ledger).Txs()
	if id == 0 || id > uint64(txs.Len()) {
		return nil
	}

	return RenderTxRecord(txs.Get(int(id - 1)))
}

// TxPageReq is a ListTransactions request as the matchers read it: AfterID is
// the decoded cursor (0 for the first page).
type TxPageReq struct {
	Filter   *commonpb.QueryFilter
	AfterID  uint64
	PageSize uint32
	Reverse  bool
}

// RenderTxPage is ListTransactions' page: the required rows of the model's
// window (match.TransactionWindowRows; rows whose filter verdict hinges on an
// unlearned stamp are left out, which is legal), truncated at the effective
// page size, and the next-cursor token when a required row is left behind.
func RenderTxPage(gs oracle.GlobalState, ledger string, req TxPageReq) (page []*commonpb.Transaction, nextToken string) {
	ls := gs.Ledger(ledger)
	pageSize := match.EffectivePageSize(int(req.PageSize))

	more := false

	for _, row := range match.TransactionWindowRows(ls, req.Filter, req.AfterID, req.Reverse) {
		if !row.Required {
			continue
		}

		if len(page) == pageSize {
			more = true

			break
		}

		page = append(page, RenderTx(gs, ledger, row.ID))
	}

	if more {
		nextToken = match.PageToken(match.LastTransactionKey(page))
	}

	return page, nextToken
}

// ---- logs ----

// RenderLog is the ledger log id of ledger as ListLogs and GetLog serve it:
// payload rebuilt from the model's canonical rendering, the transaction it
// announces, the learned date and global sequence (nil/0 while unlearned), and
// the three volume annotations. ok is false for an id the model does not hold
// or a payload this renderer cannot rebuild (order_skipped logs).
func RenderLog(gs oracle.GlobalState, ledger string, id uint64) (log *commonpb.Log, ok bool) {
	ls := gs.Ledger(ledger)

	for _, row := range ls.LogRows() {
		if row.ID == id {
			return renderLogRow(ls, ledger, row)
		}
	}

	return nil, false
}

func renderLogRow(ls oracle.LedgerState, ledger string, row oracle.LogRow) (*commonpb.Log, bool) {
	data, ok := renderLogPayload(ls, row)
	if !ok {
		return nil, false
	}

	entry := &commonpb.LedgerLog{
		Data:             data,
		Date:             row.Date,
		Id:               row.ID,
		PurgedVolumes:    parseTouched(row.PurgedVolumes),
		NewKeptVolumes:   parseTouched(row.NewKeptVolumes),
		EphemeralVolumes: parseTouched(row.EphemeralVolumes),
	}

	return &commonpb.Log{
		Sequence: row.Sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: ledger,
			Log:        entry,
		}}},
	}, true
}

// parseTouched inverts the oracle's "account:asset:color" rendering. Account
// addresses may contain ':', asset and colour never do.
func parseTouched(s string) []*commonpb.TouchedVolume {
	if s == "" {
		return nil
	}

	var out []*commonpb.TouchedVolume

	for _, part := range strings.Split(s, ",") {
		c := strings.LastIndexByte(part, ':')
		a := strings.LastIndexByte(part[:c], ':')
		out = append(out, &commonpb.TouchedVolume{Account: part[:a], Asset: part[a+1 : c], Color: part[c+1:]})
	}

	return out
}

func renderLogPayload(ls oracle.LedgerState, row oracle.LogRow) (*commonpb.LedgerLogPayload, bool) {
	fields := payloadFields(row.Payload)

	switch row.Kind {
	case "created_transaction":
		rec := match.ModelTxForLog(ls, row.TxID)
		if rec == nil {
			return nil, false
		}

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{Transaction: RenderTxRecord(rec)},
		}}, true
	case "reverted_transaction":
		rec := match.ModelTxForLog(ls, row.TxID)
		if rec == nil {
			return nil, false
		}

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: rec.RevertsTransaction(),
				RevertTransaction:     RenderTxRecord(rec),
			},
		}}, true
	case "updated_default_enforcement_mode":
		mode, _ := strconv.Atoi(fields["mode"])

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{
			UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{EnforcementMode: commonpb.ChartEnforcementMode(mode)},
		}}, true
	case "added_account_type":
		name, pattern, _ := strings.Cut(strings.TrimPrefix(row.Payload, "type="), "|pattern=")

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: &commonpb.AccountType{Name: name, Pattern: pattern}},
		}}, true
	case "removed_account_type":
		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
			RemovedAccountType: &commonpb.RemovedAccountTypeLog{Name: strings.TrimPrefix(row.Payload, "type=")},
		}}, true
	case "set_metadata_field_type":
		target, _ := strconv.Atoi(fields["target"])
		typ, _ := strconv.Atoi(fields["type"])

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
			SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
				TargetType: commonpb.TargetType(target), Key: fields["key"], Type: commonpb.MetadataType(typ),
			},
		}}, true
	case "removed_metadata_field_type":
		target, _ := strconv.Atoi(fields["target"])

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{
			RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{TargetType: commonpb.TargetType(target), Key: fields["key"]},
		}}, true
	case "create_index", "drop_index":
		id, err := indexes.ParseCanonical(strings.TrimPrefix(row.Payload, "index="))
		if err != nil {
			return nil, false
		}

		if row.Kind == "create_index" {
			return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{
				CreateIndex: &commonpb.CreatedIndexLog{Id: id},
			}}, true
		}

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DropIndex{
			DropIndex: &commonpb.DroppedIndexLog{Id: id},
		}}, true
	case "saved_metadata":
		target, rest, _ := strings.Cut(strings.TrimPrefix(row.Payload, "target="), "|")
		meta, ok := parseMetadata(rest)
		if !ok {
			return nil, false
		}

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{Target: parseTarget(target), Metadata: meta},
		}}, true
	case "deleted_metadata":
		target, key, _ := strings.Cut(strings.TrimPrefix(row.Payload, "target="), "|key=")

		return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{Target: parseTarget(target), Key: key},
		}}, true
	default:
		return nil, false
	}
}

// payloadFields splits a "k=v|k=v" rendering.
func payloadFields(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, "|") {
		if k, v, ok := strings.Cut(part, "="); ok {
			out[k] = v
		}
	}

	return out
}

func parseTarget(s string) *commonpb.Target {
	if addr, ok := strings.CutPrefix(s, "acct:"); ok {
		return &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: addr}}}
	}

	id, _ := strconv.ParseUint(strings.TrimPrefix(s, "tx:"), 10, 64)

	return &commonpb.Target{Target: &commonpb.Target_TransactionId{TransactionId: id}}
}

// parseMetadata inverts the oracle's "k=<tag>:<value>,…" rendering. Values
// containing ',' cannot be inverted (ok=false).
func parseMetadata(s string) (map[string]*commonpb.MetadataValue, bool) {
	out := map[string]*commonpb.MetadataValue{}
	if s == "" {
		return out, true
	}

	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, false
		}

		tag, val, ok := strings.Cut(v, ":")
		if !ok {
			return nil, false
		}

		mv := &commonpb.MetadataValue{}

		switch tag {
		case "s":
			mv.Type = &commonpb.MetadataValue_StringValue{StringValue: val}
		case "i":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, false
			}
			mv.Type = &commonpb.MetadataValue_IntValue{IntValue: n}
		case "u":
			n, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return nil, false
			}
			mv.Type = &commonpb.MetadataValue_UintValue{UintValue: n}
		case "b":
			mv.Type = &commonpb.MetadataValue_BoolValue{BoolValue: val == "true"}
		case "n":
			mv.Type = &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{Original: val}}
		case "d":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, false
			}
			mv.Type = &commonpb.MetadataValue_DatetimeValue{DatetimeValue: n}
		default:
			return nil, false
		}

		out[k] = mv
	}

	return out, true
}

// LogPageReq is a ListLogs request as the matchers read it: AfterID is the
// decoded cursor (a ledger-local log id; 0 for the first page).
type LogPageReq struct {
	Filter   *commonpb.QueryFilter
	AfterID  uint64
	PageSize uint32
	Reverse  bool
}

// RenderLogPage is ListLogs' page: the required rows of the model's window
// (match.LogWindowRows), each rendered by RenderLog, truncated at the
// effective page size, with the next-cursor token when a required row is left
// behind. ok is false when a row could not be rendered.
func RenderLogPage(gs oracle.GlobalState, ledger string, req LogPageReq) (page []*commonpb.Log, nextToken string, ok bool) {
	ls := gs.Ledger(ledger)
	pageSize := match.EffectivePageSize(int(req.PageSize))

	rows := map[uint64]oracle.LogRow{}
	for _, r := range ls.LogRows() {
		rows[r.ID] = r
	}

	more := false

	for _, row := range match.LogWindowRows(ls, ledger, req.Filter, req.AfterID, req.Reverse) {
		if !row.Required {
			continue
		}

		if len(page) == pageSize {
			more = true

			break
		}

		l, rendered := renderLogRow(ls, ledger, rows[row.ID])
		if !rendered {
			return nil, "", false
		}

		page = append(page, l)
	}

	if more {
		nextToken = match.PageToken(match.LastLogKey(match.ServerLogRows(page)))
	}

	return page, nextToken, true
}

// ---- ledgers ----

// SyntheticIdentity is the deterministic (id, created_at) RenderLedgerInfo
// stamps on a ledger whose identity the caller does not supply: the model
// derives neither, so tests only need a stable, non-zero pair.
func SyntheticIdentity(ledger string) match.LedgerIdentity {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ledger))

	return match.LedgerIdentity{ID: h.Sum32() | 1, CreatedAt: &commonpb.Timestamp{Data: uint64(h.Sum32()) + 1}}
}

// RenderLedgerInfo is GetLedger's LedgerInfo for ledger: chart, ledger
// metadata, metadata schema, mode, mirror source and progress presence,
// default enforcement, with SyntheticIdentity's id and created_at. nil when
// the model holds no live ledger of that name.
func RenderLedgerInfo(gs oracle.GlobalState, ledger string) *commonpb.LedgerInfo {
	return RenderLedgerInfoWithIdentity(gs, ledger, SyntheticIdentity(ledger))
}

// RenderLedgerInfoWithIdentity is RenderLedgerInfo with a given identity.
func RenderLedgerInfoWithIdentity(gs oracle.GlobalState, ledger string, id match.LedgerIdentity) *commonpb.LedgerInfo {
	lc, exists := gs.Lifecycle(ledger)
	if !exists || lc.Deleted {
		return nil
	}

	ls := gs.Ledger(ledger)

	info := &commonpb.LedgerInfo{
		Name:                   ledger,
		Id:                     id.ID,
		CreatedAt:              id.CreatedAt,
		Mode:                   lc.Mode,
		MirrorSource:           lc.MirrorSource,
		DefaultEnforcementMode: ls.DefaultEnforcementMode(),
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields:     schemaFields(ls.AccountFieldTypes()),
			TransactionFields: schemaFields(ls.TransactionFieldTypes()),
			LedgerFields:      schemaFields(ls.LedgerFieldTypes()),
		},
	}

	if lc.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		info.MirrorSyncProgress = &commonpb.MirrorSyncProgress{}
	}

	if ls.Types().Len() > 0 {
		info.AccountTypes = map[string]*commonpb.AccountType{}
		for name, t := range ls.Types().All() {
			info.AccountTypes[name] = &commonpb.AccountType{Name: name, Pattern: t.Pattern, Persistence: t.Persistence}
		}
	}

	if ls.LedgerMeta().Len() > 0 {
		info.Metadata = map[string]*commonpb.MetadataValue{}
		for k, v := range ls.LedgerMeta().All() {
			info.Metadata[k] = v
		}
	}

	return info
}

func schemaFields(m oracle.Map[string, commonpb.MetadataType]) map[string]*commonpb.MetadataFieldSchema {
	if m.Len() == 0 {
		return nil
	}

	out := map[string]*commonpb.MetadataFieldSchema{}
	for k, t := range m.All() {
		out[k] = &commonpb.MetadataFieldSchema{Type: t}
	}

	return out
}

// RenderLedgerPage is ListLedgers' page over the model's live ledgers with the
// tracked prefix plus the foreign ledger names (which the oracle does not
// track; rendered as bare rows with SyntheticIdentity), interleaved in name
// order the way the server lists them, past the request cursor, capped at the
// effective page size, with the next-cursor token when more names follow.
// A request cursor that is not a forward ledger cursor yields an empty page.
func RenderLedgerPage(gs oracle.GlobalState, prefix string, foreign []string, req match.ListLedgersReq) (page []*commonpb.LedgerInfo, nextToken string) {
	cur, err := pagecursorDecode(req.Cursor)
	if err != nil {
		return nil, ""
	}

	names := slices.Clone(foreign)
	for _, name := range gs.LiveLedgers() {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}

	pageSize := match.EffectivePageSize(int(req.PageSize))
	window, more := match.LedgerWindow(names, cur, pageSize, req.Reverse)

	for _, name := range window {
		if strings.HasPrefix(name, prefix) {
			page = append(page, RenderLedgerInfo(gs, name))

			continue
		}

		id := SyntheticIdentity(name)
		page = append(page, &commonpb.LedgerInfo{Name: name, Id: id.ID, CreatedAt: id.CreatedAt})
	}

	if more == match.CursorRequired {
		nextToken = match.PageToken(match.LastLedgerKey(window))
	}

	return page, nextToken
}

// ---- stats, aggregates, audit ----

// RenderStats is GetLedgerStats' answer with the usage store fully caught up:
// exact boundaries, usage counters equal to the model's, no numscript
// execution. Counters the model does not derive stay zero.
func RenderStats(gs oracle.GlobalState, ledger string) *commonpb.LedgerStats {
	ls := gs.Ledger(ledger)
	u := match.ModelUsage(ls)

	return &commonpb.LedgerStats{
		TransactionCount: uint64(ls.Txs().Len()),
		LogCount:         uint64(len(ls.LogDates())),
		PostingCount:     u.Postings,
		ReferenceCount:   u.References,
		RevertCount:      u.Reverts,
	}
}

// RenderAggregate is AggregateVolumes' result over every account under opts.
func RenderAggregate(gs oracle.GlobalState, ledger string, opts match.AggOptions) *commonpb.AggregateResult {
	return RenderAggregateFiltered(gs, ledger, nil, opts)
}

// RenderAggregateFiltered is RenderAggregate over the accounts filter selects:
// grouped (one group per requested prefix, in request order) when
// opts.GroupByPrefixes is set, flat otherwise; buckets sorted.
func RenderAggregateFiltered(gs oracle.GlobalState, ledger string, filter *commonpb.QueryFilter, opts match.AggOptions) *commonpb.AggregateResult {
	ls := gs.Ledger(ledger)

	if len(opts.GroupByPrefixes) > 0 {
		res := &commonpb.AggregateResult{}
		for _, g := range match.ModelAggregateGroups(ls, filter, opts) {
			res.Groups = append(res.Groups, &commonpb.GroupedAggregateResult{Prefix: g.Prefix, Volumes: aggVolumes(g.Sums)})
		}

		return res
	}

	return &commonpb.AggregateResult{Volumes: aggVolumes(match.ModelAggregate(ls, filter, opts))}
}

func aggVolumes(sums map[match.AssetColor]*match.AggPair) []*commonpb.AggregatedVolume {
	keys := make([]match.AssetColor, 0, len(sums))
	for k := range sums {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	out := make([]*commonpb.AggregatedVolume, 0, len(keys))
	for _, k := range keys {
		p := sums[k]
		out = append(out, &commonpb.AggregatedVolume{
			Asset:  k.Asset,
			Color:  k.Color,
			Input:  commonpb.NewUint256(&p.In),
			Output: commonpb.NewUint256(&p.Out),
		})
	}

	return out
}

// RenderAuditEntry is the successful audit entry the server writes for one
// committed bulk: sequence auditSeq, the bulk's log range and order count, one
// item per log, the ledgers its logs belong to (from logs, the committed-log
// side table), and the idempotency key. Timestamp and proposal id, which the
// model does not pin, are left zero.
func RenderAuditEntry(committed match.CommittedBulk, logs map[uint64]match.CommittedLog, auditSeq uint64, key string) *auditpb.AuditEntry {
	e := &auditpb.AuditEntry{
		Sequence:   auditSeq,
		OrderCount: committed.Orders,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: committed.MinSeq,
			MaxLogSequence: committed.MaxSeq,
		}},
	}

	if key != "" {
		e.Idempotency = &commonpb.Idempotency{Key: key}
	}

	seen := map[string]bool{}

	for seq := committed.MinSeq; seq <= committed.MaxSeq && seq != 0; seq++ {
		e.Items = append(e.Items, &auditpb.AuditItem{OrderIndex: uint32(seq - committed.MinSeq), LogSequence: seq})

		if l, ok := logs[seq]; ok && !seen[l.Ledger] {
			seen[l.Ledger] = true
			e.Ledgers = append(e.Ledgers, l.Ledger)
		}
	}

	slices.Sort(e.Ledgers)

	return e
}
