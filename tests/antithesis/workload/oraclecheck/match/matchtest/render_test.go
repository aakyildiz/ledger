package matchtest

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func clone[T proto.Message](m T) T { return proto.Clone(m).(T) }

// ---- accounts ----

func TestRenderAccountMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	for _, addr := range append(match.AccountUniverse(ls), "t-9:absent") {
		for _, collapsed := range []bool{false, true} {
			acct := RenderAccount(gs, fixtureLedger, addr, collapsed)
			require.True(t, match.AccountMatchesCollapsed(ls, addr, acct, collapsed), "%s collapsed=%v", addr, collapsed)

			vols, ok := match.AccountVolumeSet(acct)
			require.True(t, ok)
			require.True(t, match.AccountVolumesMatch(ls, addr, vols, collapsed))
			require.True(t, match.MetadataMatches(ls, addr, acct.GetMetadata()))
		}
	}

	require.True(t, match.ModelKnowsAccount(ls, "t-0:1"))
	require.False(t, match.ModelKnowsAccount(ls, "t-9:absent"))
}

func TestRenderAccountMutationsFail(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)
	base := RenderAccount(gs, fixtureLedger, "t-0:1", false)
	require.Len(t, base.GetVolumes(), 2)

	cases := map[string]func(a *commonpb.Account){
		"input":          func(a *commonpb.Account) { a.Volumes[0].Volumes.Input = commonpb.MustBigUintFromDecimal("99") },
		"output":         func(a *commonpb.Account) { a.Volumes[0].Volumes.Output = commonpb.MustBigUintFromDecimal("99") },
		"colour":         func(a *commonpb.Account) { a.Volumes[1].Color = "SILVER" },
		"asset":          func(a *commonpb.Account) { a.Volumes[0].Asset = "EUR/2" },
		"dropped cell":   func(a *commonpb.Account) { a.Volumes = a.Volumes[:1] },
		"duplicate cell": func(a *commonpb.Account) { a.Volumes = append(a.Volumes, a.Volumes[0]) },
		"metadata value": func(a *commonpb.Account) { a.Metadata["k0"] = Str("other") },
		"metadata key":   func(a *commonpb.Account) { a.Metadata["k9"] = Str("v0") },
		"first usage":    func(a *commonpb.Account) { a.FirstUsage = &commonpb.Timestamp{Data: 1} },
		"updated at":     func(a *commonpb.Account) { a.UpdatedAt = &commonpb.Timestamp{Data: 1} },
	}

	for name, mutate := range cases {
		a := clone(base)
		mutate(a)
		require.False(t, match.AccountMatches(ls, "t-0:1", a), name)
	}
}

func TestRenderAccountPageMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	for _, reverse := range []bool{false, true} {
		for _, size := range []uint32{0, 1, 2, 3, 1001} {
			// Walk every page through the published token.
			cursor := ""
			for pages := 0; ; pages++ {
				require.Less(t, pages, 20)

				req := AccountPageReq{Cursor: cursor, PageSize: size, Reverse: reverse}
				page, token := RenderAccountPage(gs, fixtureLedger, req)
				eff := match.EffectivePageSize(int(size))
				require.True(t, match.AccountPageMatches(ls, nil, cursor, eff, reverse, page, match.NextCursorOf(token)),
					"reverse=%v size=%d cursor=%q", reverse, size, cursor)

				if token == "" {
					break
				}

				cursor = match.NextCursorOf(token)
			}
		}
	}

	page, token := RenderAccountPage(gs, fixtureLedger, AccountPageReq{PageSize: 2})
	require.NotEmpty(t, token)

	require.False(t, match.AccountPageMatches(ls, nil, "", 2, false, page, ""), "a full page with rows left must carry a token")
	require.False(t, match.AccountPageMatches(ls, nil, "", 2, false, page[:1], match.NextCursorOf(token)), "a dropped row")
	require.False(t, match.AccountPageMatches(ls, nil, "", 2, false, []*commonpb.Account{page[1], page[0]}, match.NextCursorOf(token)), "rows out of order")
}

// ---- transactions ----

func TestRenderTxMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	for id := uint64(1); id <= uint64(ls.Txs().Len()); id++ {
		tx := RenderTx(gs, fixtureLedger, id)
		require.True(t, match.TxRecordMatches(ls.Txs().Get(int(id-1)), tx), "tx %d", id)
		require.True(t, match.PCVSnapshotMatches(ls.Txs().Get(int(id-1)).PostCommitVolumes(), tx.GetPostCommitVolumes()))
		require.True(t, match.LogTxMatches(ls.Txs().Get(int(id-1)), tx))
		require.True(t, match.CheckpointTransactionReadMatches(gs, fixtureLedger, id, tx, true))
	}

	require.Nil(t, RenderTx(gs, fixtureLedger, 99))
	require.True(t, match.CheckpointTransactionReadMatches(gs, fixtureLedger, 99, nil, false))
}

func TestRenderTxMutationsFail(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)
	rec := ls.Txs().Get(0) // reverted, every stamp learned
	base := RenderTx(gs, fixtureLedger, 1)
	require.True(t, base.GetReverted())

	cases := map[string]func(tx *commonpb.Transaction){
		"id":          func(tx *commonpb.Transaction) { tx.Id = 9 },
		"reference":   func(tx *commonpb.Transaction) { tx.Reference = "other" },
		"reverted":    func(tx *commonpb.Transaction) { tx.Reverted = false },
		"reverted by": func(tx *commonpb.Transaction) { tx.RevertedByTransaction = 9 },
		"timestamp":   func(tx *commonpb.Transaction) { tx.Timestamp = &commonpb.Timestamp{Data: 101} },
		"inserted at": func(tx *commonpb.Transaction) {
			tx.InsertedAt = &commonpb.Timestamp{Data: 111}
			tx.UpdatedAt = tx.InsertedAt
		},
		"updated at":  func(tx *commonpb.Transaction) { tx.UpdatedAt = &commonpb.Timestamp{Data: 999} },
		"reverted at": func(tx *commonpb.Transaction) { tx.RevertedAt = &commonpb.Timestamp{Data: 501} },
		"posting":     func(tx *commonpb.Transaction) { tx.Postings[0].Destination = "t-0:9" },
		"metadata":    func(tx *commonpb.Transaction) { tx.Metadata = map[string]*commonpb.MetadataValue{"x": Str("y")} },
		"pcv":         func(tx *commonpb.Transaction) { tx.PostCommitVolumes = nil },
	}

	for name, mutate := range cases {
		tx := clone(base)
		mutate(tx)
		require.False(t, match.TxRecordMatches(rec, tx), name)
	}
}

func TestRenderTxPageMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	for _, reverse := range []bool{false, true} {
		for _, size := range []uint32{0, 1, 2, 3} {
			var after uint64
			for pages := 0; ; pages++ {
				require.Less(t, pages, 20)

				page, token := RenderTxPage(gs, fixtureLedger, TxPageReq{AfterID: after, PageSize: size, Reverse: reverse})
				eff := match.EffectivePageSize(int(size))
				require.True(t, match.TxWindowMatches(ls, nil, after, eff, reverse, page, match.NextCursorOf(token)),
					"reverse=%v size=%d after=%d", reverse, size, after)

				if token == "" {
					break
				}

				after, _ = strconv.ParseUint(match.NextCursorOf(token), 10, 64)
			}
		}
	}

	page, token := RenderTxPage(gs, fixtureLedger, TxPageReq{PageSize: 2})
	require.Len(t, page, 2)
	require.False(t, match.TxWindowMatches(ls, nil, 0, 2, false, page, ""), "a missing token")
	require.False(t, match.TxWindowMatches(ls, nil, 0, 2, false, page[:1], match.NextCursorOf(token)), "a dropped row")
	require.False(t, match.TxWindowMatches(ls, nil, 0, 2, false, []*commonpb.Transaction{page[1], page[0]}, match.NextCursorOf(token)), "rows out of order")
}

// ---- logs ----

func TestRenderLogMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	rows := ls.LogRows()
	require.NotEmpty(t, rows)

	kinds := map[string]bool{}

	for _, row := range rows {
		l, ok := RenderLog(gs, fixtureLedger, row.ID)
		require.True(t, ok, "log %d (%s)", row.ID, row.Kind)
		kinds[row.Kind] = true

		got := match.ServerLogRows([]*commonpb.Log{l})[0]
		require.Equal(t, row.Kind, match.ServerLogKind(l))
		require.True(t, match.LogRowMatches(fixtureLedger, match.LogWindowRowOf(ls, row, true), got), "log %d (%s)", row.ID, row.Kind)

		if row.Sequence != 0 {
			target := match.CommittedLogTarget{Ledger: fixtureLedger, ID: row.ID, Sequence: row.Sequence}
			require.True(t, match.CommittedLogMatches(gs, target, got), "log %d by sequence", row.ID)
		}
	}

	for _, k := range []string{"created_transaction", "reverted_transaction", "saved_metadata", "added_account_type", "set_metadata_field_type", "create_index"} {
		require.True(t, kinds[k], "the fixture covers %s", k)
	}

	_, ok := RenderLog(gs, fixtureLedger, 999)
	require.False(t, ok)
}

func TestRenderLogMutationsFail(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	var dated oracle.LogRow
	for _, row := range ls.LogRows() {
		if row.Date != nil && row.Kind == "created_transaction" {
			dated = row

			break
		}
	}
	require.NotZero(t, dated.ID)

	base, ok := RenderLog(gs, fixtureLedger, dated.ID)
	require.True(t, ok)
	want := match.LogWindowRowOf(ls, dated, true)

	cases := map[string]func(l *commonpb.Log){
		"ledger":   func(l *commonpb.Log) { l.GetPayload().GetApply().LedgerName = "other" },
		"date":     func(l *commonpb.Log) { l.GetPayload().GetApply().GetLog().Date = &commonpb.Timestamp{Data: 1} },
		"no date":  func(l *commonpb.Log) { l.GetPayload().GetApply().GetLog().Date = nil },
		"sequence": func(l *commonpb.Log) { l.Sequence++ },
		"tx": func(l *commonpb.Log) {
			l.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().Transaction.Reference = "x"
		},
		"kind": func(l *commonpb.Log) {
			l.GetPayload().GetApply().GetLog().Data = &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{RemovedAccountType: &commonpb.RemovedAccountTypeLog{Name: "x"}}}
		},
		"annotation": func(l *commonpb.Log) {
			e := l.GetPayload().GetApply().GetLog()
			e.NewKeptVolumes = append(e.NewKeptVolumes, &commonpb.TouchedVolume{Account: "x", Asset: "USD/2"})
		},
	}

	for name, mutate := range cases {
		l := clone(base)
		mutate(l)
		require.False(t, match.LogRowMatches(fixtureLedger, want, match.ServerLogRows([]*commonpb.Log{l})[0]), name)
	}
}

func TestRenderLogPageMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	for _, reverse := range []bool{false, true} {
		for _, size := range []uint32{0, 1, 2, 5} {
			var after uint64
			for pages := 0; ; pages++ {
				require.Less(t, pages, 30)

				page, token, ok := RenderLogPage(gs, fixtureLedger, LogPageReq{AfterID: after, PageSize: size, Reverse: reverse})
				require.True(t, ok)
				eff := match.EffectivePageSize(int(size))
				require.True(t, match.LogWindowMatches(ls, fixtureLedger, nil, after, reverse, eff, match.ServerLogRows(page), match.NextCursorOf(token)),
					"reverse=%v size=%d after=%d", reverse, size, after)

				if token == "" {
					break
				}

				after, _ = strconv.ParseUint(match.NextCursorOf(token), 10, 64)
			}
		}
	}

	page, token, ok := RenderLogPage(gs, fixtureLedger, LogPageReq{PageSize: 2})
	require.True(t, ok)
	rows := match.ServerLogRows(page)
	require.False(t, match.LogWindowMatches(ls, fixtureLedger, nil, 0, false, 2, rows, ""), "a missing token")
	require.False(t, match.LogWindowMatches(ls, fixtureLedger, nil, 0, false, 2, rows[1:], match.NextCursorOf(token)), "a skipped row")
}

// ---- ledgers ----

func TestRenderLedgerInfoMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	info := RenderLedgerInfo(gs, fixtureLedger)
	require.True(t, match.LedgerInfoMatches(gs, info))
	require.Empty(t, match.LedgerInfoStructureViolation(info))
	require.True(t, SyntheticIdentity(fixtureLedger).Matches(info))
	require.True(t, match.LedgerReadMatches(ls, info.GetAccountTypes(), info.GetMetadata(), info.GetDefaultEnforcementMode()))
	require.True(t, match.MetadataSchemaMatches(ls, info.GetMetadataSchema()))
	require.Nil(t, RenderLedgerInfo(gs, "absent"))

	cases := map[string]func(i *commonpb.LedgerInfo){
		"name":          func(i *commonpb.LedgerInfo) { i.Name = "absent" },
		"metadata":      func(i *commonpb.LedgerInfo) { i.Metadata["owner"] = Str("other") },
		"chart pattern": func(i *commonpb.LedgerInfo) { i.AccountTypes["t-9"].Pattern = "x" },
		"chart extra":   func(i *commonpb.LedgerInfo) { i.AccountTypes["t-8"] = &commonpb.AccountType{Pattern: "t-8"} },
		"schema type": func(i *commonpb.LedgerInfo) {
			i.MetadataSchema.AccountFields["k1"].Type = commonpb.MetadataType_METADATA_TYPE_BOOL
		},
		"schema extra": func(i *commonpb.LedgerInfo) {
			i.MetadataSchema.LedgerFields = map[string]*commonpb.MetadataFieldSchema{"x": {}}
		},
		"mode":            func(i *commonpb.LedgerInfo) { i.Mode = commonpb.LedgerMode_LEDGER_MODE_MIRROR },
		"mirror progress": func(i *commonpb.LedgerInfo) { i.MirrorSyncProgress = &commonpb.MirrorSyncProgress{} },
		"enforcement mode": func(i *commonpb.LedgerInfo) {
			i.DefaultEnforcementMode = commonpb.ChartEnforcementMode(1 + int32(i.DefaultEnforcementMode))
		},
	}

	for name, mutate := range cases {
		i := clone(info)
		mutate(i)
		require.False(t, match.LedgerInfoMatches(gs, i), name)
	}

	other := clone(info)
	other.Id++
	require.False(t, SyntheticIdentity(fixtureLedger).Matches(other), "identity id")
}

// ---- stats ----

func TestRenderStatsMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	stats := RenderStats(gs, fixtureLedger)
	require.True(t, match.StatsMatches(ls, stats))
	require.Equal(t, match.ModelUsage(ls), match.ServedStatsUsage(stats))

	lagging := clone(stats)
	lagging.PostingCount--
	require.True(t, match.StatsMatches(ls, lagging), "a lagging usage store stays under the bound")

	cases := map[string]func(s *commonpb.LedgerStats){
		"tx count":   func(s *commonpb.LedgerStats) { s.TransactionCount++ },
		"log count":  func(s *commonpb.LedgerStats) { s.LogCount-- },
		"postings":   func(s *commonpb.LedgerStats) { s.PostingCount++ },
		"references": func(s *commonpb.LedgerStats) { s.ReferenceCount++ },
		"reverts":    func(s *commonpb.LedgerStats) { s.RevertCount++ },
		"numscript":  func(s *commonpb.LedgerStats) { s.NumscriptExecutionCount = 1 },
	}

	for name, mutate := range cases {
		s := clone(stats)
		mutate(s)
		require.False(t, match.StatsMatches(ls, s), name)
	}
}

// ---- aggregates ----

func TestRenderAggregateMatches(t *testing.T) {
	t.Parallel()
	gs := Fixture(t)
	ls := gs.Ledger(fixtureLedger)

	optsList := []match.AggOptions{
		{},
		{CollapseColors: true},
		{UseMaxPrecision: true},
		{GroupByPrefixes: []string{"t-0:1", "t-0:", "nobody"}},
		{GroupByPrefixes: []string{"world"}, CollapseColors: true},
	}

	for _, opts := range optsList {
		res := RenderAggregate(gs, fixtureLedger, opts)
		ok, why := match.AggregateResultMatches(ls, nil, opts, res)
		require.True(t, ok, "%+v: %s", opts, why)
	}

	flat := RenderAggregate(gs, fixtureLedger, match.AggOptions{})
	require.True(t, match.AggregateMatches(ls, nil, flat))

	grouped := RenderAggregate(gs, fixtureLedger, optsList[3])

	cases := map[string]struct {
		opts   match.AggOptions
		res    *commonpb.AggregateResult
		mutate func(r *commonpb.AggregateResult)
	}{
		"input":            {match.AggOptions{}, flat, func(r *commonpb.AggregateResult) { r.Volumes[0].Input = commonpb.NewUint256FromUint64(12345) }},
		"colour":           {match.AggOptions{}, flat, func(r *commonpb.AggregateResult) { r.Volumes[0].Color = "SILVER" }},
		"duplicate bucket": {match.AggOptions{}, flat, func(r *commonpb.AggregateResult) { r.Volumes = append(r.Volumes, r.Volumes[0]) }},
		"dropped bucket":   {match.AggOptions{}, flat, func(r *commonpb.AggregateResult) { r.Volumes = r.Volumes[1:] }},
		"flat as grouped":  {match.AggOptions{}, flat, func(r *commonpb.AggregateResult) { r.Groups = []*commonpb.GroupedAggregateResult{{Prefix: "x"}} }},
		"group prefix":     {optsList[3], grouped, func(r *commonpb.AggregateResult) { r.Groups[0].Prefix = "x" }},
		"group order":      {optsList[3], grouped, func(r *commonpb.AggregateResult) { r.Groups[0], r.Groups[1] = r.Groups[1], r.Groups[0] }},
	}

	for name, c := range cases {
		r := clone(c.res)
		c.mutate(r)
		ok, _ := match.AggregateResultMatches(ls, nil, c.opts, r)
		require.False(t, ok, name)
	}
}

// ---- audit ----

func TestRenderAuditEntryMatches(t *testing.T) {
	t.Parallel()

	logs := map[uint64]match.CommittedLog{
		10: {Ledger: "A", ID: 1, Kind: "created_transaction"},
		11: {Ledger: "B", ID: 1, Kind: "created_transaction"},
		12: {Ledger: "A", ID: 2, Kind: "saved_metadata"},
	}
	bulk := match.CommittedBulk{MinSeq: 10, MaxSeq: 12, Orders: 3, Wid: "w1"}
	bulks := map[uint64]match.CommittedBulk{10: bulk}

	entry := RenderAuditEntry(bulk, logs, 7, "o-run-k1")
	e := match.AuditEntryOf(entry)
	require.Equal(t, uint64(7), e.Seq)
	require.Equal(t, "o-run-k1", e.IdemKey)
	why, _ := match.AuditSuccessMismatch(e, logs, bulks, 12)
	require.Empty(t, why)
	require.Equal(t, map[string]bool{"create_transaction": true, "add_metadata": true}, match.CommittedOrderTypes(e, logs))

	cases := map[string]func(a *auditpb.AuditEntry){
		"order count": func(a *auditpb.AuditEntry) { a.OrderCount = 2 },
		"max log":     func(a *auditpb.AuditEntry) { a.GetSuccess().MaxLogSequence = 11 },
		"min log":     func(a *auditpb.AuditEntry) { a.GetSuccess().MinLogSequence = 11 },
		"ledgers":     func(a *auditpb.AuditEntry) { a.Ledgers = []string{"A"} },
		"item":        func(a *auditpb.AuditEntry) { a.Items[0].LogSequence = 12 },
		"past frontier": func(a *auditpb.AuditEntry) {
			a.GetSuccess().MaxLogSequence = 13
			a.OrderCount = 4
			a.Items = append(a.Items, &auditpb.AuditItem{LogSequence: 13})
		},
	}

	for name, mutate := range cases {
		a := clone(entry)
		mutate(a)
		why, _ := match.AuditSuccessMismatch(match.AuditEntryOf(a), logs, bulks, 12)
		require.NotEmpty(t, why, name)
	}
}
