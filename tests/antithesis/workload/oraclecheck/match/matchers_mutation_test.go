package match_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func cloneMsg[T proto.Message](m T) T { return proto.Clone(m).(T) }

// matcherCase is one exported matcher: the model-consistent value must
// match, and every single-field mutation must not. The renderer-backed
// matchers (accounts, transactions, logs, pages, ledger info, stats,
// aggregates, audit entries) have the same table in matchtest/render_test.go.
type matcherCase struct {
	name       string
	consistent func() bool
	mutations  map[string]func() bool
}

func TestMatchers_ConsistentPassesMutationFails(t *testing.T) {
	t.Parallel()

	gs := matchtest.Fixture(t)
	const L = matchtest.FixtureLedger
	ls := gs.Ledger(L)
	info := matchtest.RenderLedgerInfo(gs, L)

	accountIdx := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1"))
	served := map[string]bool{accountIdx: true}
	for canon := range ls.Indexes().All() {
		served[canon] = true
	}

	fieldStatus := func() map[string]*servicepb.MetadataFieldStatus {
		out := map[string]*servicepb.MetadataFieldStatus{}
		for k, typ := range ls.AccountFieldTypes().All() {
			out[k] = &servicepb.MetadataFieldStatus{DeclaredType: typ}
		}

		return out
	}

	tx1 := ls.Txs().Get(0)
	tx1Served := matchtest.RenderTx(gs, L, 1)

	var seqRow oracle.LogRow
	for _, row := range ls.LogRows() {
		if row.Sequence != 0 {
			seqRow = row

			break
		}
	}
	require.NotZero(t, seqRow.Sequence)
	seqLog, ok := matchtest.RenderLog(gs, L, seqRow.ID)
	require.True(t, ok)
	target := match.CommittedLogTarget{Ledger: L, ID: seqRow.ID, Sequence: seqRow.Sequence}

	acct := matchtest.RenderAccount(gs, L, "t-0:1", false)

	cases := []matcherCase{
		{
			name:       "ChartMatches",
			consistent: func() bool { return match.ChartMatches(ls, info.GetAccountTypes()) },
			mutations: map[string]func() bool{
				"pattern": func() bool {
					i := cloneMsg(info)
					i.AccountTypes["t-9"].Pattern = "other"

					return match.ChartMatches(ls, i.GetAccountTypes())
				},
				"persistence": func() bool {
					i := cloneMsg(info)
					i.AccountTypes["t-9"].Persistence = commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL

					return match.ChartMatches(ls, i.GetAccountTypes())
				},
				"missing type": func() bool { return match.ChartMatches(ls, nil) },
			},
		},
		{
			name:       "LedgerMetaMatches",
			consistent: func() bool { return match.LedgerMetaMatches(ls, info.GetMetadata()) },
			mutations: map[string]func() bool{
				"value": func() bool {
					return match.LedgerMetaMatches(ls, map[string]*commonpb.MetadataValue{"owner": matchtest.Str("other")})
				},
				"typed value": func() bool {
					return match.LedgerMetaMatches(ls, map[string]*commonpb.MetadataValue{"owner": {Type: &commonpb.MetadataValue_BoolValue{BoolValue: true}}})
				},
				"extra key": func() bool {
					return match.LedgerMetaMatches(ls, map[string]*commonpb.MetadataValue{"owner": matchtest.Str("treasury"), "x": matchtest.Str("y")})
				},
			},
		},
		{
			name: "LedgerReadMatches",
			consistent: func() bool {
				return match.LedgerReadMatches(ls, info.GetAccountTypes(), info.GetMetadata(), info.GetDefaultEnforcementMode())
			},
			mutations: map[string]func() bool{
				"mode": func() bool {
					return match.LedgerReadMatches(ls, info.GetAccountTypes(), info.GetMetadata(), info.GetDefaultEnforcementMode()+1)
				},
			},
		},
		{
			name:       "FieldTypesMatch",
			consistent: func() bool { return match.FieldTypesMatch(ls.AccountFieldTypes(), fieldStatus()) },
			mutations: map[string]func() bool{
				"type": func() bool {
					s := fieldStatus()
					s["k1"].DeclaredType = commonpb.MetadataType_METADATA_TYPE_BOOL

					return match.FieldTypesMatch(ls.AccountFieldTypes(), s)
				},
				"extra": func() bool {
					s := fieldStatus()
					s["k9"] = &servicepb.MetadataFieldStatus{}

					return match.FieldTypesMatch(ls.AccountFieldTypes(), s)
				},
			},
		},
		{
			name: "SchemaFieldsMatch",
			consistent: func() bool {
				return match.SchemaFieldsMatch(ls.AccountFieldTypes(), info.GetMetadataSchema().GetAccountFields())
			},
			mutations: map[string]func() bool{
				"missing": func() bool { return match.SchemaFieldsMatch(ls.AccountFieldTypes(), nil) },
			},
		},
		{
			name:       "LedgerIdentity.Matches",
			consistent: func() bool { return matchtest.SyntheticIdentity(L).Matches(info) },
			mutations: map[string]func() bool{
				"created at": func() bool {
					i := cloneMsg(info)
					i.CreatedAt = &commonpb.Timestamp{Data: 1}

					return matchtest.SyntheticIdentity(L).Matches(i)
				},
			},
		},
		{
			name: "PCVSnapshotMatches",
			consistent: func() bool {
				return match.PCVSnapshotMatches(tx1.PostCommitVolumes(), tx1Served.GetPostCommitVolumes())
			},
			mutations: map[string]func() bool{
				"input": func() bool {
					p := cloneMsg(tx1Served.GetPostCommitVolumes())
					for _, v := range p.GetVolumesByAccount() {
						v.Volumes[0].Volumes.Input = commonpb.MustBigUintFromDecimal("987654")

						break
					}

					return match.PCVSnapshotMatches(tx1.PostCommitVolumes(), p)
				},
				"extra cell": func() bool {
					p := cloneMsg(tx1Served.GetPostCommitVolumes())
					p.VolumesByAccount["ghost"] = &commonpb.VolumesByAssets{Volumes: []*commonpb.VolumeEntry{{Asset: "USD/2", Volumes: &commonpb.Volumes{}}}}

					return match.PCVSnapshotMatches(tx1.PostCommitVolumes(), p)
				},
			},
		},
		{
			name:       "LogTxMatches",
			consistent: func() bool { return match.LogTxMatches(tx1, tx1Served) },
			mutations: map[string]func() bool{
				"reference": func() bool {
					tx := cloneMsg(tx1Served)
					tx.Reference = "other"

					return match.LogTxMatches(tx1, tx)
				},
				"posting amount": func() bool {
					tx := cloneMsg(tx1Served)
					tx.Postings[0].Amount = commonpb.NewUint256FromUint64(4242)

					return match.LogTxMatches(tx1, tx)
				},
			},
		},
		{
			name: "CommittedLogMatches",
			consistent: func() bool {
				return match.CommittedLogMatches(gs, target, match.ServerLogRows([]*commonpb.Log{seqLog})[0])
			},
			mutations: map[string]func() bool{
				"sequence": func() bool {
					l := cloneMsg(seqLog)
					l.Sequence++

					return match.CommittedLogMatches(gs, target, match.ServerLogRows([]*commonpb.Log{l})[0])
				},
				"id": func() bool {
					l := cloneMsg(seqLog)
					l.GetPayload().GetApply().GetLog().Id++

					return match.CommittedLogMatches(gs, target, match.ServerLogRows([]*commonpb.Log{l})[0])
				},
			},
		},
		{
			name:       "CheckpointAccountReadMatches",
			consistent: func() bool { return match.CheckpointAccountReadMatches(gs, L, "t-0:1", acct, true, false) },
			mutations: map[string]func() bool{
				"not found": func() bool { return match.CheckpointAccountReadMatches(gs, L, "t-0:1", nil, false, false) },
				"address": func() bool {
					a := cloneMsg(acct)
					a.Address = "t-0:2"

					return match.CheckpointAccountReadMatches(gs, L, "t-0:1", a, true, false)
				},
			},
		},
		{
			name:       "IndexSetEqual",
			consistent: func() bool { return match.IndexSetEqual(ls, served) },
			mutations: map[string]func() bool{
				"missing": func() bool { return match.IndexSetEqual(ls, map[string]bool{}) },
				"extra": func() bool {
					s := map[string]bool{"ghost": true}
					for k := range served {
						s[k] = true
					}

					return match.IndexSetEqual(ls, s)
				},
			},
		},
		{
			name:       "IndexPresenceMatches",
			consistent: func() bool { return match.IndexPresenceMatches(gs, L, accountIdx, true) },
			mutations: map[string]func() bool{
				"found flipped": func() bool { return match.IndexPresenceMatches(gs, L, accountIdx, false) },
				"ghost index":   func() bool { return match.IndexPresenceMatches(gs, L, "ghost", true) },
			},
		},
		{
			name: "IndexListingMatches",
			consistent: func() bool {
				return match.IndexListingMatches(gs, map[string]map[string]bool{L: served}, nil)
			},
			mutations: map[string]func() bool{
				"foreign owner": func() bool {
					return match.IndexListingMatches(gs, map[string]map[string]bool{L: served, "gone": {"x": true}}, nil)
				},
				"missing ledger": func() bool { return match.IndexListingMatches(gs, map[string]map[string]bool{}, nil) },
			},
		},
		{
			name: "TrackedIndexListingMatches",
			consistent: func() bool {
				// L is tracked under prefix "L"; foreign owners are ignored.
				return match.TrackedIndexListingMatches(gs, "L", map[string]map[string]bool{L: served, "r-ns-1": {"x": true}, "": {"y": true}}, nil)
			},
			mutations: map[string]func() bool{
				"tracked set": func() bool {
					return match.TrackedIndexListingMatches(gs, "L", map[string]map[string]bool{L: {}}, nil)
				},
				"tracked ghost owner": func() bool {
					return match.TrackedIndexListingMatches(gs, "L", map[string]map[string]bool{L: served, "L-gone": {"x": true}}, nil)
				},
			},
		},
		{
			name:       "LedgerWindowMatches",
			consistent: func() bool { return match.LedgerWindowMatches(gs, []string{L}, "", 10, false, "") },
			mutations: map[string]func() bool{
				"extra row": func() bool { return match.LedgerWindowMatches(gs, []string{L, "M"}, "", 10, false, "") },
				"token":     func() bool { return match.LedgerWindowMatches(gs, []string{L}, "", 10, false, L) },
			},
		},
		{
			name: "StatsBoundsMatch",
			consistent: func() bool {
				return match.StatsBoundsMatch(ls, matchtest.RenderStats(gs, L))
			},
			mutations: map[string]func() bool{
				"tx count": func() bool {
					s := matchtest.RenderStats(gs, L)
					s.TransactionCount--

					return match.StatsBoundsMatch(ls, s)
				},
			},
		},
	}

	for _, c := range cases {
		require.True(t, c.consistent(), "%s: the model-consistent value must match", c.name)
		require.NotEmpty(t, c.mutations, c.name)

		for m, mutated := range c.mutations {
			require.False(t, mutated(), "%s: mutation %q must not match", c.name, m)
		}
	}
}
