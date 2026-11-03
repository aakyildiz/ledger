package match

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
)

// modelLedgerForInfo builds a ledger exercising every field ledgerInfoMatches
// compares, and the LedgerInfo the server must answer for it.
func modelLedgerForInfo(t *testing.T) (oracle.GlobalState, *commonpb.LedgerInfo) {
	t.Helper()

	applied := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
			Name:                   "L",
			Mode:                   commonpb.LedgerMode_LEDGER_MODE_MIRROR,
			MirrorSource:           &commonpb.MirrorSourceConfig{LedgerName: "src"},
			DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
			AccountTypes:           map[string]*commonpb.AccountType{"known": {Name: "known", Pattern: "known:{id}"}},
			InitialSchema: []*commonpb.SetMetadataFieldTypeCommand{{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "rank",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			}},
		}}},
		actions.SaveLedgerMetadataAction("L", map[string]string{"owner": "treasury"}),
	}})
	require.True(t, applied.OK)

	return applied.State, &commonpb.LedgerInfo{
		Name:                   "L",
		Id:                     7,
		CreatedAt:              &commonpb.Timestamp{Data: 1000},
		Mode:                   commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource:           &commonpb.MirrorSourceConfig{LedgerName: "src"},
		MirrorSyncProgress:     &commonpb.MirrorSyncProgress{},
		DefaultEnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		AccountTypes:           map[string]*commonpb.AccountType{"known": {Name: "known", Pattern: "known:{id}"}},
		MetadataSchema: &commonpb.MetadataSchema{AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"rank": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		}},
		Metadata: map[string]*commonpb.MetadataValue{
			"owner": {Type: &commonpb.MetadataValue_StringValue{StringValue: "treasury"}},
		},
	}
}

// Every field the model derives must break the match on its own: a comparison
// that quietly stops covering one field would otherwise keep passing.
func TestLedgerInfoMatchesComparesEveryDerivedField(t *testing.T) {
	t.Parallel()

	state, info := modelLedgerForInfo(t)
	require.True(t, LedgerInfoMatches(state, info))

	for name, corrupt := range map[string]func(*commonpb.LedgerInfo){
		"name": func(i *commonpb.LedgerInfo) { i.Name = "other" },
		"mode": func(i *commonpb.LedgerInfo) { i.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL },
		"mirror source": func(i *commonpb.LedgerInfo) {
			i.MirrorSource = &commonpb.MirrorSourceConfig{LedgerName: "elsewhere"}
		},
		"dropped mirror source":   func(i *commonpb.LedgerInfo) { i.MirrorSource = nil },
		"dropped mirror progress": func(i *commonpb.LedgerInfo) { i.MirrorSyncProgress = nil },
		"enforcement mode": func(i *commonpb.LedgerInfo) {
			i.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		},
		"account type pattern": func(i *commonpb.LedgerInfo) {
			i.AccountTypes = map[string]*commonpb.AccountType{"known": {Name: "known", Pattern: "other:{id}"}}
		},
		"extra account type": func(i *commonpb.LedgerInfo) {
			i.AccountTypes["extra"] = &commonpb.AccountType{Name: "extra", Pattern: "extra:{id}"}
		},
		"schema type": func(i *commonpb.LedgerInfo) {
			i.MetadataSchema.AccountFields["rank"] = &commonpb.MetadataFieldSchema{Type: commonpb.MetadataType_METADATA_TYPE_UINT64}
		},
		"schema target": func(i *commonpb.LedgerInfo) {
			i.MetadataSchema = &commonpb.MetadataSchema{LedgerFields: i.GetMetadataSchema().GetAccountFields()}
		},
		"dropped schema": func(i *commonpb.LedgerInfo) { i.MetadataSchema = nil },
		"metadata value": func(i *commonpb.LedgerInfo) {
			i.Metadata["owner"] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "other"}}
		},
		"dropped metadata": func(i *commonpb.LedgerInfo) { i.Metadata = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, corrupted := modelLedgerForInfo(t)
			corrupt(corrupted)
			require.False(t, LedgerInfoMatches(state, corrupted))
		})
	}
}

func TestLedgerInfoMatchesRejectsAnAbsentOrDeletedLedger(t *testing.T) {
	t.Parallel()

	state, info := modelLedgerForInfo(t)

	require.False(t, LedgerInfoMatches(oracle.NewGlobalState(), info))

	deleted := state.Apply(oracle.Bulk{Requests: []*servicepb.Request{actions.DeleteLedgerAction("L")}})
	require.True(t, deleted.OK)
	require.False(t, LedgerInfoMatches(deleted.State, info))
}

func TestLedgerInfoStructureViolation(t *testing.T) {
	t.Parallel()

	_, info := modelLedgerForInfo(t)
	require.Empty(t, LedgerInfoStructureViolation(info))

	for name, corrupt := range map[string]func(*commonpb.LedgerInfo){
		"listed ledger has no name":               func(i *commonpb.LedgerInfo) { i.Name = "" },
		"listed ledger has no id":                 func(i *commonpb.LedgerInfo) { i.Id = 0 },
		"listed ledger has no creation timestamp": func(i *commonpb.LedgerInfo) { i.CreatedAt = nil },
		"listing served a soft-deleted ledger":    func(i *commonpb.LedgerInfo) { i.DeletedAt = &commonpb.Timestamp{Data: 2} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, corrupted := modelLedgerForInfo(t)
			corrupt(corrupted)
			require.Equal(t, name, LedgerInfoStructureViolation(corrupted))
		})
	}
}

func TestLedgerIdentityMatchesBothFields(t *testing.T) {
	t.Parallel()

	_, info := modelLedgerForInfo(t)
	identity := LedgerIdentity{ID: 7, CreatedAt: &commonpb.Timestamp{Data: 1000}}
	require.True(t, identity.Matches(info))

	require.False(t, LedgerIdentity{ID: 8, CreatedAt: identity.CreatedAt}.Matches(info))
	require.False(t, LedgerIdentity{ID: 7, CreatedAt: &commonpb.Timestamp{Data: 1001}}.Matches(info))
	require.False(t, LedgerIdentity{ID: 7}.Matches(info))
}

// One base must explain the whole listing page. A bulk that updates A's
// metadata and deletes B splits the page across two states: the old fleet
// explains the continuation toward B, the new state explains A's row.
func TestLedgerListingMatchesNeedsOneBase(t *testing.T) {
	t.Parallel()

	created := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "A"}}},
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "B"}}},
	}})
	require.True(t, created.OK, created.Reason)
	before := created.State

	updated := before.Apply(oracle.Bulk{Requests: []*servicepb.Request{
		actions.SaveLedgerMetadataAction("A", map[string]string{"owner": "treasury"}),
		actions.DeleteLedgerAction("B"),
	}})
	require.True(t, updated.OK, updated.Reason)
	after := updated.State

	row := &commonpb.LedgerInfo{Name: "A", Metadata: map[string]*commonpb.MetadataValue{
		"owner": {Type: &commonpb.MetadataValue_StringValue{StringValue: "treasury"}},
	}}
	require.True(t, LedgerInfoMatches(after, row), "the row is the new state's A")
	names := []string{"A"}
	page := []*commonpb.LedgerInfo{row}
	req := ListLedgersReq{PageSize: 1}

	require.True(t, LedgerWindowMatches(before, names, "", 1, false, "A"), "the old fleet still holds B past A")
	ok, _ := LedgerListingMatches(before, "", req, page, "A")
	require.False(t, ok, "the old state's A has no metadata")
	// DESIGN §3.2 rule 5: on a full page a next cursor is legal whatever
	// follows, since foreign ledgers may sort past the tracked range. The
	// singleton model's exclusive-fleet version rejected it here.
	ok, why := LedgerListingMatches(after, "", req, page, "A")
	require.True(t, ok, why)
}

// ListIndexes reads the registry from an iterator opened first but checks each
// owner's liveness later, so a ledger deleted mid-listing may contribute
// nothing; a ledger that cannot have been deleted must still contribute its
// whole set.
func TestIndexListingMatchesAVanishedLedger(t *testing.T) {
	t.Parallel()

	created := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "A"}}},
		{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "B"}}},
	}})
	require.True(t, created.OK, created.Reason)
	before := created.State

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0")
	indexB := oracletest.CreateIndexReq(id)
	indexB.GetCreateIndex().Ledger = "B"
	declareB := oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0", commonpb.MetadataType_METADATA_TYPE_STRING)
	declareB.GetSetMetadataFieldType().Ledger = "B"
	withIndex := before.Apply(oracle.Bulk{Requests: []*servicepb.Request{declareB, indexB}})
	require.True(t, withIndex.OK, withIndex.Reason)

	// The listing served nothing for B: its rows were checked after B went away.
	served := map[string]map[string]bool{}

	require.False(t, IndexListingMatches(withIndex.State, served, nil), "B cannot have vanished: its set is owed")
	require.True(t, IndexListingMatches(withIndex.State, served, map[string]bool{"B": true}), "B deleted mid-listing contributes nothing")
}
