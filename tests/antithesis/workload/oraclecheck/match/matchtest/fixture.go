package matchtest

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

// FixtureLedger is the ledger Fixture populates.
const FixtureLedger = "L"

const fixtureLedger = FixtureLedger

func apply(t testing.TB, gs oracle.GlobalState, reqs ...*servicepb.Request) oracle.GlobalState {
	t.Helper()

	res := gs.Apply(oracle.Bulk{Requests: reqs})
	require.True(t, res.OK, res.Reason)

	return res.State
}

// CreateLedger is the CreateLedger request for name.
func CreateLedger(name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: name}}}
}

// Str is a string metadata value.
func Str(v string) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: v}}
}

// Fixture is a ledger "L" exercising every rendered field: a chart type,
// coloured and uncoloured cells across two assets, a referenced transaction,
// account, transaction and ledger metadata, a declared field type, an index, a
// revert, and learned stamps, dates and sequences on part of the history (the
// rest stays unlearned).
func Fixture(t testing.TB) oracle.GlobalState {
	t.Helper()

	gs := apply(t, oracle.NewGlobalState(), CreateLedger(fixtureLedger))
	gs = apply(t, gs,
		oracletest.TxReq("world", "t-0:1", "USD/2", 10),
		oracletest.TxReqColoredL(fixtureLedger, "world", "t-0:1", "USD/2", "GOLD", 4),
		oracletest.TxReqRefL(fixtureLedger, "r1", "world", "t-0:2", "EUR/2", 7),
		oracletest.TxReq("t-0:1", "t-0:3", "USD/2", 3),
		oracletest.AddAccountMetaReq("t-0:1", "k0", Str("v0")),
		oracletest.SetFieldTypeReq(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1", commonpb.MetadataType_METADATA_TYPE_STRING),
		oracletest.CreateIndexReq(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k1")),
		actions.SaveLedgerMetadataAction(fixtureLedger, map[string]string{"owner": "treasury"}),
	)
	gs = apply(t, gs, oracletest.AddTxMetaReq(2, map[string]*commonpb.MetadataValue{"note": Str("n")}))
	gs = apply(t, gs, oracletest.RevertReqL(fixtureLedger, 1, true))
	// The chart comes last: with a type declared, the default enforcement
	// would refuse the untyped addresses above.
	gs = apply(t, gs, oracletest.AddTypeReq("t-9"))

	gs.LearnTxStamps(fixtureLedger, 1, &commonpb.Timestamp{Data: 100}, &commonpb.Timestamp{Data: 110}, &commonpb.Timestamp{Data: 500})
	gs.LearnTxStamps(fixtureLedger, 2, &commonpb.Timestamp{Data: 200}, &commonpb.Timestamp{Data: 210}, nil)

	for i, row := range gs.Ledger(fixtureLedger).LogRows() {
		if i%2 == 0 {
			gs.LearnLogDate(fixtureLedger, row.ID, &commonpb.Timestamp{Data: 1000 + row.ID})
			gs.LearnLogSequence(fixtureLedger, row.ID, 50+row.ID)
		}
	}

	return gs
}
