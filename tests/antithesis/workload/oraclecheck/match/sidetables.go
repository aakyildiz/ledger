package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// LedgerIsLive reports whether state holds name as a created, undeleted ledger.
func LedgerIsLive(state oracle.GlobalState, name string) bool {
	lifecycle, exists := state.Lifecycle(name)

	return exists && !lifecycle.Deleted
}

// CommittedBulk is the log range one committed bulk produced, with the number
// of orders that produced it. Side table entry keyed by MinSeq (the Tracker's
// CommittedBulks in oraclecheck has the identical shape; 00d converts).
type CommittedBulk struct {
	MinSeq, MaxSeq uint64
	Orders         uint32
	Wid            string // the write that committed it (DESIGN §5.2); "" when unknown
}

// requestLogKind names the order a request carries, the way the audit index
// keys it: the accepted intent's payload variant. An order apply skipped is
// still the order that was accepted, so the skip does not change its kind.
func RequestLogKind(req *servicepb.Request) string {
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		switch r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return "created_transaction"
		case *servicepb.LedgerAction_RevertTransaction:
			return "reverted_transaction"
		case *servicepb.LedgerAction_AddMetadata:
			return "saved_metadata"
		case *servicepb.LedgerAction_DeleteMetadata:
			return "deleted_metadata"
		case *servicepb.LedgerAction_AddAccountType:
			return "added_account_type"
		case *servicepb.LedgerAction_RemoveAccountType:
			return "removed_account_type"
		case *servicepb.LedgerAction_SetDefaultEnforcementMode:
			return "updated_default_enforcement_mode"
		default:
			return ""
		}
	case *servicepb.Request_SetMetadataFieldType:
		return "set_metadata_field_type"
	case *servicepb.Request_RemoveMetadataFieldType:
		return "removed_metadata_field_type"
	case *servicepb.Request_CreateIndex:
		return "create_index"
	case *servicepb.Request_DropIndex:
		return "drop_index"
	case *servicepb.Request_AddAccountType:
		return "added_account_type"
	case *servicepb.Request_RemoveAccountType:
		return "removed_account_type"
	case *servicepb.Request_SetDefaultEnforcementMode:
		return "updated_default_enforcement_mode"
	default:
		return LedgerLogKindOf(req)
	}
}

// ledgerLogKindOf names the order behind a ledger-metadata log, empty for a
// request that produces none.
func LedgerLogKindOf(req *servicepb.Request) string {
	switch req.GetType().(type) {
	case *servicepb.Request_SaveLedgerMetadata:
		return "saved_ledger_metadata"
	case *servicepb.Request_DeleteLedgerMetadata:
		return "deleted_ledger_metadata"
	default:
		return ""
	}
}
