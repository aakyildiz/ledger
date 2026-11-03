package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// LedgerInfoMatches reports whether a served LedgerInfo is the record base holds
// for its ledger. Every field the model derives is compared here, so a listing
// row and a GetLedger answer are held to one contract.
//
// id and created_at are absent: the model assigns neither. They are pinned
// separately, against what the ledger's own creation log reported — see
// LedgerIdentity. Mirror sync progress is the mirror worker's, which the model
// does not run, so only its presence is held: on a mirror ledger and nowhere
// else.
func LedgerInfoMatches(base oracle.GlobalState, info *commonpb.LedgerInfo) bool {
	name := info.GetName()

	lifecycle, exists := base.Lifecycle(name)
	if !exists || lifecycle.Deleted {
		return false
	}

	ls := base.Ledger(name)

	return ChartMatches(ls, info.GetAccountTypes()) &&
		LedgerMetaMatches(ls, info.GetMetadata()) &&
		MetadataSchemaMatches(ls, info.GetMetadataSchema()) &&
		info.GetMode() == lifecycle.Mode &&
		info.GetMirrorSource().EqualVT(lifecycle.MirrorSource) &&
		(info.GetMirrorSyncProgress() != nil) == (lifecycle.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR) &&
		info.GetDefaultEnforcementMode() == ls.DefaultEnforcementMode()
}

// MetadataSchemaMatches reports whether ls's declared field types equal the
// schema a LedgerInfo carries, per target and key-for-key. GetMetadataSchemaStatus
// is served by projecting this same field, so the two reads answer one record.
func MetadataSchemaMatches(ls oracle.LedgerState, schema *commonpb.MetadataSchema) bool {
	return SchemaFieldsMatch(ls.AccountFieldTypes(), schema.GetAccountFields()) &&
		SchemaFieldsMatch(ls.TransactionFieldTypes(), schema.GetTransactionFields()) &&
		SchemaFieldsMatch(ls.LedgerFieldTypes(), schema.GetLedgerFields())
}

// SchemaFieldsMatch compares one target's declared types — same keys, same type.
// An absent schema message reads as the empty one.
func SchemaFieldsMatch(model oracle.Map[string, commonpb.MetadataType], server map[string]*commonpb.MetadataFieldSchema) bool {
	if model.Len() != len(server) {
		return false
	}

	for key, declared := range model.All() {
		field, ok := server[key]
		if !ok || field.GetType() != declared {
			return false
		}
	}

	return true
}

// LedgerInfoStructureViolation names what is wrong with a listed LedgerInfo
// independently of any base, or "" when nothing is. The listing drops every
// soft-deleted ledger, so no row carries a deletion time whatever the fleet
// holds.
func LedgerInfoStructureViolation(info *commonpb.LedgerInfo) string {
	switch {
	case info.GetName() == "":
		return "listed ledger has no name"
	case info.GetId() == 0:
		return "listed ledger has no id"
	case info.GetCreatedAt() == nil:
		return "listed ledger has no creation timestamp"
	case info.GetDeletedAt() != nil:
		return "listing served a soft-deleted ledger"
	}

	return ""
}

// LedgerIdentity is the (id, created_at) pair a ledger's creation log reported.
// The model derives neither, so reads are held to what creation announced.
// Recreating a deleted ledger assigns a fresh pair, which replaces this one.
type LedgerIdentity struct {
	ID        uint32
	CreatedAt *commonpb.Timestamp
}

func (i LedgerIdentity) Matches(info *commonpb.LedgerInfo) bool {
	return i.ID == info.GetId() && i.CreatedAt.EqualVT(info.GetCreatedAt())
}

func BulkCreatesLedger(bulk oracle.Bulk, name string) bool {
	for _, req := range bulk.Requests {
		if req.GetCreateLedger().GetName() == name {
			return true
		}
	}

	return false
}
