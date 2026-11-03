package oraclecheck

import (
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Reserved (rule-owned) entities, DESIGN §3.1. Core views hide them unless a
// view request carries "owned": true, so no generator can touch them.

const reservedSegment = "own-"

// IsReservedLedger reports a rule-owned ledger name: <prefix>own<N>.
func IsReservedLedger(prefix, name string) bool {
	return prefix != "" && strings.HasPrefix(name, prefix+"own")
}

// IsReservedAddress reports an address with a segment after the first that
// starts with "own-" (t-0:own-drain7, users:own-pq4).
func IsReservedAddress(addr string) bool {
	segs := strings.Split(addr, ":")
	for _, s := range segs[1:] {
		if strings.HasPrefix(s, reservedSegment) {
			return true
		}
	}

	return false
}

// IsReservedKey reports a reserved metadata key or prepared-query name.
func IsReservedKey(key string) bool { return strings.HasPrefix(key, reservedSegment) }

// IsReservedReference reports a reserved transaction reference
// (<prefix>own-…).
func IsReservedReference(prefix, ref string) bool {
	return prefix != "" && strings.HasPrefix(ref, prefix+reservedSegment)
}

// IsReservedTx reports a transaction a generator must not aim at: a reserved
// reference or a posting that touches a reserved address.
func IsReservedTx(prefix, ref string, postings []*commonpb.Posting) bool {
	if IsReservedReference(prefix, ref) {
		return true
	}
	for _, p := range postings {
		if IsReservedAddress(p.GetSource()) || IsReservedAddress(p.GetDestination()) {
			return true
		}
	}

	return false
}

// TouchesReserved reports whether any request of the bulk names a reserved
// ledger, address, reference, metadata key or prepared-query name.
func TouchesReserved(prefix string, b oracle.Bulk) bool {
	for _, r := range b.Requests {
		if requestTouchesReserved(prefix, r) {
			return true
		}
	}

	return false
}

func metaKeysReserved(m map[string]*commonpb.MetadataValue) bool {
	for k := range m {
		if IsReservedKey(k) {
			return true
		}
	}

	return false
}

func targetReserved(t *commonpb.Target) bool {
	return IsReservedAddress(t.GetAccount().GetAddr())
}

func requestTouchesReserved(prefix string, r *servicepb.Request) bool {
	if l := safeLedgerOf(r); l != "" && IsReservedLedger(prefix, l) {
		return true
	}
	switch t := r.GetType().(type) {
	case *servicepb.Request_Apply:
		a := t.Apply.GetAction()
		if ct := a.GetCreateTransaction(); ct != nil {
			if IsReservedTx(prefix, ct.GetReference(), ct.GetPostings()) || metaKeysReserved(ct.GetMetadata()) {
				return true
			}
			for addr, mm := range ct.GetAccountMetadata() {
				if IsReservedAddress(addr) || metaKeysReserved(mm.GetValues()) {
					return true
				}
			}
		}
		if am := a.GetAddMetadata(); am != nil && (targetReserved(am.GetTarget()) || metaKeysReserved(am.GetMetadata())) {
			return true
		}
		if dm := a.GetDeleteMetadata(); dm != nil && (targetReserved(dm.GetTarget()) || IsReservedKey(dm.GetKey())) {
			return true
		}
	case *servicepb.Request_SaveLedgerMetadata:
		return metaKeysReserved(t.SaveLedgerMetadata.GetMetadata())
	case *servicepb.Request_DeleteLedgerMetadata:
		return IsReservedKey(t.DeleteLedgerMetadata.GetKey())
	case *servicepb.Request_SetMetadataFieldType:
		return IsReservedKey(t.SetMetadataFieldType.GetKey())
	case *servicepb.Request_RemoveMetadataFieldType:
		return IsReservedKey(t.RemoveMetadataFieldType.GetKey())
	case *servicepb.Request_CreateIndex:
		return IsReservedKey(t.CreateIndex.GetId().GetMetadata().GetKey())
	case *servicepb.Request_DropIndex:
		return IsReservedKey(t.DropIndex.GetId().GetMetadata().GetKey())
	case *servicepb.Request_CreatePreparedQuery:
		return IsReservedKey(t.CreatePreparedQuery.GetQuery().GetName())
	case *servicepb.Request_UpdatePreparedQuery:
		return IsReservedKey(t.UpdatePreparedQuery.GetName())
	case *servicepb.Request_DeletePreparedQuery:
		return IsReservedKey(t.DeletePreparedQuery.GetName())
	}

	return false
}

// safeLedgerOf is oracle.LedgerOf without its panic on unmodeled arms.
func safeLedgerOf(r *servicepb.Request) (ledger string) {
	defer func() {
		if recover() != nil {
			ledger = ""
		}
	}()

	return oracle.LedgerOf(r)
}
