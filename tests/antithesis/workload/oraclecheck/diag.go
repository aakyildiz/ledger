package oraclecheck

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Failure diagnostics lifted from M/debug.go and M/validate.go.

func requestKinds(b oracle.Bulk) string {
	parts := make([]string, len(b.Requests))

	for i, r := range b.Requests {
		switch r.GetType().(type) {
		case *servicepb.Request_CreateLedger:
			parts[i] = "createLedger"
		case *servicepb.Request_DeleteLedger:
			parts[i] = "deleteLedger"
		case *servicepb.Request_PromoteLedger:
			parts[i] = "promoteLedger"
		case *servicepb.Request_SetMaintenanceMode:
			parts[i] = "maintenance"
		case *servicepb.Request_Apply:
			switch r.GetApply().GetAction().GetData().(type) {
			case *servicepb.LedgerAction_CreateTransaction:
				parts[i] = "tx"
			case *servicepb.LedgerAction_AddMetadata:
				parts[i] = "addMeta"
			case *servicepb.LedgerAction_DeleteMetadata:
				parts[i] = "delMeta"
			case *servicepb.LedgerAction_RevertTransaction:
				parts[i] = "revert"
			default:
				parts[i] = "apply?"
			}
		case *servicepb.Request_AddAccountType:
			parts[i] = "addType"
		case *servicepb.Request_RemoveAccountType:
			parts[i] = "removeType"
		case *servicepb.Request_SaveLedgerMetadata:
			parts[i] = "saveLedgerMeta"
		case *servicepb.Request_DeleteLedgerMetadata:
			parts[i] = "delLedgerMeta"
		case *servicepb.Request_SetMetadataFieldType:
			parts[i] = "setFieldType"
		case *servicepb.Request_RemoveMetadataFieldType:
			parts[i] = "removeFieldType"
		case *servicepb.Request_CreateIndex:
			parts[i] = "createIndex"
		case *servicepb.Request_DropIndex:
			parts[i] = "dropIndex"
		default:
			parts[i] = "other"
		}
	}

	return "[" + strings.Join(parts, ",") + "]"
}

func bulkMeta(b oracle.Bulk) string {
	kvList := func(m map[string]*commonpb.MetadataValue) string {
		kvs := make([]string, 0, len(m))
		for k, v := range m {
			kvs = append(kvs, k+"="+oracle.MetaValueString(v))
		}
		sort.Strings(kvs)

		return strings.Join(kvs, ",")
	}

	var parts []string
	for _, r := range b.Requests {
		switch t := r.GetType().(type) {
		case *servicepb.Request_Apply:
			switch a := t.Apply.GetAction().GetData().(type) {
			case *servicepb.LedgerAction_CreateTransaction:
				ct := a.CreateTransaction
				if ct.GetReference() != "" || len(ct.GetMetadata()) > 0 {
					parts = append(parts, fmt.Sprintf("newtx:%s{%s}", ct.GetReference(), kvList(ct.GetMetadata())))
				}
			case *servicepb.LedgerAction_AddMetadata:
				parts = append(parts, fmt.Sprintf("add %s{%s}", metaTargetLabel(a.AddMetadata.GetTarget()), kvList(a.AddMetadata.GetMetadata())))
			case *servicepb.LedgerAction_DeleteMetadata:
				parts = append(parts, fmt.Sprintf("del %s/%s", metaTargetLabel(a.DeleteMetadata.GetTarget()), a.DeleteMetadata.GetKey()))
			}
		case *servicepb.Request_SaveLedgerMetadata:
			parts = append(parts, fmt.Sprintf("saveL %s{%s}", t.SaveLedgerMetadata.GetLedger(), kvList(t.SaveLedgerMetadata.GetMetadata())))
		case *servicepb.Request_DeleteLedgerMetadata:
			parts = append(parts, fmt.Sprintf("delL %s/%s", t.DeleteLedgerMetadata.GetLedger(), t.DeleteLedgerMetadata.GetKey()))
		case *servicepb.Request_SetMetadataFieldType:
			ft := t.SetMetadataFieldType
			parts = append(parts, fmt.Sprintf("setFT %s/tgt%d/%s=ty%d", ft.GetLedger(), ft.GetTargetType(), ft.GetKey(), ft.GetType()))
		case *servicepb.Request_RemoveMetadataFieldType:
			ft := t.RemoveMetadataFieldType
			parts = append(parts, fmt.Sprintf("rmFT %s/tgt%d/%s", ft.GetLedger(), ft.GetTargetType(), ft.GetKey()))
		case *servicepb.Request_CreateIndex:
			ci := t.CreateIndex
			parts = append(parts, fmt.Sprintf("crIdx %s/%s", ci.GetLedger(), indexes.Canonical(ci.GetId())))
		case *servicepb.Request_DropIndex:
			di := t.DropIndex
			parts = append(parts, fmt.Sprintf("drIdx %s/%s", di.GetLedger(), indexes.Canonical(di.GetId())))
		}
	}

	return "[" + strings.Join(parts, " ") + "]"
}

func typeOps(b oracle.Bulk) string {
	var ops []string
	for _, r := range b.Requests {
		switch t := r.GetType().(type) {
		case *servicepb.Request_AddAccountType:
			ops = append(ops, "+"+t.AddAccountType.GetAccountType().GetName())
		case *servicepb.Request_RemoveAccountType:
			ops = append(ops, "-"+t.RemoveAccountType.GetName())
		}
	}

	return strings.Join(ops, "")
}

func metaTargetLabel(target *commonpb.Target) string {
	switch t := target.GetTarget().(type) {
	case *commonpb.Target_Account:
		return t.Account.GetAddr()
	case *commonpb.Target_TransactionId:
		return fmt.Sprintf("tx:%d", t.TransactionId)
	default:
		return "?"
	}
}

// renderPostings formats a posting list for assertion details.
func renderPostings(ps []*commonpb.Posting) string {
	out := ""
	for _, p := range ps {
		var amt uint256.Int
		p.GetAmount().IntoUint256(&amt)
		if out != "" {
			out += ","
		}
		out += p.GetSource() + "->" + p.GetDestination() + ":" + amt.Dec() + p.GetAsset() + "|" + p.GetColor()
	}

	return "[" + out + "]"
}

// Server log sequences — for verifying drain order vs commit order.
func logSeqs(logs []*commonpb.Log) string {
	ids := make([]string, len(logs))
	for i, l := range logs {
		ids[i] = strconv.FormatUint(l.GetSequence(), 10)
	}

	return "[" + strings.Join(ids, ",") + "]"
}

// failureDiag renders a failed bulk's postings, the model's declared account
// types for the ledgers it touches, and the type-ops in the pending/in-flight
// buffer with their ticket relative to the failure's maxTicket.
func (t *Tracker) failureDiag(b oracle.Bulk, maxTicket uint64) (postings, modelTypes, buffered string) {
	var ps []string
	ledgers := map[string]bool{}
	for _, r := range b.Requests {
		l := oracle.LedgerOf(r)
		ledgers[l] = true
		if ct := r.GetApply().GetAction().GetCreateTransaction(); ct != nil {
			for _, p := range ct.GetPostings() {
				ps = append(ps, fmt.Sprintf("%s:%s->%s(%s)", l, p.GetSource(), p.GetDestination(), p.GetAsset()))
			}
		}
	}

	var types []string
	for l := range ledgers {
		var names []string
		for n, ts := range t.modelState.Ledger(l).Types().All() {
			names = append(names, fmt.Sprintf("%s(%s)", n, ts.Pattern))
		}
		sort.Strings(names)
		types = append(types, l+"=["+strings.Join(names, ",")+"]")
	}
	sort.Strings(types)

	within := func(tk uint64) string {
		if tk <= maxTicket {
			return "≤"
		}

		return ">"
	}

	var pend, infl []string
	for _, pe := range t.pending {
		if ops := typeOps(t.records[pe.wid].Bulk); ops != "" {
			pend = append(pend, fmt.Sprintf("t%d%s%s", pe.ticket, within(pe.ticket), ops))
		}
	}
	for _, rec := range t.InflightBulks() {
		if ops := typeOps(rec.Bulk); ops != "" {
			infl = append(infl, fmt.Sprintf("t%d%s%s", rec.Ticket, within(rec.Ticket), ops))
		}
	}
	sort.Strings(pend)
	sort.Strings(infl)

	buffered = fmt.Sprintf("max=%d pendingTypeOps=[%s] inflightTypeOps=[%s]", maxTicket, strings.Join(pend, " "), strings.Join(infl, " "))

	return "[" + strings.Join(ps, " ") + "]", "[" + strings.Join(types, " ") + "]", buffered
}

// DescribeCandidates renders, for offline replay diagnostics, what each
// candidate base predicts for a bulk (up to limit bases).
func (t *Tracker) DescribeCandidates(maxTicket uint64, b oracle.Bulk, limit int) []string {
	var out []string
	t.walkCandidateStates(maxTicket, func(base oracle.GlobalState) bool {
		res := base.Apply(b)
		verdict := "OK"
		if !res.OK {
			verdict = res.Reason
		}
		out = append(out, fmt.Sprintf("base#%d %s", len(out), verdict))

		return len(out) >= limit
	}, nil)

	return out
}

func sortedVolumeKeys(m map[oracle.VolumeKey]oracle.VolumePair) []oracle.VolumeKey {
	keys := make([]oracle.VolumeKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, oracle.CompareVolumeKey)

	return keys
}

// BulkLedgers, RequestKinds and BulkMeta are exported for family diagnostics.
func BulkLedgers(b oracle.Bulk) string  { return bulkLedgers(b) }
func RequestKinds(b oracle.Bulk) string { return requestKinds(b) }
func BulkMeta(b oracle.Bulk) string     { return bulkMeta(b) }

func bulkLedgers(b oracle.Bulk) string {
	seen := map[string]bool{}
	var names []string
	for _, r := range b.Requests {
		l := oracle.LedgerOf(r)
		if !seen[l] {
			seen[l] = true
			names = append(names, l)
		}
	}
	sort.Strings(names)

	return "[" + strings.Join(names, ",") + "]"
}

func renderMetaMap(m map[string]*commonpb.MetadataValue) string {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, k+"="+oracle.MetaValueString(v))
	}
	sort.Strings(parts)

	return "{" + strings.Join(parts, ",") + "}"
}

func modelIndexDump(ls oracle.LedgerState) string {
	var parts []string
	for canon, active := range ls.Indexes().All() {
		state := "?"
		if active {
			state = "a"
		}
		parts = append(parts, fmt.Sprintf("%s[%s]", canon, state))
	}
	sort.Strings(parts)

	return "[" + strings.Join(parts, " ") + "]"
}
