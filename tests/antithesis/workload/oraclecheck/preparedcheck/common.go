package preparedcheck

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Registered names (DESIGN §5.3).
const (
	KindListPreparedQueries  = "ListPreparedQueries"
	KindExecutePreparedQuery = "ExecutePreparedQuery"
	ViewPreparedQueries      = "preparedQueries"
	BeginHookSerialize       = "prepared-serialize"
	FailureHookName          = "prepared-failures"
	extKey                   = "prepared"
)

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: KindListPreparedQueries, Validate: ValidateListPreparedQueries})
	oc.RegisterReadKind(oc.ReadKind{Name: KindExecutePreparedQuery, Validate: ValidateExecutePreparedQuery})
	oc.RegisterView(ViewPreparedQueries, viewPreparedQueries)
	oc.RegisterBeginHook(BeginHookSerialize, BeginSerialize)
	oc.RegisterFailureHook(FailureHookName, FailureCoverage)
}

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// decode reads protojson b into m; an empty body is the zero message.
func decode(b []byte, m proto.Message) error {
	if len(b) == 0 {
		return nil
	}

	return unmarshal.Unmarshal(b, m)
}

func rawOrNull(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}

	return json.RawMessage(b)
}

// rpcError is the classification view of an outcome.
func rpcError(o oc.Outcome) match.RPCError {
	if o.OK {
		return match.NoError
	}

	return match.RPCErrorFrom(o.Code, o.Reason, o.Metadata)
}

func outcomeDetails(o oc.Outcome) map[string]any {
	return map[string]any{"code": o.Code, "reason": o.Reason, "message": o.Message}
}

// ParamKindName is the view's spelling of a parameter kind.
func ParamKindName(k match.ParamKind) string {
	switch k {
	case match.ParamKindAddress:
		return "address"
	case match.ParamKindString:
		return "string"
	case match.ParamKindUint:
		return "uint"
	case match.ParamKindInt:
		return "int"
	default:
		return "bool"
	}
}

// ViewParam is one parameter reference of a stored filter.
type ViewParam struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// ViewQuery is one stored prepared query as the view answers it.
type ViewQuery struct {
	Name   string      `json:"name"`
	Target string      `json:"target"`
	Params []ViewParam `json:"params"`
}

type viewParams struct {
	Ledger string `json:"ledger"`
	Owned  bool   `json:"owned"`
}

// viewPreparedQueries answers {"queries":[{name,target,params:[{name,kind}]}]}
// for the resolved ledger on the snapshot, name-sorted, capped at ViewCap.
// Reserved (`own-`) names are hidden unless owned. A ledger that is not live
// on the snapshot has no queries (aiming only).
func viewPreparedQueries(snap oracle.GlobalState, _ *oc.Tracker, params json.RawMessage) (any, error) {
	var p viewParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	queries := []ViewQuery{}
	ls, live := match.LiveLedgerState(snap, p.Ledger)
	if live {
		for _, name := range ls.PreparedQueryNames() {
			if !p.Owned && oc.IsReservedKey(name) {
				continue
			}
			if len(queries) >= oc.ViewCap {
				break
			}
			q, _ := ls.PreparedQuery(name)
			vq := ViewQuery{Name: name, Target: q.GetTarget().String(), Params: []ViewParam{}}
			seen := map[string]bool{}
			for _, need := range match.CollectParams(q.GetFilter()) {
				if seen[need.Name] {
					continue
				}
				seen[need.Name] = true
				vq.Params = append(vq.Params, ViewParam{Name: need.Name, Kind: ParamKindName(need.Kind)})
			}
			queries = append(queries, vq)
		}
	}

	return map[string]any{"queries": queries}, nil
}

// describePreparedQueries renders a registry for a finding's details,
// name-sorted (M/prepared_queries.go describePreparedQueries).
func describePreparedQueries(queries []*commonpb.PreparedQuery) string {
	sorted := append([]*commonpb.PreparedQuery(nil), queries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].GetName() < sorted[j].GetName() })
	parts := make([]string, 0, len(sorted))
	for _, q := range sorted {
		f, _ := protojson.Marshal(q.GetFilter())
		parts = append(parts, fmt.Sprintf("%s[%s]=%s", q.GetName(), q.GetTarget().String(), f))
	}

	return strings.Join(parts, " ")
}

// modelPreparedQueries renders the drained registry of ledger.
func modelPreparedQueries(gs oracle.GlobalState, ledger string) string {
	ls := gs.Ledger(ledger)
	out := make([]*commonpb.PreparedQuery, 0, ls.PreparedQueries().Len())
	for _, name := range ls.PreparedQueryNames() {
		q, _ := ls.PreparedQuery(name)
		out = append(out, q)
	}

	return describePreparedQueries(out)
}

// describeParams renders a parameter map, name-sorted.
func describeParams(params match.PreparedParams) string {
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		b, _ := protojson.Marshal(params[name])
		parts = append(parts, name+"="+string(b))
	}

	return strings.Join(parts, ",")
}
