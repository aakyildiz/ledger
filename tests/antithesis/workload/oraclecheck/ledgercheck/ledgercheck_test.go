package ledgercheck

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/match/matchtest"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck/testutil"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

const prefix = testutil.DefaultPrefix

func pj(t *testing.T, m proto.Message) json.RawMessage {
	t.Helper()
	b, err := protojson.Marshal(m)
	require.NoError(t, err)

	return b
}

// fleet builds a session with l1 (chart + metadata + STRICT), l2 (AUDIT,
// schema), l3 (deleted) and the mirror m4.
func fleet(t *testing.T) *testutil.ScriptedSession {
	t.Helper()
	s := testutil.NewSession(prefix)
	l1, l2, l3 := s.Ledger(1), s.Ledger(2), s.Ledger(3)
	require.Empty(t, testutil.Findings(s.Write(false, testutil.CreateLedgerReq(l1), testutil.CreateLedgerReq(l2), testutil.CreateLedgerReq(l3))))
	require.Empty(t, testutil.Findings(s.Write(false,
		testutil.AddTypeReq(l1, "t-3", "t-3:{id}"),
		actions.SaveLedgerMetadataAction(l1, map[string]string{"k0": "v"}),
		&servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
			Ledger: l2, EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
		}}},
	)))
	require.Empty(t, testutil.Findings(s.Write(false, testutil.DeleteLedgerReq(l3))))
	mirror := &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
		Name: prefix + "m4", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "unused"},
	}}}
	require.Empty(t, testutil.Findings(s.Write(false, mirror)))

	return s
}

func readCtx(t *testing.T, s *testutil.ScriptedSession, kind string, req proto.Message, out oc.Outcome) *oc.ReadCtx {
	t.Helper()
	ticket := s.Read(kind)

	return &oc.ReadCtx{T: s.T, Ticket: ticket, MaxTicket: s.T.ReadFrontier(), Snapshot: s.T.Model(), Kind: kind, Request: pj(t, req), Outcome: out}
}

func ids(evs []oc.Event) []oc.EventID { return testutil.IDs(evs) }

func findingIDs(evs []oc.Event) []oc.EventID { return testutil.IDs(testutil.Findings(evs)) }

func identityInfo(s *testutil.ScriptedSession, name string) *commonpb.LedgerInfo {
	return matchtest.RenderLedgerInfoWithIdentity(s.Server, name, s.T.LedgerIdentities()[name].MatchIdentity())
}

// ---- GetLedger ----

func TestGetLedger(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	l1 := s.Ledger(1)

	// A faithful answer.
	info := identityInfo(s, l1)
	require.NotEmpty(t, info.GetAccountTypes())
	evs := ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l1}, oc.Outcome{OK: true, Response: pj(t, info)}))
	require.Empty(t, findingIDs(evs))

	// One mutated field each: chart, metadata, enforcement.
	for _, mutate := range []func(*commonpb.LedgerInfo){
		func(i *commonpb.LedgerInfo) { i.AccountTypes["t-3"].Pattern = "t-3:x:{id}" },
		func(i *commonpb.LedgerInfo) { i.Metadata["k0"] = matchtest.Str("other") },
		func(i *commonpb.LedgerInfo) {
			i.DefaultEnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT
		},
	} {
		bad := proto.Clone(info).(*commonpb.LedgerInfo)
		mutate(bad)
		evs := ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l1}, oc.Outcome{OK: true, Response: pj(t, bad)}))
		require.Equal(t, []oc.EventID{EvLcLedgerReadOutsideModel}, findingIDs(evs))
	}

	// A deleted ledger served is outside the model; NotFound on it is explained.
	l3 := s.Ledger(3)
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l3}, oc.Outcome{OK: true, Response: pj(t, &commonpb.LedgerInfo{Name: l3})}))
	require.Equal(t, []oc.EventID{EvLcLedgerReadOutsideModel}, findingIDs(evs))
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l3}, testutil.Fail("NotFound", "")))
	require.Empty(t, evs)

	// NotFound on a live tracked ledger is unexplained.
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l1}, testutil.Fail("NotFound", "")))
	require.Equal(t, []oc.EventID{oc.EvLedgerNotFoundUnexplained}, findingIDs(evs))

	// ... unless its create is still in flight (a candidate before it).
	l5 := s.Ledger(5)
	tk := s.Begin(false, testutil.CreateLedgerReq(l5))
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l5}, testutil.Fail("NotFound", "")))
	require.Empty(t, evs)
	// and the served ledger is legal too while in flight.
	served := matchtest.RenderLedgerInfo(s.Server.Apply(oracle.Bulk{Requests: []*servicepb.Request{testutil.CreateLedgerReq(l5)}}).State, l5)
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l5}, oc.Outcome{OK: true, Response: pj(t, served)}))
	require.Empty(t, evs)
	s.End(tk)

	// Absent probes.
	absent := AbsentPrefix(prefix) + "77"
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: absent}, testutil.Fail("NotFound", "")))
	require.Equal(t, []oc.EventID{EvLcGetLedgerAbsentProbed}, ids(evs))
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: absent}, oc.Outcome{OK: true, Response: pj(t, &commonpb.LedgerInfo{Name: absent})}))
	require.Equal(t, []oc.EventID{EvLcGetLedgerServedAbsent}, findingIDs(evs))

	// Any other error.
	evs = ValidateGetLedger(readCtx(t, s, KindGetLedger, &servicepb.GetLedgerRequest{Ledger: l1}, testutil.Fail("Internal", "")))
	require.Equal(t, []oc.EventID{EvLcGetLedgerUnexpected}, findingIDs(evs))
}

// ---- GetMetadataSchemaStatus ----

func schemaOf(s *testutil.ScriptedSession, ledger string) *servicepb.GetMetadataSchemaStatusResponse {
	ls := s.Server.Ledger(ledger)
	conv := func(m oracle.Map[string, commonpb.MetadataType]) map[string]*servicepb.MetadataFieldStatus {
		out := map[string]*servicepb.MetadataFieldStatus{}
		for k, ty := range m.All() {
			out[k] = &servicepb.MetadataFieldStatus{DeclaredType: ty}
		}

		return out
	}

	return &servicepb.GetMetadataSchemaStatusResponse{
		AccountFields:     conv(ls.AccountFieldTypes()),
		TransactionFields: conv(ls.TransactionFieldTypes()),
		LedgerFields:      conv(ls.LedgerFieldTypes()),
	}
}

func TestSchemaStatus(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	l2 := s.Ledger(2)
	setType := &servicepb.Request{Type: &servicepb.Request_SetMetadataFieldType{SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
		Ledger: l2, TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "k1", Type: commonpb.MetadataType_METADATA_TYPE_STRING,
	}}}
	require.Empty(t, testutil.Findings(s.Write(false, setType)))

	good := schemaOf(s, l2)
	require.Len(t, good.GetAccountFields(), 1)
	evs := ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: l2}, oc.Outcome{OK: true, Response: pj(t, good)}))
	require.Empty(t, evs)

	bad := proto.Clone(good).(*servicepb.GetMetadataSchemaStatusResponse)
	bad.AccountFields["k1"].DeclaredType = commonpb.MetadataType_METADATA_TYPE_INT64
	evs = ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: l2}, oc.Outcome{OK: true, Response: pj(t, bad)}))
	require.Equal(t, []oc.EventID{EvLcSchemaReadOutsideModel}, findingIDs(evs))

	absent := AbsentPrefix(prefix) + "3"
	evs = ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: absent}, testutil.Fail("NotFound", "")))
	require.Equal(t, []oc.EventID{EvLcSchemaAbsentProbed}, ids(evs))
	evs = ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: absent}, oc.Outcome{OK: true, Response: pj(t, good)}))
	require.Equal(t, []oc.EventID{EvLcSchemaServedAbsent}, findingIDs(evs))
	evs = ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: s.Ledger(3)}, testutil.Fail("NotFound", "")))
	require.Empty(t, evs)
	evs = ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: l2}, testutil.Fail("NotFound", "")))
	require.Equal(t, []oc.EventID{oc.EvLedgerNotFoundUnexplained}, findingIDs(evs))
	evs = ValidateSchemaStatus(readCtx(t, s, KindSchemaStatus, &servicepb.GetMetadataSchemaStatusRequest{Ledger: l2}, testutil.Fail("FailedPrecondition", "VALIDATION")))
	require.Equal(t, []oc.EventID{EvLcSchemaUnexpected}, findingIDs(evs))
}

// ---- ListLedgers ----

var foreign = []string{"billing", "default", "dept-a", "marketplace", "r-maint-x", "r-ns-x-1", "treasury"}

// page renders a model-consistent page with foreign rows on both sides and
// the tracker's learned identities on the tracked rows.
func page(t *testing.T, s *testutil.ScriptedSession, req match.ListLedgersReq) ([]*commonpb.LedgerInfo, string) {
	t.Helper()
	rows, next := matchtest.RenderLedgerPage(s.Server, prefix, foreign, req)
	for i, r := range rows {
		if id, ok := s.T.LedgerIdentities()[r.GetName()]; ok {
			rows[i] = identityInfo(s, r.GetName())
			require.True(t, id.MatchIdentity().Matches(rows[i]))
		}
	}

	return rows, next
}

func listOutcome(t *testing.T, rows []*commonpb.LedgerInfo, next string) oc.Outcome {
	t.Helper()
	items := make([]json.RawMessage, 0, len(rows))
	for _, r := range rows {
		items = append(items, pj(t, r))
	}

	return oc.Outcome{OK: true, Items: items, Complete: true, NextCursor: next}
}

func listReq(req match.ListLedgersReq) *servicepb.ListLedgersRequest {
	return &servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{PageSize: req.PageSize, Cursor: req.Cursor, Reverse: req.Reverse}}
}

func TestListLedgersFaithfulPages(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	cursors := []string{"", match.PageToken(prefix), match.PageToken("o-test."), match.PageToken(s.Ledger(1)), match.PageToken(s.Ledger(3)), match.PageToken("a"), match.PageToken("zz")}
	for _, cur := range cursors {
		for _, size := range []uint32{0, 1, 2, 3, 5, 50, 2000} {
			for _, reverse := range []bool{false, true} {
				req := match.ListLedgersReq{Cursor: cur, PageSize: size, Reverse: reverse}
				rows, next := page(t, s, req)
				evs := ValidateListLedgers(readCtx(t, s, KindListLedgers, listReq(req), listOutcome(t, rows, next)))
				require.Empty(t, findingIDs(evs), "cursor %q size %d reverse %v", cur, size, reverse)
				require.Contains(t, ids(evs), EvLcListingValidated)
				if next != "" {
					require.Contains(t, ids(evs), EvLcListingTruncated)
				}
				if cur == match.PageToken(s.Ledger(3)) {
					require.Contains(t, ids(evs), EvLcListingResumedAtDeleted)
				}
			}
		}
	}
}

func TestListLedgersFindings(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	req := match.ListLedgersReq{PageSize: 50}
	rows, next := page(t, s, req)
	require.Len(t, rows, len(foreign)+3) // l1, l2, m4 live; l3 deleted
	check := func(rows []*commonpb.LedgerInfo, next string, want oc.EventID) {
		t.Helper()
		evs := ValidateListLedgers(readCtx(t, s, KindListLedgers, listReq(req), listOutcome(t, rows, next)))
		require.Equal(t, []oc.EventID{want}, findingIDs(evs))
	}
	clone := func() []*commonpb.LedgerInfo {
		out := make([]*commonpb.LedgerInfo, len(rows))
		for i, r := range rows {
			out[i] = proto.Clone(r).(*commonpb.LedgerInfo)
		}

		return out
	}
	trackedAt := slices.IndexFunc(rows, func(r *commonpb.LedgerInfo) bool { return r.GetName() == s.Ledger(1) })
	require.GreaterOrEqual(t, trackedAt, 0)

	// A tracked ledger dropped from the window.
	c := clone()
	check(append(c[:trackedAt:trackedAt], c[trackedAt+1:]...), next, EvLcListingNotWindow)
	// The deleted ledger listed.
	c = clone()
	ghost := identityInfo(s, s.Ledger(1))
	ghost.Name, ghost.Id = s.Ledger(3), 999
	check(append(c[:trackedAt+1:trackedAt+1], append([]*commonpb.LedgerInfo{ghost}, c[trackedAt+1:]...)...), next, EvLcListingNotWindow)
	// A foreign row between two tracked rows.
	c = clone()
	f := c[0]
	c = append(c[1:trackedAt+1:trackedAt+1], append([]*commonpb.LedgerInfo{f}, c[trackedAt+1:]...)...)
	check(c, next, EvLcListingNotWindow)
	// A wrong chart on a tracked row.
	c = clone()
	c[trackedAt].AccountTypes["t-3"].Persistence = commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT
	check(c, next, EvLcListingOutsideModel)
	// The mirror row without sync progress.
	c = clone()
	m := slices.IndexFunc(c, func(r *commonpb.LedgerInfo) bool { return r.GetName() == prefix+"m4" })
	c[m].MirrorSyncProgress = nil
	check(c, next, EvLcListingOutsideModel)
	// A short page publishing a next cursor.
	check(clone(), match.PageToken("zzz"), EvLcListingNotWindow)
	// Structural, foreign rows included.
	c = clone()
	check(append(c, proto.Clone(c[0]).(*commonpb.LedgerInfo)), next, EvLcListingRepeated)
	c = clone()
	c[0].CreatedAt = nil
	check(c, next, EvLcListingMalformed)
	c = clone()
	c[1].Id = c[0].GetId()
	check(c, next, EvLcListingReusedID)
	c = clone()
	c[0].DeletedAt = &commonpb.Timestamp{Data: 1}
	check(c, next, EvLcListingMalformed)
	// A tracked identity other than the one creation reported.
	c = clone()
	c[trackedAt].Id += 1000
	check(c, next, EvLcListingIdentity)
	// Errors and filters.
	evs := ValidateListLedgers(readCtx(t, s, KindListLedgers, listReq(req), testutil.Fail("Internal", "")))
	require.Equal(t, []oc.EventID{EvLcListingUnexpected}, findingIDs(evs))
	filtered := listReq(req)
	filtered.Options.Filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "t-3:"}}}}
	evs = ValidateListLedgers(readCtx(t, s, KindListLedgers, filtered, testutil.Fail("InvalidArgument", "")))
	require.Equal(t, []oc.EventID{EvLcListingRejectedFilter}, ids(evs))
	evs = ValidateListLedgers(readCtx(t, s, KindListLedgers, filtered, listOutcome(t, rows, next)))
	require.Equal(t, []oc.EventID{EvLcListingAcceptedFilter}, findingIDs(evs))
	// A stream the client cut short is not judged.
	out := listOutcome(t, rows[:1], "")
	out.Complete = false
	require.Empty(t, ValidateListLedgers(readCtx(t, s, KindListLedgers, listReq(req), out)))
}

// A ledger whose create is in flight may be listed with any identity (none
// is learned yet), and may be absent from the page.
func TestListLedgersInflightCreate(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	l6 := s.Ledger(6)
	tk := s.Begin(false, testutil.CreateLedgerReq(l6))
	req := match.ListLedgersReq{PageSize: 50, Cursor: match.PageToken(prefix)}
	after := s.Server.Apply(oracle.Bulk{Requests: []*servicepb.Request{testutil.CreateLedgerReq(l6)}}).State
	with, next := matchtest.RenderLedgerPage(after, prefix, foreign, req)
	for i, r := range with {
		if id, ok := s.T.LedgerIdentities()[r.GetName()]; ok {
			with[i] = matchtest.RenderLedgerInfoWithIdentity(after, r.GetName(), id.MatchIdentity())
		}
	}
	evs := ValidateListLedgers(readCtx(t, s, KindListLedgers, listReq(req), listOutcome(t, with, next)))
	require.Empty(t, findingIDs(evs))
	without, next := page(t, s, req)
	evs = ValidateListLedgers(readCtx(t, s, KindListLedgers, listReq(req), listOutcome(t, without, next)))
	require.Empty(t, findingIDs(evs))
	s.End(tk)
}

// ---- coverage hooks ----

func rawIDs(evs []oc.Event) []oc.EventID {
	var out []oc.EventID
	for _, e := range evs {
		if e.Kind == oc.EvRaw && e.Cond {
			out = append(out, e.ID)
		}
	}

	return out
}

func nestedAdd(ledger, name string, skip bool) *servicepb.Request {
	a := &servicepb.LedgerApplyRequest{Ledger: ledger, Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{
		AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: &commonpb.AccountType{Name: name, Pattern: name + ":{id}"}},
	}}}
	if skip {
		a.SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS}
	}

	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: a}}
}

func nestedRemove(ledger, name string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger, SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND},
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: name}}},
	}}}
}

func setMode(ledger string, mode commonpb.ChartEnforcementMode, nested bool, badSkip bool) *servicepb.Request {
	if !nested {
		return &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{Ledger: ledger, EnforcementMode: mode}}}
	}
	a := &servicepb.LedgerApplyRequest{Ledger: ledger, Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{
		SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{EnforcementMode: mode},
	}}}
	if badSkip {
		a.SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND}
	}

	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: a}}
}

// applyHooks commits reqs through the session's tracker, whose registered
// drain hooks (this package's DrainHook) run after the agreed commit.
func applyHooks(t *testing.T, s *testutil.ScriptedSession, reqs ...*servicepb.Request) []oc.Event {
	t.Helper()
	evs := s.Write(false, reqs...)
	require.Empty(t, testutil.Findings(evs))

	return evs
}

func TestApplySurfaceCoverage(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	l1 := s.Ledger(1)
	strict, audit := commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT

	evs := applyHooks(t, s, nestedAdd(l1, "t-3", true), setMode(l1, audit, false, false), nestedRemove(l1, "t-9"), setMode(l1, strict, true, false))
	require.ElementsMatch(t, []oc.EventID{EvLcCovSkipTypeExists, EvLcCovSkipTypeNotFound, EvLcCovTopLevelAudit, EvLcCovLedgerActionStrict}, rawIDs(evs))
	evs = applyHooks(t, s, setMode(l1, audit, true, false), setMode(l1, strict, false, false))
	require.ElementsMatch(t, []oc.EventID{EvLcCovLedgerActionAudit, EvLcCovTopLevelStrict}, rawIDs(evs))
	// Two skips in a row: only the second one is followed by an executed order.
	evs = applyHooks(t, s, nestedAdd(l1, "t-3", true), nestedRemove(l1, "t-9"), setMode(l1, strict, false, false))
	require.ElementsMatch(t, []oc.EventID{EvLcCovSkipTypeNotFound, EvLcCovTopLevelStrict}, rawIDs(evs))
	// A skip as the last order proves no continuation.
	evs = applyHooks(t, s, nestedAdd(l1, "t-3", true))
	require.Empty(t, rawIDs(evs))

	// The invalid opt-in: VALIDATION at admission.
	bad := setMode(l1, strict, true, true)
	res := s.Server.Apply(oracle.Bulk{Requests: []*servicepb.Request{bad}})
	require.False(t, res.OK)
	require.Equal(t, "VALIDATION", res.Reason)
	require.Equal(t, []oc.EventID{EvLcCovInvalidSkip}, rawIDs(FailureHook(s.T, oc.WriteRecord{Requests: []*servicepb.Request{bad}}, res, res.Reason)))
	require.Empty(t, rawIDs(FailureHook(s.T, oc.WriteRecord{Requests: []*servicepb.Request{testutil.EmptyTxReq(l1)}}, res, "VALIDATION")))
}

func TestLifecycleCoverage(t *testing.T) {
	t.Parallel()
	s := fleet(t)
	l7 := s.Ledger(7)
	applyHooks(t, s, testutil.CreateLedgerReq(l7))
	applyHooks(t, s, testutil.DeleteLedgerReq(l7))
	recreate := []*servicepb.Request{testutil.CreateLedgerReq(l7)}
	res := s.Server.Apply(oracle.Bulk{Requests: recreate})
	require.Equal(t, "LEDGER_DELETED", res.Reason)
	require.Equal(t, []oc.EventID{EvLcCovDeletion}, rawIDs(FailureHook(s.T, oc.WriteRecord{Requests: recreate}, res, res.Reason)))
	require.Empty(t, rawIDs(FailureHook(s.T, oc.WriteRecord{Requests: recreate}, res, res.Reason)), "once per deletion")

	// Promotion: a chart order on the promoted ledger does not count; a
	// committed transaction does.
	m4 := prefix + "m4"
	applyHooks(t, s, &servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: m4}}})
	require.Empty(t, rawIDs(applyHooks(t, s, nestedRemove(m4, "t-9"))), "a skipped chart order proves nothing")
	require.Equal(t, []oc.EventID{EvLcCovPromotion}, rawIDs(applyHooks(t, s, testutil.TxReq(m4, "", "world", "users:1", "COIN", 1))))
	require.Empty(t, rawIDs(applyHooks(t, s, testutil.TxReq(m4, "", "world", "users:1", "COIN", 1))))
}

// The hooks are registered and wired through the tracker: a real session
// emits the deletion probe at /write/end.
func TestHooksRegistered(t *testing.T) {
	t.Parallel()
	_, ok := oc.LookupReadKind(KindGetLedger)
	require.True(t, ok)
	_, ok = oc.LookupReadKind(KindListLedgers)
	require.True(t, ok)
	_, ok = oc.LookupReadKind(KindSchemaStatus)
	require.True(t, ok)
	require.True(t, oc.IsCoverageID(EvLcCovDeletion))
	s := testutil.NewSession(prefix)
	l1 := s.Ledger(1)
	s.Write(false, testutil.CreateLedgerReq(l1))
	s.Write(false, testutil.DeleteLedgerReq(l1))
	evs := s.Write(false, testutil.CreateLedgerReq(l1))
	require.Contains(t, rawIDs(evs), EvLcCovDeletion)
}
