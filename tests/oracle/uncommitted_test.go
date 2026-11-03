package oracle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
)

// uncommittedValues is one value of every type that implements
// domain.UncommittedFailure, built with the exported constructors, sentinels
// or struct literals the server uses.
func uncommittedValues() []domain.UncommittedFailure {
	return []domain.UncommittedFailure{
		// internal/domain/errors.go:208-223
		domain.ErrMetadataLimitsUnconfigured.(domain.UncommittedFailure),
		&domain.ErrBalanceNotPreloaded{},
		&domain.ErrFilterCompilation{Detail: "x"},
		&domain.ErrInvalidApplyType{},
		&domain.ErrInvalidExecutionPlan{},
		&domain.ErrInvalidOrderType{},
		&domain.ErrMirrorV2LogIDGap{},
		&domain.ErrMirrorV2LogIDInvalid{},
		&domain.ErrNumscriptRuntime{},
		&domain.ErrSequenceExhausted{},
		&domain.ErrStaleClusterPolicy{},
		domain.ErrStaleInputsResolution.(domain.UncommittedFailure),
		domain.ErrStaleProposal.(domain.UncommittedFailure),
		&domain.ErrStorageOperation{},
		&domain.ErrTransactionStateInconsistent{},
		&domain.ErrVolumeNotMaterialized{},
		// internal/domain/errors.go:1288, 1459, 1474
		domain.NewUnclassifiedNumscriptCompile("x").(domain.UncommittedFailure),
		&domain.ErrClusterPolicyRevisionConflict{},
		&domain.ErrClusterPolicyInvalid{},
		// internal/infra/state/scope.go
		&state.ErrCoverageMiss{},
	}
}

func isFreezableReason(reason string) bool {
	return domain.IsFreezableFailure(domain.KindForReason(domain.ReasonCode(reason)))
}

// TestUncommittedReasonsMatchServerTypes derives each uncommitted error's
// reason with Describable.Reason() — the value internal/adapter/grpc/errors.go
// describableToGRPCStatus puts into ErrorInfo.reason — and requires the
// freezable ones to be exactly UncommittedReasons' freezable members.
// UncommittedReasons may list non-freezable reasons too (RecordFailure skips
// those by kind anyway), but each must come from an uncommitted type or from
// the documented PRELOAD_UNAVAILABLE path.
func TestUncommittedReasonsMatchServerTypes(t *testing.T) {
	t.Parallel()

	derived := map[string]bool{}
	derivedFreezable := map[string]bool{}
	for _, v := range uncommittedValues() {
		reason := v.Reason()
		require.NotEmpty(t, reason, "%T", v)
		_, known := domain.LookupReasonCode(reason)
		require.True(t, known, "%T reason %q is not an ErrorReason", v, reason)
		// The kind the server freezes on is the typed error's own Kind(); the
		// model decides by KindForReason. They must agree.
		require.Equal(t, v.Kind(), domain.KindForReason(domain.ReasonCode(reason)), "%T", v)
		derived[reason] = true
		if isFreezableReason(reason) {
			derivedFreezable[reason] = true
		}
	}

	listedFreezable := map[string]bool{}
	for reason := range UncommittedReasons {
		if isFreezableReason(reason) {
			listedFreezable[reason] = true
		}
	}
	require.Equal(t, derivedFreezable, listedFreezable)

	extra := map[string]bool{domain.ErrReasonPreloadUnavailable: true}
	for reason := range UncommittedReasons {
		require.True(t, derived[reason] || extra[reason], "%s has no uncommitted producer", reason)
	}
	for reason := range derived {
		require.True(t, UncommittedReasons[reason], "%s is raised uncommitted but not listed", reason)
	}
}

// TestUncommittedValuesCoverEveryType parses internal/ for every method
// declaration named UncommittedFailure and requires uncommittedValues to hold
// one value of each receiver type, so a new uncommitted error type fails here
// until it is classified.
func TestUncommittedValuesCoverEveryType(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..", "internal")
	declared := map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(src), "UncommittedFailure()") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "UncommittedFailure" {
				continue
			}
			recv := fn.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			declared[file.Name.Name+"."+recv.(*ast.Ident).Name] = true
		}

		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, declared)

	built := map[string]bool{}
	for _, v := range uncommittedValues() {
		typ := reflect.TypeOf(v)
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		built[filepath.Base(typ.PkgPath())+"."+typ.Name()] = true
	}

	keys := func(m map[string]bool) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)

		return out
	}
	require.Equal(t, keys(declared), keys(built))
}

// TestRecordFailureSkipsUncommittedReasons: no reason in UncommittedReasons is
// ever frozen, freezable kind or not.
func TestRecordFailureSkipsUncommittedReasons(t *testing.T) {
	t.Parallel()

	g := NewGlobalState()
	bulk := keyedBulk("k", pqUpdateReq("q", pqFilter("a:")))
	for reason := range UncommittedReasons {
		after := g.RecordFailure(bulk, ApplyResult{Reason: reason, Stage: StageFSM})
		require.Equal(t, g.Fingerprint(), after.Fingerprint(), reason)
	}
}
