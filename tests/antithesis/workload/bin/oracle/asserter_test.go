package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
	oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"
)

// DESIGN §5.3: every EventID constant of oraclecheck/** (and of this
// package) that is not a RegisterCoverage probe has exactly one
// registerAsserter in bin/oracle/*.go whose body calls assert.<Kind> with a
// string literal equal to the constant's value; no assertion literal is used
// twice in the service.

func parseDir(t *testing.T, root string, recursive bool) []*ast.File {
	t.Helper()
	fset := token.NewFileSet()
	var files []*ast.File
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (!recursive || d.Name() == "testdata") {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		files = append(files, f)

		return nil
	})
	require.NoError(t, err)

	return files
}

func isEventIDType(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name == "EventID"
	case *ast.SelectorExpr:
		return x.Sel.Name == "EventID"
	}

	return false
}

// eventIDConsts maps constant name → literal value.
func eventIDConsts(t *testing.T, files []*ast.File) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				if vs.Type == nil || !isEventIDType(vs.Type) {
					continue
				}
				for i, name := range vs.Names {
					lit, ok := vs.Values[i].(*ast.BasicLit)
					require.True(t, ok, "%s must be a string literal", name.Name)
					v, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					out[name.Name] = v
				}
			}
		}
	}

	return out
}

func identName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	}

	return ""
}

func isAssertCall(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "assert" {
		return "", false
	}

	return sel.Sel.Name, true
}

// assertLiteral returns the message literal of an assert.<Kind> call.
func assertLiteral(call *ast.CallExpr, kind string) (string, bool) {
	idx := 0
	switch kind {
	case "Always", "AlwaysOrUnreachable", "Sometimes":
		idx = 1
	case "Unreachable", "Reachable":
		idx = 0
	default:
		return "", false
	}
	if len(call.Args) <= idx {
		return "", false
	}
	lit, ok := call.Args[idx].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)

	return v, err == nil
}

func TestAsserters_OnePerEventID(t *testing.T) {
	t.Parallel()
	libFiles := parseDir(t, "../../oraclecheck", true)
	svcFiles := parseDir(t, ".", false)

	consts := eventIDConsts(t, libFiles)
	for k, v := range eventIDConsts(t, svcFiles) {
		consts[k] = v
	}
	require.NotEmpty(t, consts)

	// RegisterCoverage arguments are exempt.
	coverage := map[string]bool{}
	for _, f := range append(append([]*ast.File{}, libFiles...), svcFiles...) {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if ok && identName(call.Fun) == "RegisterCoverage" {
				for _, a := range call.Args {
					coverage[identName(a)] = true
				}
			}

			return true
		})
	}

	asserterCount := map[string]int{}
	literalUses := map[string]int{}
	for _, f := range svcFiles {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if kind, isAssert := isAssertCall(call); isAssert {
				if lit, ok := assertLiteral(call, kind); ok {
					literalUses[lit]++
				}
			}
			if identName(call.Fun) != "registerAsserter" || len(call.Args) != 2 {
				return true
			}
			name := identName(call.Args[0])
			value, known := consts[name]
			require.True(t, known, "registerAsserter for unknown id %s", name)
			asserterCount[name]++
			fn, ok := call.Args[1].(*ast.FuncLit)
			require.True(t, ok, "asserter of %s must be a function literal", name)
			var lits []string
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if kind, isAssert := isAssertCall(c); isAssert {
						lit, ok := assertLiteral(c, kind)
						require.True(t, ok, "asserter of %s must call assert.%s with a literal", name, kind)
						lits = append(lits, lit)
					}
				}

				return true
			})
			require.Equal(t, []string{value}, lits, "asserter of %s", name)

			return true
		})
	}

	for name := range consts {
		if coverage[name] {
			require.Zero(t, asserterCount[name], "%s is a Raw probe and must not have an asserter", name)

			continue
		}
		require.Equal(t, 1, asserterCount[name], "%s must have exactly one asserter", name)
	}
	for lit, n := range literalUses {
		require.Equal(t, 1, n, "assertion literal %q is used %d times", lit, n)
	}

	// The runtime registry matches.
	for name, value := range consts {
		if !coverage[name] {
			_, ok := asserters[oc.EventID(value)]
			require.True(t, ok, "%s has no runtime asserter", name)
		}
	}
}

// The asserters really emit their literal through the SDK.
func TestAsserters_EmitLiterals(t *testing.T) {
	t.Parallel()
	if !assert.Enabled {
		t.Skip("SDK emission needs -tags enable_antithesis_sdk")
	}
	events := sdktest.Capture(t, func() {
		assertEvents([]oc.Event{
			oc.Reached(oc.EvModelOutcomeVerified, nil),
			oc.Violation(oc.EvFailureUnexplained, map[string]any{"reason": "X"}),
			oc.Sometimes(oc.EvTwoDefinitiveOutcomes, true, nil),
			{ID: "oracle: no such event", Kind: oc.EvViolation},
		})
	})
	if events == nil {
		return
	}
	require.True(t, sdktest.Find(t, events, string(oc.EvModelOutcomeVerified)).Hit)
	require.False(t, sdktest.Find(t, events, string(oc.EvFailureUnexplained)).Condition)
	require.True(t, sdktest.Find(t, events, string(oc.EvTwoDefinitiveOutcomes)).Condition)
	sdktest.Find(t, events, "oracle protocol: unmapped event")
}
