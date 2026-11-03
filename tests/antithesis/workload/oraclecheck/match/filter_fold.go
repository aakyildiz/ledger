package match

import (
	"cmp"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// FilterFold reduces a QueryFilter tree bottom-up. FoldFilter owns the
// And/Or/Not traversal, so every walk over a filter — index needs, validity,
// matching, rendering — shares one combinator step and differs only in how it
// judges a leaf. leaf receives every non-combinator node, the nil filter
// included, so each walk decides in one place what "no condition" means.
type FilterFold[T any] struct {
	And  func(children []T) T
	Or   func(children []T) T
	Not  func(child T) T
	Leaf func(f *commonpb.QueryFilter) T
}

func FoldFilter[T any](f *commonpb.QueryFilter, fold FilterFold[T]) T {
	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		return fold.And(FoldChildren(x.And.GetFilters(), fold))
	case *commonpb.QueryFilter_Or:
		return fold.Or(FoldChildren(x.Or.GetFilters(), fold))
	case *commonpb.QueryFilter_Not:
		return fold.Not(FoldFilter(x.Not.GetFilter(), fold))
	default:
		return fold.Leaf(f)
	}
}

func FoldChildren[T any](children []*commonpb.QueryFilter, fold FilterFold[T]) []T {
	out := make([]T, 0, len(children))
	for _, child := range children {
		out = append(out, FoldFilter(child, fold))
	}

	return out
}

// AnyLeaf reports whether pred holds for some leaf of f; combinators only
// propagate their children's verdicts.
func AnyLeaf(f *commonpb.QueryFilter, pred func(*commonpb.QueryFilter) bool) bool {
	return FoldFilter(f, FilterFold[bool]{And: anyOf, Or: anyOf, Not: identity[bool], Leaf: pred})
}

// VisitLeaves calls visit on every leaf of f in tree order.
func VisitLeaves(f *commonpb.QueryFilter, visit func(*commonpb.QueryFilter)) {
	FoldFilter(f, FilterFold[struct{}]{
		And: func([]struct{}) struct{} { return struct{}{} },
		Or:  func([]struct{}) struct{} { return struct{}{} },
		Not: identity[struct{}],
		Leaf: func(leaf *commonpb.QueryFilter) struct{} {
			visit(leaf)

			return struct{}{}
		},
	})
}

func anyOf(vs []bool) bool {
	for _, v := range vs {
		if v {
			return true
		}
	}

	return false
}

func allOf(vs []bool) bool {
	for _, v := range vs {
		if !v {
			return false
		}
	}

	return true
}

func negate(v bool) bool { return !v }

func identity[T any](v T) T { return v }

// Kleene is a three-valued verdict: known=false means the answer hinges on a
// server stamp the model has not learned yet. Booleans propagate unknowns
// Kleene-style — a decided AND/OR short-circuits, an undecided one stays
// unknown.
type Kleene struct {
	Match, Known bool
}

func KleeneAnd(children []Kleene) Kleene {
	known := true
	for _, c := range children {
		if c.Known && !c.Match {
			return Kleene{Match: false, Known: true}
		}

		known = known && c.Known
	}

	return Kleene{Match: true, Known: known}
}

func KleeneOr(children []Kleene) Kleene {
	known := true
	for _, c := range children {
		if c.Known && c.Match {
			return Kleene{Match: true, Known: true}
		}

		known = known && c.Known
	}

	return Kleene{Match: false, Known: known}
}

func KleeneNot(c Kleene) Kleene { return Kleene{Match: !c.Match, Known: c.Known} }

// WithinBounds reports whether v lies within the bounds an Int/UintCondition
// carries — mirroring resolveUintBounds: each side honors its exclusive flag
// and an absent bound is open on that side.
func WithinBounds[T cmp.Ordered](v T, lo *T, loExclusive bool, hi *T, hiExclusive bool) bool {
	if lo != nil && (v < *lo || (loExclusive && v == *lo)) {
		return false
	}

	if hi != nil && (v > *hi || (hiExclusive && v == *hi)) {
		return false
	}

	return true
}

// MatchUintBounds reports whether v lies within cond. An absent condition
// bounds nothing, as the compiler reads it.
func MatchUintBounds(cond *commonpb.UintCondition, v uint64) bool {
	if cond == nil {
		return true
	}

	return WithinBounds(v, cond.Min, cond.GetMinExclusive(), cond.Max, cond.GetMaxExclusive())
}

func MatchIntBounds(cond *commonpb.IntCondition, v int64) bool {
	return WithinBounds(v, cond.Min, cond.GetMinExclusive(), cond.Max, cond.GetMaxExclusive())
}

// RenderBound renders one side of a bound for filter descriptions: "_" when
// open, the value otherwise, an exclusive bound parenthesized on its outer
// side — "(3" for a lower bound, "7)" for an upper one.
func RenderBound[T cmp.Ordered](v *T, exclusive, upper bool) string {
	if v == nil {
		return "_"
	}

	s := fmt.Sprint(*v)

	switch {
	case exclusive && upper:
		return s + ")"
	case exclusive:
		return "(" + s
	default:
		return s
	}
}
