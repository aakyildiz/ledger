package match

import (
	"math"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// PreparedParams is the parameter map an ExecutePreparedQuery call carries.
type PreparedParams map[string]*commonpb.ParameterValue

// ParamKind names the value shape a parameter reference expects. Address is
// split out from the generic string kind only so generated values land in the
// account-address space, where they match something.
type ParamKind int

const (
	ParamKindAddress ParamKind = iota
	ParamKindString
	ParamKindUint
	ParamKindInt
	ParamKindBool
)

// ParamNeed is one parameter reference a stored filter carries.
type ParamNeed struct {
	Name string
	Kind ParamKind
}

// SubstituteParams returns a copy of f with every parameter reference replaced
// by its bound value, and reports whether every reference resolved. It mirrors
// the compiler's resolution rules: a reference to a parameter that was not
// supplied, or one supplied with the wrong value type, is a compilation error
// (extractString / extractUint64 / extractInt64 in internal/query/compile.go) —
// here that surfaces as ok=false, so a base whose stored filter needs a missing
// parameter predicts a rejection rather than a window.
func SubstituteParams(f *commonpb.QueryFilter, params PreparedParams) (*commonpb.QueryFilter, bool) {
	if f == nil {
		return nil, true
	}

	out := f.CloneVT()
	ok := substituteInto(out, params)

	return out, ok
}

func substituteInto(f *commonpb.QueryFilter, params PreparedParams) bool {
	switch c := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		return substituteChildren(c.And.GetFilters(), params)
	case *commonpb.QueryFilter_Or:
		return substituteChildren(c.Or.GetFilters(), params)
	case *commonpb.QueryFilter_Not:
		return substituteInto(c.Not.GetFilter(), params)
	case *commonpb.QueryFilter_Address:
		return substituteAddress(c.Address, params)
	case *commonpb.QueryFilter_Reference:
		return substituteString(c.Reference.GetCond(), params)
	case *commonpb.QueryFilter_Ledger:
		return substituteString(c.Ledger.GetCond(), params)
	case *commonpb.QueryFilter_LogId:
		return substituteUint(c.LogId.GetCond(), params)
	case *commonpb.QueryFilter_BuiltinUint:
		return substituteUint(c.BuiltinUint.GetCond(), params)
	case *commonpb.QueryFilter_LogBuiltinUint:
		return substituteUint(c.LogBuiltinUint.GetCond(), params)
	case *commonpb.QueryFilter_Field:
		return substituteField(c.Field, params)
	default:
		return true
	}
}

// substituteChildren resolves every child rather than stopping at the first
// failure: the compiler walks the whole tree, and an unresolved reference
// anywhere rejects the query regardless of where it sits.
func substituteChildren(children []*commonpb.QueryFilter, params PreparedParams) bool {
	ok := true
	for _, child := range children {
		if !substituteInto(child, params) {
			ok = false
		}
	}

	return ok
}

func substituteAddress(am *commonpb.AddressMatch, params PreparedParams) bool {
	switch m := am.GetMatch().(type) {
	case *commonpb.AddressMatch_ParamPrefix:
		v, ok := lookupString(params, m.ParamPrefix)
		if !ok {
			return false
		}

		am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: v}
	case *commonpb.AddressMatch_ParamExact:
		v, ok := lookupString(params, m.ParamExact)
		if !ok {
			return false
		}

		am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: v}
	}

	return true
}

func substituteString(sc *commonpb.StringCondition, params PreparedParams) bool {
	m, isParam := sc.GetValue().(*commonpb.StringCondition_Param)
	if !isParam {
		return true
	}

	v, ok := lookupString(params, m.Param)
	if !ok {
		return false
	}

	sc.Value = &commonpb.StringCondition_Hardcoded{Hardcoded: v}

	return true
}

func substituteBool(bc *commonpb.BoolCondition, params PreparedParams) bool {
	m, isParam := bc.GetValue().(*commonpb.BoolCondition_Param)
	if !isParam {
		return true
	}

	pv, present := params[m.Param]
	if !present {
		return false
	}

	v, isBool := pv.GetValue().(*commonpb.ParameterValue_BoolValue)
	if !isBool {
		return false
	}

	bc.Value = &commonpb.BoolCondition_Hardcoded{Hardcoded: v.BoolValue}

	return true
}

func substituteUint(uc *commonpb.UintCondition, params PreparedParams) bool {
	if uc == nil {
		return true
	}

	ok := true

	if uc.GetParamMin() != "" {
		if v, found := lookupUint(params, uc.GetParamMin()); found {
			uc.Min = &v
			uc.ParamMin = ""
		} else {
			ok = false
		}
	}

	if uc.GetParamMax() != "" {
		if v, found := lookupUint(params, uc.GetParamMax()); found {
			uc.Max = &v
			uc.ParamMax = ""
		} else {
			ok = false
		}
	}

	return ok
}

func substituteInt(ic *commonpb.IntCondition, params PreparedParams) bool {
	if ic == nil {
		return true
	}

	ok := true

	if ic.GetParamMin() != "" {
		if v, found := lookupInt(params, ic.GetParamMin()); found {
			ic.Min = &v
			ic.ParamMin = ""
		} else {
			ok = false
		}
	}

	if ic.GetParamMax() != "" {
		if v, found := lookupInt(params, ic.GetParamMax()); found {
			ic.Max = &v
			ic.ParamMax = ""
		} else {
			ok = false
		}
	}

	return ok
}

func substituteField(fc *commonpb.FieldCondition, params PreparedParams) bool {
	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		return substituteString(c.StringCond, params)
	case *commonpb.FieldCondition_IntCond:
		return substituteInt(c.IntCond, params)
	case *commonpb.FieldCondition_UintCond:
		return substituteUint(c.UintCond, params)
	case *commonpb.FieldCondition_BoolCond:
		return substituteBool(c.BoolCond, params)
	default:
		return true
	}
}

func lookupString(params PreparedParams, name string) (string, bool) {
	pv, present := params[name]
	if !present {
		return "", false
	}

	v, isString := pv.GetValue().(*commonpb.ParameterValue_StringValue)
	if !isString {
		return "", false
	}

	return v.StringValue, true
}

func lookupUint(params PreparedParams, name string) (uint64, bool) {
	pv, present := params[name]
	if !present {
		return 0, false
	}

	switch v := pv.GetValue().(type) {
	case *commonpb.ParameterValue_Uint64Value:
		return v.Uint64Value, true
	case *commonpb.ParameterValue_Int64Value:
		if v.Int64Value < 0 {
			return 0, false
		}

		return uint64(v.Int64Value), true
	case *commonpb.ParameterValue_StringValue:
		n, err := strconv.ParseUint(v.StringValue, 10, 64)

		return n, err == nil
	default:
		return 0, false
	}
}

func lookupInt(params PreparedParams, name string) (int64, bool) {
	pv, present := params[name]
	if !present {
		return 0, false
	}

	switch v := pv.GetValue().(type) {
	case *commonpb.ParameterValue_Int64Value:
		return v.Int64Value, true
	case *commonpb.ParameterValue_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return 0, false
		}

		return int64(v.Uint64Value), true
	case *commonpb.ParameterValue_StringValue:
		n, err := strconv.ParseInt(v.StringValue, 10, 64)

		return n, err == nil
	default:
		return 0, false
	}
}

// CollectParams walks a stored filter and returns every parameter reference it
// carries, in tree order. The execution path generates a value per need; the
// stored filter is the only source of truth for what a query expects, since the
// driver keeps no copy of what it created.
func CollectParams(f *commonpb.QueryFilter) []ParamNeed {
	var out []ParamNeed
	collectParamsInto(f, &out)

	return out
}

func collectParamsInto(f *commonpb.QueryFilter, out *[]ParamNeed) {
	switch c := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range c.And.GetFilters() {
			collectParamsInto(child, out)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range c.Or.GetFilters() {
			collectParamsInto(child, out)
		}
	case *commonpb.QueryFilter_Not:
		collectParamsInto(c.Not.GetFilter(), out)
	case *commonpb.QueryFilter_Address:
		switch m := c.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_ParamPrefix:
			*out = append(*out, ParamNeed{m.ParamPrefix, ParamKindAddress})
		case *commonpb.AddressMatch_ParamExact:
			*out = append(*out, ParamNeed{m.ParamExact, ParamKindAddress})
		}
	case *commonpb.QueryFilter_Reference:
		collectStringParam(c.Reference.GetCond(), out)
	case *commonpb.QueryFilter_Ledger:
		collectStringParam(c.Ledger.GetCond(), out)
	case *commonpb.QueryFilter_LogId:
		collectUintParams(c.LogId.GetCond(), out)
	case *commonpb.QueryFilter_BuiltinUint:
		collectUintParams(c.BuiltinUint.GetCond(), out)
	case *commonpb.QueryFilter_LogBuiltinUint:
		collectUintParams(c.LogBuiltinUint.GetCond(), out)
	case *commonpb.QueryFilter_Field:
		collectFieldParams(c.Field, out)
	}
}

func collectStringParam(sc *commonpb.StringCondition, out *[]ParamNeed) {
	if m, ok := sc.GetValue().(*commonpb.StringCondition_Param); ok {
		*out = append(*out, ParamNeed{m.Param, ParamKindString})
	}
}

func collectUintParams(uc *commonpb.UintCondition, out *[]ParamNeed) {
	if uc.GetParamMin() != "" {
		*out = append(*out, ParamNeed{uc.GetParamMin(), ParamKindUint})
	}

	if uc.GetParamMax() != "" {
		*out = append(*out, ParamNeed{uc.GetParamMax(), ParamKindUint})
	}
}

func collectIntParams(ic *commonpb.IntCondition, out *[]ParamNeed) {
	if ic.GetParamMin() != "" {
		*out = append(*out, ParamNeed{ic.GetParamMin(), ParamKindInt})
	}

	if ic.GetParamMax() != "" {
		*out = append(*out, ParamNeed{ic.GetParamMax(), ParamKindInt})
	}
}

func collectFieldParams(fc *commonpb.FieldCondition, out *[]ParamNeed) {
	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_StringCond:
		collectStringParam(c.StringCond, out)
	case *commonpb.FieldCondition_IntCond:
		collectIntParams(c.IntCond, out)
	case *commonpb.FieldCondition_UintCond:
		collectUintParams(c.UintCond, out)
	case *commonpb.FieldCondition_BoolCond:
		if m, ok := c.BoolCond.GetValue().(*commonpb.BoolCondition_Param); ok {
			*out = append(*out, ParamNeed{m.Param, ParamKindBool})
		}
	}
}
