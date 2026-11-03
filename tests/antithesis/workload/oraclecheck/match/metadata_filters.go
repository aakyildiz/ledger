package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func FieldRef(key string) *commonpb.FieldRef {
	return &commonpb.FieldRef{Metadata: key}
}

func FilterFieldString(key, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field: FieldRef(key),
		Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
		}},
	}}}
}

// FilterFieldInt matches key values in [lo, hi], both bounds inclusive; nil
// leaves a side open.
func FilterFieldInt(key string, lo, hi *int64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     FieldRef(key),
		Condition: &commonpb.FieldCondition_IntCond{IntCond: &commonpb.IntCondition{Min: lo, Max: hi}},
	}}}
}

// FilterFieldUint matches key values in [lo, hi], both bounds inclusive; nil
// leaves a side open.
func FilterFieldUint(key string, lo, hi *uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     FieldRef(key),
		Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{Min: lo, Max: hi}},
	}}}
}

func FilterFieldBool(key string, value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field: FieldRef(key),
		Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{
			Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: value},
		}},
	}}}
}

func FilterFieldExists(key string, includeNull bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     FieldRef(key),
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{IncludeNull: includeNull}},
	}}}
}

// MatchFieldCondition evaluates a Field leaf against an entity's stored
// metadata under the declared types. The index stores the value coerced to the
// declared type, so the model coerces identically (the server's own
// ConvertMetadataValue) before comparing. An undeclared key or a missing value
// has no index row and never matches.
func MatchFieldCondition(declared oracle.Map[string, commonpb.MetadataType], lookup func(string) (*commonpb.MetadataValue, bool), fc *commonpb.FieldCondition) bool {
	key := fc.GetField().GetMetadata()

	declaredType, ok := declared.Get(key)
	if !ok {
		return false
	}

	stored, present := lookup(key)
	if !present {
		return false
	}

	coerced := stored
	if !commonpb.TypeMatches(stored, declaredType) {
		coerced = commonpb.ConvertMetadataValue(stored, declaredType)
	}

	switch c := fc.GetCondition().(type) {
	case *commonpb.FieldCondition_ExistsCond:
		if c.ExistsCond.GetIncludeNull() {
			return true
		}
		_, isNull := coerced.GetType().(*commonpb.MetadataValue_NullValue)

		return !isNull
	case *commonpb.FieldCondition_StringCond:
		sv, isStr := coerced.GetType().(*commonpb.MetadataValue_StringValue)

		return isStr && sv.StringValue == c.StringCond.GetHardcoded()
	case *commonpb.FieldCondition_BoolCond:
		bv, isBool := coerced.GetType().(*commonpb.MetadataValue_BoolValue)

		return isBool && bv.BoolValue == c.BoolCond.GetHardcoded()
	case *commonpb.FieldCondition_IntCond:
		// Datetime shares the order-preserving int64 index encoding, so an int
		// range scans datetime rows too (validateAndCoerceCondition admits it).
		// On an unsigned-declared field the compiler coerces the int bounds
		// into a uint condition (coerceIntToUint), so a UintValue matches by
		// the same numeric bounds; negative bounds never reach here — they are
		// a compilation rejection (FieldKindMismatch).
		switch t := coerced.GetType().(type) {
		case *commonpb.MetadataValue_IntValue:
			return MatchIntBounds(c.IntCond, t.IntValue)
		case *commonpb.MetadataValue_DatetimeValue:
			return MatchIntBounds(c.IntCond, t.DatetimeValue)
		case *commonpb.MetadataValue_UintValue:
			uc, ok := IntCondAsUint(c.IntCond)

			return ok && MatchUintBounds(uc, t.UintValue)
		default:
			return false
		}
	case *commonpb.FieldCondition_UintCond:
		uv, isUint := coerced.GetType().(*commonpb.MetadataValue_UintValue)

		return isUint && MatchUintBounds(c.UintCond, uv.UintValue)
	default:
		return false
	}
}

// IntCondAsUint mirrors the compiler's coerceIntToUint: the int bounds carried
// into the unsigned domain, ok=false on a negative bound (the compiler rejects
// those, so a match verdict is never reached).
func IntCondAsUint(cond *commonpb.IntCondition) (*commonpb.UintCondition, bool) {
	uc := &commonpb.UintCondition{
		MinExclusive: cond.GetMinExclusive(),
		MaxExclusive: cond.GetMaxExclusive(),
	}

	if cond.Min != nil {
		if cond.GetMin() < 0 {
			return nil, false
		}

		v := uint64(cond.GetMin())
		uc.Min = &v
	}

	if cond.Max != nil {
		if cond.GetMax() < 0 {
			return nil, false
		}

		v := uint64(cond.GetMax())
		uc.Max = &v
	}

	return uc, true
}

// FieldKindMismatch reports whether f contains a Field leaf whose condition
// kind is incompatible with the key's declared type — the shapes
// validateAndCoerceCondition rejects with FILTER_COMPILATION_ERROR
// (InvalidArgument). Undeclared keys are NOT mismatches: the compiler rejects
// them earlier as index-not-found. The compiler only reaches the coercion
// check after requireIndexReady, so a mismatch verdict competes with the
// not-ready rejection — validateFieldMismatchProbe accepts either.
func FieldKindMismatch(ls oracle.LedgerState, f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	return AnyLeaf(f, func(leaf *commonpb.QueryFilter) bool {
		x, ok := leaf.GetFilter().(*commonpb.QueryFilter_Field)
		if !ok {
			return false
		}

		declaredType, ok := DeclaredFieldTypes(ls, target).Get(x.Field.GetField().GetMetadata())
		if !ok {
			return false
		}

		switch cond := x.Field.GetCondition().(type) {
		case *commonpb.FieldCondition_ExistsCond:
			return false
		case *commonpb.FieldCondition_IntCond:
			// Signed and datetime verbatim; unsigned via coerceIntToUint,
			// which rejects negative bounds.
			if commonpb.IsSignedType(declaredType) || commonpb.IsDatetimeType(declaredType) {
				return false
			}

			if commonpb.IsUnsignedType(declaredType) {
				return (cond.IntCond.Min != nil && cond.IntCond.GetMin() < 0) ||
					(cond.IntCond.Max != nil && cond.IntCond.GetMax() < 0)
			}

			return true
		case *commonpb.FieldCondition_UintCond:
			return !commonpb.IsUnsignedType(declaredType)
		case *commonpb.FieldCondition_StringCond:
			return declaredType != commonpb.MetadataType_METADATA_TYPE_STRING
		case *commonpb.FieldCondition_BoolCond:
			return declaredType != commonpb.MetadataType_METADATA_TYPE_BOOL
		default:
			return false
		}
	})
}

// DeclaredFieldTypes returns the declared-type map matching a query target.
func DeclaredFieldTypes(ls oracle.LedgerState, target commonpb.QueryTarget) oracle.Map[string, commonpb.MetadataType] {
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return ls.TransactionFieldTypes()
	}

	return ls.AccountFieldTypes()
}

// FieldSeed is one declared (key, type) with an optional committed value the
// generator can aim a hit at.
type FieldSeed struct {
	Key          string
	DeclaredType commonpb.MetadataType
	Sample       *commonpb.MetadataValue
}

// SampleFieldSeeds snapshots the declared fields of the target plus one stored
// value per key (coerced-comparable as-is; the generator coerces itself).
// A view helper: families compute seeds on a view snapshot and hand them to
// the driver for aiming.
func SampleFieldSeeds(ls oracle.LedgerState, target commonpb.QueryTarget) []FieldSeed {
	var seeds []FieldSeed
	for key, dt := range DeclaredFieldTypes(ls, target).All() {
		seed := FieldSeed{Key: key, DeclaredType: dt}

		if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
			for _, rec := range ls.Txs().All() {
				if v, ok := rec.Metadata()[key]; ok {
					seed.Sample = v

					break
				}
			}
		} else {
			for mk, v := range ls.Metadata().All() {
				if mk.Key == key {
					seed.Sample = v

					break
				}
			}
		}

		seeds = append(seeds, seed)
	}

	return seeds
}
