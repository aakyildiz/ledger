package match

import "github.com/formancehq/ledger/v3/internal/proto/commonpb"

// CollectNodes lists every node of f in tree order. Repeated children are
// never nil: a nil element marshals as an empty node, which genAnyFilter
// produces explicitly when it wants one.
func CollectNodes(f *commonpb.QueryFilter, out *[]*commonpb.QueryFilter) {
	*out = append(*out, f)

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			CollectNodes(child, out)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			CollectNodes(child, out)
		}
	case *commonpb.QueryFilter_Not:
		if child := x.Not.GetFilter(); child != nil {
			CollectNodes(child, out)
		}
	}
}
