package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// ModelKnowsAccount reports whether ls holds any volume cell or metadata for
// addr — an O(log n) seek to the start of addr's key range in each table (an
// empty asset/key sorts before any real one, so the first entry at or after
// the probe shares addr's address iff the account has state).
func ModelKnowsAccount(ls oracle.LedgerState, addr string) bool {
	for k := range ls.Volumes().From(oracle.VolumeKey{Address: addr}) {
		if k.Address == addr {
			return true
		}

		break // A later volume address does not rule out metadata on addr.
	}
	for k := range ls.Metadata().From(oracle.MetaKey{Address: addr}) {
		return k.Address == addr
	}

	return false
}

// AccountVolumeSet extracts the account's full volume set as (asset, color) ->
// volumes, so validation covers every returned cell — a ghost row under any
// bucket is caught, not just the probed one. ok=false marks a response shape no
// model state explains — a repeated bucket, or an unparseable amount — and the
// caller's validation fails it outright rather than mistaking it for an empty
// reading.
func AccountVolumeSet(acct *commonpb.Account) (map[AssetColor]oracle.VolumePair, bool) {
	if acct == nil {
		return nil, true
	}

	out := make(map[AssetColor]oracle.VolumePair, len(acct.GetVolumes()))
	for _, entry := range acct.GetVolumes() {
		key := AssetColor{Asset: entry.GetAsset(), Color: entry.GetColor()}
		if _, dup := out[key]; dup {
			return nil, false
		}

		var vp oracle.VolumePair
		vols := entry.GetVolumes()
		if vols == nil || vols.GetInput() == nil || vols.GetOutput() == nil {
			return nil, false
		}
		if vp.Input.SetFromDecimal(vols.GetInput().DecimalString()) != nil ||
			vp.Output.SetFromDecimal(vols.GetOutput().DecimalString()) != nil {
			return nil, false
		}

		out[key] = vp
	}

	return out, true
}
