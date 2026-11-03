package match

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/holiman/uint256"
)

func LiveLedgerState(base oracle.GlobalState, ledger string) (oracle.LedgerState, bool) {
	if !LedgerIsLive(base, ledger) {
		return oracle.LedgerState{}, false
	}

	return base.Ledger(ledger), true
}

// AssetColor identifies one (asset, color) bucket of an account's volume list.
// Color "" is the uncolored bucket, which segregates from every colored one.
type AssetColor struct{ Asset, Color string }

func (k AssetColor) String() string { return k.Asset + "|" + k.Color }

// AccountVolumesMatch reports whether ls holds exactly the returned volume set
// for addr — same buckets, same cumulative volumes. GetAccount returns the
// account's whole volume set in one linearizable snapshot, so a returned cell
// the base lacks (a ghost row under ANY (asset, color), e.g. a stranded
// zero-balance row the base's purge sweep removed) and a base cell the server
// omitted are both mismatches. Seeks to addr's key range — O(log n + cells of
// addr), not a table walk (this runs per candidate base).
func AccountVolumesMatch(ls oracle.LedgerState, addr string, got map[AssetColor]oracle.VolumePair, collapsed bool) bool {
	want := map[AssetColor]oracle.VolumePair{}

	for k, vp := range ls.Volumes().From(oracle.VolumeKey{Address: addr}) {
		if k.Address != addr {
			break
		}

		key := AssetColor{Asset: k.Asset, Color: k.Color}
		if collapsed {
			// Every colour of an asset is summed into the uncolored bucket.
			key = AssetColor{Asset: k.Asset}
		}

		acc := want[key]
		acc.Input.Add(&acc.Input, &vp.Input)
		acc.Output.Add(&acc.Output, &vp.Output)
		want[key] = acc
	}

	if len(want) != len(got) {
		return false
	}

	for key, vp := range want {
		g, ok := got[key]
		if !ok || g.Input.Cmp(&vp.Input) != 0 || g.Output.Cmp(&vp.Output) != 0 {
			return false
		}
	}

	return true
}

// MetadataMatches reports whether ls holds exactly serverMeta for addr — same
// keys, same verbatim values. Reads surface the stored value as-written; the
// declared type is an index hint, not applied on read.
func MetadataMatches(ls oracle.LedgerState, addr string, serverMeta map[string]*commonpb.MetadataValue) bool {
	modelMeta := ls.AccountMetadata(addr)
	if len(modelMeta) != len(serverMeta) {
		return false
	}

	for k, v := range modelMeta {
		sv, ok := serverMeta[k]
		if !ok || oracle.MetaValueString(sv) != oracle.MetaValueString(v) {
			return false
		}
	}

	return true
}

func LedgerReadMatches(ls oracle.LedgerState, types map[string]*commonpb.AccountType, meta map[string]*commonpb.MetadataValue, mode commonpb.ChartEnforcementMode) bool {
	return ChartMatches(ls, types) && LedgerMetaMatches(ls, meta) && ls.DefaultEnforcementMode() == mode
}

// ChartMatches reports whether ls's chart equals the server's account types
// exactly — same names, patterns, and persistence.
func ChartMatches(ls oracle.LedgerState, serverTypes map[string]*commonpb.AccountType) bool {
	if ls.Types().Len() != len(serverTypes) {
		return false
	}

	for name, t := range ls.Types().All() {
		st, ok := serverTypes[name]
		if !ok || st.GetPattern() != t.Pattern || st.GetPersistence() != t.Persistence {
			return false
		}
	}

	return true
}

// LedgerMetaMatches reports whether ls's ledger metadata equals serverMeta
// exactly — same keys, same verbatim values. Reads surface the stored value
// as-written; the declared type is an index hint, not applied on read.
func LedgerMetaMatches(ls oracle.LedgerState, serverMeta map[string]*commonpb.MetadataValue) bool {
	if ls.LedgerMeta().Len() != len(serverMeta) {
		return false
	}

	for k, v := range ls.LedgerMeta().All() {
		sv, ok := serverMeta[k]
		if !ok || oracle.MetaValueString(sv) != oracle.MetaValueString(v) {
			return false
		}
	}

	return true
}

// FieldTypesMatch reports whether the model's declared field types equal the
// server's for one target — same keys, same declared type.
func FieldTypesMatch(model oracle.Map[string, commonpb.MetadataType], server map[string]*servicepb.MetadataFieldStatus) bool {
	if model.Len() != len(server) {
		return false
	}

	for k, t := range model.All() {
		st, ok := server[k]
		if !ok || st.GetDeclaredType() != t {
			return false
		}
	}

	return true
}

// postingsEqual reports whether two posting lists match field-for-field
// (source, destination, asset, amount).
func postingsEqual(a, b []*commonpb.Posting) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		var x, y uint256.Int
		a[i].GetAmount().IntoUint256(&x)
		b[i].GetAmount().IntoUint256(&y)

		if a[i].GetSource() != b[i].GetSource() ||
			a[i].GetDestination() != b[i].GetDestination() ||
			a[i].GetAsset() != b[i].GetAsset() ||
			a[i].GetColor() != b[i].GetColor() ||
			!x.Eq(&y) {
			return false
		}
	}

	return true
}

// RenderPostings formats a posting list for assertion details.
func RenderPostings(ps []*commonpb.Posting) string {
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

// metaMapEqual reports whether two metadata maps hold the same keys and the same
// type-tagged values.
func metaMapEqual(a, b map[string]*commonpb.MetadataValue) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		bv, ok := b[k]
		if !ok || oracle.MetaValueString(v) != oracle.MetaValueString(bv) {
			return false
		}
	}

	return true
}

// AccountMetaMapEqual reports whether two account-metadata maps (address -> map)
// hold the same addresses and, per address, the same metadata.
func AccountMetaMapEqual(a, b map[string]*commonpb.MetadataMap) bool {
	if len(a) != len(b) {
		return false
	}

	for addr, am := range a {
		bm, ok := b[addr]
		if !ok || !metaMapEqual(am.GetValues(), bm.GetValues()) {
			return false
		}
	}

	return true
}

// postCommitVolume extracts (input, output) for one cell from a server response,
// parsing the decimal-string volumes into uint256 — the ledger's native volume
// type. The cell is addressed by (account, asset, color), so a bucket the key
// does not name is not a match. ok is false when the cell is absent or the
// values don't parse.
func postCommitVolume(pcv *commonpb.PostCommitVolumes, key oracle.VolumeKey) (in, out uint256.Int, ok bool) {
	byAsset, found := pcv.GetVolumesByAccount()[key.Address]
	if !found {
		return in, out, false
	}

	var vol *commonpb.Volumes
	for _, entry := range byAsset.GetVolumes() {
		if entry.GetAsset() == key.Asset && entry.GetColor() == key.Color {
			vol = entry.GetVolumes()

			break
		}
	}

	if vol == nil {
		return in, out, false
	}

	if vol.GetInput() == nil || vol.GetOutput() == nil {
		return in, out, false
	}
	if err := in.SetFromDecimal(vol.GetInput().DecimalString()); err != nil {
		return in, out, false
	}
	if err := out.SetFromDecimal(vol.GetOutput().DecimalString()); err != nil {
		return in, out, false
	}

	return in, out, true
}
