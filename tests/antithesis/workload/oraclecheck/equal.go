package oraclecheck

import (
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Pure comparison helpers lifted from M/validate.go. oraclecheck/match keeps its
// own copies; neither package imports the other.

// responseMetaEffect extracts the server's stored (as-written) values for a
// committed metadata write from its response log, dispatching on the request
// type. Returns nil for non-metadata requests and for deletes.
func responseMetaEffect(req *servicepb.Request, log *commonpb.Log) (saved map[string]*commonpb.MetadataValue) {
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		data := log.GetPayload().GetApply().GetLog().GetData()
		switch r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return data.GetCreatedTransaction().GetTransaction().GetMetadata()
		case *servicepb.LedgerAction_AddMetadata:
			return data.GetSavedMetadata().GetMetadata()
		}
	case *servicepb.Request_SaveLedgerMetadata:
		return log.GetPayload().GetSavedLedgerMetadata().GetMetadata()
	}

	return nil
}

// postingsEqual reports whether two posting lists match field-for-field
// (source, destination, asset, color, amount).
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

// metaMapEqual reports whether two metadata maps hold the same keys and the
// same type-tagged values.
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

// accountMetaMapEqual reports whether two account-metadata maps hold the same
// addresses and, per address, the same metadata.
func accountMetaMapEqual(a, b map[string]*commonpb.MetadataMap) bool {
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

// postCommitVolume extracts (input, output) for one cell from a server
// response. ok is false when the cell is absent or the values don't parse.
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
