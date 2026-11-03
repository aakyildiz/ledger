package oraclecheck

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Core views of DESIGN §4.7. Views are for aiming only; they never validate.
// Every list is capped (ViewCap) and deterministic. Reserved entities (§3.1)
// are hidden unless the request carries "owned": true.

func init() {
	RegisterView("ledgers", viewLedgers)
	RegisterView("ledger", viewLedger)
	RegisterView("seek", viewSeek)
	RegisterView("replayable", viewReplayable)
	RegisterView("stamps", viewStamps)
}

// ViewParams are the fields the core reads from every view request.
type ViewParams struct {
	Name           string  `json:"name"`
	Ledger         *string `json:"ledger,omitempty"`
	LedgerPick     *uint64 `json:"ledgerPick,omitempty"`
	IncludeDeleted bool    `json:"includeDeleted,omitempty"`
	Owned          bool    `json:"owned,omitempty"`
}

// ErrViewRequest marks a malformed view request (answered `400 protocol`).
var ErrViewRequest = errors.New("bad view request")

// ComputeViews answers a list of view requests on one snapshot. ledgerPick and
// owned are resolved here, before any view function runs; all views of one
// call that use the same ledgerPick resolve to the same ledger.
func (t *Tracker) ComputeViews(snap oracle.GlobalState, reqs []json.RawMessage) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0, len(reqs))
	for i, raw := range reqs {
		v, err := t.computeView(snap, raw)
		if err != nil {
			return nil, fmt.Errorf("view %d: %w", i, err)
		}
		out = append(out, v)
	}

	return out, nil
}

func (t *Tracker) computeView(snap oracle.GlobalState, raw json.RawMessage) (json.RawMessage, error) {
	var p ViewParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrViewRequest, err)
	}
	fn, ok := lookupView(p.Name)
	if !ok {
		return nil, fmt.Errorf("%w: unknown view %q", ErrViewRequest, p.Name)
	}

	var params map[string]json.RawMessage
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrViewRequest, err)
	}

	resolved := ""
	echo := false
	switch {
	case p.LedgerPick != nil:
		live, deleted, _ := t.ledgerLists(snap, p.Owned)
		list := live
		if p.IncludeDeleted {
			list = append(slices.Clone(live), deleted...)
		}
		if len(list) == 0 {
			return json.RawMessage(`{"ledger":null}`), nil
		}
		resolved = list[*p.LedgerPick%uint64(len(list))]
		echo = true
	case p.Ledger != nil:
		resolved = *p.Ledger
		echo = true
	}
	if echo {
		b, _ := json.Marshal(resolved)
		params["ledger"] = b
	}
	merged, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	res, err := fn(snap, t, merged)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrViewRequest, err)
	}
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	if !echo {
		return b, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil || obj == nil {
		return nil, fmt.Errorf("view %q returned a non-object", p.Name)
	}
	lb, _ := json.Marshal(resolved)
	obj["ledger"] = lb

	return json.Marshal(obj)
}

// ledgerLists partitions the tracked ledgers of snap: live normal-mode,
// deleted, live mirror-mode. Reserved ledgers only with owned.
func (t *Tracker) ledgerLists(snap oracle.GlobalState, owned bool) (live, deleted, mirror []string) {
	live, deleted, mirror = []string{}, []string{}, []string{}
	for _, name := range t.LedgerNames() {
		if !owned && IsReservedLedger(t.cfg.TrackedPrefix, name) {
			continue
		}
		lc, ok := snap.Lifecycle(name)
		if !ok {
			continue
		}
		switch {
		case lc.Deleted:
			deleted = append(deleted, name)
		case lc.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR:
			mirror = append(mirror, name)
		default:
			live = append(live, name)
		}
	}

	return live, deleted, mirror
}

// LiveLedgers returns the live tracked normal-mode ledgers of snap.
func (t *Tracker) LiveLedgers(snap oracle.GlobalState, owned bool) []string {
	live, _, _ := t.ledgerLists(snap, owned)

	return live
}

func capList[T any](l []T) []T {
	if len(l) > ViewCap {
		return l[:ViewCap]
	}

	return l
}

func viewLedgers(snap oracle.GlobalState, t *Tracker, params json.RawMessage) (any, error) {
	var p ViewParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	live, deleted, mirror := t.ledgerLists(snap, p.Owned)

	next := uint64(1)
	for _, name := range t.LedgerNames() {
		rest, ok := strings.CutPrefix(name, t.cfg.TrackedPrefix)
		if !ok || len(rest) < 2 || (rest[0] != 'l' && rest[0] != 'm') {
			continue
		}
		if n, err := strconv.ParseUint(rest[1:], 10, 64); err == nil && n+1 > next {
			next = n + 1
		}
	}

	return map[string]any{
		"trackedPrefix": t.cfg.TrackedPrefix,
		"live":          capList(live),
		"deleted":       capList(deleted),
		"mirror":        capList(mirror),
		"maintenance":   snap.MaintenanceMode(),
		"nextLedger":    next,
	}, nil
}

type ledgerParams struct {
	Ledger string `json:"ledger"`
	Owned  bool   `json:"owned"`
	Table  string `json:"table"`
	Probe  string `json:"probe"`
	Filter string `json:"filter"`
}

func fieldTypeMap(m oracle.Map[string, commonpb.MetadataType], owned bool) map[string]string {
	out := map[string]string{}
	for k, ty := range m.All() {
		if !owned && IsReservedKey(k) {
			continue
		}
		if len(out) >= ViewCap {
			break
		}
		out[k] = ty.String()
	}

	return out
}

func viewLedger(snap oracle.GlobalState, t *Tracker, params json.RawMessage) (any, error) {
	var p ledgerParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	lc, exists := snap.Lifecycle(p.Ledger)
	ls := snap.Ledger(p.Ledger)

	types := []map[string]any{}
	for name, ty := range ls.Types().All() {
		if len(types) >= ViewCap {
			break
		}
		types = append(types, map[string]any{"name": name, "pattern": ty.Pattern, "persistence": ty.Persistence.String()})
	}
	metaKeys := []string{}
	for k := range ls.LedgerMeta().All() {
		if !p.Owned && IsReservedKey(k) {
			continue
		}
		metaKeys = append(metaKeys, k)
	}
	idx := []map[string]any{}
	for canonical, active := range ls.Indexes().All() {
		id, err := indexes.ParseCanonical(canonical)
		if err != nil {
			continue
		}
		if !p.Owned && IsReservedKey(id.GetMetadata().GetKey()) {
			continue
		}
		if len(idx) >= ViewCap {
			break
		}
		idJSON, err := protojson.Marshal(id)
		if err != nil {
			return nil, err
		}
		state := "ambiguous"
		if active {
			state = "active"
		}
		idx = append(idx, map[string]any{"canonical": canonical, "id": json.RawMessage(idJSON), "state": state})
	}
	pqs := []string{}
	for _, name := range ls.PreparedQueryNames() {
		if !p.Owned && IsReservedKey(name) {
			continue
		}
		pqs = append(pqs, name)
	}

	return map[string]any{
		"name":           p.Ledger,
		"live":           exists && !lc.Deleted,
		"mode":           lc.Mode.String(),
		"txCount":        ls.Txs().Len(),
		"logCount":       len(ls.LogRows()),
		"defaultMode":    ls.DefaultEnforcementMode().String(),
		"types":          types,
		"ledgerMetaKeys": capList(metaKeys),
		"fieldTypes": map[string]any{
			"account":     fieldTypeMap(ls.AccountFieldTypes(), p.Owned),
			"transaction": fieldTypeMap(ls.TransactionFieldTypes(), p.Owned),
			"ledger":      fieldTypeMap(ls.LedgerFieldTypes(), p.Owned),
		},
		"indexes":         idx,
		"preparedQueries": capList(pqs),
	}, nil
}

// seekScanCap bounds how many entries a filtered seek inspects.
const seekScanCap = 1 << 16

// pickAtOrAfter returns the first entry at or after probe that accept takes,
// wrapping to the smallest key (M/actions.go pickAtOrAfter, with a filter).
func pickAtOrAfter[K, V any](m oracle.Map[K, V], probe K, accept func(K, V) bool) (k K, v V, found, wrapped bool) {
	scanned := 0
	for kk, vv := range m.From(probe) {
		if scanned++; scanned > seekScanCap {
			return k, v, false, false
		}
		if accept(kk, vv) {
			return kk, vv, true, false
		}
	}
	for kk, vv := range m.All() {
		if scanned++; scanned > seekScanCap {
			break
		}
		if accept(kk, vv) {
			return kk, vv, true, true
		}
	}

	return k, v, false, false
}

func seekResult(found, wrapped bool, entry any) map[string]any {
	if !found {
		entry = nil
		wrapped = false
	}

	return map[string]any{"found": found, "wrapped": wrapped, "entry": entry}
}

func parseProbeID(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}

	return n
}

func viewSeek(snap oracle.GlobalState, t *Tracker, params json.RawMessage) (any, error) {
	var p ledgerParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	switch p.Filter {
	case "", "ephemeral-positive", "transient", "has-metadata":
	default:
		return nil, fmt.Errorf("unknown seek filter %q", p.Filter)
	}
	prefix := t.cfg.TrackedPrefix
	ls := snap.Ledger(p.Ledger)
	persistence := func(addr string) commonpb.AccountTypePersistence {
		if ts := ls.MatchAddress(addr); ts != nil {
			return ts.Persistence
		}

		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL
	}

	switch p.Table {
	case "volumes":
		k, v, found, wrapped := pickAtOrAfter(ls.Volumes(), oracle.VolumeKey{Address: p.Probe}, func(k oracle.VolumeKey, v oracle.VolumePair) bool {
			if !p.Owned && IsReservedAddress(k.Address) {
				return false
			}
			switch p.Filter {
			case "ephemeral-positive":
				return persistence(k.Address) == commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL && v.Input.Gt(&v.Output)
			case "transient":
				return persistence(k.Address) == commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT
			case "has-metadata":
				return len(ls.AccountMetadata(k.Address)) > 0
			}

			return true
		})
		var entry any
		if found {
			entry = map[string]any{"address": k.Address, "asset": k.Asset, "color": k.Color,
				"input": v.Input.Dec(), "output": v.Output.Dec(), "persistence": persistence(k.Address).String()}
		}

		return seekResult(found, wrapped, entry), nil

	case "accountMeta":
		k, _, found, wrapped := pickAtOrAfter(ls.Metadata(), oracle.MetaKey{Address: p.Probe}, func(k oracle.MetaKey, _ *commonpb.MetadataValue) bool {
			return p.Owned || (!IsReservedAddress(k.Address) && !IsReservedKey(k.Key))
		})
		var entry any
		if found {
			entry = map[string]any{"address": k.Address, "key": k.Key}
		}

		return seekResult(found, wrapped, entry), nil

	case "txByRef":
		txs := ls.Txs()
		ref, id, found, wrapped := pickAtOrAfter(ls.TxByRef(), p.Probe, func(ref string, id int) bool {
			if p.Owned {
				return true
			}
			if IsReservedReference(prefix, ref) {
				return false
			}
			if id >= 1 && id <= txs.Len() {
				return !IsReservedTx(prefix, ref, txs.Get(id-1).Postings())
			}

			return true
		})
		var entry any
		if found {
			entry = map[string]any{"reference": ref, "id": strconv.Itoa(id)}
		}

		return seekResult(found, wrapped, entry), nil

	case "txs", "txMeta":
		txs := ls.Txs()
		n := txs.Len()
		if n == 0 {
			return seekResult(false, false, nil), nil
		}
		start := int(min(parseProbeID(p.Probe), uint64(n+1)))
		if start < 1 {
			start = 1
		}
		for step := range n {
			id := start + step
			wrapped := false
			if id > n {
				id -= n
				wrapped = true
			}
			if step >= seekScanCap {
				break
			}
			tx := txs.Get(id - 1)
			if !p.Owned && IsReservedTx(prefix, tx.Reference(), tx.Postings()) {
				continue
			}
			if p.Filter == "has-metadata" && len(tx.Metadata()) == 0 {
				continue
			}
			if p.Table == "txs" {
				return seekResult(true, wrapped, map[string]any{
					"id": strconv.Itoa(id), "reference": tx.Reference(), "reverted": tx.Reverted(),
					"postingCount": len(tx.Postings()), "force": t.TxForced(p.Ledger, uint64(id)),
				}), nil
			}
			keys := make([]string, 0, len(tx.Metadata()))
			for k := range tx.Metadata() {
				if p.Owned || !IsReservedKey(k) {
					keys = append(keys, k)
				}
			}
			if len(keys) == 0 {
				continue
			}
			slices.Sort(keys)

			return seekResult(true, wrapped, map[string]any{"id": strconv.Itoa(id), "key": keys[0]}), nil
		}

		return seekResult(false, false, nil), nil
	default:
		return nil, fmt.Errorf("unknown seek table %q", p.Table)
	}
}

// ReplayableView samples the registry: the newest 32 plus every ⌈n/32⌉-th
// older entry, oldest first (at most 64).
func ReplayableView(all []ReplayEntry) []ReplayEntry {
	if len(all) <= replayableViewNewest {
		return slices.Clone(all)
	}
	older := all[:len(all)-replayableViewNewest]
	step := (len(older) + replayableViewNewest - 1) / replayableViewNewest
	out := make([]ReplayEntry, 0, 2*replayableViewNewest)
	for i := 0; i < len(older); i += step {
		out = append(out, older[i])
	}

	return append(out, all[len(all)-replayableViewNewest:]...)
}

func viewReplayable(_ oracle.GlobalState, t *Tracker, _ json.RawMessage) (any, error) {
	entries := ReplayableView(t.Replayable())
	if entries == nil {
		entries = []ReplayEntry{}
	}

	return map[string]any{"entries": entries}, nil
}

// sampleSorted dedups, sorts ascending and keeps at most n values, evenly
// spaced, always including the newest.
func sampleSorted(vals []uint64, n int) []string {
	slices.Sort(vals)
	vals = slices.Compact(vals)
	if len(vals) > n {
		step := (len(vals) + n - 1) / n
		var keep []uint64
		for i := len(vals) - 1; i >= 0 && len(keep) < n; i -= step {
			keep = append(keep, vals[i])
		}
		slices.Sort(keep)
		vals = keep
	}
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		out = append(out, strconv.FormatUint(v, 10))
	}

	return out
}

func viewStamps(snap oracle.GlobalState, _ *Tracker, params json.RawMessage) (any, error) {
	var p ledgerParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	ls := snap.Ledger(p.Ledger)
	var txDates, logDates []uint64
	for _, tx := range ls.Txs().All() {
		if ts := tx.Timestamp(); ts != nil {
			txDates = append(txDates, ts.GetData())
		}
	}
	var maxSeq uint64
	for _, row := range ls.LogRows() {
		if row.Date != nil {
			logDates = append(logDates, row.Date.GetData())
		}
		maxSeq = max(maxSeq, row.Sequence)
	}

	return map[string]any{
		"txDates":   sampleSorted(txDates, StampsCap),
		"logDates":  sampleSorted(logDates, StampsCap),
		"maxLogSeq": strconv.FormatUint(maxSeq, 10),
	}, nil
}
