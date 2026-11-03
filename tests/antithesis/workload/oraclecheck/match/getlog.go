package match

import "github.com/formancehq/ledger/v3/tests/oracle"

// CommittedLogTarget names a committed log by ledger and id together with the
// global sequence the model learned for it at commit.
type CommittedLogTarget struct {
	Ledger   string
	ID       uint64
	Sequence uint64
}

// CommittedLogMatches reports whether got is the log the base's global stream
// holds at target's sequence: the same owner, the same id, and the same row.
// The stream outlives its ledgers, so a deleted ledger's log still resolves.
func CommittedLogMatches(base oracle.GlobalState, target CommittedLogTarget, got ServerLogRow) bool {
	entry, ok := base.Log(target.Sequence)
	if !ok || entry.Ledger != target.Ledger || entry.ID != target.ID {
		return false
	}

	if got.Sequence != target.Sequence || got.ID != target.ID {
		return false
	}

	return LogRowMatches(target.Ledger, GlobalLogWindowRow(entry), got)
}

// GlobalLogWindowRow is one entry of the global log stream as a comparable row.
func GlobalLogWindowRow(entry oracle.GlobalLogRow) LogWindowRow {
	row := LogWindowRow{
		ID: entry.Row.ID, Kind: entry.Row.Kind, Payload: entry.Row.Payload,
		Date: entry.Row.Date, Sequence: entry.Row.Sequence,
		Purged: entry.Row.PurgedVolumes, NewKept: entry.Row.NewKeptVolumes, Ephemeral: entry.Row.EphemeralVolumes,
		Required: true,
	}

	if entry.Tx != nil {
		row.Tx = entry.Tx
		row.RevertsID = entry.Tx.RevertsTransaction()
	}

	return row
}
