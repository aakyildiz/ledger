package oracle

// Business fingerprint: the identity of the state the server's audit trail
// determines. Fingerprint (model.go) also covers observation state — facts the
// checker learns from responses and read probes, which an audit replay cannot
// reproduce and which tracker bookkeeping (MutateModel) may change without any
// write committing:
//
//   - index readiness (ambiguous vs active): the index set itself stays;
//   - retype windows;
//   - learned transaction stamps (LearnTxStamps: a server-dated timestamp, the
//     always-server-stamped insertedAt, a server-dated revertedAt, and values
//     an at-effective-date revert inherited from a learned timestamp);
//   - learned log dates and log sequences (LearnLogDate, LearnLogSequence),
//     including the global log stream those sequences fill.
//
// Client-supplied stamps stay in: they are part of the audited orders.
//
// The per-entry terms reuse Fingerprint's term functions on masked copies, so
// every business field is hashed exactly as Fingerprint hashes it. Collections
// whose entries carry no observation state contribute their maintained
// fingerprint unchanged; the others are re-summed, which is O(entries).

// BusinessFingerprint hashes the state the server's audit trail determines: everything
// Fingerprint covers except observation state — index readiness (the index set stays),
// retype windows, learned transaction stamps, learned log dates and log sequences.
func (g GlobalState) BusinessFingerprint() Digest {
	var d Digest
	for name, ls := range g.ledgers {
		if ls.IsEmpty() {
			continue
		}

		t := newTerm("BL")
		t.str(name)
		t.digest(ls.businessFingerprint())
		d = d.add(t.sum())
	}

	t := newTerm("query-checkpoint-state")
	t.digest(g.checkpoints.Fingerprint())
	t.u64(g.nextCheckpointID, g.checkpointLimit)
	t.str(g.checkpointSchedule)

	maintenance := newTerm("maintenance")
	maintenance.u64(0)
	if g.maintenance {
		maintenance.u64(1)
	}

	// g.logs (the global stream filled by LearnLogSequence) is learned, so it
	// is left out.
	return d.add(g.idempotency.Fingerprint()).add(g.lifecycle.Fingerprint()).add(t.sum()).add(maintenance.sum())
}

// businessFingerprint is LedgerState.Fingerprint without observation state:
// the same fields in the same order, retypeWindows dropped, and txs, indexes
// and logs re-summed over masked terms.
func (s LedgerState) businessFingerprint() Digest {
	t := newTerm("business-ledger-state")
	t.u64(uint64(s.defaultEnforcementMode))
	for _, d := range []Digest{
		s.types.Fingerprint(), s.volumes.Fingerprint(), s.metadata.Fingerprint(), s.ledgerMeta.Fingerprint(),
		s.accountFieldTypes.Fingerprint(), s.ledgerFieldTypes.Fingerprint(), s.transactionFieldTypes.Fingerprint(),
		businessTxsFingerprint(s.txs), businessIndexesFingerprint(s.indexes), s.preparedQueries.Fingerprint(),
		s.everAsset.Fingerprint(), businessLogsFingerprint(s.logs),
	} {
		t.digest(d)
	}

	return t.sum()
}

func businessTxsFingerprint(txs List[*txRecord]) Digest {
	var d Digest
	for i, tx := range txs.All() {
		rec := *tx
		if rec.timestampLearned {
			rec.timestamp = nil
		}
		if rec.revertedAtLearned {
			rec.revertedAt = nil
		}
		rec.insertedAt = nil
		d = d.add(txTerm(i, &rec))
	}

	return d
}

func businessIndexesFingerprint(ix Map[string, bool]) Digest {
	var d Digest
	for canonical := range ix.All() {
		d = d.add(indexTerm(canonical, false))
	}

	return d
}

func businessLogsFingerprint(logs List[*logRecord]) Digest {
	var d Digest
	for i, l := range logs.All() {
		rec := *l
		rec.date = nil
		rec.sequence = 0
		d = d.add(logTerm(i, &rec))
	}

	return d
}
