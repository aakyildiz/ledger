// Package match holds the pure read-side matchers of the oracle: windows,
// cursors, filter folds, index legality and row comparisons lifted from the
// singleton model driver (W/bin/cmds/model/singleton_driver_model, "M/"),
// exported and assertion-free (DESIGN §5.1).
//
// Every function here is a function of oracle.GlobalState / oracle.LedgerState,
// protobufs and plain values. Nothing takes a Tracker, a generation source, a
// gRPC client or the Antithesis SDK; verdicts are booleans and "why" strings.
// gRPC errors are passed as RPCError (built from the client outcome with
// RPCErrorFrom); side tables the Tracker keeps (committed logs and bulks) are
// passed in as CommittedLog / CommittedBulk maps; candidate walks are passed in
// as CandidateMatcher or walk closures.
//
// Names follow the singleton model with an upper-case first letter
// (accountWindow → AccountWindow, pcvSnapshotMatches → PCVSnapshotMatches).
// The pieces that read Checker state were not lifted; each family task owns
// that tracker-bound part:
//
//	M/ validator (tracker-bound)                         lifted pure core                         family task
//	validateAccountRead, validateTransactionRead         AccountVolumesMatch, TxRecordMatches      05
//	validateLedgerStats                                  StatsMatches, StatsBoundsMatch            05
//	validateAggregate                                    AggregateResultMatches                    05
//	runGetLog / describeCommittedLog                     CommittedLogMatches                       05
//	runLedgersList, ledgerIdentityViolation              LedgerListingMatches, LedgerIdentity      04, 09
//	validateLedgerRead, validateSchemaRead               LedgerReadMatches, FieldTypesMatch        04
//	validateAccountQuery, validateTransactionQuery       AccountPageMatches, TxWindowMatches       06
//	validateLogQuery                                     LogOutcomeLegal, LogWindowMatches         06
//	handleRejectedFilterError                            ClassifyRejectedFilter, RefusedFilter…    06
//	validateIndexed*Query, validateAssetAccountQuery     IndexedQueryOutcomeLegal, AssetWindow     06, 07
//	validateIndexPresence, runIndexList*, classify…      IndexPresenceMatches, Index*Listing*      07
//	validateInspectSummary, resumeInspectIndex           ModelInspectCounts, InspectPageViolation  07
//	validateExecuteList / Aggregate, …ListPrepared…      PreparedListOutcomeLegal, RegistryMatches 08
//	checkpointReadOutcomeMatches (+Ledger…)              CheckpointReadOutcomeMatches (+Ledger…)   09
//	validateAuditPage, validateAuditEntry, …             AuditSuccessMismatch, AuditPageViolation  10
//
// The ListLedgers matcher is new: LedgerListingMatches implements DESIGN §3.2
// (foreign ledgers sort on both sides of the tracked range), replacing the
// singleton model's exclusive-fleet comparison.
package match
