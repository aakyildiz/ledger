# Ported tests

Every test function of the `M/` (singleton model driver) test files listed in
`tasks/00b-go-matchers.md`, with where it went. "lifted" tests live under the same
name in `W/oraclecheck/match/<same file>`; they were renamed mechanically (type-aware
rename of the lifted identifiers) and, where they passed a gRPC `error`, now pass
`RPCErrorOf(err)` / `NoError` (marked "lifted (RPCError)"). "deferred to task NN" tests need
Checker state, a generator or a gRPC stream, which the named family owns.

| Source | Test | Status |
|---|---|---|
| M/queries_test.go | TestFilterNeedsIndex | lifted |
| M/queries_test.go | TestFilterInvalidForTarget | lifted |
| M/queries_test.go | TestMatchAccountFilter | lifted |
| M/queries_test.go | TestMatchTxIDBounds | lifted |
| M/queries_test.go | TestMatchTxFilter | lifted |
| M/queries_test.go | TestAccountUniverse_OrderIsByteAscending | lifted |
| M/queries_test.go | TestAccountWindow | lifted |
| M/queries_test.go | TestAccountPageMatches_JudgesTheResumeToken | lifted |
| M/queries_test.go | TestTransactionWindow | lifted |
| M/queries_test.go | TestMatchTxFilter_TxBuiltinLeaves | lifted |
| M/queries_test.go | TestMatchTxFilter_UnknownStamps | lifted |
| M/queries_test.go | TestTxWindowMatches_OptionalRows | lifted |
| M/queries_test.go | TestMatchTxAddress_RolesAndExclusions | lifted |
| M/queries_test.go | TestMatchTxAddress_UniverseDrop | lifted |
| M/queries_test.go | TestNeededIndexCanonicals_AddressRoles | lifted |
| M/queries_test.go | TestOracle_MetadataIndexLifecycle | lifted |
| M/queries_test.go | TestMatchFieldCondition_Coercion | lifted |
| M/queries_test.go | TestTxRecordMatches_LearnedInsertedAt | lifted |
| M/queries_test.go | TestTxRecordMatches_UpdatedAtTracksInsertedAt | lifted |
| M/queries_test.go | TestTxRecordMatches_ComparesPostCommitVolumes | lifted |
| M/queries_test.go | TestTxRecordMatches_ComparesPostingColour | lifted |
| M/queries_test.go | TestAccountMatches_RejectsUnmodelledTimestamps | lifted |
| M/queries_test.go | TestAccountMatches_SegregatesColourBuckets | lifted |
| M/queries_test.go | TestVolumeComparisons_RejectDuplicateRows | lifted |
| M/queries_logs_test.go | TestMatchLogFilter_EmptyCombinators | lifted |
| M/queries_logs_test.go | TestNeededLogIndexes | lifted |
| M/queries_logs_test.go | TestClassifyLogQueryError | lifted (RPCError) |
| M/queries_logs_test.go | TestLogOutcome_AbsentDateIndexRequiresTheRefusal | lifted |
| M/queries_logs_test.go | TestLogOutcome_ActiveDateIndexRequiresThePage | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_UnlearnedDateIsOptional | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_LearnedDateIsRequired | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_ComparesEveryPinnedField | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_UnlearnedDateSkipsOnlyTheDate | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_ComparesCanonicalPayload | lifted |
| M/queries_logs_test.go | TestCanonicalServedLogPayload_MetadataValuesAreTypeTagged | lifted |
| M/queries_logs_test.go | TestLearnTxStamps_RecordsLogDateAndSequence | deferred to task 00a (learnTxStamps is part of the commit path) |
| M/queries_logs_test.go | TestLogWindowMatches_GlobalSequenceComparedOnceLearned | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_RequiredRowMayNotBeSkipped | lifted |
| M/queries_logs_test.go | TestWorkloadIndexes_IncludesTheLogDateIndex | lifted |
| M/queries_logs_test.go | TestLogOutcome_IndexFreeFilterMayNotBeGated | lifted |
| M/queries_logs_test.go | TestServerLogRows_ReadsVolumeAnnotations | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_ColourSplitIsAMismatch | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_ComparesTheEmbeddedTransaction | lifted |
| M/queries_logs_test.go | TestLogTxMatches_IgnoresPostCreationMutation | lifted |
| M/queries_logs_test.go | TestLogWindowMatches_ReverseDescendsBelowTheCursor | lifted |
| M/cursors_test.go | TestNextCursorLegal_PresenceFollowsTheModelsVerdict | lifted |
| M/cursors_test.go | TestNextCursorLegal_OnlyAFullPageCarriesAToken | lifted |
| M/cursors_test.go | TestMalformedCursors_AreRejectedByParseUint64 | deferred to task 06 (malformed-cursor draws move to the Rust list rules) |
| M/cursors_test.go | TestEffectivePageSize_MirrorsTheServerClamp | lifted |
| M/cursors_test.go | TestQueryPageSize_RollsTheSubstitutedShapes | deferred to task 06 (page-size draws are a Rust domain, DESIGN §6.8; the clamp itself is TestEffectivePageSize_*) |
| M/filter_shapes_test.go | TestAnyFilterShapeIsTotal | deferred to task 06 (filter shape generation moves to Rust) |
| M/filter_shapes_test.go | TestClassifyRejectedFilter | lifted |
| M/filter_shapes_test.go | TestHasAssetComposesOverCurrentAccounts | lifted |
| M/filter_shapes_test.go | TestRefusedFilterAcceptsAWindowedEarlierLeaf | lifted (RPCError) |
| M/aggregates_test.go | TestModelAggregateGroups_AssignsToTheFirstMatchingPrefix | lifted |
| M/aggregates_test.go | TestAggGroupsEqual_ComparesPrefixOrder | lifted |
| M/aggregates_test.go | TestModelAggregateOptionsMirrorTheResultStage | lifted |
| M/aggregates_test.go | TestServerAggregateKeysByAssetAndColor | lifted |
| M/aggregates_zero_test.go | TestModelAggregateRetainsZeroPostingBucket | lifted |
| M/aggregates_zero_test.go | TestModelAggregateZeroCellsParticipateInOptions | lifted |
| M/aggregates_zero_test.go | TestModelAggregateDoesNotInventPurgedZeroCells | lifted |
| M/stats_test.go | TestModelUsage_CountsWhatTheUsagebuilderCounts | lifted |
| M/stats_test.go | TestModelUsage_EmptyLedger | lifted |
| M/stats_test.go | TestModelUsage_ReferenceCountedOncePerTransaction | lifted |
| M/getlog_test.go | TestPickLogSequenceUsesTheModelsLearnedSequences | deferred to task 05 (GetLog target picking is a view + Rust draw) |
| M/getlog_test.go | TestCommittedLogMatchesTheModelsRow | lifted |
| M/getlog_test.go | TestCommittedLogSurvivesLedgerDeletion | deferred to task 05 (uses the GetLog picker) |
| M/getlog_test.go | TestServerLogKindMatchesModelVocabulary | lifted |
| M/getlog_test.go | TestCommittedLogComparesTheLearnedStamps | deferred to task 05 (needs 00a's learnTxStamps; CommittedLogMatches itself is covered by TestCommittedLogMatchesTheModelsRow and the matchtest renderer tests) |
| M/ledger_info_test.go | TestLedgerInfoMatchesComparesEveryDerivedField | lifted |
| M/ledger_info_test.go | TestLedgerInfoMatchesRejectsAnAbsentOrDeletedLedger | lifted |
| M/ledger_info_test.go | TestLedgerInfoStructureViolation | lifted |
| M/ledger_info_test.go | TestLedgerIdentityMatchesBothFields | lifted |
| M/ledger_info_test.go | TestLedgerIdentityIsNotPinnedWhileACreateIsUnsettled | deferred to task 04 (ledgerCreateUnsettled reads Checker in-flight state) |
| M/ledger_info_test.go | TestListLedgersRowMatchesTheWholeModel | deferred to task 04 (drives runLedgersList through a fake gRPC stream) |
| M/ledger_info_test.go | TestListLedgersRejectsAFilter | deferred to task 04 (drives runLedgersList through a fake gRPC stream) |
| M/ledger_info_test.go | TestLedgerListingMatchesNeedsOneBase | lifted, adapted to the DESIGN §3.2 signature; the "new fleet" case now passes (rule 5: a next cursor on a full page is legal) |
| M/ledger_info_test.go | TestIndexListingMatchesAVanishedLedger | lifted |
| M/ledgers_list_test.go | TestLedgerWindow_OrdersSkipsAndTruncates | lifted |
| M/ledgers_list_test.go | TestLastLedgerKey_IsTheLastNameServed | lifted |
| M/index_inspect_test.go | TestInspectPageViolation_DistinctValues | lifted |
| M/index_inspect_test.go | TestInspectPageViolation_FacetsAndSummary | lifted |
| M/index_inspect_test.go | TestModelInspectCountsSplitsNullsFromCarriers | lifted |
| M/index_inspect_test.go | TestModelInspectCountsIgnoresOtherKeysAndTargets | lifted |
| M/index_introspect_test.go | TestIndexSetEqual | lifted |
| M/index_introspect_test.go | TestIndexSetEqual_EmptyRegistry | lifted |
| M/index_introspect_test.go | TestIndexSetEqual_FieldRemovalCascade | lifted |
| M/index_introspect_test.go | TestIndexStatePresence | lifted |
| M/index_introspect_test.go | TestJoinSortedKeys | lifted |
| M/indexes_test.go | TestIndexNeeds | lifted |
| M/indexes_test.go | TestAccountHasAsset | lifted |
| M/indexes_test.go | TestGenAccountAssetFilterAssetOnly | deferred to task 07 (filter generator moves to Rust) |
| M/indexes_test.go | TestAssetWindow | lifted |
| M/indexes_test.go | TestIndexedQueryOutcomeLegal_MismatchAndAbsentCoexist | lifted |
| M/indexes_test.go | TestReconcileIndexes_IncarnationGuard | deferred to task 07 (index readiness reconciliation is tracker-bound) |
| M/indexes_test.go | TestReconcileIndexes_PromotionWaitsForEarlierReads | deferred to task 07 (index readiness reconciliation is tracker-bound) |
| M/indexes_test.go | TestTrackedIndexesExcludesDeletedLedgers | deferred to task 07 (Checker.trackedIndexes) |
| M/indexes_test.go | TestIndexBuildingErrorReachesValidators | lifted (RPCError) |
| M/indexes_test.go | TestIndexNotFoundMatchedByReason | lifted (RPCError) |
| M/indexes_test.go | TestIndexNotReadyRequiresDocumentedCode | lifted (RPCError) |
| M/indexes_test.go | TestIndexedQueryOutcomeLegal_RetypeRefusalAttribution | lifted (RPCError) |
| M/indexes_test.go | TestGenerateIndexOp_UndeclaredFieldsOnBothTargets | deferred to task 07 (index op generator moves to Rust) |
| M/prepared_queries_read_test.go | TestModelAggregateFoldsMatchedAccountsOnly | lifted |
| M/prepared_queries_read_test.go | TestModelAggregateKeepsColorBucketsSeparate | lifted |
| M/prepared_queries_read_test.go | TestAggregateMatchesRejectsDivergence | lifted |
| M/prepared_queries_read_test.go | TestAggregateMatchesRejectsDuplicateBucket | lifted |
| M/prepared_queries_read_test.go | TestAggregateTargetRejectionUsesCandidateDefinition | lifted |
| M/prepared_queries_read_test.go | TestClassifyAggregateTargetRejectionByCode | lifted (RPCError) |
| M/prepared_queries_read_test.go | TestPreparedLedgerOutcomeFollowsLifecycle | lifted |
| M/prepared_queries_read_test.go | TestAggregateRecreationValidatesAccountsCandidateResult | lifted |
| M/prepared_queries_read_test.go | TestListRejectsAggregateTargetErrorForEmptyWindow | lifted |
| M/prepared_queries_read_test.go | TestPreparedOutcomesRejectWrongResponseArm | lifted |
| M/prepared_queries_read_test.go | TestPreparedPaginationPreservesRawCursorOrderingAcrossTargets | lifted |
| M/prepared_queries_read_test.go | TestPreparedTransactionContinuationMustMatchRemainingRows | lifted |
| M/prepared_queries_read_test.go | TestRegistryMatches | lifted |
| M/prepared_queries_read_test.go | TestPreparedLogsUseActiveDateIndex | lifted |
| M/prepared_queries_read_test.go | TestPreparedLogPageUnknownDatesAndHasMore | lifted |
| M/prepared_queries_read_test.go | TestPreparedLogPageRejectsMissingAndInventedRows | lifted |
| M/prepared_queries_read_test.go | TestPreparedReverseWindowsDescend | lifted |
| M/prepared_queries_read_test.go | TestPreparedCursorResumesInEitherDirection | lifted |
| M/checkpoint_reads_test.go | TestCheckpointReadsRejectLiveMutation | lifted |
| M/checkpoint_reads_test.go | TestCheckpointMissingRequiresNotFoundStatus | lifted, adapted: CheckpointNotFound on RPCError |
| M/checkpoint_reads_test.go | TestCheckpointMetadataOnlyAccountWithLaterVolume | lifted |
| M/checkpoint_reads_test.go | TestCheckpointReadAcceptsEmptyAccountWithoutInventingState | lifted |
| M/checkpoint_reads_test.go | TestDeletedCheckpointReadAcceptsOnlyFrozenSuccessOrNotFound | lifted, adapted: CheckpointReadOutcomeMatches with an explicit CandidateMatcher; frozen snapshot built directly (snapshot capture is task 09) |
| M/checkpoint_reads_test.go | TestLiveCheckpointTransactionAbsenceMatchesFrozenState | lifted, adapted as above; also covers CheckpointLedgerReadOutcomeMatches |
| M/audit_test.go | TestAuditSuccessMismatchPinsOneBulk | lifted |
| M/audit_test.go | TestRejectionExplainsMatchesLedgersOrdersAndReason | deferred to task 10 (Checker.rejectionExplains reads the rejection side table) |
| M/audit_test.go | TestAuditPageViolationIsAnExtraOverTheModel | lifted |
| M/audit_test.go | TestAuditPageViolationZoneScanIsDense | lifted |
| M/audit_test.go | TestValidateAuditPageKnownEntriesAreStillOwed | deferred to task 10 (Checker.validateAuditPage) |
| M/audit_test.go | TestValidateAuditPageKnownEntriesMustCertainlyMatch | deferred to task 10 (Checker.validateAuditPage) |
| M/audit_test.go | TestAuditPageViolationHonoursTheCursor | lifted |
| M/audit_test.go | TestAuditEntrySatisfiesEveryLeaf | lifted |
| M/audit_test.go | TestAuditOrderTypeTableMatchesTheDomain | lifted |
| M/audit_test.go | TestValidateAuditPageOrderTypesAndLogCoverage | deferred to task 10 (Checker.validateAuditPage) |
| M/audit_test.go | TestValidateAuditPageReverseTail | deferred to task 10 (Checker.validateAuditPage) |
| M/audit_test.go | TestValidateAuditPageSetupEraAndLedgerLevelLogs | deferred to task 10 (Checker.validateAuditPage) |
| M/audit_test.go | TestValidateAuditPageBeforeFirstDrain | deferred to task 10 (Checker.validateAuditPage) |
| M/audit_test.go | TestAuditHistoryIsNeverPruned | deferred to task 10 (Checker side tables) |
| M/audit_test.go | TestRequestLogKindCoversEveryAuditToken | lifted |
| M/audit_test.go | TestAuditEntryMatchesMixedOrInOnePass | lifted |
| M/reads_test.go | TestModelKnowsAccount | lifted, adapted: the Checker only supplied the initial state (newModel) |
| M/reads_test.go | TestLiveLedgerNamesFiltersTombstones | deferred to task 00a (core ledgers view) / 05 |
| M/reads_test.go | TestPickAbsentAccount | deferred to task 05 (read-target picking is a view + Rust draw) |
| M/reads_test.go | TestPickLedgerReadTarget | deferred to task 05 (read-target picking is a view + Rust draw) |
| M/validate_test.go | TestAccountVolumesMatch | lifted, adapted: the Checker only supplied the initial state (newModel) |
| M/validate_test.go | TestSuccessfulBusinessWriteClassification | deferred to task 00a (isSuccessfulBusinessWrite is part of the commit check) |

## New tests in `match`

| Test | Covers |
|---|---|
| TestLedgerListingMatches_ServerTraversalsPass (ledgers_list_ext_test.go) | DESIGN §3.2: every page of every server traversal (sizes 0–9, both directions, resumed through the token) over a fleet with foreign names on both sides of the tracked range |
| TestLedgerListingMatches_DesignCases | DESIGN §3.2 cases: foreign names before and after the range, pages straddling each boundary in both directions, full pages of foreign rows, short pages omitting a tracked ledger, a foreign row between two tracked rows, page length and cursor rules |
| TestLedgerListingMatches_RowContent, _RefusesUnsupportedCursors | tracked rows compared with the model, foreign row content ignored; undecodable cursors |
| TestEffectivePageSize_DefaultInRangeAndClamp (cursors_test.go) | 0 → default, in-range, > 1000 → clamp |
| TestMatchers_ConsistentPassesMutationFails (matchers_mutation_test.go) | model-consistent value matches, single-field mutation does not, for the matchers not covered by matchtest |
| TestNoSDKImport (deps_test.go) | no Antithesis SDK beyond what tests/oracle already links, no workload/internal, no oraclecheck |
| matchtest/render_test.go (TestRender*Matches, TestRender*MutationsFail) | each renderer's output passes its match predicate; one-field mutations fail (accounts, account pages, transactions, transaction pages, logs, log pages, ledger info, stats, aggregates, audit entries) |
