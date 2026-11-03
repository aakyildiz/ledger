package ledgercheck

import oc "github.com/formancehq/ledger/v3/tests/antithesis/workload/oraclecheck"

func init() {
	oc.RegisterReadKind(oc.ReadKind{Name: KindGetLedger, Validate: ValidateGetLedger})
	oc.RegisterReadKind(oc.ReadKind{Name: KindSchemaStatus, Validate: ValidateSchemaStatus})
	oc.RegisterReadKind(oc.ReadKind{Name: KindListLedgers, Validate: ValidateListLedgers})
	oc.RegisterDrainHook("ledgers", DrainHook)
	oc.RegisterFailureHook("ledgers", FailureHook)
	oc.RegisterCoverage(
		EvLcCovDeletion,
		EvLcCovPromotion,
		EvLcCovTopLevelStrict,
		EvLcCovTopLevelAudit,
		EvLcCovLedgerActionStrict,
		EvLcCovLedgerActionAudit,
		EvLcCovSkipTypeExists,
		EvLcCovSkipTypeNotFound,
		EvLcCovInvalidSkip,
	)
}
