//! RPC classification for the restore cycle: the ported busy-destination
//! retry (`W/internal/backup.go`), the no-full-checkpoint precondition, the
//! ambiguity rule, and the conservative corrupt-backup classifier.
use crate::clients::Outcome;
use serde_json::{json, Value};

/// The backup storage of every call of the cycle (restore.md §4.3).
pub fn storage() -> Value {
    json!({"s3": {"bucket": "backups", "region": "us-east-1", "endpoint": "http://minio:9000", "accessKeyId": "minioadmin", "secretAccessKey": "minioadmin"}})
}

/// `IsBackupInProgress`: FailedPrecondition with the destination-busy suffix.
pub fn is_busy(o: &Outcome) -> bool {
    !o.is_unknown() && o.code == "FailedPrecondition" && o.message.ends_with("backup: destination already has a running job")
}

/// `IsNoFullCheckpoint`: the namespace has no full checkpoint (reset or lost).
pub fn is_no_full_checkpoint(o: &Outcome) -> bool {
    !o.is_unknown() && o.code == "FailedPrecondition" && o.message.contains("full checkpoint")
}

/// An outcome that says nothing about whether the call took effect, or a
/// transient refusal worth retrying: no answer, a local client error, or a
/// transport-ish code. Never a finding on a restore RPC (gotcha 5).
pub fn ambiguous(o: &Outcome) -> bool {
    if o.is_unknown() || o.local {
        return true;
    }
    if o.ok {
        return false;
    }
    matches!(o.code.as_str(), "" | "Unavailable" | "DeadlineExceeded" | "Canceled" | "Aborted" | "ResourceExhausted")
        || o.reason == "EXTERNAL_SERVICE_ERROR"
}

/// A restore-mode node answers a bucket/cluster RPC, or a node that is not
/// in restore mode (yet) answers a RestoreService RPC, with Unimplemented
/// (gotcha 9, 00a C3): the child is not (yet) in the expected mode.
pub fn unimplemented(o: &Outcome) -> bool {
    !o.is_unknown() && o.code == "Unimplemented"
}

pub fn failed_precondition(o: &Outcome, fragment: &str) -> bool {
    !o.is_unknown() && !o.ok && o.code == "FailedPrecondition" && o.message.contains(fragment)
}

/// The integrity failures of a restore download or bootstrap: exact
/// fragments of the errors in `internal/infra/backup/restore.go` (export
/// segments), `internal/infra/backup/manifest.go` (checkpoint files) and
/// `internal/adapter/grpc/server_restore_download.go` (manifest entries).
/// Transport errors (MinIO unreachable, `downloading ...: ...`) are not
/// listed: only the driver writes the namespace and Antithesis does not
/// corrupt disks, so a match is a bug.
pub const CORRUPT_FRAGMENTS: &[&str] = &[
    "is not valid for export segment type",
    "outside export range",
    "invalid export segment",
    "audit item index gap or disorder in segment",
    "sequence gap or disorder in segment",
    "incomplete export segment",
    "trailing bytes after footer in segment",
    "size mismatch in segment",
    "checkpoint file does not match its manifest entry",
    "invalid manifest entry",
];

pub fn corrupt_backup(message: &str) -> Option<&'static str> {
    CORRUPT_FRAGMENTS.iter().copied().find(|f| message.contains(f))
}

/// Download states of `GetDownloadStatus` (restore.proto DownloadState).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DlState {
    Running,
    Succeeded,
    Failed,
    Canceled,
    Unknown,
}

pub fn download_state(resp: &Value) -> DlState {
    match resp.get("state").and_then(Value::as_str).unwrap_or("") {
        "DOWNLOAD_STATE_PENDING" | "DOWNLOAD_STATE_RUNNING" => DlState::Running,
        "DOWNLOAD_STATE_SUCCEEDED" => DlState::Succeeded,
        "DOWNLOAD_STATE_FAILED" => DlState::Failed,
        "DOWNLOAD_STATE_CANCELED" => DlState::Canceled,
        _ => DlState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn o(v: Value) -> Outcome {
        Outcome::from_json(&v)
    }

    #[test]
    fn classifiers() {
        assert!(is_busy(&o(json!({"ok": false, "code": "FailedPrecondition", "message": "x: backup: destination already has a running job"}))));
        assert!(!is_busy(&o(json!({"ok": false, "code": "Unavailable", "message": "backup: destination already has a running job"}))));
        assert!(is_no_full_checkpoint(&o(json!({"ok": false, "code": "FailedPrecondition", "message": "no full checkpoint at destination"}))));
        assert!(ambiguous(&Outcome::unknown("timeout")));
        assert!(ambiguous(&o(json!({"ok": false, "code": "Unavailable"}))));
        assert!(!ambiguous(&o(json!({"ok": false, "code": "FailedPrecondition"}))));
        assert!(!ambiguous(&o(json!({"ok": true, "response": {}}))));
        assert!(unimplemented(&o(json!({"ok": false, "code": "Unimplemented"}))));
        assert_eq!(corrupt_backup("applying exports: sequence gap or disorder in segment exports/logs-1: previous 3, current 5"), Some("sequence gap or disorder in segment"));
        assert_eq!(corrupt_backup("downloading data/x: dial tcp minio:9000: connection refused"), None);
        assert_eq!(corrupt_backup("rpc error: code = InvalidArgument desc = invalid manifest entry: bad"), Some("invalid manifest entry"));
        assert_eq!(download_state(&json!({"state": "DOWNLOAD_STATE_RUNNING"})), DlState::Running);
        assert_eq!(download_state(&json!({})), DlState::Unknown);
    }
}
