package main

import (
	"os"
	"strings"
	"time"
)

type config struct {
	addr         string
	dataDir      string // the server's --data-dir (RESTORED marker, checkpoints/, restore-staging/)
	walDir       string // the server's --wal-dir
	stateFile    string
	serverBin    string
	ledgerctlBin string
	hostname     string
	stopTimeout  time.Duration

	s3Bucket, s3Endpoint, s3Region, s3AccessKey, s3SecretKey string

	// childEnv is the child's whole environment, built from scratch.
	childEnv []string
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}

	return def
}

func configFromEnv() config {
	host, _ := os.Hostname()

	return config{
		addr:         getenv("AGENT_ADDR", ":8090"),
		dataDir:      getenv("DATA_DIR", "/data/app"),
		walDir:       getenv("WAL_DIR", "/data/raft"),
		stateFile:    getenv("AGENT_STATE", "/data/agent.json"),
		serverBin:    getenv("LEDGER_SERVER_BIN", "/app/ledger-server"),
		ledgerctlBin: getenv("LEDGERCTL_BIN", "/app/ledgerctl"),
		hostname:     getenv("AGENT_HOSTNAME", host),
		stopTimeout:  30 * time.Second,
		s3Bucket:     getenv("S3_BUCKET", "backups"),
		s3Endpoint:   getenv("S3_ENDPOINT", "http://minio:9000"),
		s3Region:     getenv("S3_REGION", "us-east-1"),
		s3AccessKey:  getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
		s3SecretKey:  getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
		childEnv:     childEnv(os.Environ()),
	}
}

// childEnvNames are passed to the child unchanged; childEnvPrefixes pass every
// variable that starts with them. Everything else is dropped: in particular
// CLUSTER_ID (the image's `default` must never reach a restore target, gotcha
// 2), NODE_ID, DATA_DIR and WAL_DIR (the agent passes flags), and the agent's
// own settings.
var childEnvNames = map[string]bool{
	"PATH": true, "HOME": true, "TZ": true,
	"INSECURE": true, "JSON_FORMATTING_LOGGER": true, "LOG_LEVEL": true,
	"SENTINEL_MODE": true, "IDEMPOTENCY_TTL": true, "QUERY_CHECKPOINT_LIMIT": true,
	"MAINTENANCE_INTERVAL": true,
	"AWS_ACCESS_KEY_ID":    true, "AWS_SECRET_ACCESS_KEY": true, "AWS_REGION": true,
	"GORACE": true, "GOMEMLIMIT": true, "GOMAXPROCS": true, "GOCOVERDIR": true, "GOTRACEBACK": true,
}

// HEALTH_ carries the disk-full gate thresholds (raised on local runs only).
var childEnvPrefixes = []string{"PEBBLE_", "READ_INDEX_", "BLOOM_", "HEALTH_"}

func childEnv(environ []string) []string {
	var out []string
	for _, kv := range environ {
		name, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		keep := childEnvNames[name]
		for _, p := range childEnvPrefixes {
			if strings.HasPrefix(name, p) {
				keep = true
			}
		}
		if keep {
			out = append(out, kv)
		}
	}

	return out
}

// serverArgs is the child's command line for a mode (restore.md §4.1). The
// restore-mode listener binds 127.0.0.1 by default (cmd/server/server.go
// --restore-listen), which the clients could not reach.
func (c config) serverArgs(mode, clusterID string, nodeID uint64, join string) []string {
	args := []string{"run"}
	if mode == modeRestore {
		args = append(args, "--restore", "--restore-listen", "0.0.0.0")
	}
	args = append(args,
		"--node-id", uitoa(nodeID),
		"--cluster-id", clusterID,
		"--data-dir", c.dataDir,
		"--wal-dir", c.walDir,
		"--advertise-addr", c.hostname+":7777",
	)
	if mode == modeJoin {
		args = append(args, "--join", join)
	}

	return args
}

// bootstrapArgs is the ledgerctl command line of the offline path.
func (c config) bootstrapArgs(bucketID string, validate bool) []string {
	args := []string{
		"store", "bootstrap",
		"--driver", "s3",
		"--s3-bucket", c.s3Bucket,
		"--s3-endpoint", c.s3Endpoint,
		"--s3-region", c.s3Region,
		"--s3-access-key-id", c.s3AccessKey,
		"--s3-secret-access-key", c.s3SecretKey,
		"--bucket-id", bucketID,
		"--data-dir", c.dataDir,
	}
	if validate {
		args = append(args, "--validate")
	}

	return append(args, "--yes")
}
