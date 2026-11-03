// Command restore-agent supervises one ledger-server child in a restore target
// container (restore-0, restore-1) of the hegel Compose deployment (task 18,
// restore.md §4.1). The driver restores the source cluster's backups into it
// while the source keeps running, then compares the restored cluster with the
// source at the backup cut.
//
// It is infrastructure: it makes no choice of its own (every action comes from
// a driver command), asserts nothing (findings are the driver's) and imports
// nothing from the workload's internal package.
//
// HTTP (AGENT_ADDR, default :8090); every mutation carries the driver's cycle
// generation `gen`, and a stale `gen` is answered with 409:
//
//	GET  /status
//	POST /reset {gen}                       stop the child (wait for exit), wipe /data/app and /data/raft
//	POST /start {gen, mode, clusterId, nodeId, join?}   mode: restore | normal | join
//	POST /stop {gen, graceful}              stop the child, keep the data
//	POST /offline-bootstrap {gen, bucketId, validate}   ledgerctl store bootstrap
//	POST /wipe-namespace {gen, bucketId}    delete every backup object under <bucketId>/ (restore-g* only)
//
// State lives in AGENT_STATE (default /data/agent.json) and is fsynced before
// every action, so a container restart resumes as restore.md §4.1 says. The
// child's stdout and stderr are copied to the agent's stdout line by line,
// unbuffered and without a prefix, so forward-logs delivers the server's JSON
// records and its race, panic and red-flag lines to the driver unchanged.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := configFromEnv()
	a, err := newAgent(cfg)
	if err != nil {
		log.Fatalf("restore-agent: %v", err)
	}
	a.wiper = newS3Wiper(cfg)
	a.resume()

	srv := &http.Server{Addr: cfg.addr, Handler: a.routes(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("restore-agent: listen %s: %v", cfg.addr, err)
		}
	}()
	log.Printf("restore-agent: listening on %s (gen %d, mode %s, boot %d)", cfg.addr, a.state.Gen, a.state.Mode, a.state.BootCount)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Printf("restore-agent: stopping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Shutdown(ctx)
	cancel()
	a.shutdown(10 * time.Second)
}
