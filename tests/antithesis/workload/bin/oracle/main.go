// Command oracle is the hegel workload's oracle service (DESIGN §4, §5): a
// passive, deterministic HTTP service that the Rust driver tells about every
// tracked write and read. It validates them against the forward model of
// tests/oracle through the oraclecheck library and asserts every finding with
// a literal Antithesis assertion at its own call site.
//
// Environment:
//
//	ORACLE_ADDR                listen address (default :8090)
//	QUERY_CHECKPOINT_LIMIT     the cluster's limit; /setup must agree
//	ORACLE_JOURNAL_RING_BYTES  in-memory journal bound (default 64 MiB)
//	ORACLE_JOURNAL_FILE        append every journal entry, unelided (local only)
//	ORACLE_DUMP_BATCHES=1      print a [batch-dump] line at every /write/end
package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	addr := os.Getenv("ORACLE_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	ring, _ := strconv.Atoi(os.Getenv("ORACLE_JOURNAL_RING_BYTES")) // 0 = default
	s, err := newServer(options{
		QueryCheckpointLimit: os.Getenv("QUERY_CHECKPOINT_LIMIT"),
		JournalRingBytes:     ring,
		JournalFile:          os.Getenv("ORACLE_JOURNAL_FILE"),
		DumpBatches:          os.Getenv("ORACLE_DUMP_BATCHES") == "1",
		Out:                  os.Stdout,
	})
	if err != nil {
		log.Fatalf("oracle: %v", err)
	}
	catalogCoverage()
	log.Printf("oracle: boot %s listening on %s", s.boot, addr)
	srv := &http.Server{Addr: addr, Handler: s, ReadHeaderTimeout: 30 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
