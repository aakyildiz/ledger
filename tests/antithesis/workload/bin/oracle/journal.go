package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Journal (DESIGN §4.12): one entry per handled request, kept in a ring
// bounded by bytes; large bodies are elided in the ring, never in the file.

const (
	defaultJournalRingBytes = 64 << 20
	elideRequestOver        = 1 << 20
	elideResponseOver       = 16 << 10
	findingDumpBytes        = 4 << 20
)

type journalEntry struct {
	N          uint64          `json:"n"`
	Boot       string          `json:"boot"`
	Ep         string          `json:"ep"`
	Req        json.RawMessage `json:"req"`
	Status     int             `json:"status"`
	Resp       json.RawMessage `json:"resp"`
	Violations []string        `json:"violations"`
	Ms         int64           `json:"ms"`
}

type elided struct {
	Elided bool   `json:"elided"`
	Sha256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}

func elide(body json.RawMessage, limit int) json.RawMessage {
	if len(body) <= limit {
		return body
	}
	sum := sha256.Sum256(body)
	b, _ := json.Marshal(elided{Elided: true, Sha256: hex.EncodeToString(sum[:]), Bytes: len(body)})

	return b
}

// isElided reports an elided body and returns its marker.
func isElided(body json.RawMessage) (elided, bool) {
	var e elided
	if len(body) == 0 || body[0] != '{' {
		return e, false
	}
	if err := json.Unmarshal(body, &e); err != nil || !e.Elided || e.Sha256 == "" {
		return e, false
	}

	return e, true
}

type ringEntry struct {
	n    uint64
	data []byte
}

type journal struct {
	next     uint64
	ring     []ringEntry
	bytes    int
	maxBytes int
	file     io.WriteCloser
}

func newJournal(maxBytes int, path string) (*journal, error) {
	if maxBytes <= 0 {
		maxBytes = defaultJournalRingBytes
	}
	j := &journal{next: 1, maxBytes: maxBytes}
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open journal file: %w", err)
		}
		j.file = f
	}

	return j, nil
}

// append records an entry; it assigns n.
func (j *journal) append(e journalEntry) uint64 {
	e.N = j.next
	j.next++
	if e.Violations == nil {
		e.Violations = []string{}
	}
	if j.file != nil {
		if b, err := json.Marshal(e); err == nil {
			_, _ = j.file.Write(append(b, '\n')) // best effort: the ring stays authoritative
		}
	}
	e.Req = elide(e.Req, elideRequestOver)
	e.Resp = elide(e.Resp, elideResponseOver)
	b, err := json.Marshal(e)
	if err != nil {
		return e.N
	}
	j.ring = append(j.ring, ringEntry{n: e.N, data: b})
	j.bytes += len(b)
	for j.bytes > j.maxBytes && len(j.ring) > 1 {
		j.bytes -= len(j.ring[0].data)
		j.ring = j.ring[1:]
	}

	return e.N
}

// page returns up to limit entries with n >= from and the next n to ask for.
func (j *journal) page(from uint64, limit int) ([]json.RawMessage, uint64) {
	if limit <= 0 {
		limit = 100
	}
	out := []json.RawMessage{}
	next := from
	for _, e := range j.ring {
		if e.n < from {
			continue
		}
		if len(out) >= limit {
			break
		}
		out = append(out, json.RawMessage(e.data))
		next = e.n + 1
	}
	if len(out) == 0 && len(j.ring) > 0 && from < j.ring[0].n {
		next = j.ring[0].n
	}

	return out, next
}

// newest returns the newest entries whose total size stays within limit, in
// chronological order.
func (j *journal) newest(limit int) [][]byte {
	total := 0
	start := len(j.ring)
	for start > 0 && total+len(j.ring[start-1].data) <= limit {
		start--
		total += len(j.ring[start].data)
	}
	out := make([][]byte, 0, len(j.ring)-start)
	for _, e := range j.ring[start:] {
		out = append(out, e.data)
	}

	return out
}
