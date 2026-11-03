package main

import (
	"crypto/sha256"
)

// Idempotency of calls (DESIGN §4.3): a repeated identical body under the
// same call key answers the cached response. Keys: "wb:<wid>", "we:<ticket>",
// "wl:<ticket>", "wa:<ticket>", "rb:<rid>", "re:<ticket>", "ra:<ticket>",
// "qb:<epoch>", "qs:<epoch>:<section>:<seq>", "qe:<epoch>". The cache is
// bounded (oldest dropped); a dropped /write/end entry still answers through
// the tracker, which treats an identical outcome as a repeat.

const idemCacheCap = 1 << 16

type cachedResp struct {
	hash   [32]byte
	status int
	body   map[string]any
}

type idemCache struct {
	m     map[string]cachedResp
	order []string
}

func newIdemCache() *idemCache { return &idemCache{m: map[string]cachedResp{}} }

func (c *idemCache) get(key string, body []byte) (cachedResp, bool) {
	r, ok := c.m[key]
	if !ok || r.hash != sha256.Sum256(body) {
		return cachedResp{}, false
	}

	return r, true
}

func (c *idemCache) put(key string, body []byte, status int, resp map[string]any) {
	if _, ok := c.m[key]; !ok {
		c.order = append(c.order, key)
	}
	c.m[key] = cachedResp{hash: sha256.Sum256(body), status: status, body: resp}
	for len(c.order) > idemCacheCap {
		delete(c.m, c.order[0])
		c.order = c.order[1:]
	}
}
