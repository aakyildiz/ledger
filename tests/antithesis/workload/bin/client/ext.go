package main

import (
	"net/http"
	"sync"
)

// Extension registry (DESIGN §7 C7). Families add endpoints in their own
// files (e.g. task 17's ext_block.go) from init():
//
//	func init() { registerExt("POST /block", handleBlock) }
//
// main mounts every registered handler on the client's mux. A handler reaches
// the client's connections through extServer() (route selection with
// conns.pick, UDP completion events with report) and must call
// assertClassified after every RPC it sends itself.

type extHandler struct {
	pattern string
	h       http.HandlerFunc
}

var (
	extMu       sync.Mutex
	extHandlers []extHandler
	extSrv      *server
)

// registerExt registers an extension endpoint. pattern is an http.ServeMux
// pattern ("POST /block"); registering a pattern twice panics at mount.
func registerExt(pattern string, h http.HandlerFunc) {
	extMu.Lock()
	defer extMu.Unlock()
	extHandlers = append(extHandlers, extHandler{pattern: pattern, h: h})
}

// mountExts mounts every registered extension and makes s available to them.
func mountExts(mux *http.ServeMux, s *server) {
	extMu.Lock()
	defer extMu.Unlock()
	extSrv = s
	for _, e := range extHandlers {
		mux.HandleFunc(e.pattern, e.h)
	}
}

// extServer returns the running client for extension handlers.
func extServer() *server {
	extMu.Lock()
	defer extMu.Unlock()

	return extSrv
}
