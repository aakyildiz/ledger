package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// exitRecord is one exit of a child process.
type exitRecord struct {
	Mode    string `json:"mode"`
	Code    int    `json:"code"`
	Signal  string `json:"signal,omitempty"`
	ByAgent bool   `json:"byAgent"`
	Pid     int    `json:"pid"`
}

// proc is one running child (the server or ledgerctl).
type proc struct {
	cmd      *exec.Cmd
	mode     string
	done     chan struct{}
	mu       sync.Mutex
	stopping bool
	exit     exitRecord
	pumps    sync.WaitGroup
}

// lineWriter serializes whole lines onto one writer, so the child's stdout
// and stderr lines never interleave mid-line.
type lineWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lineWriter) writeLine(b []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// A failed write to our own stdout leaves nothing to report it to.
	_, _ = l.w.Write(b)
}

// pumpLines copies r to out line by line, unchanged and unbuffered: each line
// is written as soon as its newline arrives. A final line without a newline
// gets one. tee, when set, also receives every line (ledgerctl's tail).
func pumpLines(r io.Reader, out *lineWriter, tee func(string)) {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				line = append(line, '\n')
			}
			out.writeLine(line)
			if tee != nil {
				tee(string(line[:len(line)-1]))
			}
		}
		if err != nil {
			return
		}
	}
}

// startProc starts bin with args and env, pumping its output to out, and
// records its exit. onExit runs after the exit is recorded.
func startProc(bin string, args, env []string, mode string, out *lineWriter, tee func(string), onExit func(exitRecord)) (*proc, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = env
	// Its own process group: a signal to the agent's group (a Ctrl-C in a
	// local run) does not reach the child behind the agent's back.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &proc{cmd: cmd, mode: mode, done: make(chan struct{})}
	p.pumps.Add(2)
	go func() { defer p.pumps.Done(); pumpLines(stdout, out, tee) }()
	go func() { defer p.pumps.Done(); pumpLines(stderr, out, tee) }()
	go func() {
		// Drain the pipes before Wait closes them.
		p.pumps.Wait()
		err := cmd.Wait()
		rec := exitRecord{Mode: mode, Pid: cmd.Process.Pid}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				rec.Signal = ws.Signal().String()
				rec.Code = -1
			} else {
				rec.Code = ee.ExitCode()
			}
		} else if err != nil {
			rec.Code = -1
			rec.Signal = err.Error()
		}
		p.mu.Lock()
		rec.ByAgent = p.stopping
		p.exit = rec
		p.mu.Unlock()
		if onExit != nil {
			onExit(rec)
		}
		close(p.done)
	}()

	return p, nil
}

func (p *proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// stop ends the process and waits for its exit (a live Pebble LOCK would
// otherwise break the next boot, restore.md gotcha 11). graceful sends
// SIGTERM first and SIGKILL after timeout.
func (p *proc) stop(graceful bool, timeout time.Duration) exitRecord {
	p.mu.Lock()
	p.stopping = true
	p.mu.Unlock()
	if p.alive() {
		sig := syscall.SIGKILL
		if graceful {
			sig = syscall.SIGTERM
		}
		// The process may have exited since alive(); the wait below covers it.
		_ = p.cmd.Process.Signal(sig)
		if graceful {
			select {
			case <-p.done:
			case <-time.After(timeout):
				_ = p.cmd.Process.Signal(syscall.SIGKILL) // see above
			}
		}
	}
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.exit
}

var stdoutLines = &lineWriter{w: os.Stdout}
