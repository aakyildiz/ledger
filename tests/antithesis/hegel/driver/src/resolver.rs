//! Resolver threads and the late poster (DESIGN §4.9). Resolvers retry
//! handed-off tickets forever with a deterministic schedule (attempt k:
//! client `k mod n`, route plain, backoff `min(250 ms · 2^k, 5 s)`), so
//! nothing escapes hegel. The poster reports definitive late outcomes of
//! ended tickets with `/write/late`.
use crate::clients::{definitive, Route};
use crate::machine::Driver;
use crate::oracle::{Via, WriteLateReq};
use crate::shared::Shared;
use crate::write::{attempt, finish, late_definitive, TicketState};
use serde_json::json;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub const RESOLVER_THREADS: usize = 2;
/// Q3 timeouts a ticket may survive before it is reported.
pub const MAX_Q3_SURVIVALS: u64 = 3;

pub(crate) struct Entry {
    st: Option<TicketState>,
    handed_at: Instant,
    k: u32,
    next_at: Instant,
    q3_at_handoff: u64,
    reported: bool,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Entry>,
    /// Handoff time of every entry a resolver thread is working on.
    in_flight: Vec<(u64, Instant)>,
}

#[derive(Default)]
pub struct Resolvers {
    state: Mutex<State>,
    cv: Condvar,
    paused: AtomicBool,
    q3_timeouts: AtomicU64,
    started: AtomicBool,
}

impl Resolvers {
    pub(crate) fn push(&self, st: TicketState) {
        let now = Instant::now();
        let q3 = self.q3_timeouts.load(Ordering::SeqCst);
        self.state.lock().unwrap_or_else(|e| e.into_inner()).queue.push_back(Entry { st: Some(st), handed_at: now, k: 0, next_at: now, q3_at_handoff: q3, reported: false });
        self.cv.notify_all();
    }

    /// No handed-off ticket is queued or being worked on.
    pub fn is_empty(&self) -> bool {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.queue.is_empty() && s.in_flight.is_empty()
    }

    #[allow(dead_code)] // family-facing API
    pub fn len(&self) -> usize {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.queue.len() + s.in_flight.len()
    }

    /// Age of the oldest handed-off ticket still unresolved.
    pub fn oldest_handoff_age(&self) -> Option<Duration> {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.queue.iter().map(|e| e.handed_at).chain(s.in_flight.iter().map(|(_, t)| *t)).min().map(|t| t.elapsed())
    }

    /// Tickets of the queue (for diagnostics).
    pub fn tickets(&self) -> Vec<u64> {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.queue.iter().filter_map(|e| e.st.as_ref().map(|t| t.ticket)).chain(s.in_flight.iter().map(|(t, _)| *t)).collect()
    }

    /// Unit tests hold the resolvers to observe a handoff.
    #[allow(dead_code)] // test-facing API
    pub fn set_paused(&self, p: bool) {
        self.paused.store(p, Ordering::SeqCst);
        self.cv.notify_all();
    }

    /// A quiet period's Q3 timed out with these tickets unresolved.
    pub fn q3_timeout(&self) {
        let n = self.q3_timeouts.fetch_add(1, Ordering::SeqCst) + 1;
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for e in s.queue.iter_mut() {
            report_survivor(e, n);
        }
    }

    fn next_ready(&self, wait: Duration) -> Option<Entry> {
        let deadline = Instant::now() + wait;
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if !self.paused.load(Ordering::SeqCst) {
                let now = Instant::now();
                if let Some(i) = s.queue.iter().position(|e| e.next_at <= now) {
                    let e = s.queue.remove(i).unwrap();
                    let ticket = e.st.as_ref().map(|t| t.ticket).unwrap_or(0);
                    s.in_flight.push((ticket, e.handed_at));
                    return Some(e);
                }
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let until = s.queue.iter().map(|e| e.next_at).min().unwrap_or(deadline).min(deadline).max(now + Duration::from_millis(1));
            s = self.cv.wait_timeout(s, until - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    fn done(&self, ticket: u64, requeue: Option<Entry>) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = s.in_flight.iter().position(|(t, _)| *t == ticket) {
            s.in_flight.remove(i);
        }
        if let Some(e) = requeue {
            s.queue.push_back(e);
        }
        drop(s);
        self.cv.notify_all();
    }
}

fn report_survivor(e: &mut Entry, q3_now: u64) {
    if !e.reported && q3_now.saturating_sub(e.q3_at_handoff) >= MAX_Q3_SURVIVALS {
        e.reported = true;
        let (wid, ticket) = e.st.as_ref().map(|t| (t.wid.clone(), t.ticket)).unwrap_or_default();
        check_unreachable!(
            "a tracked write reaches a definitive outcome once faults stop",
            json!({"wid": wid, "ticket": ticket, "quietPeriods": q3_now - e.q3_at_handoff, "handedOffSecondsAgo": e.handed_at.elapsed().as_secs()})
        );
    }
}

/// Starts the resolver threads and the late poster (once per `Shared`).
pub fn start(s: &Arc<Shared>) {
    if s.resolvers.started.swap(true, Ordering::SeqCst) {
        return;
    }
    for i in 0..RESOLVER_THREADS {
        let w = Arc::downgrade(s);
        crate::assertions::spawn_inheriting(&format!("resolver-{i}"), move || loop {
            let Some(s) = w.upgrade() else { return };
            let Some(e) = s.resolvers.next_ready(Duration::from_millis(100)) else { continue };
            resolve_once(&Driver { s }, e);
        });
    }
    let w = Arc::downgrade(s);
    crate::assertions::spawn_inheriting("late-poster", move || loop {
        let Some(s) = w.upgrade() else { return };
        let ev = {
            let mut q = s.clients.late_queue.lock().unwrap_or_else(|e| e.into_inner());
            if q.is_empty() {
                q = s.clients.late_queue_cv.wait_timeout(q, Duration::from_millis(100)).unwrap_or_else(|e| e.into_inner()).0;
            }
            q.pop_front()
        };
        let Some(ev) = ev else { continue };
        if !s.oracle_ok() {
            continue;
        }
        Driver::helper_iteration("late-poster", || {
            let req = WriteLateReq { ticket: ev.ticket, outcome: ev.outcome.clone(), via: Via { op_id: ev.op, client: ev.client, route: ev.route.clone(), late: true } };
            if let Err(e) = s.oracle.write_late(&req) {
                println!("driver: /write/late {}: {:?} {}", ev.ticket, e.kind, e.message);
            }
        });
    });
}

/// One resolver pass over one ticket.
fn resolve_once(d: &Driver, mut e: Entry) {
    let ticket = e.st.as_ref().map(|t| t.ticket).unwrap_or(0);
    if !d.s.oracle_ok() {
        // Degraded mode: drop the ticket without posting.
        if let Some(st) = e.st.take() {
            d.s.clients.unwatch(&st.op_ids());
            d.s.registry.remove(st.ticket);
        }
        d.s.resolvers.done(ticket, None);
        return;
    }
    report_survivor(&mut e, d.s.resolvers.q3_timeouts.load(Ordering::SeqCst));
    let n = d.s.clients.len().max(1);
    let k = e.k;
    let settled = Driver::helper_iteration("resolver", || {
        let st = e.st.as_mut()?;
        if let Some((op, out)) = late_definitive(d, st) {
            let st = e.st.take()?;
            return Some(finish(d, st, out, op, true));
        }
        let route = Route::plain(k as usize % n);
        let (op, out) = attempt(d, st, &route);
        if definitive(&out) {
            let st = e.st.take()?;
            return Some(finish(d, st, out, op, false));
        }
        None
    })
    .flatten();
    check_sometimes!(settled.is_some(), "a resolver settles a handed-off write", json!({"attempt": k}));
    if settled.is_some() || e.st.is_none() {
        d.s.resolvers.done(ticket, None);
        return;
    }
    let t = d.s.timing();
    let backoff = t.resolver_backoff_first.saturating_mul(1u32 << e.k.min(16)).min(t.resolver_backoff_cap);
    e.k += 1;
    e.next_at = Instant::now() + backoff;
    d.s.resolvers.done(ticket, Some(e));
}

#[cfg(test)]
mod tests {
    use crate::clients::Outcome;
    use crate::testkit::{self, FakeReply};
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn late_definitive_outcome_of_an_ended_ticket_is_posted() {
        let (d, ft, fo) = testkit::driver();
        ft.script("bucket", "Apply", vec![FakeReply::Outcome(Outcome::unknown("timeout")), FakeReply::Outcome(Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": "4"}]}})))]);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let mut p = crate::write::WritePlan::new("test", vec![json!({"createLedger": {"name": "x"}})]);
        p.backoffs_ms = vec![1];
        assert!(d.dispatch(p, unit).is_success());
        let first = ft.calls_of("bucket", "Apply")[0]["opId"].as_u64().unwrap();
        // The first attempt's success arrives after the ticket ended.
        d.s.clients.deliver(first, Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": "4"}]}})));
        // A non-definitive late outcome is ignored, and so is the UDP echo of
        // the attempt that ended the ticket.
        d.s.clients.deliver(first + 100, Outcome::unknown("timeout"));
        let second = ft.calls_of("bucket", "Apply")[1]["opId"].as_u64().unwrap();
        d.s.clients.deliver(second, Outcome::from_json(&json!({"ok": true, "response": {"logs": [{"sequence": "4"}]}})));
        testkit::wait_until(Duration::from_secs(5), || fo.calls("/write/late").len() == 1);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(fo.calls("/write/late").len(), 1);
        let late = &fo.calls("/write/late")[0];
        assert_eq!(late["via"]["opId"], first);
        assert_eq!(late["via"]["late"], true);
    }

    #[test]
    fn survivors_of_three_q3_timeouts_are_reported() {
        let (d, ft, _) = testkit::driver();
        testkit::reset();
        ft.set_default("bucket", "Apply", FakeReply::Outcome(Outcome::unknown("timeout")));
        d.s.resolvers.set_paused(true);
        let unit = d.s.slots.try_acquire_up_to(1).unwrap().take_one().unwrap();
        let t = d.begin_write(crate::write::WritePlan::new("test", vec![json!({"createLedger": {"name": "x"}})]), unit).unwrap();
        let _ = t.run(Duration::from_millis(1));
        d.s.resolvers.q3_timeout();
        d.s.resolvers.q3_timeout();
        assert!(!testkit::fired("a tracked write reaches a definitive outcome once faults stop"));
        d.s.resolvers.q3_timeout();
        assert!(testkit::fired("a tracked write reaches a definitive outcome once faults stop"));
    }
}
