//! The job runner (DESIGN §4.13): long operations drawn in a rule run on two
//! job threads, FIFO, each under `catch_unwind`. Jobs draw nothing.
//! `blocks_quiet` jobs hold quiet-period Q2/Q3 until they finish.
use crate::machine::Driver;
use crate::shared::Shared;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub const JOB_THREADS: usize = 2;

pub struct Job {
    pub name: &'static str,
    pub blocks_quiet: bool,
    pub run: Box<dyn FnOnce(&Driver) + Send>,
}

#[derive(Default)]
pub struct Jobs {
    queue: Mutex<VecDeque<Job>>,
    cv: Condvar,
    /// Queued + running jobs with `blocks_quiet`.
    blocking: AtomicUsize,
    running: AtomicUsize,
    started: AtomicBool,
}

impl Jobs {
    /// Queued plus running quiet-blocking jobs.
    pub fn blocking(&self) -> usize {
        self.blocking.load(Ordering::SeqCst)
    }

    /// Queued plus running jobs.
    #[allow(dead_code)] // family-facing API
    pub fn len(&self) -> usize {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).len() + self.running.load(Ordering::SeqCst)
    }

    fn push(&self, job: Job) {
        if job.blocks_quiet {
            self.blocking.fetch_add(1, Ordering::SeqCst);
        }
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).push_back(job);
        self.cv.notify_one();
    }

    fn pop(&self, wait: Duration) -> Option<Job> {
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        if q.is_empty() {
            q = self.cv.wait_timeout(q, wait).unwrap_or_else(|e| e.into_inner()).0;
        }
        let j = q.pop_front();
        if j.is_some() {
            self.running.fetch_add(1, Ordering::SeqCst);
        }
        j
    }
}

impl Driver {
    /// Queues a job for the job threads.
    pub fn submit_job(&self, job: Job) {
        self.s.jobs.push(job);
    }
}

/// Starts the job threads (once per `Shared`).
pub fn start(s: &Arc<Shared>) {
    if s.jobs.started.swap(true, Ordering::SeqCst) {
        return;
    }
    for i in 0..JOB_THREADS {
        let w = Arc::downgrade(s);
        crate::assertions::spawn_inheriting(&format!("job-{i}"), move || {
            crate::shared::mark_job_thread();
            loop {
                let Some(s) = w.upgrade() else { return };
                let Some(job) = s.jobs.pop(Duration::from_millis(100)) else { continue };
                let (name, blocks) = (job.name, job.blocks_quiet);
                let d = Driver { s: s.clone() };
                let started = std::time::Instant::now();
                Driver::helper_iteration(&format!("job {name}"), move || (job.run)(&d));
                if started.elapsed() > Duration::from_secs(60) {
                    println!("driver: job {name} took {} ms", started.elapsed().as_millis());
                }
                s.jobs.running.fetch_sub(1, Ordering::SeqCst);
                if blocks {
                    s.jobs.blocking.fetch_sub(1, Ordering::SeqCst);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit;

    #[test]
    fn fifo_panic_isolation_and_blocking_accounting() {
        let (d, _, _) = testkit::driver();
        testkit::reset();
        let order = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        // One gated job holds one thread, so the other runs the rest alone,
        // in submission order.
        {
            let gate = gate.clone();
            d.submit_job(Job {
                name: "gated",
                blocks_quiet: true,
                run: Box::new(move |_| {
                    let (m, cv) = &*gate;
                    let mut open = m.lock().unwrap();
                    while !*open {
                        open = cv.wait(open).unwrap();
                    }
                }),
            });
        }
        testkit::wait_until(Duration::from_secs(5), || d.s.jobs.running.load(Ordering::SeqCst) == 1);
        let hold = Arc::new(Mutex::new(()));
        let held = hold.lock().unwrap();
        {
            let hold = hold.clone();
            d.submit_job(Job { name: "hold", blocks_quiet: false, run: Box::new(move |_| drop(hold.lock().unwrap())) });
        }
        d.submit_job(Job { name: "panics", blocks_quiet: true, run: Box::new(|_| panic!("job bug")) });
        for i in 0..5 {
            let order = order.clone();
            d.submit_job(Job { name: "plain", blocks_quiet: i % 2 == 0, run: Box::new(move |_| order.lock().unwrap().push(i)) });
        }
        testkit::wait_until(Duration::from_secs(5), || d.s.jobs.running.load(Ordering::SeqCst) == 2);
        // gated + panics + j0, j2, j4
        assert_eq!(d.s.blocking_jobs(), 5);
        drop(held);
        testkit::wait_until(Duration::from_secs(5), || order.lock().unwrap().len() == 5);
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3, 4], "FIFO");
        assert_eq!(d.s.blocking_jobs(), 1, "only the gated job is left");
        assert!(testkit::fired("a driver helper thread panics"), "the panic was isolated and asserted");
        {
            let (m, cv) = &*gate;
            *m.lock().unwrap() = true;
            cv.notify_all();
        }
        testkit::wait_until(Duration::from_secs(5), || d.s.jobs.len() == 0 && d.s.blocking_jobs() == 0);
    }
}
