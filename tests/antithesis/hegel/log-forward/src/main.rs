//! Copies stdin to stdout line by line and sends each line to LOG_FORWARD_ADDR
//! over UDP as {"node","boot","seq","line"}. Never blocks or exits on network
//! errors: lines it cannot send are dropped.
use std::io::{BufRead, Write};
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn main() {
    let node = std::env::var("HOSTNAME").unwrap_or_default();
    let boot = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let addr = std::env::var("LOG_FORWARD_ADDR").ok();

    // A sender thread owns the socket, so stdout copying never waits on DNS.
    let (tx, rx) = mpsc::sync_channel::<String>(4096);
    if let Some(addr) = addr {
        std::thread::spawn(move || {
            let sock = match UdpSocket::bind("0.0.0.0:0") {
                Ok(s) => s,
                Err(_) => return,
            };
            let mut target = None;
            let mut last_try = Instant::now() - Duration::from_secs(2);
            for msg in rx {
                if target.is_none() && last_try.elapsed() >= Duration::from_secs(1) {
                    last_try = Instant::now();
                    target = addr.to_socket_addrs().ok().and_then(|mut a| a.next());
                }
                if let Some(t) = target {
                    let _ = sock.send_to(msg.as_bytes(), t);
                }
            }
        });
    }

    let node = escape(&node);
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut seq: u64 = 0;
    let mut buf = Vec::new();
    let mut lock = stdin.lock();
    loop {
        buf.clear();
        match lock.read_until(b'\n', &mut buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let _ = stdout.write_all(&buf);
        let _ = stdout.flush();
        let line = String::from_utf8_lossy(&buf);
        let line = line.trim_end_matches(['\n', '\r']);
        let mut text = line.to_string();
        if text.len() > 8000 {
            let mut cut = 8000;
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            text.truncate(cut);
        }
        let msg = format!(
            "{{\"node\":\"{}\",\"boot\":{},\"seq\":{},\"line\":\"{}\"}}",
            node, boot, seq, escape(&text)
        );
        seq += 1;
        let _ = tx.try_send(msg);
    }
}
