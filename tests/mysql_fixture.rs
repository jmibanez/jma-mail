//! Smoke test for the MySQL sidecar fixture. Spawns MySQL via
//! `common::spawn_mysql`, asserts the host-mapped port accepts TCP
//! connections, and tears down on drop. No SQL connectivity check
//! here: the testcontainers wait for the second "ready for
//! connections" log line is the readiness contract, and pulling in
//! a MySQL client crate purely for a connection probe is overkill.
//!
//! Marked `#[ignore]` so `cargo test` on a Docker-less machine still
//! passes; the dedicated E2E workflow runs `--ignored`.

mod common;

use tokio::net::TcpStream;

#[tokio::test]
#[ignore = "requires Docker; run via cargo test -- --ignored"]
async fn mysql_fixture_boots() {
    let fx = common::spawn_mysql(None)
        .await
        .expect("spawn MySQL fixture");
    TcpStream::connect((fx.host.as_str(), fx.host_port))
        .await
        .expect("connect to host-mapped MySQL port");
}
