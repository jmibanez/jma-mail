//! Shared fixture for end-to-end JMAP tests. Spawns a MySQL
//! sidecar initialised from `tests/fixtures/stalwart/mysql.sql`,
//! then a Stalwart Mail container on the same docker network with
//! `tests/fixtures/stalwart/config.json` pointing at it. Waits for
//! JMAP and IMAP to come up, and returns a `JmapFixture` that tests
//! can point a real `jma_mail::Config` at.
//!
//! We boot from a checked-in fixture because Stalwart v0.16's
//! persisted config is a typed JSON blob with `@type` discriminators
//! rather than the TOML shape its public docs imply, and its admin
//! JMAP surface for credential creation is `x:ApiKey/set` (note the
//! `x:` prefix) -- `AppPassword/set` and the unprefixed variants
//! return `unknownMethod`. Bootstrapping a domain/account/credential
//! set programmatically from an empty config runs into a catch-22:
//! the admin calls only work once the server is in normal operation,
//! which requires a populated config in the first place. Walking the
//! install wizard once and committing the result sidesteps both
//! issues.
//!
//! The fixture pair under `tests/fixtures/stalwart/` was produced by
//! that wizard pass plus three follow-up admin tweaks via the JMAP
//! admin API: an `x:ApiKey/set` to mint a long-lived Bearer for the
//! test client, a plaintext IMAP listener on port 143, and
//! `allowPlainTextAuth=true` on the IMAP service so the seeder can
//! `LOGIN` over TCP without TLS. The DB carries the password hashes
//! for the wizard-generated admin and the API key; the secret value
//! for the latter is encoded in `FIXTURE_BEARER` below. Regenerate
//! the pair by repeating the wizard pass when Stalwart's persisted
//! schema changes between releases.
//!
//! Each `spawn_stalwart` call is a fresh container; testcontainers
//! handles teardown when the fixture drops. Don't share fixtures
//! across tests -- per-test isolation is cheaper than debugging an
//! order-dependent failure on the rare day one shows up.
//!
//! `examples/bench-server.rs` path-includes this module to spawn
//! the same fixture for the bench scripts' testcontainer mode, so
//! refactors of the public surface here (`spawn_stalwart`,
//! `spawn_mysql`, `JmapFixture`, `MysqlFixture`, `seed_inbox`,
//! `SeedMessage`) need to be checked against that consumer too.
//!
//! Seeding goes through IMAP APPEND (`seed_inbox`) rather than
//! `Email/import` so a `jmap-client` breakage can't silently
//! corrupt the seed corpus: a failure at seed time is structurally
//! distinguishable from a failure in the system under test.

#![allow(dead_code)]

use anyhow::{Context, Result, anyhow};
use async_imap::Client as ImapClient;
use reqwest::Client as HttpClient;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{
        IntoContainerPort,
        wait::{LogWaitStrategy, WaitFor},
    },
    runners::AsyncRunner,
};
use tokio::net::{TcpListener, TcpStream};

const STALWART_IMAGE: &str = "stalwartlabs/stalwart";
const STALWART_TAG: &str = "v0.16";

/// Wizard-generated admin email; used as the IMAP login for seeding
/// and as the `[account].email` in the test's `jma_mail::Config`.
const ACCOUNT_EMAIL: &str = "admin@example.org";

/// Wizard-generated admin password; only used for IMAP authentication
/// when seeding the inbox. JMAP authentication for the system under
/// test goes through `FIXTURE_BEARER` instead. Bound to the password
/// hash committed in tests/fixtures/stalwart/mysql.sql (regenerate
/// via tools/regen-stalwart-fixture.sh when changing).
const ACCOUNT_IMAP_PASSWORD: &str = "iYHO4AGScvDuy5v6"; // fixture-only

/// Pre-minted Stalwart API key whose hash is persisted in the
/// fixture DB. Stalwart accepts this as `Authorization: Bearer ...`
/// for any JMAP call against the wizard admin's account, so
/// jma-mail's standard Bearer flow works end-to-end without any
/// auth-mode changes. Bound to the API-key hash committed in
/// tests/fixtures/stalwart/mysql.sql (regenerate via
/// tools/regen-stalwart-fixture.sh when changing).
const FIXTURE_BEARER: &str = "API_AAAAAQAAAAGBjWN9_6ZXfU-BrT4DzzgfLYl9kA"; // fixture-only

/// Path the Stalwart image's default `CMD` reads at boot. Mounting
/// our fixture here is what tells the server to skip the bootstrap
/// wizard and start in normal operation.
const CONTAINER_CONFIG_PATH: &str = "/etc/stalwart/config.json";

/// Path inside the MySQL sidecar at which the dump is mounted.
/// The MySQL official image's entrypoint runs any `.sql` file in
/// this directory against `MYSQL_DATABASE` on first boot.
const MYSQL_INITDB_PATH: &str = "/docker-entrypoint-initdb.d/01-stalwart.sql";

/// Compile-time embed of the wizard-seeded Stalwart schema and
/// rows. Applied to the MySQL sidecar at boot via initdb.
const MYSQL_INIT_SQL: &[u8] = include_bytes!("../fixtures/stalwart/mysql.sql");

pub struct JmapFixture {
    pub session_url: String,
    pub bearer: String,
    pub account_email: String,
    pub account_password: String,
    pub imap_host: String,
    pub imap_port: u16,
    /// Holding the container handle gates teardown on fixture drop.
    /// Declared before `_mysql` so the Stalwart container is removed
    /// first; testcontainers force-kills containers in Drop (no
    /// graceful-shutdown window), so this ordering only matters to
    /// keep Stalwart from being the one logging "MySQL gone" errors
    /// during the teardown race.
    _container: ContainerAsync<GenericImage>,
    _mysql: MysqlFixture,
}

pub struct SeedMessage {
    pub from: String,
    pub subject: String,
    pub body: String,
    pub flags: Vec<&'static str>,
    /// Explicit `Message-ID` header. When `None`, `seed_inbox`
    /// generates one as `<seed-{i}@test.local>`. Set this when a
    /// test needs to force a Message-ID collision (e.g. the remote-
    /// dedupe path requires two Email objects sharing a header).
    pub message_id: Option<String>,
}

impl SeedMessage {
    pub fn simple(from: &str, subject: &str, body: &str) -> Self {
        Self {
            from: from.to_string(),
            subject: subject.to_string(),
            body: body.to_string(),
            flags: Vec::new(),
            message_id: None,
        }
    }

    pub fn with_flags(mut self, flags: &[&'static str]) -> Self {
        self.flags = flags.to_vec();
        self
    }

    pub fn with_message_id(mut self, mid: &str) -> Self {
        self.message_id = Some(mid.to_string());
        self
    }
}

pub async fn spawn_stalwart() -> Result<JmapFixture> {
    // Bring up the MySQL sidecar first, with the wizard-seeded
    // dump applied via initdb. spawn_mysql blocks until the real
    // mysqld has bound TCP 3306 (see the wait condition there), so
    // by the time Stalwart starts the data store is already
    // accepting queries on the shared network.
    let mysql = spawn_mysql(Some(MYSQL_INIT_SQL)).await?;

    // `with_copy_to` takes `Vec<u8>` (via `CopyDataSource::Data`), so
    // each call clones the embedded bytes onto the heap. The
    // alternative `CopyDataSource::File` variant wants a path on
    // disk, not a `&'static [u8]`, so there's no slice-borrowing
    // shortcut in testcontainers 0.27 -- the clone is the API.
    let config_bytes = include_bytes!("../fixtures/stalwart/config.json").to_vec();

    // Stalwart bakes the configured public hostname/port into the
    // discovery document's `apiUrl`, `downloadUrl`, etc., and the
    // JMAP client uses those URLs verbatim for follow-up calls. We
    // need the advertised port to match the host port Docker maps,
    // so we pin both: pick a free host port up front, hand it to
    // Docker via `with_mapped_port`, and pass it to Stalwart via
    // `STALWART_PUBLIC_URL` so the discovery document advertises a
    // URL that's actually reachable from the test process. Same
    // approach for IMAP (free port + mapped port). The free-port
    // probe has a tiny TOCTOU window before Docker binds, but tests
    // run sequentially within a binary by default and we'd prefer
    // a clear bind failure to a hardcoded port collision.
    let http_port = pick_free_port().await?;
    let imap_port = pick_free_port().await?;
    let public_url = format!("http://127.0.0.1:{http_port}");

    let image = GenericImage::new(STALWART_IMAGE, STALWART_TAG)
        .with_mapped_port(http_port, 8080.tcp())
        .with_mapped_port(imap_port, 143.tcp())
        .with_copy_to(CONTAINER_CONFIG_PATH, config_bytes)
        .with_env_var("STALWART_PUBLIC_URL", &public_url)
        .with_network(&mysql.network)
        .with_startup_timeout(Duration::from_secs(60));

    let container = image.start().await.context("start Stalwart container")?;

    // `jmap-client::Client::connect` appends `/.well-known/jmap` to
    // whatever URL it's given, so `session_url` must be a path under
    // which the server serves a JMAP session document at that
    // suffix. Stalwart serves its session doc at `/jmap/session` and
    // honours `/jmap/session/.well-known/jmap` against the same
    // resource, so we point at `/jmap/session` here. The
    // `/.well-known/jmap` path at the server root is a 307 redirect
    // we deliberately don't follow: jmap-client wouldn't traverse it
    // (its default redirect policy refuses unknown hosts) and
    // configuring it to do so isn't worth the surface area.
    let session_url = format!("{public_url}/jmap/session");

    let http = HttpClient::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build HTTP client")?;

    wait_for_jmap(&http, &session_url).await?;
    wait_for_imap(imap_port).await?;

    Ok(JmapFixture {
        session_url,
        bearer: FIXTURE_BEARER.to_string(),
        account_email: ACCOUNT_EMAIL.to_string(),
        account_password: ACCOUNT_IMAP_PASSWORD.to_string(),
        imap_host: "127.0.0.1".to_string(),
        imap_port,
        _container: container,
        _mysql: mysql,
    })
}

/// Ask the kernel for a free TCP port by binding `127.0.0.1:0` and
/// reading back the assigned port. The listener is dropped before
/// the port number is returned, leaving a race window until Docker
/// binds the same port during container startup. A collision
/// manifests as a clear `bind: address already in use` failure on
/// `start().await`, not as a silent hang -- acceptable for a test
/// fixture. Running two `spawn_stalwart` calls in parallel is
/// unsupported; if a future test binary needs that, the cleanest
/// fix is `with_userns_mode` plus a fresh Docker network per
/// fixture rather than reaching for tighter port-race handling
/// here.
async fn pick_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind 127.0.0.1:0 to discover a free port")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

async fn wait_for_jmap(http: &HttpClient, session_url: &str) -> Result<()> {
    // Insist on a session document that parses and advertises an
    // `apiUrl`. A 200-but-empty page, a half-booted config, or a 401
    // from a server still loading auth would all fail this and keep
    // polling; the seeder needs the real session surface, not a
    // TCP+HTTP-only shell. Probe the same URL jmap-client will hit:
    // the configured `session_url` itself (which is the session
    // resource on Stalwart) carrying the fixture bearer.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let probe = async {
            let r = http
                .get(session_url)
                .bearer_auth(FIXTURE_BEARER)
                .send()
                .await
                .ok()?;
            if !r.status().is_success() {
                return None;
            }
            let body: Value = r.json().await.ok()?;
            body.get("apiUrl").and_then(Value::as_str)?;
            Some(())
        }
        .await;
        if probe.is_some() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!("timed out polling {session_url}"));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_imap(port: u16) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(_) => return Ok(()),
            Err(_) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(anyhow!("timed out connecting to IMAP on :{port}"));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

pub async fn seed_inbox(fx: &JmapFixture, msgs: &[SeedMessage]) -> Result<()> {
    let stream = TcpStream::connect((fx.imap_host.as_str(), fx.imap_port))
        .await
        .context("connect to test container IMAP")?;
    let client = ImapClient::new(stream);
    let mut session = client
        .login(&fx.account_email, &fx.account_password)
        .await
        .map_err(|(e, _)| anyhow!("IMAP login as {}: {e}", fx.account_email))?;

    for (i, msg) in msgs.iter().enumerate() {
        let msgid = msg
            .message_id
            .clone()
            .unwrap_or_else(|| format!("<seed-{i}@test.local>"));
        let eml = render_eml(msg, &msgid);
        let flag_clause = if msg.flags.is_empty() {
            None
        } else {
            Some(format!("({})", msg.flags.join(" ")))
        };
        session
            .append("INBOX", flag_clause.as_deref(), None, eml.as_bytes())
            .await
            .with_context(|| format!("APPEND seed message {i}"))?;
    }

    // logout is best-effort -- the connection drops cleanly either
    // way when `session` goes out of scope, and a failed logout
    // doesn't invalidate the appended messages.
    let _ = session.logout().await;
    Ok(())
}

const MYSQL_IMAGE: &str = "mysql";
const MYSQL_TAG: &str = "8.4";
const MYSQL_INTERNAL_PORT: u16 = 3306;

/// In-network DNS name MySQL is published as via `with_hostname`.
/// The committed Stalwart config.json references this as `host`,
/// so it must match: changing one without the other breaks the
/// e2e boot.
const MYSQL_NETWORK_HOSTNAME: &str = "mysql";

/// Fixture-only credentials. Must match the values committed in
/// tests/fixtures/stalwart/config.json (which the wizard wrote from
/// the same values via tools/regen-stalwart-fixture.sh).
const MYSQL_ROOT_PASSWORD: &str = "stalwart-fixture-root";
const MYSQL_DATABASE: &str = "stalwart";
const MYSQL_USER: &str = "stalwart";
const MYSQL_PASSWORD: &str = "stalwart-fixture-user";

pub struct MysqlFixture {
    /// Docker network name. Pass into another container's
    /// `.with_network(...)` to put it on the same network as the
    /// MySQL sidecar so it can reach MySQL by hostname.
    pub network: String,
    /// Host address (typically 127.0.0.1) for host-side access.
    pub host: String,
    /// Host-mapped port for host-side access (e.g. the standalone
    /// MySQL smoke test in tests/mysql_fixture.rs).
    pub host_port: u16,
    /// Holding the container handle gates teardown on fixture drop.
    _container: ContainerAsync<GenericImage>,
}

pub async fn spawn_mysql(init_sql: Option<&[u8]>) -> Result<MysqlFixture> {
    // Per-spawn suffix combining pid and a monotonic counter.
    // cargo runs tests in a binary in parallel by default, so a
    // per-process suffix isn't enough -- two concurrent
    // `spawn_mysql` calls in the same binary would collide on the
    // network name. The pid disambiguates across test binaries,
    // the counter disambiguates within one. The in-network
    // hostname (MYSQL_NETWORK_HOSTNAME) is constant across spawns
    // because each spawn gets its own network; collisions are
    // network-scoped in docker DNS, so the same hostname on
    // different networks is unambiguous.
    static SPAWN_COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let seq = SPAWN_COUNTER.fetch_add(1, Ordering::Relaxed);
    let network = format!("jma-mysql-net-{pid}-{seq}");

    // The MySQL official image's init phase runs a temporary
    // mysqld on a UNIX socket only (logged as `port: 0`), runs the
    // init scripts, stops it, then starts the real mysqld bound to
    // TCP 3306. The substring `ready for connections` matches the
    // X-Plugin and main-mysqld lines in both phases (four hits
    // total). Matching `port: 3306` instead pinpoints the one log
    // line that means "TCP 3306 is now accepting connections" --
    // the bare `ready for connections` + with_times(2) variant
    // fires on the init-phase mysqld's socket-only ready line,
    // which makes peers on the docker network race with the
    // init->real-server restart and get TCP connection refused.
    //
    // The InnoDB CMD args relax durability and right-size memory
    // for the throwaway test workload. With defaults, large-write
    // benches against this fixture stall on `Threads are unable
    // to reserve space in redo log` warnings: the 100 MB default
    // `redo_log_capacity` fills faster than the checkpointer
    // drains. Raising it gives the checkpointer more headroom;
    // `innodb-flush-log-at-trx-commit=0` removes the
    // per-transaction redo fsync from the critical path; turning
    // off the double-write buffer halves InnoDB's write volume;
    // `innodb-buffer-pool-size=512M` (up from the 128 MB default)
    // keeps more dirty pages in memory for the checkpointer to
    // coalesce; `innodb-io-capacity{,-max}` raise the IOPS budget
    // InnoDB assumes the storage can sustain (defaults 200/2000),
    // which feeds the adaptive-flushing rate; `skip-log-bin`
    // removes the binary log write stream. Safe because the
    // container's state is discarded at the end of every test
    // run.
    let mut image = GenericImage::new(MYSQL_IMAGE, MYSQL_TAG)
        .with_wait_for(WaitFor::log(
            LogWaitStrategy::stderr("port: 3306").with_times(1),
        ))
        .with_cmd([
            "mysqld",
            "--innodb-redo-log-capacity=2G",
            "--innodb-flush-log-at-trx-commit=0",
            "--innodb-doublewrite=OFF",
            "--innodb-buffer-pool-size=512M",
            "--innodb-io-capacity=2000",
            "--innodb-io-capacity-max=4000",
            "--skip-log-bin",
        ])
        .with_env_var("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .with_env_var("MYSQL_DATABASE", MYSQL_DATABASE)
        .with_env_var("MYSQL_USER", MYSQL_USER)
        .with_env_var("MYSQL_PASSWORD", MYSQL_PASSWORD)
        .with_network(&network)
        .with_hostname(MYSQL_NETWORK_HOSTNAME)
        .with_startup_timeout(Duration::from_secs(120));

    if let Some(sql) = init_sql {
        image = image.with_copy_to(MYSQL_INITDB_PATH, sql.to_vec());
    }

    let container = image.start().await.context("start MySQL container")?;

    let host = container
        .get_host()
        .await
        .context("read MySQL container host")?
        .to_string();
    let host_port = container
        .get_host_port_ipv4(MYSQL_INTERNAL_PORT.tcp())
        .await
        .context("read MySQL host-mapped port")?;

    Ok(MysqlFixture {
        network,
        host,
        host_port,
        _container: container,
    })
}

fn render_eml(msg: &SeedMessage, msgid: &str) -> String {
    // Stalwart's IMAP APPEND wants CRLF line endings; the maildir
    // store on the jma side normalizes to LF on download so the
    // on-disk encoding stays native. Don't shortcut to LF here.
    format!(
        "From: {}\r\n\
         To: admin@example.org\r\n\
         Subject: {}\r\n\
         Message-ID: {}\r\n\
         Date: Thu, 01 Jan 2026 00:00:00 +0000\r\n\
         MIME-Version: 1.0\r\n\
         Content-Type: text/plain; charset=us-ascii\r\n\
         \r\n\
         {}\r\n",
        msg.from, msg.subject, msgid, msg.body
    )
}
