//! Native Stalwart v0.16 fixture. Run only via the isolated Linux runner.
//! Fresh typed-JSON RocksDB bootstrap; no Docker, MySQL dump or user data.

use super::{JmapFixture, pick_free_port};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, RequestBuilder};
use serde_json::{Value, json};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CORE: &str = "urn:ietf:params:jmap:core";
const ADMIN: &str = "urn:stalwart:jmap";

pub(super) struct NativeProcess {
    child: Option<Child>,
    root: tempfile::TempDir,
    executable: OsString,
    recovery_secret: String,
}

impl NativeProcess {
    fn config(&self) -> PathBuf {
        self.root.path().join("config.json")
    }

    fn start(&mut self, url: &str, recovery_port: Option<u16>) -> Result<()> {
        ensure!(self.child.is_none(), "native server already running");
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join("server.log"))?;
        let mut command = Command::new(&self.executable);
        // CONFIG_PATH takes precedence over --config in Stalwart. Never inherit
        // a user's config, recovery mode, hostname or external store settings.
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("STALWART_") || key == "CONFIG_PATH" {
                command.env_remove(key);
            }
        }
        command
            .arg("--config")
            .arg(self.config())
            .current_dir(self.root.path())
            .env("CONFIG_PATH", self.config())
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("XDG_DATA_HOME", self.root.path().join("data"))
            .env("STALWART_HOSTNAME", "mail.example.org")
            .env("STALWART_PUBLIC_URL", url)
            .env(
                "STALWART_RECOVERY_ADMIN",
                format!("admin:{}", self.recovery_secret),
            )
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        if let Some(port) = recovery_port {
            command
                .env("STALWART_RECOVERY_MODE", "true")
                .env("STALWART_RECOVERY_MODE_PORT", port.to_string());
        }
        self.child = Some(
            command
                .spawn()
                .context("start disposable native Stalwart")?,
        );
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        if child.try_wait()?.is_none() {
            #[cfg(unix)]
            // SAFETY: this is our live owned child; it has not been reaped, so
            // its PID cannot be reused. Graceful stop flushes the test store.
            unsafe {
                libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
            }
            #[cfg(not(unix))]
            child.kill()?;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if child.try_wait()?.is_some() {
                    break;
                }
                if Instant::now() >= deadline {
                    child.kill()?;
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        child.wait()?;
        Ok(())
    }

    fn log_tail(&self) -> String {
        let text = fs::read_to_string(self.root.path().join("server.log")).unwrap_or_default();
        text.chars()
            .rev()
            .take(6000)
            .collect::<String>()
            .chars()
            .rev()
            .collect()
    }
}

impl Drop for NativeProcess {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

enum Auth {
    Basic(String, String),
    Bearer(String),
}
impl Auth {
    fn apply(&self, request: RequestBuilder) -> RequestBuilder {
        match self {
            Self::Basic(user, password) => request.basic_auth(user, Some(password)),
            Self::Bearer(token) => request.bearer_auth(token),
        }
    }
}

async fn session(
    process: &mut NativeProcess,
    http: &Client,
    url: &str,
    auth: &Auth,
) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = process
            .child
            .as_mut()
            .context("native server not started")?
            .try_wait()?
        {
            bail!("native Stalwart exited {status}: {}", process.log_tail());
        }
        if let Ok(response) = auth
            .apply(http.get(format!("{url}/jmap/session")))
            .send()
            .await
            && response.status().is_success()
            && let Ok(value) = response.json::<Value>().await
            && value["apiUrl"].is_string()
        {
            return Ok(value);
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "native JMAP readiness timed out: {}",
            process.log_tail()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn rpc(http: &Client, url: &str, auth: &Auth, method: &str, args: Value) -> Result<Value> {
    let response: Value = auth
        .apply(http.post(format!("{url}/jmap/")))
        .json(&json!({"using":[CORE,ADMIN],"methodCalls":[[method,args,"native"]]}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let result = response["methodResponses"]
        .as_array()
        .and_then(|calls| calls.first())
        .context("native management response missing")?;
    ensure!(result[0] == method, "native {method} rejected: {response}");
    Ok(result[1].clone())
}

fn acknowledged_singleton(response: &Value) -> Result<&Value> {
    response["updated"]
        .as_object()
        .and_then(|updated| updated.get("singleton"))
        .with_context(|| format!("native singleton update not acknowledged: {response}"))
}

#[cfg(target_os = "linux")]
fn verify_isolation() -> Result<()> {
    let map = fs::read_to_string("/proc/self/uid_map")?;
    let mappings: Vec<_> = map.split_whitespace().collect();
    ensure!(
        mappings.len() == 3 && mappings[0] == "0" && mappings[2] == "1",
        "native fixture requires a disposable user namespace; use tools/test-stalwart-native.sh"
    );
    let interfaces = fs::read_to_string("/proc/self/net/dev")?;
    ensure!(
        interfaces.lines().skip(2).all(|line| line
            .split_once(':')
            .is_some_and(|(name, _)| name.trim() == "lo")),
        "native fixture requires loopback-only isolated networking"
    );
    let mounts = fs::read_to_string("/proc/self/mountinfo")?;
    ensure!(
        mounts.lines().any(
            |line| line.split_once(" - ").is_some_and(|(before, after)| before
                .split_whitespace()
                .nth(4)
                == Some("/var")
                && after.starts_with("tmpfs "))
        ),
        "native fixture requires a private tmpfs /var; bootstrap defaults must not write host logs/data"
    );
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn verify_isolation() -> Result<()> {
    bail!("native fixture requires the isolated Linux runner; use Docker on this platform")
}

pub(super) async fn spawn(executable: OsString) -> Result<JmapFixture> {
    verify_isolation()?;
    // Even --version must not inherit CONFIG_PATH, which otherwise causes a
    // normal server start instead of parsing CLI flags.
    let output = Command::new(&executable)
        .env_remove("CONFIG_PATH")
        .arg("--version")
        .output()?;
    let version = String::from_utf8(output.stdout)?;
    ensure!(
        output.status.success() && version.trim().starts_with("0.16."),
        "native fixture needs Stalwart v0.16, got {version:?}"
    );
    let mut ports = Vec::new();
    for _ in 0..100 {
        let port = pick_free_port().await?;
        if port > 1024 && !ports.contains(&port) {
            ports.push(port);
        }
        if ports.len() == 3 {
            break;
        }
    }
    ensure!(
        ports.len() == 3,
        "cannot reserve distinct native fixture ports"
    );
    let (bootstrap_port, http_port, imap_port) = (ports[0], ports[1], ports[2]);
    let mut process = NativeProcess {
        child: None,
        root: tempfile::tempdir()?,
        executable,
        recovery_secret: uuid::Uuid::new_v4().to_string(),
    };
    let http = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()?;
    let bootstrap_url = format!("http://127.0.0.1:{bootstrap_port}");
    let temporary = Auth::Basic("admin".into(), process.recovery_secret.clone());
    process.start(&bootstrap_url, Some(bootstrap_port))?;
    let initial = session(&mut process, &http, &bootstrap_url, &temporary).await?;
    let result = rpc(
        &http,
        &bootstrap_url,
        &temporary,
        "x:Bootstrap/set",
        json!({"update":{"singleton":{
            "serverHostname":"mail.example.org", "defaultDomain":"example.org",
            "requestTlsCertificate":false,"generateDkimKeys":false,
            "dataStore":{"@type":"RocksDb","path":process.root.path().join("rocks")}
        }}}),
    )
    .await?;
    let credentials = acknowledged_singleton(&result)?;
    let username = credentials["username"]
        .as_str()
        .context("bootstrap did not create a username")?
        .to_owned();
    let password = credentials["secret"]
        .as_str()
        .context("bootstrap did not return a password")?
        .to_owned();
    let persisted: Value = serde_json::from_slice(&fs::read(process.config())?)?;
    ensure!(
        persisted["@type"] == "RocksDb"
            && persisted["path"] == json!(process.root.path().join("rocks")),
        "native bootstrap did not persist the requested disposable backend/path"
    );
    process.stop()?;

    // Normal startup briefly uses Stalwart's default listeners, all confined
    // to the private namespace. Replace every listener before the test phase.
    let setup_url = "http://127.0.0.1:8080";
    let admin = Auth::Basic(username.clone(), password.clone());
    process.start(setup_url, None)?;
    let normal = session(&mut process, &http, setup_url, &admin).await?;
    ensure!(
        normal["primaryAccounts"]["urn:ietf:params:jmap:mail"]
            != initial["primaryAccounts"]["urn:ietf:params:jmap:mail"],
        "bootstrap temporary admin was not replaced"
    );
    let minted = rpc(&http, setup_url, &admin, "x:ApiKey/set", json!({"create":{"key":{"description":"jma-native-fixture","permissions":{"@type":"Inherit"}}}})).await?;
    let bearer = minted["created"]["key"]["secret"]
        .as_str()
        .context("native API key creation not acknowledged")?
        .to_owned();
    let listeners = rpc(&http, setup_url, &admin, "x:NetworkListener/get", json!({})).await?;
    let old: Vec<_> = listeners["list"]
        .as_array()
        .context("native listener catalog missing")?
        .iter()
        .map(|listener| listener["id"].clone())
        .collect();
    let changed = rpc(&http, setup_url, &admin, "x:NetworkListener/set", json!({
        "destroy":old, "create":{
            "http":{"name":"jma-http","protocol":"http","bind":{format!("127.0.0.1:{http_port}"):true},"useTls":false,"tlsImplicit":false},
            "imap":{"name":"jma-imap","protocol":"imap","bind":{format!("127.0.0.1:{imap_port}"):true},"useTls":false,"tlsImplicit":false}
        }})).await?;
    ensure!(
        changed["created"]["http"]["id"].is_string()
            && changed["created"]["imap"]["id"].is_string(),
        "native listener creation not acknowledged: {changed}"
    );
    let destroyed = changed["destroyed"]
        .as_array()
        .context("native listener destruction not acknowledged")?;
    ensure!(
        destroyed.len() == old.len() && old.iter().all(|id| destroyed.contains(id)),
        "native default listeners were not removed"
    );
    acknowledged_singleton(
        &rpc(
            &http,
            setup_url,
            &admin,
            "x:Imap/set",
            json!({"update":{"singleton":{"allowPlainTextAuth":true}}}),
        )
        .await?,
    )?;
    // Match the Docker fixture's search configuration before any APPEND.
    // Bootstrap defaults leave headers unindexed: inMailbox pulls and flag
    // tests work, but rebindfolders' header queries then find no messages.
    let search = rpc(
        &http,
        setup_url,
        &admin,
        "x:Search/set",
        json!({"create":{"singleton":{"indexEmailFields":{
            "from":true,"to":true,"cc":true,"bcc":true,
            "subject":true,"body":true,"attachment":true,
            "receivedAt":true,"sentAt":true,"size":true,
            "hasAttachment":true,"headers":true
        }}}}),
    )
    .await?;
    let search_id = search["created"]["singleton"]["id"]
        .as_str()
        .with_context(|| {
            format!("native search configuration creation not acknowledged: {search}")
        })?
        .to_owned();
    process.stop()?;

    let public_url = format!("http://127.0.0.1:{http_port}");
    process.start(&public_url, None)?;
    let verified = session(
        &mut process,
        &http,
        &public_url,
        &Auth::Bearer(bearer.clone()),
    )
    .await?;
    ensure!(
        verified["primaryAccounts"] == normal["primaryAccounts"],
        "native Bearer authenticates a different account"
    );
    ensure!(
        verified["apiUrl"]
            .as_str()
            .is_some_and(|url| url.starts_with(&public_url)),
        "native API URL is not the final loopback listener"
    );
    let search = rpc(
        &http,
        &public_url,
        &Auth::Bearer(bearer.clone()),
        "x:Search/get",
        json!({"ids":[&search_id]}),
    )
    .await?;
    ensure!(
        search["list"].as_array().is_some_and(|configs| {
            configs.len() == 1
                && configs[0]["id"].as_str() == Some(search_id.as_str())
                && configs[0]["indexEmailFields"]["headers"] == true
        }),
        "native header indexing readback not verified after restart: {search}"
    );
    super::wait_for_imap(imap_port).await?;
    Ok(JmapFixture {
        session_url: format!("{public_url}/jmap/session"),
        bearer,
        account_email: username,
        account_password: password,
        imap_host: "127.0.0.1".into(),
        imap_port,
        _container: None,
        _mysql: None,
        _native: Some(process),
    })
}
