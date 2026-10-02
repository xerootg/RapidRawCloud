//! Garage test harness for the app-wiring end-to-end test: boots one real
//! Garage v2.2.0 server per test binary and configures it (layout, access
//! key, buckets) through the `garage` CLI.
//!
//! This mirrors `crates/rrcloud-core/tests/common/garage.rs` (which proved
//! the exact CLI contract against the real binary); it is duplicated here
//! rather than shared because that module lives under the core crate's
//! `tests/` tree, which is not reachable from the app's integration tests.
//! It reaches the S3 client through the `rapidraw_lib::rrcloud_core`
//! re-export, so no extra dev-dependency is needed. Set
//! `GARAGE_BIN=/tmp/claude-0/garage` to run it.

use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use rapidraw_lib::rrcloud_core::s3::{S3Client, S3Config};

const DEFAULT_GARAGE_BIN_CANDIDATES: &[&str] = &["/tmp/claude-0/garage"];
const KEY_NAME: &str = "rrcloud-test-key";
const BOOT_TIMEOUT: Duration = Duration::from_secs(30);
const CLI_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const CLI_CALL_TIMEOUT: Duration = Duration::from_secs(20);

/// A running, configured Garage server.
pub struct Garage {
    bin: PathBuf,
    config_path: PathBuf,
    pub s3_port: u16,
    pub access_key_id: String,
    pub secret_access_key: String,
    child: Mutex<Option<Child>>,
    bucket_seq: AtomicU32,
}

static INSTANCE: OnceLock<Option<Garage>> = OnceLock::new();
static CHILD_PID: AtomicI32 = AtomicI32::new(0);

/// Returns the shared Garage instance, booting it on first call. Returns
/// `None` (after a loud eprintln) when the binary is absent and `CI` is
/// unset, so a local run without the binary skips gracefully.
pub fn shared() -> Option<&'static Garage> {
    INSTANCE
        .get_or_init(|| {
            let bin = match std::env::var_os("GARAGE_BIN").map(PathBuf::from) {
                Some(bin) => (bin.is_file()).then_some(bin),
                None => DEFAULT_GARAGE_BIN_CANDIDATES
                    .iter()
                    .map(PathBuf::from)
                    .find(|p| p.is_file()),
            };
            let Some(bin) = bin else {
                if std::env::var_os("CI").is_some() {
                    panic!("CI is set but no Garage binary was found (GARAGE_BIN / defaults)");
                }
                eprintln!(
                    "SKIP: no Garage binary found (checked GARAGE_BIN and \
                     {DEFAULT_GARAGE_BIN_CANDIDATES:?}); the Garage-backed e2e test \
                     will pass without asserting anything."
                );
                return None;
            };
            let mut last_err = String::new();
            for attempt in 1..=3 {
                match Garage::start(bin.clone()) {
                    Ok(garage) => return Some(garage),
                    Err(e) => {
                        eprintln!("Garage boot attempt {attempt}/3 failed: {e}");
                        last_err = e;
                    }
                }
            }
            panic!("failed to boot the Garage test server after 3 attempts: {last_err}")
        })
        .as_ref()
}

impl Garage {
    fn start(bin: PathBuf) -> Result<Garage, String> {
        let dir = std::env::temp_dir().join(format!(
            "rrcloud-app-garage-{}-{}",
            std::process::id(),
            nanos_now()
        ));
        fs::create_dir_all(dir.join("meta")).map_err(|e| e.to_string())?;
        fs::create_dir_all(dir.join("data")).map_err(|e| e.to_string())?;

        let (rpc_port, s3_port, admin_port) = {
            let l1 = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
            let l2 = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
            let l3 = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
            (
                l1.local_addr().map_err(|e| e.to_string())?.port(),
                l2.local_addr().map_err(|e| e.to_string())?.port(),
                l3.local_addr().map_err(|e| e.to_string())?.port(),
            )
        };

        let rpc_secret = random_hex_64();
        let config_path = dir.join("garage.toml");
        let config = format!(
            r#"metadata_dir = "{meta}"
data_dir = "{data}"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "{rpc_secret}"

[s3_api]
api_bind_addr = "127.0.0.1:{s3_port}"
s3_region = "garage"
root_domain = ".s3.garage.localhost"

[admin]
api_bind_addr = "127.0.0.1:{admin_port}"
admin_token = "{admin_token}"
"#,
            meta = dir.join("meta").display(),
            data = dir.join("data").display(),
            admin_token = random_hex_64(),
        );
        fs::write(&config_path, config).map_err(|e| e.to_string())?;

        let log = fs::File::create(dir.join("server.log")).map_err(|e| e.to_string())?;
        let mut cmd = Command::new(&bin);
        cmd.arg("-c")
            .arg(&config_path)
            .arg("server")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?))
            .stderr(Stdio::from(log));
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                cmd.pre_exec(|| {
                    libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                    Ok(())
                });
            }
        }
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("garage-keeper".to_string())
            .spawn(move || {
                tx.send(cmd.spawn()).ok();
                loop {
                    std::thread::park();
                }
            })
            .map_err(|e| format!("spawning keeper thread: {e}"))?;
        let child = rx
            .recv()
            .map_err(|e| format!("keeper thread died: {e}"))?
            .map_err(|e| format!("spawning {}: {e}", bin.display()))?;
        CHILD_PID.store(child.id() as i32, Ordering::SeqCst);
        unsafe {
            libc::atexit(kill_child_at_exit);
        }

        let garage = Garage {
            bin,
            config_path,
            s3_port,
            access_key_id: String::new(),
            secret_access_key: String::new(),
            child: Mutex::new(Some(child)),
            bucket_seq: AtomicU32::new(0),
        };

        garage.wait_for_s3_port()?;
        garage.configure_cluster()
    }

    fn wait_for_s3_port(&self) -> Result<(), String> {
        let deadline = Instant::now() + BOOT_TIMEOUT;
        loop {
            if TcpStream::connect_timeout(
                &([127, 0, 0, 1], self.s3_port).into(),
                Duration::from_millis(250),
            )
            .is_ok()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "Garage did not open its S3 port within {BOOT_TIMEOUT:?}; see server.log next to {}",
                    self.config_path.display()
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn configure_cluster(mut self) -> Result<Garage, String> {
        let status = self.cli_retry_until(&["status"], |out| {
            parse_node_id(out).is_some().then(|| out.to_string())
        })?;
        let node_id = parse_node_id(&status)
            .ok_or_else(|| format!("no node id in status output:\n{status}"))?;

        self.cli_retry(&["layout", "assign", "-z", "dc1", "-c", "1G", &node_id])?;
        self.cli_retry(&["layout", "apply", "--version", "1"])?;

        let key_out = self.cli_retry(&["key", "create", KEY_NAME])?;
        let (key_id, secret) = parse_key_create(&key_out)
            .ok_or_else(|| format!("could not parse key id/secret from:\n{key_out}"))?;
        self.access_key_id = key_id;
        self.secret_access_key = secret;
        Ok(self)
    }

    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.s3_port)
    }

    pub fn region(&self) -> &'static str {
        "garage"
    }

    pub fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint: self.endpoint(),
            region: self.region().to_string(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
            connect_timeout: Some(Duration::from_secs(10)),
            read_timeout: Some(Duration::from_secs(30)),
            request_timeout: None,
        }
    }

    pub fn client(&self) -> S3Client {
        S3Client::new(self.s3_config()).expect("S3Client construction failed")
    }

    pub fn create_bucket(&self, name: &str) {
        self.cli_retry(&["bucket", "create", name])
            .unwrap_or_else(|e| panic!("bucket create {name}: {e}"));
        self.cli_retry(&[
            "bucket", "allow", "--read", "--write", "--owner", name, "--key", KEY_NAME,
        ])
        .unwrap_or_else(|e| panic!("bucket allow {name}: {e}"));
    }

    pub fn create_unique_bucket(&self, tag: &str) -> String {
        let n = self.bucket_seq.fetch_add(1, Ordering::Relaxed);
        let tag: String = tag
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect();
        let name = format!("t{}-{}-{}", std::process::id(), n, tag);
        let name = name.chars().take(60).collect::<String>();
        self.create_bucket(&name);
        name
    }

    pub fn cli(&self, args: &[&str]) -> Result<String, String> {
        use std::io::Read as _;

        let mut child = Command::new(&self.bin)
            .arg("-c")
            .arg(&self.config_path)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawning garage {args:?}: {e}"))?;

        let mut stdout_pipe = child.stdout.take().expect("stdout piped");
        let mut stderr_pipe = child.stderr.take().expect("stderr piped");
        let out_thread = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut buf);
            buf
        });
        let err_thread = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf);
            buf
        });

        let deadline = Instant::now() + CLI_CALL_TIMEOUT;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(format!(
                            "garage {args:?} did not exit within {CLI_CALL_TIMEOUT:?}; killed"
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(format!("waiting for garage {args:?}: {e}")),
            }
        };
        let stdout = String::from_utf8_lossy(&out_thread.join().unwrap_or_default()).into_owned();
        let stderr = String::from_utf8_lossy(&err_thread.join().unwrap_or_default()).into_owned();
        if status.success() {
            Ok(stdout)
        } else {
            Err(format!(
                "garage {args:?} failed ({status}):\nstdout:\n{stdout}\nstderr:\n{stderr}"
            ))
        }
    }

    fn cli_retry(&self, args: &[&str]) -> Result<String, String> {
        self.cli_retry_until(args, |out| Some(out.to_string()))
    }

    fn cli_retry_until<T>(
        &self,
        args: &[&str],
        accept: impl Fn(&str) -> Option<T>,
    ) -> Result<T, String> {
        let deadline = Instant::now() + CLI_RETRY_TIMEOUT;
        let mut last_err: String;
        loop {
            match self.cli(args) {
                Ok(out) => {
                    if let Some(v) = accept(&out) {
                        return Ok(v);
                    }
                    last_err = format!("output not accepted:\n{out}");
                }
                Err(e) => last_err = e,
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "garage {args:?} did not succeed within {CLI_RETRY_TIMEOUT:?}; last: {last_err}"
                ));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

impl Drop for Garage {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.child.lock() {
            if let Some(mut child) = guard.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

extern "C" fn kill_child_at_exit() {
    let pid = CHILD_PID.load(Ordering::SeqCst);
    if pid > 0 {
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}

fn parse_node_id(status: &str) -> Option<String> {
    status
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .find(|tok| tok.len() == 16 && tok.chars().all(|c| c.is_ascii_hexdigit()))
        .map(str::to_string)
}

fn parse_key_create(out: &str) -> Option<(String, String)> {
    let field = |label: &str| {
        out.lines()
            .find_map(|l| l.strip_prefix(label))
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    Some((field("Key ID:")?, field("Secret key:")?))
}

fn random_hex_64() -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(std::process::id().to_le_bytes());
    h.update(nanos_now().to_le_bytes());
    let addr = &h as *const _ as usize;
    h.update(addr.to_le_bytes());
    hex::encode(h.finalize())
}

fn nanos_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
