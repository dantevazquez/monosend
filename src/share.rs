//! Simple terminal workflow for outgoing LocalSend shares.

use crate::events::{AppEvent, IncomingTransferRequest};
use crate::localsend::client::LocalSendClient;
use crate::localsend::discovery::{DiscoveryEngine, get_local_v4_ips};
use crate::localsend::protocol::Peer;
use crate::localsend::tls::generate_self_signed_cert;
use crate::receive::device_alias;
use crate::utils::format_size;
use color_eyre::{
    Result,
    eyre::{WrapErr, eyre},
};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

const ADDITIONAL_DEVICE_WAIT: Duration = Duration::from_secs(1);

pub async fn run(paths: Vec<PathBuf>, include_clipboard: bool) -> Result<()> {
    let mut paths = validate_files(paths)?;
    let clipboard_file = if include_clipboard {
        let file = ClipboardFile::create()?;
        paths.push(file.path.clone());
        Some(file)
    } else {
        None
    };

    let result = run_cli(paths).await;
    drop(clipboard_file);
    result
}

async fn run_cli(paths: Vec<PathBuf>) -> Result<()> {
    let alias = device_alias();
    let tls = generate_self_signed_cert(&alias)
        .map_err(|error| eyre!("could not create LocalSend identity: {error}"))?;
    let fingerprint = tls.fingerprint;
    let service_port = available_port()?;
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let discovery = Arc::new(DiscoveryEngine::new(
        alias.clone(),
        fingerprint.clone(),
        service_port,
        event_tx.clone(),
        tls.client_identity.clone(),
    )?);
    let client = LocalSendClient::new(
        alias.clone(),
        fingerprint.clone(),
        service_port,
        tls.client_identity,
    )?;

    // A small LocalSend server is needed while sharing because devices answer
    // an active discovery scan through the HTTPS registration endpoint.
    let server_events = event_tx.clone();
    let server_alias = alias.clone();
    let server_fingerprint = fingerprint.clone();
    let server_config = tls.server_config;
    let server_dir = Arc::new(Mutex::new(std::env::current_dir()?));
    tokio::spawn(async move {
        if let Err(error) = crate::localsend::server::start_server(
            server_alias,
            server_fingerprint,
            service_port,
            server_config,
            server_dir,
            server_events.clone(),
        )
        .await
        {
            let _ = server_events.send(AppEvent::StatusMessage(format!(
                "LocalSend registration service stopped: {error}"
            )));
        }
    });

    let discovery_task = discovery.clone();
    let discovery_events = event_tx.clone();
    tokio::spawn(async move {
        if let Err(error) = discovery_task.start().await {
            let _ = discovery_events.send(AppEvent::StatusMessage(format!(
                "Device discovery stopped: {error}"
            )));
        }
    });

    let total_size = paths
        .iter()
        .filter_map(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum();
    println!(
        "Sharing {} file(s) ({})",
        paths.len(),
        format_size(total_size)
    );
    println!("Searching for nearby LocalSend devices...");

    // Give the server and UDP listener a chance to bind before announcing.
    tokio::task::yield_now().await;
    trigger_scan(discovery).await;

    let peers = discover_peers(&mut event_rx, &alias, &fingerprint).await?;
    if peers.is_empty() {
        return Err(eyre!(
            "no nearby LocalSend devices found; make sure the receiving device is visible and try again"
        ));
    }

    let peer = prompt_for_peer(&peers)?;
    println!("Waiting for {} to accept the transfer...", peer.alias);

    let monitor = monitor_transfer(&mut event_rx);
    let (_, result) = tokio::join!(client.send_files(peer, paths, event_tx), monitor);
    result
}

async fn discover_peers(
    event_rx: &mut mpsc::UnboundedReceiver<AppEvent>,
    alias: &str,
    fingerprint: &str,
) -> Result<Vec<Peer>> {
    let mut peers = Vec::new();

    loop {
        let event = if peers.is_empty() {
            event_rx.recv().await
        } else {
            match timeout(ADDITIONAL_DEVICE_WAIT, event_rx.recv()).await {
                Ok(event) => event,
                Err(_) => break,
            }
        };

        match event {
            Some(AppEvent::PeerDiscovered(peer)) => {
                add_peer(&mut peers, peer, alias, fingerprint);
            }
            Some(AppEvent::IncomingTransfer(request)) => decline_transfer(request),
            Some(AppEvent::StatusMessage(message)) => return Err(eyre!(message)),
            Some(_) => {}
            None => return Err(eyre!("device discovery stopped unexpectedly")),
        }
    }

    Ok(peers)
}

fn add_peer(peers: &mut Vec<Peer>, peer: Peer, alias: &str, fingerprint: &str) {
    if peer.fingerprint == fingerprint || is_own_receiver(&peer, alias) {
        return;
    }

    if let Some(index) = peers
        .iter()
        .position(|known| known.fingerprint == peer.fingerprint)
    {
        peers[index] = peer;
    } else {
        peers.push(peer);
    }
}

fn is_own_receiver(peer: &Peer, alias: &str) -> bool {
    if peer.alias != alias {
        return false;
    }

    let local_ips = get_local_v4_ips();
    peer.ip
        .parse()
        .map(|ip| local_ips.contains(&ip))
        .unwrap_or(false)
}

fn prompt_for_peer(peers: &[Peer]) -> Result<Peer> {
    println!();
    println!("Choose who you want to send to:");
    for (index, peer) in peers.iter().enumerate() {
        let model = peer.device_model.as_deref().unwrap_or("Unknown device");
        println!("  {}. {} - {} ({})", index + 1, peer.alias, model, peer.ip);
    }

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    loop {
        print!("Enter a number (1-{}): ", peers.len());
        std::io::stdout()
            .flush()
            .wrap_err("could not display the device prompt")?;

        let mut input = String::new();
        if stdin
            .read_line(&mut input)
            .wrap_err("could not read the device selection")?
            == 0
        {
            return Err(eyre!("no device was selected"));
        }

        if let Some(index) = parse_selection(&input, peers.len()) {
            return Ok(peers[index].clone());
        }

        println!("Please enter a number from 1 to {}.", peers.len());
    }
}

fn parse_selection(input: &str, peer_count: usize) -> Option<usize> {
    let selection = input.trim().parse::<usize>().ok()?;
    selection.checked_sub(1).filter(|index| *index < peer_count)
}

async fn monitor_transfer(event_rx: &mut mpsc::UnboundedReceiver<AppEvent>) -> Result<()> {
    let mut last_reported_percent = 0;

    while let Some(event) = event_rx.recv().await {
        match event {
            AppEvent::TransferProgress {
                file_id,
                bytes_transferred,
                total_bytes,
                is_upload: true,
                ..
            } if total_bytes > 0 => {
                let percent = (bytes_transferred.saturating_mul(100) / total_bytes).min(100);
                if percent == 100 || percent >= last_reported_percent + 10 {
                    println!(
                        "Sending {file_id}: {} / {} ({percent}%)",
                        format_size(bytes_transferred),
                        format_size(total_bytes)
                    );
                    last_reported_percent = percent;
                }
            }
            AppEvent::TransferCompleted { message, .. } => {
                println!("{message}");
                return Ok(());
            }
            AppEvent::TransferFailed { error, .. } => return Err(eyre!(error)),
            AppEvent::IncomingTransfer(request) => decline_transfer(request),
            AppEvent::StatusMessage(message) => eprintln!("{message}"),
            AppEvent::PeerDiscovered(_)
            | AppEvent::FileReceived { .. }
            | AppEvent::TransferProgress { .. } => {}
        }
    }

    Err(eyre!("the transfer stopped before it completed"))
}

fn decline_transfer(request: IncomingTransferRequest) {
    let sender = request
        .response_tx
        .lock()
        .ok()
        .and_then(|mut response| response.take());
    if let Some(sender) = sender {
        let _ = sender.send(false);
    }
}

async fn trigger_scan(discovery: Arc<DiscoveryEngine>) {
    if let Ok(socket) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
        let _ = socket.set_broadcast(true);
        let _ = discovery.announce(&socket).await;
    }
}

fn validate_files(paths: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    paths
        .into_iter()
        .map(|path| {
            let metadata = std::fs::metadata(&path)
                .wrap_err_with(|| format!("could not read {}", path.display()))?;
            if !metadata.is_file() {
                return Err(eyre!("{} is not a file", path.display()));
            }
            path.canonicalize()
                .wrap_err_with(|| format!("could not resolve {}", path.display()))
        })
        .collect()
}

fn available_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind(("0.0.0.0", 0))
        .wrap_err("could not reserve a LocalSend registration port")?;
    listener
        .local_addr()
        .map(|address| address.port())
        .wrap_err("could not determine the LocalSend registration port")
}

struct ClipboardFile {
    path: PathBuf,
    directory: PathBuf,
}

impl ClipboardFile {
    fn create() -> Result<Self> {
        let contents = read_clipboard()?;
        if contents.is_empty() {
            return Err(eyre!("the clipboard is empty"));
        }

        let directory =
            std::env::temp_dir().join(format!("monosend-clipboard-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)
            .wrap_err("could not create a temporary clipboard directory")?;
        let path = directory.join("clipboard.txt");
        std::fs::write(&path, contents).wrap_err("could not create a temporary clipboard file")?;
        Ok(Self { path, directory })
    }
}

impl Drop for ClipboardFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_dir(&self.directory);
    }
}

fn read_clipboard() -> Result<Vec<u8>> {
    let commands: [(&str, &[&str]); 3] = [
        ("wl-paste", &["--no-newline"]),
        ("xclip", &["-selection", "clipboard", "-out"]),
        ("xsel", &["--clipboard", "--output"]),
    ];
    let mut found_command = false;

    for (program, args) in commands {
        match Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
        {
            Ok(output) => {
                found_command = true;
                if output.status.success() {
                    return Ok(output.stdout);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => found_command = true,
        }
    }

    if found_command {
        Err(eyre!("could not read the current text clipboard"))
    } else {
        Err(eyre!(
            "clipboard support needs wl-paste, xclip, or xsel to be installed"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_directories_as_share_inputs() {
        let result = validate_files(vec![std::env::temp_dir()]);
        assert!(result.is_err());
    }

    #[test]
    fn parses_numbered_device_selections() {
        assert_eq!(parse_selection("1\n", 3), Some(0));
        assert_eq!(parse_selection(" 3 ", 3), Some(2));
        assert_eq!(parse_selection("0", 3), None);
        assert_eq!(parse_selection("4", 3), None);
        assert_eq!(parse_selection("device", 3), None);
    }
}
