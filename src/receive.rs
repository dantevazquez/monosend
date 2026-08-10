//! Headless LocalSend receiver controlled through desktop notifications.

use crate::events::{AppEvent, IncomingTransferRequest};
use crate::localsend::discovery::DiscoveryEngine;
use crate::localsend::server::start_server;
use crate::localsend::tls::generate_self_signed_cert;
use crate::utils::format_size;
use color_eyre::{
    Result,
    eyre::{WrapErr, eyre},
};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::mpsc;

pub async fn run(port: u16, autoaccept: bool) -> Result<()> {
    let download_dir = std::env::current_dir()
        .wrap_err("could not determine the current download directory")?
        .canonicalize()
        .wrap_err("could not resolve the current download directory")?;
    let alias = device_alias();
    let tls = generate_self_signed_cert(&alias)
        .map_err(|error| eyre!("could not create LocalSend TLS identity: {error}"))?;
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let shared_download_dir = Arc::new(Mutex::new(download_dir.clone()));

    let discovery = Arc::new(DiscoveryEngine::new(
        alias.clone(),
        tls.fingerprint.clone(),
        port,
        event_tx.clone(),
    ));
    let discovery_future = discovery.start();
    let server_future = start_server(
        alias,
        tls.fingerprint,
        port,
        tls.server_config,
        shared_download_dir,
        event_tx,
    );
    tokio::pin!(discovery_future);
    tokio::pin!(server_future);

    println!(
        "Listening for LocalSend transfers on port {port}; saving to {}",
        download_dir.display()
    );
    if autoaccept {
        println!("Automatic acceptance is enabled. Press Ctrl+C to stop.");
    } else {
        println!("Incoming requests will appear as desktop notifications. Press Ctrl+C to stop.");
    }

    loop {
        tokio::select! {
            result = &mut discovery_future => {
                return result.map_err(|error| eyre!("LocalSend discovery stopped: {error}"));
            }
            result = &mut server_future => {
                return result.map_err(|error| eyre!("LocalSend receiver stopped: {error}"));
            }
            signal = tokio::signal::ctrl_c() => {
                signal.wrap_err("failed to listen for Ctrl+C")?;
                println!("Stopping receiver.");
                return Ok(());
            }
            Some(event) = event_rx.recv() => handle_event(event, autoaccept, &download_dir),
        }
    }
}

fn handle_event(event: AppEvent, autoaccept: bool, download_dir: &std::path::Path) {
    match event {
        AppEvent::IncomingTransfer(request) => {
            let destination = download_dir.to_path_buf();
            tokio::spawn(async move {
                answer_request(request, autoaccept, destination).await;
            });
        }
        AppEvent::TransferCompleted {
            session_id,
            message,
        } => {
            println!("[{session_id}] {message}");
            tokio::spawn(show_information("LocalSend transfer complete", message));
        }
        AppEvent::TransferFailed { session_id, error } => {
            eprintln!("Transfer {session_id} failed: {error}");
            tokio::spawn(show_information("LocalSend transfer failed", error));
        }
        AppEvent::TransferProgress {
            bytes_transferred,
            total_bytes,
            is_upload,
            session_id,
            ..
        } => {
            if !is_upload && total_bytes > 0 && bytes_transferred == total_bytes {
                println!("[{session_id}] Received {}", format_size(total_bytes));
            }
        }
        AppEvent::FileReceived { path } => {
            if is_text_file(&path) {
                tokio::spawn(copy_received_text_to_clipboard(path));
            }
        }
        AppEvent::PeerDiscovered(_) | AppEvent::StatusMessage(_) => {}
    }
}

async fn answer_request(request: IncomingTransferRequest, autoaccept: bool, download_dir: PathBuf) {
    let accepted = if autoaccept {
        let body = request_body(&request, &download_dir, "Accepting automatically");
        tokio::spawn(show_information("Incoming LocalSend transfer", body));
        true
    } else {
        match ask_through_notification(&request, &download_dir).await {
            Ok(decision) => decision,
            Err(error) => {
                eprintln!("Could not show actionable notification: {error}");
                eprintln!("Declining the transfer because no decision could be collected.");
                false
            }
        }
    };

    let sender = request
        .response_tx
        .lock()
        .ok()
        .and_then(|mut response| response.take());
    if let Some(sender) = sender {
        let _ = sender.send(accepted);
    }

    let action = if accepted { "Accepted" } else { "Declined" };
    println!("{action} transfer from {}.", request.peer.alias);
}

async fn ask_through_notification(
    request: &IncomingTransferRequest,
    download_dir: &std::path::Path,
) -> Result<bool> {
    let body = request_body(request, download_dir, "Choose whether to receive it");
    let output = Command::new("notify-send")
        .arg("--app-name=monosend")
        .arg("--icon=document-save")
        .arg("--expire-time=0")
        .arg("--wait")
        .arg("--action=accept=Accept")
        .arg("--action=decline=Decline")
        .arg("Incoming LocalSend file")
        .arg(body)
        .stderr(Stdio::piped())
        .output()
        .await
        .wrap_err("failed to run notify-send; make sure libnotify is installed")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(eyre!(if stderr.is_empty() {
            "notify-send exited without a selection".to_string()
        } else {
            stderr
        }));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim() == "accept")
}

async fn show_information(title: impl AsRef<str>, body: impl AsRef<str>) {
    let _ = Command::new("notify-send")
        .arg("--app-name=monosend")
        .arg("--icon=document-save")
        .arg(title.as_ref())
        .arg(body.as_ref())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

async fn copy_received_text_to_clipboard(path: PathBuf) {
    let contents = match tokio::fs::read(&path).await {
        Ok(contents) => contents,
        Err(error) => {
            eprintln!(
                "Could not read {} for the clipboard: {error}",
                path.display()
            );
            return;
        }
    };

    match write_clipboard(&contents).await {
        Ok(program) => {
            let file_name = path.file_name().unwrap_or_default().to_string_lossy();
            println!("Copied {file_name} to the clipboard using {program}.");
        }
        Err(error) => {
            eprintln!(
                "Could not copy {} to the clipboard: {error}",
                path.display()
            );
        }
    }
}

async fn write_clipboard(contents: &[u8]) -> std::result::Result<&'static str, String> {
    let commands: [(&str, &[&str]); 3] = [
        ("wl-copy", &[]),
        ("xclip", &["-selection", "clipboard", "-in"]),
        ("xsel", &["--clipboard", "--input"]),
    ];
    let mut errors = Vec::new();

    for (program, args) in commands {
        let mut child = match Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                errors.push(format!("{program}: {error}"));
                continue;
            }
        };

        let Some(mut stdin) = child.stdin.take() else {
            errors.push(format!("{program}: failed to open stdin"));
            continue;
        };
        if let Err(error) = stdin.write_all(contents).await {
            errors.push(format!("{program}: {error}"));
            let _ = child.kill().await;
            continue;
        }
        if let Err(error) = stdin.shutdown().await {
            errors.push(format!("{program}: {error}"));
            let _ = child.kill().await;
            continue;
        }
        drop(stdin);

        match tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await {
            Ok(Ok(status)) if status.success() => return Ok(program),
            Ok(Ok(status)) => errors.push(format!("{program}: exited with {status}")),
            Ok(Err(error)) => errors.push(format!("{program}: {error}")),
            Err(_) => {
                errors.push(format!("{program}: timed out"));
                let _ = child.kill().await;
            }
        }
    }

    if errors.is_empty() {
        Err("install wl-clipboard, xclip, or xsel".to_string())
    } else {
        Err(errors.join("; "))
    }
}

fn is_text_file(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("txt"))
}

fn request_body(
    request: &IncomingTransferRequest,
    download_dir: &std::path::Path,
    prompt: &str,
) -> String {
    let total_size: u64 = request.files.iter().map(|file| file.size).sum();
    let names = request
        .files
        .iter()
        .take(4)
        .map(|file| file.file_name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let remaining = request.files.len().saturating_sub(4);
    let names = if remaining > 0 {
        format!("{names}, and {remaining} more")
    } else {
        names
    };

    format!(
        "{} wants to send {} file(s) ({})\n{}\nSave to: {}\n{}.",
        request.peer.alias,
        request.files.len(),
        format_size(total_size),
        names,
        download_dir.display(),
        prompt
    )
}

pub fn device_alias() -> String {
    let hostname = hostname::get()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "device".to_string());
    format!("monosend ({hostname})")
}

#[cfg(test)]
mod tests {
    use super::is_text_file;
    use std::path::Path;

    #[test]
    fn only_txt_files_are_copied_to_the_clipboard() {
        assert!(is_text_file(Path::new("message.txt")));
        assert!(is_text_file(Path::new("MESSAGE.TXT")));
        assert!(!is_text_file(Path::new("message.md")));
        assert!(!is_text_file(Path::new("txt")));
    }
}
