//! Focused LocalSend device-picker TUI for outgoing shares.

use crate::events::AppEvent;
use crate::localsend::client::LocalSendClient;
use crate::localsend::discovery::{DiscoveryEngine, get_local_v4_ips};
use crate::localsend::protocol::Peer;
use crate::localsend::tls::generate_self_signed_cert;
use crate::receive::device_alias;
use crate::{theme, utils};
use color_eyre::{
    Result,
    eyre::{WrapErr, eyre},
};
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use futures_util::StreamExt;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Gauge, List, ListItem, ListState, Paragraph},
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub async fn run(paths: Vec<PathBuf>, include_clipboard: bool) -> Result<()> {
    let mut paths = validate_files(paths)?;
    let clipboard_file = if include_clipboard {
        let file = ClipboardFile::create()?;
        paths.push(file.path.clone());
        Some(file)
    } else {
        None
    };

    let result = run_tui(paths).await;
    drop(clipboard_file);
    result
}

async fn run_tui(paths: Vec<PathBuf>) -> Result<()> {
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
    ));
    let client = Arc::new(LocalSendClient::new(
        alias.clone(),
        fingerprint.clone(),
        service_port,
    ));

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

    trigger_scan(discovery.clone()).await;

    let mut terminal = ratatui::init();
    let result = async {
        let mut app = ShareApp::new(paths, alias, fingerprint);
        let mut input = EventStream::new();
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));

        loop {
            terminal.draw(|frame| render(frame, &app))?;

            tokio::select! {
                _ = tick.tick() => {}
                maybe_input = input.next() => {
                    match maybe_input {
                        Some(Ok(Event::Key(key)))
                            if key.kind == KeyEventKind::Press
                                && handle_key(
                                    key,
                                    &mut app,
                                    &client,
                                    &event_tx,
                                    discovery.clone(),
                                )
                                .await =>
                        {
                            break;
                        }
                        Some(Err(error)) => return Err(error.into()),
                        None => break,
                        _ => {}
                    }
                }
                Some(event) = event_rx.recv() => app.handle_event(event),
            }
        }

        Ok(())
    }
    .await;
    ratatui::restore();
    result
}

#[derive(Debug)]
enum ShareState {
    Selecting,
    Sending {
        bytes: u64,
        total: u64,
        label: String,
    },
    Completed(String),
    Failed(String),
}

struct ShareApp {
    paths: Vec<PathBuf>,
    alias: String,
    fingerprint: String,
    peers: Vec<Peer>,
    peer_indexes: HashMap<String, usize>,
    selected: usize,
    state: ShareState,
    status: String,
}

impl ShareApp {
    fn new(paths: Vec<PathBuf>, alias: String, fingerprint: String) -> Self {
        Self {
            paths,
            alias,
            fingerprint,
            peers: Vec::new(),
            peer_indexes: HashMap::new(),
            selected: 0,
            state: ShareState::Selecting,
            status: "Searching for nearby LocalSend devices…".to_string(),
        }
    }

    fn handle_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::PeerDiscovered(peer) => self.add_peer(peer),
            AppEvent::TransferProgress {
                bytes_transferred,
                total_bytes,
                file_id,
                is_upload,
                ..
            } => {
                if is_upload {
                    self.state = ShareState::Sending {
                        bytes: bytes_transferred,
                        total: total_bytes,
                        label: file_id,
                    };
                }
            }
            AppEvent::TransferCompleted { message, .. } => {
                self.state = ShareState::Completed(message);
            }
            AppEvent::TransferFailed { error, .. } => {
                self.state = ShareState::Failed(error);
            }
            AppEvent::StatusMessage(message) => self.status = message,
            AppEvent::FileReceived { .. } => {}
            AppEvent::IncomingTransfer(request) => {
                let sender = request
                    .response_tx
                    .lock()
                    .ok()
                    .and_then(|mut response| response.take());
                if let Some(sender) = sender {
                    let _ = sender.send(false);
                }
            }
        }
    }

    fn add_peer(&mut self, peer: Peer) {
        if peer.fingerprint == self.fingerprint || self.is_own_receiver(&peer) {
            return;
        }

        if let Some(index) = self.peer_indexes.get(&peer.fingerprint).copied() {
            self.peers[index] = peer;
        } else {
            let index = self.peers.len();
            self.peer_indexes.insert(peer.fingerprint.clone(), index);
            self.peers.push(peer);
            self.status = format!("Found {} nearby device(s)", self.peers.len());
        }
    }

    fn is_own_receiver(&self, peer: &Peer) -> bool {
        if peer.alias != self.alias {
            return false;
        }
        let local_ips = get_local_v4_ips();
        peer.ip
            .parse()
            .map(|ip| local_ips.contains(&ip))
            .unwrap_or(false)
    }

    fn total_size(&self) -> u64 {
        self.paths
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|metadata| metadata.len())
            .sum()
    }
}

async fn handle_key(
    key: KeyEvent,
    app: &mut ShareApp,
    client: &Arc<LocalSendClient>,
    event_tx: &mpsc::UnboundedSender<AppEvent>,
    discovery: Arc<DiscoveryEngine>,
) -> bool {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return true;
    }

    match &app.state {
        ShareState::Completed(_) | ShareState::Failed(_) => {
            return matches!(key.code, KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q'));
        }
        ShareState::Sending { .. } => {
            return matches!(key.code, KeyCode::Esc | KeyCode::Char('q'));
        }
        ShareState::Selecting => {}
    }

    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => true,
        KeyCode::Up | KeyCode::Char('k') => {
            if !app.peers.is_empty() {
                app.selected = app.selected.checked_sub(1).unwrap_or(app.peers.len() - 1);
            }
            false
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if !app.peers.is_empty() {
                app.selected = (app.selected + 1) % app.peers.len();
            }
            false
        }
        KeyCode::Char('r') | KeyCode::F(5) => {
            app.status = "Scanning the local network…".to_string();
            trigger_scan(discovery).await;
            false
        }
        KeyCode::Enter => {
            if let Some(peer) = app.peers.get(app.selected).cloned() {
                let paths = app.paths.clone();
                let total = app.total_size();
                app.state = ShareState::Sending {
                    bytes: 0,
                    total,
                    label: format!("Waiting for {} to accept…", peer.alias),
                };
                let client = client.clone();
                let events = event_tx.clone();
                tokio::spawn(async move {
                    client.send_files(peer, paths, events).await;
                });
            }
            false
        }
        _ => false,
    }
}

async fn trigger_scan(discovery: Arc<DiscoveryEngine>) {
    if let Ok(socket) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
        let _ = socket.set_broadcast(true);
        let _ = discovery.announce(&socket).await;
    }
}

fn render(frame: &mut Frame, app: &ShareApp) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(theme::BASE)),
        area,
    );
    let area = centered_rect(82, 82, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme::GREEN))
        .title(Span::styled(
            " monosend share ",
            Style::default()
                .fg(theme::GREEN)
                .add_modifier(Modifier::BOLD),
        ))
        .style(Style::default().bg(theme::CRUST));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    match &app.state {
        ShareState::Selecting => render_picker(frame, app, inner),
        ShareState::Sending {
            bytes,
            total,
            label,
        } => render_progress(frame, inner, *bytes, *total, label),
        ShareState::Completed(message) => render_result(frame, inner, true, message),
        ShareState::Failed(message) => render_result(frame, inner, false, message),
    }
}

fn render_picker(frame: &mut Frame, app: &ShareApp, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(4),
            Constraint::Length(2),
        ])
        .split(area);

    let names = app
        .paths
        .iter()
        .take(3)
        .map(|path| path.file_name().unwrap_or_default().to_string_lossy())
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if app.paths.len() > 3 { ", …" } else { "" };
    let summary = Paragraph::new(vec![
        Line::from(Span::styled(
            format!(
                "Share {} file(s) · {}",
                app.paths.len(),
                utils::format_size(app.total_size())
            ),
            Style::default()
                .fg(theme::TEXT)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            format!("{names}{suffix}"),
            Style::default().fg(theme::SUBTEXT0),
        )),
    ])
    .alignment(Alignment::Center)
    .block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(theme::SURFACE2)),
    );
    frame.render_widget(summary, chunks[0]);

    if app.peers.is_empty() {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(""),
                Line::from(Span::styled(
                    "Searching for nearby LocalSend devices…",
                    Style::default()
                        .fg(theme::YELLOW)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(Span::styled(
                    &app.status,
                    Style::default().fg(theme::SUBTEXT0),
                )),
            ])
            .alignment(Alignment::Center),
            chunks[1],
        );
    } else {
        let items = app
            .peers
            .iter()
            .map(|peer| {
                let model = peer.device_model.as_deref().unwrap_or(&peer.version);
                ListItem::new(format!("{}  {}  ({})", peer.alias, model, peer.ip))
                    .style(Style::default().fg(theme::TEXT))
            })
            .collect::<Vec<_>>();
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(theme::SURFACE2))
                    .title(" Receivers "),
            )
            .highlight_symbol("› ")
            .highlight_style(
                Style::default()
                    .fg(theme::YELLOW)
                    .bg(theme::SURFACE0)
                    .add_modifier(Modifier::BOLD),
            );
        let mut state = ListState::default().with_selected(Some(app.selected));
        frame.render_stateful_widget(list, chunks[1], &mut state);
    }

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "↑/↓ select · Enter send · r rescan · q quit",
            Style::default().fg(theme::SUBTEXT0),
        )))
        .alignment(Alignment::Center),
        chunks[2],
    );
}

fn render_progress(frame: &mut Frame, area: Rect, bytes: u64, total: u64, label: &str) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(35),
            Constraint::Length(4),
            Constraint::Min(3),
        ])
        .split(area);
    let ratio = if total == 0 {
        0.0
    } else {
        (bytes as f64 / total as f64).clamp(0.0, 1.0)
    };
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(" Sending "))
        .gauge_style(Style::default().fg(theme::GREEN).bg(theme::SURFACE0))
        .ratio(ratio)
        .label(format!("{:.0}%", ratio * 100.0));
    frame.render_widget(gauge, chunks[1]);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(label, Style::default().fg(theme::TEXT))),
            Line::from(Span::styled(
                format!(
                    "{} / {} · q to close",
                    utils::format_size(bytes),
                    utils::format_size(total)
                ),
                Style::default().fg(theme::SUBTEXT0),
            )),
        ])
        .alignment(Alignment::Center),
        chunks[2],
    );
}

fn render_result(frame: &mut Frame, area: Rect, success: bool, message: &str) {
    let color = if success { theme::GREEN } else { theme::RED };
    let title = if success {
        "Transfer complete"
    } else {
        "Transfer failed"
    };
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(""),
            Line::from(Span::styled(
                title,
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from(Span::styled(message, Style::default().fg(theme::TEXT))),
            Line::from(""),
            Line::from(Span::styled(
                "Press Enter or q to close",
                Style::default().fg(theme::SUBTEXT0),
            )),
        ])
        .alignment(Alignment::Center),
        area,
    );
}

fn centered_rect(horizontal_percent: u16, vertical_percent: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - vertical_percent) / 2),
            Constraint::Percentage(vertical_percent),
            Constraint::Percentage((100 - vertical_percent) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - horizontal_percent) / 2),
            Constraint::Percentage(horizontal_percent),
            Constraint::Percentage((100 - horizontal_percent) / 2),
        ])
        .split(vertical[1])[1]
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
}
