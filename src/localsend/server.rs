//! LocalSend HTTPS server implementation for Axum handling v2 API endpoints.

use crate::events::{AppEvent, IncomingTransferRequest};
use crate::localsend::protocol::{
    DeviceType, FileDto, InfoDto, LOCALSEND_DEFAULT_PORT, PROTOCOL_VERSION, Peer,
    PrepareUploadRespDto, RegisterDto,
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Query, State},
    http::StatusCode,
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::StreamExt;
use hyper_util::service::TowerToHyperService;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

/// Shared application state injected into Axum web handlers.
#[derive(Clone)]
pub struct ServerState {
    pub alias: String,
    pub fingerprint: String,
    pub event_tx: tokio::sync::mpsc::UnboundedSender<AppEvent>,
    pub download_dir: Arc<Mutex<PathBuf>>,
    pub active_sessions: Arc<Mutex<HashMap<String, ActiveSession>>>,
}

/// Information tracking an active file upload session.
#[derive(Clone)]
pub struct ActiveSession {
    pub files: HashMap<String, (String, u64)>, // file_id -> (file_name, size)
    pub tokens: HashMap<String, String>,       // file_id -> token
}

/// Query parameters passed to `/api/localsend/v2/upload`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadQuery {
    pub session_id: String,
    pub file_id: String,
    pub token: String,
}

/// Starts the LocalSend HTTPS server on the specified port with TLS support.
pub async fn start_server(
    alias: String,
    fingerprint: String,
    port: u16,
    server_config: Arc<rustls::ServerConfig>,
    download_dir: Arc<Mutex<PathBuf>>,
    event_tx: tokio::sync::mpsc::UnboundedSender<AppEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = ServerState {
        alias,
        fingerprint,
        event_tx,
        download_dir,
        active_sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/api/localsend/v2/info", get(handle_info))
        .route("/api/localsend/v2/register", post(handle_register))
        .route(
            "/api/localsend/v2/prepare-upload",
            post(handle_prepare_upload),
        )
        .route("/api/localsend/v2/upload", post(handle_upload))
        .route("/api/localsend/v2/cancel", post(handle_cancel))
        .with_state(state);

    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    let tls_acceptor = TlsAcceptor::from(server_config);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let acceptor = tls_acceptor.clone();
        let app_clone = app.clone();
        let client_ip = peer_addr.ip().to_string();

        tokio::spawn(async move {
            if let Ok(tls_stream) = acceptor.accept(stream).await {
                let io = hyper_util::rt::TokioIo::new(tls_stream);
                let ip_extension = client_ip.clone();

                let app_with_ip = app_clone.layer(middleware::from_fn(
                    move |mut req: axum::http::Request<Body>, next: Next| {
                        let ip = ip_extension.clone();
                        async move {
                            req.extensions_mut().insert(ip);
                            next.run(req).await
                        }
                    },
                ));

                let hyper_service = TowerToHyperService::new(app_with_ip);

                let _ = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(io, hyper_service)
                .await;
            }
        });
    }
}

async fn handle_info(State(state): State<ServerState>) -> Json<InfoDto> {
    Json(InfoDto {
        alias: state.alias,
        version: PROTOCOL_VERSION.to_string(),
        device_model: Some("monosend CLI".to_string()),
        device_type: Some(DeviceType::Desktop),
        fingerprint: state.fingerprint,
        download: false,
    })
}

async fn handle_register(
    State(state): State<ServerState>,
    Extension(peer_ip): Extension<String>,
    body: String,
) -> impl IntoResponse {
    let payload: RegisterDto = match serde_json::from_str(&body) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to parse HTTP register payload: {e}");
            return (
                StatusCode::OK,
                Json(InfoDto {
                    alias: state.alias.clone(),
                    version: PROTOCOL_VERSION.to_string(),
                    device_model: Some("monosend CLI".to_string()),
                    device_type: Some(DeviceType::Desktop),
                    fingerprint: state.fingerprint.clone(),
                    download: false,
                }),
            )
                .into_response();
        }
    };

    let peer_port = payload.port.unwrap_or(LOCALSEND_DEFAULT_PORT);
    let peer_protocol = payload.protocol.unwrap_or_else(|| "https".to_string());
    let alias = if payload.alias.is_empty() {
        format!("Device ({peer_ip})")
    } else {
        payload.alias
    };
    let fingerprint = if payload.fingerprint.is_empty() {
        format!("{peer_ip}:{peer_port}")
    } else {
        payload.fingerprint
    };

    let peer = Peer {
        alias,
        version: payload.version,
        device_model: payload.device_model,
        device_type: payload.device_type,
        fingerprint,
        ip: peer_ip,
        port: peer_port,
        protocol: peer_protocol,
    };
    let _ = state.event_tx.send(AppEvent::PeerDiscovered(peer));

    (
        StatusCode::OK,
        Json(InfoDto {
            alias: state.alias,
            version: PROTOCOL_VERSION.to_string(),
            device_model: Some("monosend CLI".to_string()),
            device_type: Some(DeviceType::Desktop),
            fingerprint: state.fingerprint,
            download: false,
        }),
    )
        .into_response()
}

async fn handle_prepare_upload(
    State(state): State<ServerState>,
    Extension(peer_ip): Extension<String>,
    body: String,
) -> impl IntoResponse {
    let json_val: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Failed to parse prepare-upload body: {e}");
            return (StatusCode::BAD_REQUEST, "Invalid prepare-upload payload").into_response();
        }
    };

    let session_id = Uuid::new_v4().to_string();
    let mut response_files = HashMap::new();
    let mut session_files = HashMap::new();
    let mut file_dtos = Vec::new();

    let info_obj = json_val.get("info");
    let peer_alias = info_obj
        .and_then(|i| i.get("alias"))
        .and_then(|a| a.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("LocalSend Device")
        .to_string();
    let peer_version = info_obj
        .and_then(|i| i.get("version"))
        .and_then(|v| v.as_str())
        .unwrap_or("2.0")
        .to_string();
    let peer_model = info_obj
        .and_then(|i| i.get("deviceModel"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    let peer_port = info_obj
        .and_then(|i| i.get("port"))
        .and_then(|p| p.as_u64())
        .map(|p| p as u16)
        .unwrap_or(LOCALSEND_DEFAULT_PORT);
    let peer_protocol = info_obj
        .and_then(|i| i.get("protocol"))
        .and_then(|p| p.as_str())
        .unwrap_or("https")
        .to_string();
    let peer_fingerprint = info_obj
        .and_then(|i| i.get("fingerprint"))
        .and_then(|f| f.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{peer_ip}:{peer_port}"));

    let peer = Peer {
        alias: peer_alias,
        version: peer_version,
        device_model: peer_model,
        device_type: Some(DeviceType::Mobile),
        fingerprint: peer_fingerprint,
        ip: peer_ip,
        port: peer_port,
        protocol: peer_protocol,
    };

    if let Some(files_val) = json_val.get("files") {
        if let Some(files_map) = files_val.as_object() {
            for (key, fval) in files_map {
                // In the v2 protocol the map key is the file identifier. Use a
                // single canonical ID so a malformed inner `id` cannot leave a
                // duplicate session entry that never completes.
                let fid = key.clone();
                let fname = fval
                    .get("fileName")
                    .or_else(|| fval.get("file_name"))
                    .and_then(|n| n.as_str())
                    .map(safe_file_name)
                    .unwrap_or_else(|| "received_file".to_string());
                let fsize = fval.get("size").and_then(|s| s.as_u64()).unwrap_or(0);
                let ftype = fval
                    .get("fileType")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string());
                let token = Uuid::new_v4().to_string();

                response_files.insert(fid.clone(), token.clone());

                session_files.insert(fid.clone(), (fname.clone(), fsize));

                file_dtos.push(FileDto {
                    id: fid,
                    file_name: fname,
                    size: fsize,
                    file_type: ftype,
                    sha256: None,
                    preview: None,
                    metadata: None,
                });
            }
        } else if let Some(files_arr) = files_val.as_array() {
            for (idx, fval) in files_arr.iter().enumerate() {
                let fid = fval
                    .get("id")
                    .and_then(|i| i.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{idx}"));
                let fname = fval
                    .get("fileName")
                    .or_else(|| fval.get("file_name"))
                    .and_then(|n| n.as_str())
                    .map(safe_file_name)
                    .unwrap_or_else(|| "received_file".to_string());
                let fsize = fval.get("size").and_then(|s| s.as_u64()).unwrap_or(0);
                let ftype = fval
                    .get("fileType")
                    .and_then(|t| t.as_str())
                    .map(|s| s.to_string());
                let token = Uuid::new_v4().to_string();

                response_files.insert(fid.clone(), token.clone());
                session_files.insert(fid.clone(), (fname.clone(), fsize));

                file_dtos.push(FileDto {
                    id: fid,
                    file_name: fname,
                    size: fsize,
                    file_type: ftype,
                    sha256: None,
                    preview: None,
                    metadata: None,
                });
            }
        }
    }

    let session = ActiveSession {
        files: session_files,
        tokens: response_files.clone(),
    };

    state
        .active_sessions
        .lock()
        .unwrap()
        .insert(session_id.clone(), session);

    let (response_tx, response_rx) = tokio::sync::oneshot::channel::<bool>();

    let _ = state
        .event_tx
        .send(AppEvent::IncomingTransfer(IncomingTransferRequest {
            peer,
            files: file_dtos,
            response_tx: Arc::new(Mutex::new(Some(response_tx))),
        }));

    match response_rx.await {
        Ok(true) => (
            StatusCode::OK,
            Json(PrepareUploadRespDto {
                session_id,
                files: response_files,
            }),
        )
            .into_response(),
        _ => {
            let _ = state.event_tx.send(AppEvent::TransferFailed {
                session_id,
                error: "Incoming transfer request was declined or cancelled".to_string(),
            });
            StatusCode::FORBIDDEN.into_response()
        }
    }
}

async fn handle_upload(
    State(state): State<ServerState>,
    Query(query): Query<UploadQuery>,
    body: Body,
) -> StatusCode {
    let file_info = {
        let sessions = state.active_sessions.lock().unwrap();
        if let Some(session) = sessions.get(&query.session_id) {
            if session.tokens.get(&query.file_id) != Some(&query.token) {
                return StatusCode::FORBIDDEN;
            }
            session.files.get(&query.file_id).cloned()
        } else {
            return StatusCode::NOT_FOUND;
        }
    };

    let (file_name, total_size) = match file_info {
        Some(info) => info,
        None => return StatusCode::NOT_FOUND,
    };

    let active_dir = state.download_dir.lock().unwrap().clone();
    let (mut file, save_path) = match create_destination(&active_dir, &file_name).await {
        Ok(destination) => destination,
        Err(_) => {
            let _ = state.event_tx.send(AppEvent::TransferFailed {
                session_id: query.session_id.clone(),
                error: format!("Failed creating file {file_name}"),
            });
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let mut stream = body.into_data_stream();
    let mut bytes_transferred = 0u64;

    loop {
        let session_exists = {
            let sessions = state.active_sessions.lock().unwrap();
            sessions.contains_key(&query.session_id)
        };
        if !session_exists {
            let _ = tokio::fs::remove_file(&save_path).await;
            return StatusCode::BAD_REQUEST;
        }

        let chunk_res = match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(res)) => res,
            Ok(None) => break,
            Err(_) => {
                state
                    .active_sessions
                    .lock()
                    .unwrap()
                    .remove(&query.session_id);
                let _ = tokio::fs::remove_file(&save_path).await;
                let _ = state.event_tx.send(AppEvent::TransferFailed {
                    session_id: query.session_id.clone(),
                    error: format!("Upload stream timed out or cancelled for {file_name}"),
                });
                return StatusCode::BAD_REQUEST;
            }
        };

        match chunk_res {
            Ok(chunk) => {
                if file.write_all(&chunk).await.is_err() {
                    let _ = state.event_tx.send(AppEvent::TransferFailed {
                        session_id: query.session_id.clone(),
                        error: format!("Failed writing file {file_name}"),
                    });
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
                bytes_transferred += chunk.len() as u64;

                if bytes_transferred > total_size {
                    state
                        .active_sessions
                        .lock()
                        .unwrap()
                        .remove(&query.session_id);
                    let _ = tokio::fs::remove_file(&save_path).await;
                    let _ = state.event_tx.send(AppEvent::TransferFailed {
                        session_id: query.session_id.clone(),
                        error: format!("Received more data than declared for {file_name}"),
                    });
                    return StatusCode::BAD_REQUEST;
                }

                let _ = state.event_tx.send(AppEvent::TransferProgress {
                    session_id: query.session_id.clone(),
                    file_id: query.file_id.clone(),
                    bytes_transferred,
                    total_bytes: total_size,
                    is_upload: false,
                });
            }
            Err(_) => {
                state
                    .active_sessions
                    .lock()
                    .unwrap()
                    .remove(&query.session_id);
                let _ = tokio::fs::remove_file(&save_path).await;
                let _ = state.event_tx.send(AppEvent::TransferFailed {
                    session_id: query.session_id.clone(),
                    error: format!("Upload stream interrupted or cancelled for {file_name}"),
                });
                return StatusCode::BAD_REQUEST;
            }
        }
    }

    if total_size > 0 && bytes_transferred < total_size {
        state
            .active_sessions
            .lock()
            .unwrap()
            .remove(&query.session_id);
        let _ = tokio::fs::remove_file(&save_path).await;
        let _ = state.event_tx.send(AppEvent::TransferFailed {
            session_id: query.session_id,
            error: format!(
                "Transfer cancelled by sender (received {bytes_transferred}/{total_size} bytes)"
            ),
        });
        return StatusCode::BAD_REQUEST;
    }

    if file.flush().await.is_err() {
        state
            .active_sessions
            .lock()
            .unwrap()
            .remove(&query.session_id);
        let _ = tokio::fs::remove_file(&save_path).await;
        let _ = state.event_tx.send(AppEvent::TransferFailed {
            session_id: query.session_id,
            error: format!("Failed finalizing file {file_name}"),
        });
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    drop(file);

    let _ = state.event_tx.send(AppEvent::FileReceived {
        path: save_path.clone(),
    });

    let all_completed = {
        let mut sessions = state.active_sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(&query.session_id) {
            session.files.remove(&query.file_id);
            if session.files.is_empty() {
                sessions.remove(&query.session_id);
                true
            } else {
                false
            }
        } else {
            true
        }
    };

    if all_completed {
        let _ = state.event_tx.send(AppEvent::TransferCompleted {
            session_id: query.session_id,
            message: format!("Received {file_name}"),
        });
    }

    StatusCode::OK
}

async fn handle_cancel(
    State(state): State<ServerState>,
    Query(query): Query<HashMap<String, String>>,
    body: String,
) -> StatusCode {
    let session_id = query
        .get("sessionId")
        .or_else(|| query.get("session_id"))
        .cloned()
        .or_else(|| {
            if !body.is_empty() {
                serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| {
                        v.get("sessionId")
                            .or_else(|| v.get("session_id"))
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string())
                    })
            } else {
                None
            }
        });

    let mut sessions = state.active_sessions.lock().unwrap();
    if let Some(id) = session_id {
        sessions.remove(&id);
        let _ = state.event_tx.send(AppEvent::TransferFailed {
            session_id: id,
            error: "Transfer cancelled by sender".to_string(),
        });
    } else {
        let keys: Vec<_> = sessions.keys().cloned().collect();
        for id in keys {
            sessions.remove(&id);
            let _ = state.event_tx.send(AppEvent::TransferFailed {
                session_id: id,
                error: "Transfer cancelled by sender".to_string(),
            });
        }
    }
    StatusCode::OK
}

/// Strip path components supplied by a remote peer so uploads cannot escape
/// the directory selected by the receiver.
fn safe_file_name(file_name: &str) -> String {
    Path::new(file_name)
        .file_name()
        .filter(|name| !name.is_empty())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "received_file".to_string())
}

/// Create a new file without truncating an existing destination. Duplicate
/// names are written as `name (1).ext`, `name (2).ext`, and so on.
async fn create_destination(directory: &Path, file_name: &str) -> std::io::Result<(File, PathBuf)> {
    let path = Path::new(file_name);
    let stem = path.file_stem().unwrap_or_default().to_string_lossy();
    let extension = path.extension().map(|value| value.to_string_lossy());

    for index in 0..10_000 {
        let candidate_name = if index == 0 {
            file_name.to_string()
        } else if let Some(extension) = &extension {
            format!("{stem} ({index}).{extension}")
        } else {
            format!("{stem} ({index})")
        };
        let candidate = directory.join(candidate_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .await
        {
            Ok(file) => return Ok((file, candidate)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "too many files share this name",
    ))
}

#[cfg(test)]
mod tests {
    use super::{create_destination, safe_file_name};
    use uuid::Uuid;

    #[test]
    fn incoming_names_cannot_escape_download_directory() {
        assert_eq!(safe_file_name("../../secret.txt"), "secret.txt");
        assert_eq!(safe_file_name("folder/photo.jpg"), "photo.jpg");
        assert_eq!(safe_file_name(".."), "received_file");
    }

    #[tokio::test]
    async fn incoming_files_do_not_overwrite_existing_files() {
        let directory = std::env::temp_dir().join(format!("monosend-test-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("note.txt"), "original").unwrap();

        let (_, destination) = create_destination(&directory, "note.txt").await.unwrap();
        assert_eq!(destination.file_name().unwrap(), "note (1).txt");
        assert_eq!(
            std::fs::read_to_string(directory.join("note.txt")).unwrap(),
            "original"
        );

        std::fs::remove_dir_all(directory).unwrap();
    }
}
