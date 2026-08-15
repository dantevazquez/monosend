//! LocalSend HTTP/HTTPS client for initiating file transfers to remote peers.

use crate::events::AppEvent;
use crate::localsend::protocol::{
    DeviceType, FileDto, PROTOCOL_VERSION, Peer, PrepareUploadReqDto, PrepareUploadRespDto,
    RegisterDto,
};
use crate::localsend::tls::build_client;
use futures_util::StreamExt;
use reqwest::{Client, Identity};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

/// Client responsible for executing file upload sessions to remote LocalSend peers.
pub struct LocalSendClient {
    client: Client,
    alias: String,
    fingerprint: String,
    port: u16,
}

impl LocalSendClient {
    /// Creates a new `LocalSendClient` instance configured with local device credentials.
    pub fn new(
        alias: String,
        fingerprint: String,
        port: u16,
        identity: Identity,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: build_client(identity)?,
            alias,
            fingerprint,
            port,
        })
    }

    /// Uploads specified files to a target `Peer` device, emitting progress events.
    pub async fn send_files(
        &self,
        peer: Peer,
        file_paths: Vec<PathBuf>,
        event_tx: tokio::sync::mpsc::UnboundedSender<AppEvent>,
    ) {
        let base_url = format!("{}://{}:{}", peer.protocol, peer.ip, peer.port);
        let mut file_dto_map = HashMap::new();
        let mut path_by_id = HashMap::new();

        for path in file_paths {
            let metadata = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(_) => continue,
            };

            let file_name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();

            let file_id = Uuid::new_v4().to_string();

            let file_dto = FileDto {
                id: file_id.clone(),
                file_name,
                size: metadata.len(),
                file_type: Some(file_type(&path).to_string()),
                sha256: None,
                preview: None,
                metadata: None,
            };

            path_by_id.insert(file_id.clone(), path.clone());
            file_dto_map.insert(file_id, file_dto);
        }

        if file_dto_map.is_empty() {
            let _ = event_tx.send(AppEvent::StatusMessage(
                "No valid files to send".to_string(),
            ));
            return;
        }

        let prepare_req = PrepareUploadReqDto {
            info: RegisterDto {
                alias: self.alias.clone(),
                version: PROTOCOL_VERSION.to_string(),
                device_model: Some("monosend CLI".to_string()),
                device_type: Some(DeviceType::Desktop),
                fingerprint: self.fingerprint.clone(),
                port: Some(self.port),
                protocol: Some("https".to_string()),
                download: Some(false),
                announce: None,
            },
            files: file_dto_map.clone(),
        };

        let prepare_url = format!("{base_url}/api/localsend/v2/prepare-upload");
        let res = match self
            .client
            .post(&prepare_url)
            .json(&prepare_req)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let _ = event_tx.send(AppEvent::TransferFailed {
                    session_id: "unknown".to_string(),
                    error: format!("Failed to connect to peer: {e}"),
                });
                return;
            }
        };

        if res.status() == reqwest::StatusCode::NO_CONTENT {
            let _ = event_tx.send(AppEvent::TransferCompleted {
                session_id: "none".to_string(),
                message: format!("{} already has the selected files", peer.alias),
            });
            return;
        }

        if res.status() == reqwest::StatusCode::FORBIDDEN {
            let _ = event_tx.send(AppEvent::TransferFailed {
                session_id: "unknown".to_string(),
                error: format!(
                    "Transfer cancelled or declined by receiver ({})",
                    peer.alias
                ),
            });
            return;
        }

        if !res.status().is_success() {
            let status = res.status();
            let _ = event_tx.send(AppEvent::TransferFailed {
                session_id: "unknown".to_string(),
                error: format!("Receiver returned {status} while preparing the transfer"),
            });
            return;
        }

        let prepare_resp: PrepareUploadRespDto = match res.json().await {
            Ok(r) => r,
            Err(e) => {
                let _ = event_tx.send(AppEvent::TransferFailed {
                    session_id: "unknown".to_string(),
                    error: format!("Transfer rejected or cancelled by peer: {e}"),
                });
                return;
            }
        };

        let session_id = prepare_resp.session_id;
        let accepted_files = prepare_resp.files;
        let total_size_all: u64 = accepted_files
            .keys()
            .filter_map(|file_id| file_dto_map.get(file_id))
            .map(|file| file.size)
            .sum();
        let mut cumulative_bytes_sent: u64 = 0;

        if accepted_files.is_empty() {
            let _ = event_tx.send(AppEvent::TransferCompleted {
                session_id,
                message: format!("{} did not request any of the selected files", peer.alias),
            });
            return;
        }

        for (file_id, token) in accepted_files {
            let path = match path_by_id.get(&file_id) {
                Some(p) => p,
                None => continue,
            };

            let file_dto = match file_dto_map.get(&file_id) {
                Some(f) => f,
                None => continue,
            };

            let file = match File::open(path).await {
                Ok(f) => f,
                Err(e) => {
                    let _ = event_tx.send(AppEvent::TransferFailed {
                        session_id: session_id.clone(),
                        error: format!("Failed to open file {path:?}: {e}"),
                    });
                    let cancel_url =
                        format!("{base_url}/api/localsend/v2/cancel?sessionId={session_id}");
                    let _ = self.client.post(cancel_url).send().await;
                    return;
                }
            };

            let upload_url = format!(
                "{base_url}/api/localsend/v2/upload?sessionId={session_id}&fileId={file_id}&token={token}"
            );

            let event_tx_clone = event_tx.clone();
            let session_id_clone = session_id.clone();
            let file_name = file_dto.file_name.clone();
            let peer_alias = peer.alias.clone();
            let initial_cumulative = cumulative_bytes_sent;

            let mut file_bytes_sent = 0u64;

            let stream = ReaderStream::new(file).map(move |item| {
                if let Ok(ref bytes) = item {
                    file_bytes_sent += bytes.len() as u64;
                    let _ = event_tx_clone.send(AppEvent::TransferProgress {
                        session_id: session_id_clone.clone(),
                        file_id: format!("{file_name} to {peer_alias}"),
                        bytes_transferred: initial_cumulative + file_bytes_sent,
                        total_bytes: total_size_all,
                        is_upload: true,
                    });
                }
                item
            });
            let body = reqwest::Body::wrap_stream(stream);

            let res = match self.client.post(&upload_url).body(body).send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = event_tx.send(AppEvent::TransferFailed {
                        session_id: session_id.clone(),
                        error: format!("Upload cancelled or failed: {e}"),
                    });
                    return;
                }
            };

            if res.status().is_success() {
                cumulative_bytes_sent += file_dto.size;
                let _ = event_tx.send(AppEvent::TransferProgress {
                    session_id: session_id.clone(),
                    file_id: format!("{} to {}", file_dto.file_name, peer.alias),
                    bytes_transferred: cumulative_bytes_sent,
                    total_bytes: total_size_all,
                    is_upload: true,
                });
            } else {
                let err_msg = if res.status() == reqwest::StatusCode::FORBIDDEN {
                    format!("Transfer cancelled by receiver ({})", peer.alias)
                } else {
                    format!("Upload failed with status {}", res.status())
                };
                let _ = event_tx.send(AppEvent::TransferFailed {
                    session_id: session_id.clone(),
                    error: err_msg,
                });
                return;
            }
        }

        let _ = event_tx.send(AppEvent::TransferCompleted {
            session_id,
            message: format!("Successfully sent files to {}", peer.alias),
        });
    }
}

fn file_type(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("txt" | "md" | "log") => "text/plain",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("pdf") => "application/pdf",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::file_type;
    use std::path::Path;

    #[test]
    fn clipboard_text_is_sent_as_plain_text() {
        assert_eq!(file_type(Path::new("clipboard.txt")), "text/plain");
    }
}
