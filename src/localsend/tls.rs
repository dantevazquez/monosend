//! TLS self-signed certificate generation using `rcgen` and `rustls`.

use rcgen::{CertificateParams, KeyPair};
use reqwest::{Client, Identity};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Container holding rustls server configuration and calculated SHA-256 fingerprint.
pub struct TlsConfig {
    pub server_config: Arc<rustls::ServerConfig>,
    pub client_identity: Identity,
    pub fingerprint: String,
    #[cfg(test)]
    certificate_der: rustls::pki_types::CertificateDer<'static>,
}

/// Generates an in-memory self-signed certificate for LocalSend HTTPS communication.
pub fn generate_self_signed_cert(
    alias: &str,
) -> Result<TlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let subject_alt_names = vec![alias.to_string(), "localhost".to_string()];
    let params = CertificateParams::new(subject_alt_names)?;
    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    let cert_der = cert.der();
    let mut hasher = Sha256::new();
    hasher.update(cert_der.as_ref());
    let fingerprint = hex::encode_upper(hasher.finalize());

    // LocalSend v2.2 uses mutual TLS. The certificate that identifies this
    // device as a server must therefore also be presented by HTTP clients.
    let identity_pem = format!("{}{}", cert.pem(), key_pair.serialize_pem());
    let client_identity = Identity::from_pem(identity_pem.as_bytes())?;

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.to_vec())];
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()),
    );

    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)?;

    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(TlsConfig {
        server_config: Arc::new(server_config),
        client_identity,
        fingerprint,
        #[cfg(test)]
        certificate_der: rustls::pki_types::CertificateDer::from(cert_der.to_vec()),
    })
}

/// Builds an HTTP client that presents the device certificate to HTTPS peers.
pub fn build_client(identity: Identity) -> Result<Client, reqwest::Error> {
    Client::builder()
        .identity(identity)
        .danger_accept_invalid_certs(true)
        // LocalSend peers are always reached directly on the local network.
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::RootCertStore;
    use rustls::server::WebPkiClientVerifier;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    #[test]
    fn fingerprint_uses_localsend_uppercase_format() {
        let tls = generate_self_signed_cert("test").unwrap();

        assert_eq!(tls.fingerprint.len(), 64);
        assert!(
            tls.fingerprint
                .chars()
                .all(|character| character.is_ascii_digit() || ('A'..='F').contains(&character))
        );
    }

    #[tokio::test]
    async fn http_client_presents_generated_certificate() {
        let client_tls = generate_self_signed_cert("client").unwrap();

        let server_key = KeyPair::generate().unwrap();
        let server_cert = CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&server_key)
            .unwrap();

        let mut trusted_clients = RootCertStore::empty();
        trusted_clients
            .add(client_tls.certificate_der.clone())
            .unwrap();
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(trusted_clients))
            .build()
            .unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(
                vec![server_cert.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()),
                ),
            )
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = TlsAcceptor::from(Arc::new(server_config))
                .accept(stream)
                .await
                .unwrap();
            assert!(stream.get_ref().1.peer_certificates().is_some());

            let mut request = [0_u8; 1024];
            let bytes_read = stream.read(&mut request).await.unwrap();
            assert!(bytes_read > 0);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });

        let response = build_client(client_tls.client_identity)
            .unwrap()
            .get(format!("https://{address}/"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.text().await.unwrap(), "ok");
        server.await.unwrap();
    }
}
