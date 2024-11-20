use std::{future::Future, sync::Arc};

use anyhow::Context;
use fastwebsockets::{handshake, WebSocket};
use hyper::{header::{CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE}, rt::Executor, upgrade::Upgraded, Request};
use hyper_util::rt::TokioIo;
use tokio::{net::TcpStream, task};
use tokio_rustls::{rustls::{pki_types::{pem::PemObject, CertificateDer, ServerName}, ClientConfig, RootCertStore}, TlsConnector};

///
/// Connect to a websocket server using a tls connection.
///
/// # Arguments
///
/// * `domain` - The domain to connect to.
/// * `cert` - The path to the tls certificate to use.
/// * `cursor` - The cursor to start from.
///
/// # Returns
///
/// A websocket connection to the server.
///
/// # Errors
///
/// Returns an anyhow::Error if the connection fails containing the reason.
///
pub async fn connect(domain: &str, cert: &str, cursor: u64, wanted_collections: Vec<&str>) ->
    Result<WebSocket<TokioIo<Upgraded>>, anyhow::Error> {

    // prepare tls store
    let mut tls_store = RootCertStore::empty();
    let tls_cert = CertificateDer::from_pem_file(cert)
        .context("unable to read tls cert")?;
    tls_store.add(tls_cert)
        .context("unable to add tls cert to store")?;

    // create tcp conn to server
    let addr = format!("{}:443", domain);
    let tcp_stream = TcpStream::connect(&addr)
        .await.context("unable to open tcp connection")?;

    // tls encrypt the tcp stream
    let tls_config = ClientConfig::builder()
        .with_root_certificates(tls_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));
    let tls_domain = ServerName::try_from(String::from(domain))
        .context("invalid dns name")?;
    let tls_stream = connector.connect(tls_domain, tcp_stream)
        .await.context("unable to establish tls connection")?;

    // upgrade connection to websocket
    let uri = format!("wss://{}/subscribe?wantedCollections={}{}",
        domain,
        wanted_collections.join(";"),
        if cursor == 0 { String::new() } else { format!("&cursor={}", cursor.to_string()) });
    let req_builder = Request::builder()
        .method("GET")
        .uri(uri)
        .header("Host", domain)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(SEC_WEBSOCKET_KEY, handshake::generate_key())
        .header(SEC_WEBSOCKET_VERSION, "13")
        .body(String::new());
    let req = req_builder
        .context("unable to build websocket upgrade request")?;
    let (ws, _) =
        handshake::client(&TokioExecutor, req, tls_stream)
        .await.context("unable to upgrade connection to websocket")?;

    Ok(ws)
}

// simply tie the executor to tokio
struct TokioExecutor;

impl<Fut> Executor<Fut> for TokioExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        task::spawn(fut);
    }
}