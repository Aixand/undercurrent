// SPDX-License-Identifier: Apache-2.0
//! Exercise internal framing in memory; no daemon or filesystem endpoint is used.

use futures_util::{SinkExt, StreamExt};
use std::{io, sync::Arc, time::Duration};
use undercurrent_transport_local::LocalFrameStream;
use undercurrent_wire::{ClientMessage, WireCodec};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let expected: Arc<[u8]> = WireCodec::new(128)?
        .encode_client(&ClientMessage::Ping)?
        .into();
    let (sender, receiver) = tokio::io::duplex(1);
    let mut writer = LocalFrameStream::new(sender, 128)?;
    let mut reader = LocalFrameStream::new(receiver, 128)?;
    let sent = expected.clone();

    let send = async move {
        writer.send(sent).await?;
        writer.close().await
    };
    let receive = async move {
        let actual = reader
            .next()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing frame"))??;
        if actual != expected {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "frame mismatch"));
        }
        if reader.next().await.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected clean EOF",
            ));
        }
        Ok::<(), io::Error>(())
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::try_join!(send, receive)
    })
    .await??;
    println!("verified one complete UCMP Ping and clean EOF");
    Ok(())
}
