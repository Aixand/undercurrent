// SPDX-License-Identifier: Apache-2.0
use futures_util::{Sink, Stream, ready};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Local adapter 0.1.0-beta termination: capacity exceeded, retry with backoff.
/// Header: UCLC magic, version u16=1, kind u16=1, body length u32=0.
/// This replaces WebSocket close metadata; UCMP product frames remain unchanged.
/// The SDK recognizes only these exact bytes after negotiation. Backoff guides
/// connection recovery; it does not authorize replaying an ambiguous mutation.
pub const CAPACITY_CLOSE_FRAME: &[u8; 22] = b"UCLC\x00\x0a\x00\x01\x00\x00\x00\x000.1.0-beta";
const HEADER_BYTES: usize = 22;

/// Cancellation-safe bounded UCHS/UCMP framing over a local byte stream.
/// Partial reads and writes remain owned by this value across select cancellation.
///
/// Reads validate magic and the full-frame allocation bound before reading the
/// body; wire codecs still validate versions, kinds, and payloads. A read error
/// ends the stream. The sink checks frame size, not semantic validity.
///
/// Retaining this value preserves partial progress. Cancelling a send does not
/// make submitting the same frame again safe: flush retained output or let the
/// connection owner close it. No deadlines or authentication are provided here.
pub struct LocalFrameStream<S> {
    io: S,
    maximum: usize,
    input: Vec<u8>,
    filled: usize,
    ended: bool,
    output: Option<Arc<[u8]>>,
    written: usize,
}
impl<S> LocalFrameStream<S> {
    /// Creates a stream with an explicit full-frame allocation bound.
    pub fn new(io: S, maximum: usize) -> io::Result<Self> {
        if maximum < HEADER_BYTES {
            return Err(invalid("frame bound below header"));
        }
        Ok(Self {
            io,
            maximum,
            input: vec![0; HEADER_BYTES],
            filled: 0,
            ended: false,
            output: None,
            written: 0,
        })
    }
    /// Changes the bound after connection negotiation.
    ///
    /// Rejects a bound below the fixed header, current input allocation, or
    /// pending output size. Partial I/O state is preserved.
    pub fn set_maximum(&mut self, maximum: usize) -> io::Result<()> {
        if maximum < HEADER_BYTES
            || self.input.len() > maximum
            || self.output.as_ref().is_some_and(|b| b.len() > maximum)
        {
            return Err(invalid("invalid negotiated frame bound"));
        }
        self.maximum = maximum;
        Ok(())
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
impl<S: AsyncRead + Unpin> Stream for LocalFrameStream<S> {
    type Item = io::Result<Arc<[u8]>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        loop {
            if this.filled == this.input.len() {
                if this.filled == HEADER_BYTES {
                    if &this.input[..4] != b"UCHS"
                        && &this.input[..4] != b"UCMP"
                        && &this.input[..4] != b"UCLC"
                    {
                        this.ended = true;
                        return Poll::Ready(Some(Err(invalid("invalid frame magic"))));
                    }
                    let body =
                        u32::from_be_bytes(this.input[8..12].try_into().expect("fixed header"))
                            as usize;
                    let Some(total) = body
                        .checked_add(HEADER_BYTES)
                        .filter(|n| *n <= this.maximum)
                    else {
                        this.ended = true;
                        return Poll::Ready(Some(Err(invalid("frame exceeds negotiated bound"))));
                    };
                    this.input.resize(total, 0);
                }
                if this.filled == this.input.len() {
                    let frame = std::mem::replace(&mut this.input, vec![0; HEADER_BYTES]);
                    this.filled = 0;
                    return Poll::Ready(Some(Ok(Arc::from(frame))));
                }
            }
            let mut buffer = ReadBuf::new(&mut this.input[this.filled..]);
            match ready!(Pin::new(&mut this.io).poll_read(cx, &mut buffer)) {
                Err(e) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Ok(()) if buffer.filled().is_empty() => {
                    this.ended = true;
                    return Poll::Ready(if this.filled == 0 {
                        None
                    } else {
                        Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "truncated local frame",
                        )))
                    });
                }
                Ok(()) => this.filled += buffer.filled().len(),
            }
        }
    }
}
impl<S: AsyncWrite + Unpin> Sink<Arc<[u8]>> for LocalFrameStream<S> {
    type Error = io::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.as_mut().poll_flush(cx)
    }
    fn start_send(self: Pin<&mut Self>, item: Arc<[u8]>) -> io::Result<()> {
        let this = self.get_mut();
        if this.output.is_some() {
            return Err(invalid("write pending"));
        }
        if item.len() < HEADER_BYTES || item.len() > this.maximum {
            return Err(invalid("outbound frame outside bounds"));
        }
        this.output = Some(item);
        this.written = 0;
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(bytes) = &this.output {
            while this.written < bytes.len() {
                let n = ready!(Pin::new(&mut this.io).poll_write(cx, &bytes[this.written..]))?;
                if n == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "local write closed",
                    )));
                }
                this.written += n;
            }
            this.output = None;
            this.written = 0;
        }
        Pin::new(&mut this.io).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(cx))?;
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
