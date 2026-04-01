//! Simple in-memory pipes for futures-io.
//!
//! `pipe()` returns `(PipeWriter, PipeReader)`. Writing to the writer
//! makes the data readable from the reader.

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct PipeInner {
    buf: VecDeque<u8>,
    waker: Option<Waker>,
    closed: bool,
}

pub struct PipeReader {
    inner: Arc<Mutex<PipeInner>>,
}

pub struct PipeWriter {
    inner: Arc<Mutex<PipeInner>>,
}

impl futures_io::AsyncRead for PipeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.buf.is_empty() {
            if inner.closed {
                return Poll::Ready(Ok(0)); // EOF
            }
            inner.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let len = buf.len().min(inner.buf.len());
        for (i, byte) in inner.buf.drain(..len).enumerate() {
            buf[i] = byte;
        }
        Poll::Ready(Ok(len))
    }
}

impl futures_io::AsyncWrite for PipeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")));
        }
        inner.buf.extend(buf);
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for PipeWriter {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }
}

/// Create a unidirectional pipe. Data written to `PipeWriter` is readable from `PipeReader`.
pub fn pipe() -> (PipeWriter, PipeReader) {
    let inner = Arc::new(Mutex::new(PipeInner {
        buf: VecDeque::new(),
        waker: None,
        closed: false,
    }));

    (
        PipeWriter { inner: inner.clone() },
        PipeReader { inner },
    )
}
