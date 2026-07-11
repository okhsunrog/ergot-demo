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
    capacity: usize,
    reader_waker: Option<Waker>,
    writer_waker: Option<Waker>,
    writer_closed: bool,
    reader_closed: bool,
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
            if inner.writer_closed {
                return Poll::Ready(Ok(0)); // EOF
            }
            inner.reader_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let len = buf.len().min(inner.buf.len());
        for (i, byte) in inner.buf.drain(..len).enumerate() {
            buf[i] = byte;
        }
        if let Some(waker) = inner.writer_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(len))
    }
}

impl Drop for PipeReader {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.reader_closed = true;
        inner.buf.clear();
        if let Some(waker) = inner.writer_waker.take() {
            waker.wake();
        }
    }
}

impl futures_io::AsyncWrite for PipeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.reader_closed || inner.writer_closed {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")));
        }
        let available = inner.capacity.saturating_sub(inner.buf.len());
        if available == 0 {
            inner.writer_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let len = available.min(buf.len());
        inner.buf.extend(&buf[..len]);
        if let Some(waker) = inner.reader_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.inner.lock().unwrap();
        inner.writer_closed = true;
        if let Some(waker) = inner.reader_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for PipeWriter {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.writer_closed = true;
        if let Some(waker) = inner.reader_waker.take() {
            waker.wake();
        }
    }
}

/// Create a bounded unidirectional pipe. Writes wait while `capacity` bytes
/// are buffered, propagating backpressure to the transport worker.
pub fn pipe(capacity: usize) -> (PipeWriter, PipeReader) {
    assert!(capacity > 0, "pipe capacity must be non-zero");
    let inner = Arc::new(Mutex::new(PipeInner {
        buf: VecDeque::new(),
        capacity,
        reader_waker: None,
        writer_waker: None,
        writer_closed: false,
        reader_closed: false,
    }));

    (
        PipeWriter {
            inner: inner.clone(),
        },
        PipeReader { inner },
    )
}
