use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use futures_channel::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};
use futures_core::Stream;
use gloo_timers::future::TimeoutFuture;
use maitake_sync::WaitQueue;
use wasm_bindgen_futures::spawn_local;

use crate::duplex;

use super::BUF_SIZE;

/// Shared, live-tunable impairment parameters for one link.
#[derive(Clone, Default)]
pub(super) struct Impairment {
    pub(super) latency_ms: Rc<Cell<u32>>,
    pub(super) loss_pct: Rc<Cell<u8>>,
    pub(super) overflow_drops: Rc<Cell<u32>>,
}

impl Impairment {
    /// Apply the configured delay, then decide whether to drop this unit.
    async fn delay_and_drop(&self, closer: &WaitQueue) -> Option<bool> {
        let latency = self.latency_ms.get();
        if latency > 0
            && matches!(
                select(TimeoutFuture::new(latency), closer.wait()).await,
                Either::Second(_)
            )
        {
            return None;
        }
        let loss = self.loss_pct.get();
        Some(loss > 0 && js_sys::Math::random() * 100.0 < f64::from(loss))
    }
}

async fn pipe_read(reader: &mut duplex::PipeReader, buf: &mut [u8]) -> std::io::Result<usize> {
    use futures_io::AsyncRead;
    core::future::poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, buf)).await
}

async fn pipe_write_all(writer: &mut duplex::PipeWriter, mut buf: &[u8]) -> std::io::Result<()> {
    use futures_io::AsyncWrite;
    while !buf.is_empty() {
        let n = core::future::poll_fn(|cx| Pin::new(&mut *writer).poll_write(cx, buf)).await?;
        buf = &buf[n..];
    }
    Ok(())
}

/// Forward bytes between two pipes, applying latency/loss per chunk. On a
/// stream link a dropped chunk corrupts the COBS stream mid-frame; the
/// receiver's accumulator resyncs at the next frame delimiter — i.e. real
/// frame loss, the honest serial-cable failure mode.
pub(super) fn spawn_stream_impairment(
    mut rx: duplex::PipeReader,
    mut tx: duplex::PipeWriter,
    impairment: Impairment,
    closer: Arc<WaitQueue>,
) {
    spawn_local(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            let read = pipe_read(&mut rx, &mut buf);
            let n = match select(read, closer.wait()).await {
                Either::First(Ok(n)) if n > 0 => n,
                _ => return,
            };
            match impairment.delay_and_drop(&closer).await {
                Some(true) => continue,
                Some(false) => {}
                None => return,
            }
            if pipe_write_all(&mut tx, &buf[..n]).await.is_err() {
                return;
            }
        }
    });
}

/// Forward frames between two channels, applying latency/loss per frame.
pub(super) fn spawn_packet_impairment(
    mut rx: MpscReceiver<Vec<u8>>,
    mut tx: MpscSender<Vec<u8>>,
    impairment: Impairment,
    closer: Arc<WaitQueue>,
) {
    spawn_local(async move {
        loop {
            let next = core::future::poll_fn(|cx| Pin::new(&mut rx).poll_next(cx));
            let frame = match select(next, closer.wait()).await {
                Either::First(Some(frame)) => frame,
                _ => return,
            };
            match impairment.delay_and_drop(&closer).await {
                Some(true) => continue,
                Some(false) => {}
                None => return,
            }
            match tx.try_send(frame) {
                Ok(()) => {}
                Err(e) if e.is_full() => impairment
                    .overflow_drops
                    .set(impairment.overflow_drops.get().saturating_add(1)),
                Err(_) => return,
            }
        }
    });
}
