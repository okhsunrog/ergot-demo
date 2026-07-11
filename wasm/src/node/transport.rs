use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use ergot::interface_manager::{
    InterfaceState,
    profiles::{
        direct_edge::EdgeFrameProcessor,
        router::{RouterFrameProcessor, UPSTREAM_IDENT},
    },
    transports::{
        futures_io::{RxWorker, tx_worker},
        packet::{PacketReceiver, PacketRxTxWorker, PacketSender},
    },
    utils::std::StdQueue,
};
use futures_channel::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};
use futures_core::Stream;
use maitake_sync::WaitQueue;
use wasm_bindgen_futures::spawn_local;

use crate::duplex;

use super::{BUF_SIZE, EdgeStack, Impairment, RouterStack};

#[derive(Debug)]
pub(super) struct LinkClosed;

/// One end of an in-memory packet link: each channel message is one
/// complete ergot frame. `recv` also watches the link closer, which is how
/// packet workers get torn down (`PacketRxTxWorker` has no closer input).
struct ChannelRx {
    rx: MpscReceiver<Vec<u8>>,
    closer: Arc<WaitQueue>,
}

impl PacketReceiver for ChannelRx {
    type Error = LinkClosed;

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, LinkClosed> {
        let next = core::future::poll_fn(|cx| Pin::new(&mut self.rx).poll_next(cx));
        match select(next, self.closer.wait()).await {
            Either::First(Some(frame)) if frame.len() <= buf.len() => {
                buf[..frame.len()].copy_from_slice(&frame);
                Ok(frame.len())
            }
            _ => Err(LinkClosed),
        }
    }
}

pub(super) struct ChannelTx {
    tx: MpscSender<Vec<u8>>,
    overflow_drops: Rc<Cell<u32>>,
}

impl PacketSender for ChannelTx {
    type Error = LinkClosed;

    async fn send(&mut self, data: &[u8]) -> Result<(), LinkClosed> {
        match self.tx.try_send(data.to_vec()) {
            Ok(()) => Ok(()),
            // A full simulated link buffer drops the frame instead of
            // growing browser memory or tearing the interface down.
            Err(e) if e.is_full() => {
                self.overflow_drops
                    .set(self.overflow_drops.get().saturating_add(1));
                Ok(())
            }
            Err(_) => Err(LinkClosed),
        }
    }
}

pub(super) fn router_tx_half(tx: MpscSender<Vec<u8>>, impairment: &Impairment) -> ChannelTx {
    ChannelTx {
        tx,
        overflow_drops: impairment.overflow_drops.clone(),
    }
}

/// One side of a link, for transport worker spawning.
pub(super) enum StackSide {
    /// Parent downlink on a router-profile stack (router or bridge).
    RouterDown(RouterStack, u8, u16),
    /// Bridge uplink (UPSTREAM_IDENT, edge-style frame processing).
    BridgeUp(RouterStack),
    /// Edge uplink.
    Edge(EdgeStack),
}

pub(super) fn spawn_stream_rx(side: StackSide, reader: duplex::PipeReader, closer: Arc<WaitQueue>) {
    spawn_local(async move {
        let mut frame = vec![0u8; BUF_SIZE];
        let mut scratch = vec![0u8; BUF_SIZE];
        match side {
            StackSide::RouterDown(stack, ident, net_id) => {
                let mut rx_worker = RxWorker::new(
                    stack.clone(),
                    reader,
                    RouterFrameProcessor::new(net_id),
                    ident,
                )
                .with_closer(closer.clone());
                let _ = rx_worker.run(&mut frame, &mut scratch).await;
                closer.close();
                drop(rx_worker);
                stack.manage_profile(|im| {
                    let _ = im.deregister_interface(ident);
                });
            }
            StackSide::BridgeUp(stack) => {
                let mut rx_worker =
                    RxWorker::new(stack, reader, EdgeFrameProcessor::new(), UPSTREAM_IDENT)
                        .with_closer(closer.clone());
                let _ = rx_worker.run(&mut frame, &mut scratch).await;
                closer.close();
            }
            StackSide::Edge(stack) => {
                let mut rx_worker = RxWorker::new(stack, reader, EdgeFrameProcessor::new(), ())
                    .with_closer(closer.clone());
                let _ = rx_worker.run(&mut frame, &mut scratch).await;
                closer.close();
            }
        }
    });
}

pub(super) fn spawn_stream_tx(writer: duplex::PipeWriter, queue: StdQueue, closer: Arc<WaitQueue>) {
    spawn_local(async move {
        let consumer = queue.stream_consumer();
        let mut writer = writer;
        let _ = select(tx_worker(&mut writer, consumer), closer.wait()).await;
        closer.close();
    });
}

pub(super) fn spawn_packet_worker(
    side: StackSide,
    rx: MpscReceiver<Vec<u8>>,
    tx: ChannelTx,
    queue: StdQueue,
    initial_state: InterfaceState,
    closer: Arc<WaitQueue>,
) {
    let receiver = ChannelRx {
        rx,
        closer: closer.clone(),
    };
    spawn_local(async move {
        let consumer = queue.framed_consumer();
        let mut scratch = vec![0u8; BUF_SIZE];
        match side {
            StackSide::RouterDown(stack, ident, net_id) => {
                let mut worker = PacketRxTxWorker::new(
                    stack.clone(),
                    receiver,
                    tx,
                    RouterFrameProcessor::new(net_id),
                    ident,
                    consumer,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
                drop(worker);
                stack.manage_profile(|im| {
                    let _ = im.deregister_interface(ident);
                });
            }
            StackSide::BridgeUp(stack) => {
                let mut worker = PacketRxTxWorker::new(
                    stack,
                    receiver,
                    tx,
                    EdgeFrameProcessor::new(),
                    UPSTREAM_IDENT,
                    consumer,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
            }
            StackSide::Edge(stack) => {
                let mut worker = PacketRxTxWorker::new(
                    stack,
                    receiver,
                    tx,
                    EdgeFrameProcessor::new(),
                    (),
                    consumer,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
            }
        }
    });
}
