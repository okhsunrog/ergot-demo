use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use ergot::exports::bbqueue::traits::{bbqhdl::BbqHandle, notifier::AsyncNotifier};
use ergot::exports::maitake_sync::WaitQueue;
use ergot::interface_manager::{
    FrameProcessor, InterfaceState, LivenessConfig, Profile,
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
use ergot::net_stack::NetStackHandle;
use ergot::wire_frames::de_frame;
use futures_channel::mpsc::{Receiver as MpscReceiver, Sender as MpscSender};
use futures_core::Stream;
use wasm_bindgen_futures::spawn_local;

use crate::duplex;

use super::{BUF_SIZE, EdgeStack, Impairment, LIVENESS_TIMEOUT, RouterStack};

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
    /// A root router's interface on a shared bus.
    BusRouter(RouterStack, u8, u16),
    /// Bridge uplink (UPSTREAM_IDENT, edge-style frame processing).
    BridgeUp(RouterStack),
    /// Edge uplink.
    Edge(EdgeStack),
    /// Edge attached to a shared bus. Frames for other bus members must be
    /// rejected before they reach the point-to-point DirectEdge processor.
    BusEdge(EdgeStack),
}

/// How one end of a link reacts when its peer goes quiet.
#[derive(Clone, Copy)]
enum Liveness {
    /// Bus members stay quiet between lease refreshes, so silence on a bus
    /// means nothing.
    Off,
    /// A downlink goes Inactive; the child's next heartbeat brings it back.
    Downstream,
    /// An uplink reverts to link-local rather than Inactive, so it can keep
    /// sending the heartbeat that provokes its recovery.
    Upstream,
}

impl StackSide {
    fn liveness(&self) -> Liveness {
        match self {
            StackSide::RouterDown(..) => Liveness::Downstream,
            StackSide::BridgeUp(_) | StackSide::Edge(_) => Liveness::Upstream,
            StackSide::BusRouter(..) | StackSide::BusEdge(_) => Liveness::Off,
        }
    }
}

const LIVENESS: LivenessConfig = LivenessConfig {
    timeout_ms: LIVENESS_TIMEOUT.as_millis() as u64,
};

/// Run a stream receive worker under `liveness` until the link closes.
async fn run_stream_rx<N, R, P>(
    worker: RxWorker<N, R, P>,
    liveness: Liveness,
    frame: &mut [u8],
    scratch: &mut [u8],
) where
    N: NetStackHandle,
    R: futures_io::AsyncRead + Unpin,
    P: FrameProcessor<N>,
{
    let mut worker = match liveness {
        Liveness::Upstream => worker.revert_to_link_local_on_timeout(),
        Liveness::Off | Liveness::Downstream => worker,
    };
    let _ = match liveness {
        Liveness::Off => worker.run(frame, scratch).await,
        Liveness::Downstream | Liveness::Upstream => {
            worker.run_with_liveness(frame, scratch, LIVENESS).await
        }
    };
}

/// Apply `liveness` to a packet worker.
fn packet_liveness<N, Rx, Tx, Q, P>(
    worker: PacketRxTxWorker<N, Rx, Tx, Q, P>,
    liveness: Liveness,
) -> PacketRxTxWorker<N, Rx, Tx, Q, P>
where
    N: NetStackHandle,
    Rx: PacketReceiver,
    Tx: PacketSender,
    Q: BbqHandle,
    Q::Notifier: AsyncNotifier,
    P: FrameProcessor<N>,
{
    match liveness {
        Liveness::Off => worker,
        Liveness::Downstream => worker.with_liveness(LIVENESS),
        Liveness::Upstream => worker
            .with_liveness(LIVENESS)
            .revert_to_link_local_on_timeout(),
    }
}

struct BusEdgeFrameProcessor {
    inner: EdgeFrameProcessor,
}

impl BusEdgeFrameProcessor {
    fn new() -> Self {
        Self {
            inner: EdgeFrameProcessor::new(),
        }
    }
}

impl FrameProcessor<EdgeStack> for BusEdgeFrameProcessor {
    fn process_frame(&mut self, data: &[u8], stack: &EdgeStack, ident: ()) -> bool {
        let Some(frame) = de_frame(data) else {
            return self.inner.process_frame(data, stack, ident);
        };
        let own_node = stack.manage_profile(|profile| match profile.interface_state(()) {
            Some(InterfaceState::Active { node_id, .. })
            | Some(InterfaceState::ActiveLocal { node_id }) => Some(node_id),
            _ => None,
        });
        let addressed_to_us = own_node == Some(frame.hdr.dst.node_id);
        let broadcast = frame.hdr.dst.port_id == 255 || frame.hdr.dst.node_id == 255;
        if !addressed_to_us && !broadcast {
            return false;
        }
        self.inner.process_frame(data, stack, ident)
    }

    fn reset(&mut self) {
        <EdgeFrameProcessor as FrameProcessor<EdgeStack>>::reset(&mut self.inner);
    }
}

pub(super) fn spawn_stream_rx(side: StackSide, reader: duplex::PipeReader, closer: Arc<WaitQueue>) {
    let liveness = side.liveness();
    spawn_local(async move {
        let mut frame = vec![0u8; BUF_SIZE];
        let mut scratch = vec![0u8; BUF_SIZE];
        let (frame, scratch) = (&mut frame[..], &mut scratch[..]);
        match side {
            StackSide::RouterDown(stack, ident, net_id)
            | StackSide::BusRouter(stack, ident, net_id) => {
                let rx_worker = RxWorker::new(
                    stack.clone(),
                    reader,
                    RouterFrameProcessor::new(net_id),
                    ident,
                )
                .with_closer(closer.clone());
                // Consumes the worker, which sets the interface Down on drop.
                run_stream_rx(rx_worker, liveness, frame, scratch).await;
                closer.close();
                stack.manage_profile(|im| {
                    let _ = im.deregister_interface(ident);
                });
            }
            StackSide::BridgeUp(stack) => {
                let rx_worker =
                    RxWorker::new(stack, reader, EdgeFrameProcessor::new(), UPSTREAM_IDENT)
                        .with_closer(closer.clone());
                run_stream_rx(rx_worker, liveness, frame, scratch).await;
                closer.close();
            }
            StackSide::Edge(stack) => {
                let rx_worker = RxWorker::new(stack, reader, EdgeFrameProcessor::new(), ())
                    .with_closer(closer.clone());
                run_stream_rx(rx_worker, liveness, frame, scratch).await;
                closer.close();
            }
            StackSide::BusEdge(stack) => {
                let rx_worker = RxWorker::new(stack, reader, BusEdgeFrameProcessor::new(), ())
                    .with_closer(closer.clone());
                run_stream_rx(rx_worker, liveness, frame, scratch).await;
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

pub(super) fn spawn_packet_worker<T>(
    side: StackSide,
    rx: MpscReceiver<Vec<u8>>,
    tx: T,
    queue: StdQueue,
    initial_state: InterfaceState,
    closer: Arc<WaitQueue>,
) where
    T: PacketSender + 'static,
{
    let receiver = ChannelRx {
        rx,
        closer: closer.clone(),
    };
    let liveness = side.liveness();
    spawn_local(async move {
        let consumer = queue.framed_consumer();
        let mut scratch = vec![0u8; BUF_SIZE];
        match side {
            StackSide::RouterDown(stack, ident, net_id)
            | StackSide::BusRouter(stack, ident, net_id) => {
                let mut worker = packet_liveness(
                    PacketRxTxWorker::new(
                        stack.clone(),
                        receiver,
                        tx,
                        RouterFrameProcessor::new(net_id),
                        ident,
                        consumer,
                    ),
                    liveness,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
                drop(worker);
                stack.manage_profile(|im| {
                    let _ = im.deregister_interface(ident);
                });
            }
            StackSide::BridgeUp(stack) => {
                let mut worker = packet_liveness(
                    PacketRxTxWorker::new(
                        stack,
                        receiver,
                        tx,
                        EdgeFrameProcessor::new(),
                        UPSTREAM_IDENT,
                        consumer,
                    ),
                    liveness,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
            }
            StackSide::Edge(stack) => {
                let mut worker = packet_liveness(
                    PacketRxTxWorker::new(
                        stack,
                        receiver,
                        tx,
                        EdgeFrameProcessor::new(),
                        (),
                        consumer,
                    ),
                    liveness,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
            }
            StackSide::BusEdge(stack) => {
                let mut worker = packet_liveness(
                    PacketRxTxWorker::new(
                        stack,
                        receiver,
                        tx,
                        BusEdgeFrameProcessor::new(),
                        (),
                        consumer,
                    ),
                    liveness,
                );
                let _ = worker.run(initial_state, &mut scratch).await;
                closer.close();
            }
        }
    });
}
