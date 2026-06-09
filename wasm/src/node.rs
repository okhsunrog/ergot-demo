//! Handle-based WASM API: nodes, links, and per-node services.
//!
//! A [`WasmNode`] owns one ergot NetStack with a profile chosen at
//! construction: a `Router` (many downlinks, each its own network segment)
//! or a DirectEdge `Edge` (single uplink). [`WasmNode::connect_to`] wires a
//! router to an edge and spawns the transport workers; the returned
//! [`WasmLink`] tears everything down on `disconnect()`/`free()`. Freeing a
//! node stops its service tasks and all attached links.
//!
//! Links come in two kinds, mirroring ergot's two interface flavors:
//!
//! - [`LinkKind::Stream`]: a byte pipe carrying COBS-framed frames (like
//!   serial/TCP), driven by the `futures_io` transport.
//! - [`LinkKind::Packet`]: a frame channel where every message is one
//!   complete frame (like UDP/USB bulk), driven by the generic
//!   `PacketRxTxWorker`.
//!
//! An edge node's uplink kind is a node property (like a device's physical
//! transport); a router accepts any mix of link kinds across its downlinks.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_core::Stream;
use gloo_timers::future::TimeoutFuture;
use maitake_sync::WaitQueue;
use serde::{Serialize as SerdeSerialize, Serialize};
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use ergot::{
    Address, HeaderSeq, ProtocolError,
    interface_manager::{
        Interface, InterfaceSink, InterfaceState, Profile,
        profiles::{
            direct_edge::{DirectEdge, EDGE_NODE_ID, EdgeFrameProcessor},
            router::{Router, RouterFrameProcessor},
        },
        transports::{
            futures_io::{RxWorker, tx_worker},
            packet::{PacketReceiver, PacketRxTxWorker, PacketSender},
        },
        utils::{
            cobs_stream, framed_stream,
            std::{StdQueue, new_std_queue},
        },
    },
    net_stack::ArcNetStack,
    well_known::ErgotPingEndpoint,
};
use mutex::raw_impls::cs::CriticalSectionRawMutex;

use crate::duplex;

const MTU: u16 = 512;
const QUEUE_SIZE: usize = 4096;
const BUF_SIZE: usize = 2048;
const MAX_INTERFACES: usize = 16;
const MAX_SEEDS: usize = 16;

// ---------------------------------------------------------------------------
// Frame tap: every frame leaving any interface is recorded for the UI
// ---------------------------------------------------------------------------

/// One observed frame, as seen at the sending interface.
#[derive(Serialize, Tsify, Clone)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct FrameEvent {
    /// The canvas edge id this frame travelled on.
    pub link_id: String,
    /// "down" = router→edge, "up" = edge→router.
    pub dir: String,
    pub src: String,
    pub dst: String,
    /// "req" | "resp" | "topic" | "err" | numeric kind.
    pub kind: String,
    pub seq: u16,
    /// `Date.now()` timestamp.
    pub ts: f64,
}

#[derive(Serialize, Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct FrameEventBatch {
    pub events: Vec<FrameEvent>,
}

const MAX_EVENTS: usize = 256;

thread_local! {
    static FRAME_EVENTS: RefCell<VecDeque<FrameEvent>> = const { RefCell::new(VecDeque::new()) };
}

/// Drain all frame events recorded since the last call. Poll this from the UI.
#[wasm_bindgen(js_name = takeFrameEvents)]
pub fn take_frame_events() -> FrameEventBatch {
    FRAME_EVENTS.with(|q| FrameEventBatch {
        events: q.borrow_mut().drain(..).collect(),
    })
}

fn fmt_addr(a: &Address) -> String {
    format!("{}.{}:{}", a.network_id, a.node_id, a.port_id)
}

fn kind_name(kind: u8) -> String {
    match kind {
        1 => "req".into(),
        2 => "resp".into(),
        3 => "topic".into(),
        255 => "err".into(),
        other => other.to_string(),
    }
}

/// A tap attached to one interface sink. `label` is the canvas edge id of
/// the link the interface currently serves (None while disconnected).
#[derive(Clone)]
struct Tap {
    label: Rc<RefCell<Option<String>>>,
    dir: &'static str,
}

impl Tap {
    fn record(&self, hdr: &HeaderSeq) {
        let Some(link_id) = self.label.borrow().clone() else {
            return;
        };
        let ev = FrameEvent {
            link_id,
            dir: self.dir.into(),
            src: fmt_addr(&hdr.src),
            dst: fmt_addr(&hdr.dst),
            kind: kind_name(hdr.kind.0),
            seq: hdr.seq_no,
            ts: js_sys::Date::now(),
        };
        FRAME_EVENTS.with(|q| {
            let mut q = q.borrow_mut();
            if q.len() >= MAX_EVENTS {
                q.pop_front();
            }
            q.push_back(ev);
        });
    }
}

// ---------------------------------------------------------------------------
// Sink: one type dispatching both interface flavors
// ---------------------------------------------------------------------------

/// A profile's `Interface::Sink` is a single type, so supporting both link
/// kinds on one router requires dispatching at the sink level. The sink is
/// also where the frame tap lives: every outgoing frame passes through here
/// with its header in decoded form.
enum SinkInner {
    Stream(cobs_stream::Sink<StdQueue>),
    Packet(framed_stream::Sink<StdQueue>),
}

struct WasmSink {
    inner: SinkInner,
    tap: Tap,
}

impl InterfaceSink for WasmSink {
    fn mtu(&self) -> u16 {
        match &self.inner {
            SinkInner::Stream(s) => s.mtu(),
            SinkInner::Packet(s) => s.mtu(),
        }
    }

    fn send_ty<T: SerdeSerialize>(&mut self, hdr: &HeaderSeq, body: &T) -> Result<(), ()> {
        self.tap.record(hdr);
        match &mut self.inner {
            SinkInner::Stream(s) => s.send_ty(hdr, body),
            SinkInner::Packet(s) => s.send_ty(hdr, body),
        }
    }

    fn send_raw(&mut self, hdr: &HeaderSeq, body: &[u8]) -> Result<(), ()> {
        self.tap.record(hdr);
        match &mut self.inner {
            SinkInner::Stream(s) => s.send_raw(hdr, body),
            SinkInner::Packet(s) => s.send_raw(hdr, body),
        }
    }

    fn send_err(&mut self, hdr: &HeaderSeq, err: ProtocolError) -> Result<(), ()> {
        self.tap.record(hdr);
        match &mut self.inner {
            SinkInner::Stream(s) => s.send_err(hdr, err),
            SinkInner::Packet(s) => s.send_err(hdr, err),
        }
    }
}

struct WasmInterface;
impl Interface for WasmInterface {
    type Sink = WasmSink;
}

type EdgeStack = ArcNetStack<CriticalSectionRawMutex, DirectEdge<WasmInterface>>;
type RouterStack = ArcNetStack<
    CriticalSectionRawMutex,
    Router<WasmInterface, rand::rngs::StdRng, MAX_INTERFACES, MAX_SEEDS>,
>;

enum Stack {
    Router(RouterStack),
    Edge { stack: EdgeStack, queue: StdQueue },
}

// ---------------------------------------------------------------------------
// Packet links: in-memory frame channels
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct LinkClosed;

/// One end of an in-memory packet link: each channel message is one
/// complete ergot frame. `recv` also watches the link closer, which is how
/// packet workers get torn down (`PacketRxTxWorker` has no closer input).
struct ChannelRx {
    rx: UnboundedReceiver<Vec<u8>>,
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

struct ChannelTx {
    tx: UnboundedSender<Vec<u8>>,
}

impl PacketSender for ChannelTx {
    type Error = LinkClosed;

    async fn send(&mut self, data: &[u8]) -> Result<(), LinkClosed> {
        self.tx.unbounded_send(data.to_vec()).map_err(|_| LinkClosed)
    }
}

// ---------------------------------------------------------------------------
// Public API types
// ---------------------------------------------------------------------------

/// Which ergot profile a node runs.
#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeProfile {
    /// Router profile: many downlinks, each assigned its own network id.
    Router,
    /// DirectEdge target: a single uplink to a router.
    Edge,
}

/// The transport flavor of a link.
#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LinkKind {
    /// Byte stream with COBS framing in software (serial/TCP-like).
    Stream,
    /// Message channel where one message is one frame (UDP/USB-like).
    Packet,
}

/// Result of a successful ping.
#[derive(Serialize, Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct PingResult {
    /// The echoed value.
    pub value: u32,
    /// Round-trip time in milliseconds.
    pub latency_ms: f64,
}

/// Current state of a node, for display on the canvas.
#[derive(Serialize, Tsify)]
#[tsify(into_wasm_abi)]
#[serde(tag = "profile", rename_all = "lowercase", rename_all_fields = "camelCase")]
pub enum NodeStatus {
    /// Router: the network ids of its active downlinks.
    Router { nets: Vec<u16> },
    /// Edge: the state of its single uplink.
    Edge {
        status: String,
        net_id: Option<u16>,
        node_id: Option<u8>,
    },
}

// ---------------------------------------------------------------------------
// WasmNode
// ---------------------------------------------------------------------------

/// One ergot node (a full NetStack) living in this browser tab.
#[wasm_bindgen]
pub struct WasmNode {
    stack: Stack,
    profile: NodeProfile,
    /// For edge nodes: the transport kind of the (single) uplink.
    link_kind: LinkKind,
    /// Closes node-owned service tasks (ping server, ...) on drop.
    services_closer: Arc<WaitQueue>,
    /// Closers of currently attached links. Shared with [`WasmLink`]s.
    links: Rc<RefCell<Vec<Arc<WaitQueue>>>>,
    /// Edge nodes: the frame-tap label of the uplink sink, set while linked.
    edge_tap_label: Rc<RefCell<Option<String>>>,
}

#[wasm_bindgen]
impl WasmNode {
    /// Create a node. `link_kind` picks the uplink transport for edge nodes
    /// (default Stream); routers accept any mix per downlink.
    #[wasm_bindgen(constructor)]
    pub fn new(profile: NodeProfile, link_kind: Option<LinkKind>) -> WasmNode {
        let link_kind = link_kind.unwrap_or(LinkKind::Stream);
        let edge_tap_label = Rc::new(RefCell::new(None));
        let stack = match profile {
            NodeProfile::Router => {
                Stack::Router(RouterStack::new_with_profile(Router::new_std()))
            }
            NodeProfile::Edge => {
                let queue = new_std_queue(QUEUE_SIZE);
                let sink = new_sink(
                    link_kind,
                    &queue,
                    Tap {
                        label: edge_tap_label.clone(),
                        dir: "up",
                    },
                );
                Stack::Edge {
                    stack: EdgeStack::new_with_profile(DirectEdge::new_target(sink)),
                    queue,
                }
            }
        };
        WasmNode {
            stack,
            profile,
            link_kind,
            services_closer: Arc::new(WaitQueue::new()),
            links: Rc::new(RefCell::new(Vec::new())),
            edge_tap_label,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn profile(&self) -> NodeProfile {
        self.profile
    }

    /// The uplink transport kind (meaningful for edge nodes).
    #[wasm_bindgen(getter, js_name = linkKind)]
    pub fn link_kind(&self) -> LinkKind {
        self.link_kind
    }

    /// Number of currently attached links.
    #[wasm_bindgen(getter, js_name = linkCount)]
    pub fn link_count(&self) -> usize {
        self.links.borrow().len()
    }

    /// Current state of the node (router downlink nets / edge uplink state).
    pub fn status(&self) -> NodeStatus {
        match &self.stack {
            Stack::Router(stack) => NodeStatus::Router {
                nets: stack.manage_profile(|im| im.get_nets()),
            },
            Stack::Edge { stack, .. } => {
                let state = stack.manage_profile(|im| im.interface_state(()));
                match state {
                    Some(InterfaceState::Active { net_id, node_id }) => NodeStatus::Edge {
                        status: "active".into(),
                        net_id: Some(net_id),
                        node_id: Some(node_id),
                    },
                    Some(InterfaceState::Inactive) => NodeStatus::Edge {
                        status: "inactive".into(),
                        net_id: None,
                        node_id: None,
                    },
                    _ => NodeStatus::Edge {
                        status: "down".into(),
                        net_id: None,
                        node_id: None,
                    },
                }
            }
        }
    }

    /// Connect this node (router) to an edge node, using the edge's link
    /// kind. The router assigns the link its own network id. `link_id` is an
    /// opaque label (e.g. the canvas edge id) attached to tapped frames.
    #[wasm_bindgen(js_name = connectTo)]
    pub fn connect_to(&self, target: &WasmNode, link_id: Option<String>) -> Result<WasmLink, JsError> {
        let Stack::Router(router) = &self.stack else {
            return Err(JsError::new(
                "connectTo must be called as router.connectTo(edge)",
            ));
        };
        let Stack::Edge {
            stack: edge,
            queue: edge_queue,
        } = &target.stack
        else {
            return Err(JsError::new(
                "connectTo target must be an edge node (router-to-router links are not supported yet)",
            ));
        };
        if !target.links.borrow().is_empty() {
            return Err(JsError::new("edge node is already linked"));
        }
        let kind = target.link_kind;

        let closer = Arc::new(WaitQueue::new());
        let impairment = Impairment::default();
        let router_tap_label = Rc::new(RefCell::new(link_id.clone()));
        *target.edge_tap_label.borrow_mut() = link_id;

        // Router side: register a new interface; the profile assigns a net id.
        let router_queue = new_std_queue(QUEUE_SIZE);
        let res = router.manage_profile(|im| {
            let ident = im
                .register_interface(new_sink(
                    kind,
                    &router_queue,
                    Tap {
                        label: router_tap_label.clone(),
                        dir: "down",
                    },
                ))
                .ok()?;
            let state = im.interface_state(ident)?;
            let net_id = im.net_id_of(ident)?;
            im.set_interface_closer(ident, closer.clone());
            Some((ident, net_id, state))
        });
        let Some((ident, net_id, router_state)) = res else {
            return Err(JsError::new("router has no free interface slots"));
        };

        // Edge side: validate and mark the single uplink active.
        let edge_state = InterfaceState::Active {
            net_id: 0,
            node_id: EDGE_NODE_ID,
        };
        let edge_setup = edge.manage_profile(|im| {
            match im.interface_state(()) {
                Some(InterfaceState::Down) | None => {}
                _ => return Err(JsError::new("edge interface is already in use")),
            }
            im.set_closer(closer.clone());
            im.set_interface_state((), edge_state)
                .map_err(|e| JsError::new(&format!("failed to set interface state: {e:?}")))?;
            Ok(())
        });
        if let Err(e) = edge_setup {
            closer.close();
            *target.edge_tap_label.borrow_mut() = None;
            router.manage_profile(|im| {
                let _ = im.deregister_interface(ident);
            });
            return Err(e);
        }

        match kind {
            LinkKind::Stream => {
                // Per direction: tx → pipe a → impairment forwarder → pipe b → rx.
                let (router_writer, down_tap) = duplex::pipe();
                let (down_out, edge_reader) = duplex::pipe();
                let (edge_writer, up_tap) = duplex::pipe();
                let (up_out, router_reader) = duplex::pipe();
                spawn_stream_impairment(down_tap, down_out, impairment.clone(), closer.clone());
                spawn_stream_impairment(up_tap, up_out, impairment.clone(), closer.clone());

                spawn_stream_rx(
                    StreamRxSide::Router(router.clone(), ident, net_id),
                    router_reader,
                    closer.clone(),
                );
                spawn_stream_tx(router_writer, router_queue.clone(), closer.clone());
                spawn_stream_rx(
                    StreamRxSide::Edge(edge.clone()),
                    edge_reader,
                    closer.clone(),
                );
                spawn_stream_tx(edge_writer, edge_queue.clone(), closer.clone());
            }
            LinkKind::Packet => {
                // Per direction: worker → channel a → impairment forwarder → channel b → worker.
                let (router_tx, down_tap) = unbounded();
                let (down_out, edge_rx) = unbounded();
                let (edge_tx, up_tap) = unbounded();
                let (up_out, router_rx) = unbounded();
                spawn_packet_impairment(down_tap, down_out, impairment.clone(), closer.clone());
                spawn_packet_impairment(up_tap, up_out, impairment.clone(), closer.clone());

                spawn_packet_worker(
                    PacketSide::Router(router.clone(), ident),
                    router_rx,
                    router_tx_half(router_tx),
                    router_queue.clone(),
                    router_state,
                    closer.clone(),
                );
                spawn_packet_worker(
                    PacketSide::Edge(edge.clone()),
                    edge_rx,
                    router_tx_half(edge_tx),
                    edge_queue.clone(),
                    edge_state,
                    closer.clone(),
                );
            }
        }

        self.links.borrow_mut().push(closer.clone());
        target.links.borrow_mut().push(closer.clone());

        Ok(WasmLink {
            closer,
            net_id,
            kind,
            ends: [self.links.clone(), target.links.clone()],
            tap_labels: [router_tap_label, target.edge_tap_label.clone()],
            impairment,
        })
    }

    /// Attach a ping server (well-known `ErgotPingEndpoint`, name "ping").
    /// It serves until the node is freed. Resolves once the server is
    /// attached and ready.
    #[wasm_bindgen(js_name = servePing)]
    pub async fn serve_ping(&self) {
        let closer = self.services_closer.clone();
        match &self.stack {
            Stack::Router(stack) => {
                let stack = stack.clone();
                spawn_local(async move {
                    let server = stack
                        .endpoints()
                        .bounded_server::<ErgotPingEndpoint, 4>(Some("ping"));
                    let server = pin!(server);
                    let mut hdl = server.attach();
                    let serve_loop = async {
                        loop {
                            let _ = hdl
                                .serve(|val: &u32| {
                                    let v = *val;
                                    async move { v }
                                })
                                .await;
                        }
                    };
                    let _ = select(serve_loop, closer.wait()).await;
                });
            }
            Stack::Edge { stack, .. } => {
                let stack = stack.clone();
                spawn_local(async move {
                    let server = stack
                        .endpoints()
                        .bounded_server::<ErgotPingEndpoint, 4>(Some("ping"));
                    let server = pin!(server);
                    let mut hdl = server.attach();
                    let serve_loop = async {
                        loop {
                            let _ = hdl
                                .serve(|val: &u32| {
                                    let v = *val;
                                    async move { v }
                                })
                                .await;
                        }
                    };
                    let _ = select(serve_loop, closer.wait()).await;
                });
            }
        }
        // Yield once so the spawned task runs and attaches the server.
        yield_now().await;
    }

    /// Ping `network_id.node_id` (well-known ping endpoint). Resolves with
    /// the echoed value and round-trip latency, rejects on timeout
    /// (default 1000 ms) or send error.
    pub async fn ping(
        &self,
        network_id: u16,
        node_id: u8,
        timeout_ms: Option<u32>,
    ) -> Result<PingResult, JsError> {
        let addr = Address {
            network_id,
            node_id,
            port_id: 0,
        };
        let timeout = timeout_ms.unwrap_or(1_000);
        let start = js_sys::Date::now();
        let req = async {
            match &self.stack {
                Stack::Router(stack) => {
                    stack
                        .endpoints()
                        .request::<ErgotPingEndpoint>(addr, &42u32, Some("ping"))
                        .await
                }
                Stack::Edge { stack, .. } => {
                    stack
                        .endpoints()
                        .request::<ErgotPingEndpoint>(addr, &42u32, Some("ping"))
                        .await
                }
            }
        };
        match select(req, TimeoutFuture::new(timeout)).await {
            Either::First(Ok(value)) => Ok(PingResult {
                value,
                latency_ms: js_sys::Date::now() - start,
            }),
            Either::First(Err(e)) => Err(JsError::new(&format!("ping failed: {e:?}"))),
            Either::Second(()) => Err(JsError::new(&format!("ping timed out after {timeout} ms"))),
        }
    }
}

impl Drop for WasmNode {
    fn drop(&mut self) {
        self.services_closer.close();
        for closer in self.links.borrow_mut().drain(..) {
            closer.close();
        }
    }
}

// ---------------------------------------------------------------------------
// WasmLink
// ---------------------------------------------------------------------------

/// A live link between a router and an edge node. `disconnect()` (or
/// `free()`) tears down the transport workers on both sides; the edge
/// returns to Down and the router frees the interface slot.
#[wasm_bindgen]
pub struct WasmLink {
    closer: Arc<WaitQueue>,
    net_id: u16,
    kind: LinkKind,
    ends: [Rc<RefCell<Vec<Arc<WaitQueue>>>>; 2],
    tap_labels: [Rc<RefCell<Option<String>>>; 2],
    impairment: Impairment,
}

#[wasm_bindgen]
impl WasmLink {
    /// The network id the router assigned to this link.
    #[wasm_bindgen(getter, js_name = netId)]
    pub fn net_id(&self) -> u16 {
        self.net_id
    }

    /// The transport kind of this link.
    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> LinkKind {
        self.kind
    }

    /// Artificial one-way latency added to each direction, in milliseconds.
    #[wasm_bindgen(getter, js_name = latencyMs)]
    pub fn latency_ms(&self) -> u32 {
        self.impairment.latency_ms.get()
    }

    /// Probability (0-100) of dropping each chunk/frame, per direction.
    #[wasm_bindgen(getter, js_name = lossPct)]
    pub fn loss_pct(&self) -> u8 {
        self.impairment.loss_pct.get()
    }

    /// Set artificial latency and loss for both directions of this link.
    #[wasm_bindgen(js_name = setImpairment)]
    pub fn set_impairment(&self, latency_ms: u32, loss_pct: u8) {
        self.impairment.latency_ms.set(latency_ms);
        self.impairment.loss_pct.set(loss_pct.min(100));
    }

    pub fn disconnect(&self) {
        self.closer.close();
        for end in &self.ends {
            end.borrow_mut().retain(|c| !Arc::ptr_eq(c, &self.closer));
        }
        for label in &self.tap_labels {
            *label.borrow_mut() = None;
        }
    }
}

impl Drop for WasmLink {
    fn drop(&mut self) {
        self.disconnect();
    }
}

// ---------------------------------------------------------------------------
// Transport worker plumbing
// ---------------------------------------------------------------------------

fn new_sink(kind: LinkKind, queue: &StdQueue, tap: Tap) -> WasmSink {
    let inner = match kind {
        LinkKind::Stream => SinkInner::Stream(cobs_stream::Sink::new_from_handle(queue.clone(), MTU)),
        LinkKind::Packet => SinkInner::Packet(framed_stream::Sink::new_from_handle(queue.clone(), MTU)),
    };
    WasmSink { inner, tap }
}

fn router_tx_half(tx: UnboundedSender<Vec<u8>>) -> ChannelTx {
    ChannelTx { tx }
}

enum StreamRxSide {
    Router(RouterStack, u8, u16),
    Edge(EdgeStack),
}

fn spawn_stream_rx(side: StreamRxSide, reader: duplex::PipeReader, closer: Arc<WaitQueue>) {
    spawn_local(async move {
        let mut frame = vec![0u8; BUF_SIZE];
        let mut scratch = vec![0u8; BUF_SIZE];
        match side {
            StreamRxSide::Router(stack, ident, net_id) => {
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
            StreamRxSide::Edge(stack) => {
                let mut rx_worker = RxWorker::new(stack, reader, EdgeFrameProcessor::new(), ())
                    .with_closer(closer.clone());
                let _ = rx_worker.run(&mut frame, &mut scratch).await;
                closer.close();
            }
        }
    });
}

fn spawn_stream_tx(writer: duplex::PipeWriter, queue: StdQueue, closer: Arc<WaitQueue>) {
    spawn_local(async move {
        let consumer = queue.stream_consumer();
        let mut writer = writer;
        let _ = select(tx_worker(&mut writer, consumer), closer.wait()).await;
        closer.close();
    });
}

enum PacketSide {
    Router(RouterStack, u8),
    Edge(EdgeStack),
}

fn spawn_packet_worker(
    side: PacketSide,
    rx: UnboundedReceiver<Vec<u8>>,
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
            PacketSide::Router(stack, ident) => {
                let mut worker = PacketRxTxWorker::new(
                    stack.clone(),
                    receiver,
                    tx,
                    RouterFrameProcessor::new(
                        stack.manage_profile(|im| im.net_id_of(ident)).unwrap_or(0),
                    ),
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
            PacketSide::Edge(stack) => {
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

// ---------------------------------------------------------------------------
// Link impairment: artificial latency and loss per direction
// ---------------------------------------------------------------------------

/// Shared, live-tunable impairment parameters for one link.
#[derive(Clone, Default)]
struct Impairment {
    latency_ms: Rc<Cell<u32>>,
    loss_pct: Rc<Cell<u8>>,
}

impl Impairment {
    /// Apply the configured delay, then decide whether to drop this unit.
    async fn delay_and_drop(&self) -> bool {
        let latency = self.latency_ms.get();
        if latency > 0 {
            TimeoutFuture::new(latency).await;
        }
        let loss = self.loss_pct.get();
        loss > 0 && js_sys::Math::random() * 100.0 < f64::from(loss)
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
fn spawn_stream_impairment(
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
            if impairment.delay_and_drop().await {
                continue;
            }
            if pipe_write_all(&mut tx, &buf[..n]).await.is_err() {
                return;
            }
        }
    });
}

/// Forward frames between two channels, applying latency/loss per frame.
fn spawn_packet_impairment(
    mut rx: UnboundedReceiver<Vec<u8>>,
    tx: UnboundedSender<Vec<u8>>,
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
            if impairment.delay_and_drop().await {
                continue;
            }
            if tx.unbounded_send(frame).is_err() {
                return;
            }
        }
    });
}

/// Yield to the microtask queue once, letting freshly spawned tasks run.
async fn yield_now() {
    let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL)).await;
}
