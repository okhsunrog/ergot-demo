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
use std::pin::pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use futures_channel::mpsc::channel;
use gloo_timers::future::TimeoutFuture;
use maitake_sync::WaitQueue;
use serde::Serialize;
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use ergot::{
    Address,
    interface_manager::{
        InterfaceState, Profile,
        profiles::{
            direct_edge::{DirectEdge, EDGE_NODE_ID},
            router::{Router, UPSTREAM_IDENT},
        },
        utils::std::{StdQueue, new_std_queue},
    },
    net_stack::ArcNetStack,
    well_known::ErgotPingEndpoint,
};
use mutex::raw_impls::cs::CriticalSectionRawMutex;

use crate::duplex;

mod frame_tap;
mod impairment;
mod seed;
mod transport;

pub use frame_tap::{FrameEvent, FrameEventBatch, take_frame_events};
use frame_tap::{Tap, TapBinding, TapLabel, WasmInterface, new_sink, next_link_generation};
use impairment::{Impairment, spawn_packet_impairment, spawn_stream_impairment};
use seed::spawn_seed_assign;
use transport::{StackSide, router_tx_half, spawn_packet_worker, spawn_stream_rx, spawn_stream_tx};

// The demo's sensor stream: a plain f32 reading, fire-and-forget.
ergot::topic!(SensorTopic, f32, "ergot-demo/sensor");

const MTU: u16 = 512;
const MAX_SAMPLES: usize = 64;
const QUEUE_SIZE: usize = 4096;
const BUF_SIZE: usize = 2048;
const MAX_INTERFACES: usize = 16;
const MAX_SEEDS: usize = 16;
const PACKET_QUEUE_FRAMES: usize = 16;
const MAX_LATENCY_MS: u32 = 60_000;

fn fmt_addr(a: &Address) -> String {
    format!("{}.{}:{}", a.network_id, a.node_id, a.port_id)
}

fn push_sample(
    samples: &Rc<RefCell<VecDeque<SensorSample>>>,
    msg: &ergot::socket::HeaderMessage<f32>,
) {
    let mut q = samples.borrow_mut();
    if q.len() >= MAX_SAMPLES {
        q.pop_front();
    }
    q.push_back(SensorSample {
        ts: js_sys::Date::now(),
        value: msg.t,
        src: fmt_addr(&msg.hdr.src),
    });
}

type EdgeStack = ArcNetStack<CriticalSectionRawMutex, DirectEdge<WasmInterface>>;
type RouterStack = ArcNetStack<
    CriticalSectionRawMutex,
    Router<WasmInterface, rand::rngs::StdRng, MAX_INTERFACES, MAX_SEEDS>,
>;

enum Stack {
    Router(RouterStack),
    /// Router profile in bridge mode; `queue` feeds the upstream sink.
    Bridge {
        stack: RouterStack,
        queue: StdQueue,
    },
    Edge {
        stack: EdgeStack,
        queue: StdQueue,
    },
}

type LinkList = Rc<RefCell<Vec<Rc<LinkState>>>>;

/// Shared ownership record for one live link. Both endpoint nodes and the
/// public `WasmLink` handle retain it, while endpoint references are weak so
/// dropping a node cannot create a cycle.
struct LinkState {
    closer: Arc<WaitQueue>,
    disconnected: Cell<bool>,
    ends: [Weak<RefCell<Vec<Rc<LinkState>>>>; 2],
    tap_labels: [TapLabel; 2],
    generation: u64,
}

impl LinkState {
    fn disconnect(self: &Rc<Self>) {
        if self.disconnected.replace(true) {
            return;
        }
        self.closer.close();
        for end in &self.ends {
            if let Some(end) = end.upgrade() {
                end.borrow_mut().retain(|link| !Rc::ptr_eq(link, self));
            }
        }
        for label in &self.tap_labels {
            let owns_binding = label
                .borrow()
                .as_ref()
                .is_some_and(|binding| binding.generation == self.generation);
            if owns_binding {
                *label.borrow_mut() = None;
            }
        }
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
    /// Router profile in bridge mode: one uplink, downlinks get network ids
    /// leased from the upstream seed router.
    Bridge,
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
#[serde(
    tag = "profile",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum NodeStatus {
    /// Router: the network ids of its active downlinks.
    Router { nets: Vec<u16> },
    /// Bridge: uplink state plus the seed-leased downlink network ids.
    Bridge {
        upstream: String,
        upstream_net_id: Option<u16>,
        nets: Vec<u16>,
    },
    /// Edge: the state of its single uplink.
    Edge {
        status: String,
        net_id: Option<u16>,
        node_id: Option<u8>,
    },
}

/// One received sensor reading.
#[derive(Serialize, Tsify, Clone)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct SensorSample {
    /// `Date.now()` at reception.
    pub ts: f64,
    pub value: f32,
    /// Address of the publisher, e.g. "1.2:5".
    pub src: String,
}

#[derive(Serialize, Tsify)]
#[tsify(into_wasm_abi)]
#[serde(rename_all = "camelCase")]
pub struct SampleBatch {
    pub samples: Vec<SensorSample>,
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
    /// Currently attached links. Shared with [`WasmLink`] handles.
    links: LinkList,
    /// Edge/bridge nodes: the frame-tap label of the uplink sink, set while linked.
    uplink_tap_label: TapLabel,
    /// Sensor readings received by the subscriber task.
    samples: Rc<RefCell<VecDeque<SensorSample>>>,
    /// Closer of the running sensor publisher task, if any.
    publisher_closer: RefCell<Option<Arc<WaitQueue>>>,
}

#[wasm_bindgen]
impl WasmNode {
    /// Create a node. `link_kind` picks the uplink transport for edge nodes
    /// (default Stream); routers accept any mix per downlink.
    #[wasm_bindgen(constructor)]
    pub fn new(profile: NodeProfile, link_kind: Option<LinkKind>) -> WasmNode {
        let link_kind = link_kind.unwrap_or(LinkKind::Stream);
        let uplink_tap_label = Rc::new(RefCell::new(None));
        let services_closer = Arc::new(WaitQueue::new());
        let stack = match profile {
            NodeProfile::Router => Stack::Router(RouterStack::new_with_profile(Router::new_std())),
            NodeProfile::Bridge => {
                let queue = new_std_queue(QUEUE_SIZE);
                let sink = new_sink(
                    link_kind,
                    &queue,
                    Tap {
                        label: uplink_tap_label.clone(),
                        dir: "up",
                    },
                );
                Stack::Bridge {
                    stack: RouterStack::new_with_profile(Router::new_bridge_std(sink)),
                    queue,
                }
            }
            NodeProfile::Edge => {
                let queue = new_std_queue(QUEUE_SIZE);
                let sink = new_sink(
                    link_kind,
                    &queue,
                    Tap {
                        label: uplink_tap_label.clone(),
                        dir: "up",
                    },
                );
                Stack::Edge {
                    stack: EdgeStack::new_with_profile(DirectEdge::new_target(sink)),
                    queue,
                }
            }
        };
        // Router-profile nodes answer seed-router assignment requests from
        // bridges below them.
        if let Stack::Router(stack) | Stack::Bridge { stack, .. } = &stack {
            let stack = stack.clone();
            let closer = services_closer.clone();
            spawn_local(async move {
                let _ = select(
                    stack.services().seed_router_request_handler::<4>(),
                    closer.wait(),
                )
                .await;
            });
        }
        WasmNode {
            stack,
            profile,
            link_kind,
            services_closer,
            links: Rc::new(RefCell::new(Vec::new())),
            uplink_tap_label,
            samples: Rc::new(RefCell::new(VecDeque::new())),
            publisher_closer: RefCell::new(None),
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

    /// Can this node accept a new uplink? (Routers have none.)
    #[wasm_bindgen(getter, js_name = uplinkFree)]
    pub fn uplink_free(&self) -> bool {
        match &self.stack {
            Stack::Router(_) => false,
            Stack::Bridge { stack, .. } => stack.manage_profile(|im| {
                matches!(
                    im.interface_state(UPSTREAM_IDENT),
                    Some(InterfaceState::Down) | None
                )
            }),
            Stack::Edge { stack, .. } => stack.manage_profile(|im| {
                matches!(im.interface_state(()), Some(InterfaceState::Down) | None)
            }),
        }
    }

    /// Current state of the node (router downlink nets / edge uplink state).
    pub fn status(&self) -> NodeStatus {
        match &self.stack {
            Stack::Router(stack) => NodeStatus::Router {
                nets: stack.manage_profile(|im| im.get_nets()),
            },
            Stack::Bridge { stack, .. } => {
                let (state, mut nets) =
                    stack.manage_profile(|im| (im.interface_state(UPSTREAM_IDENT), im.get_nets()));
                let (upstream, upstream_net_id) = match state {
                    Some(InterfaceState::Active { net_id, .. }) => ("active", Some(net_id)),
                    Some(InterfaceState::Inactive) => ("inactive", None),
                    _ => ("down", None),
                };
                if let Some(up) = upstream_net_id {
                    nets.retain(|n| *n != up);
                }
                NodeStatus::Bridge {
                    upstream: upstream.into(),
                    upstream_net_id,
                    nets,
                }
            }
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

    /// Connect this node (router or bridge: downlink side) to a bridge or
    /// edge node (uplink side), using the child's link kind. Routers assign
    /// the link a network id from their own pool; bridge downlinks lease one
    /// from the upstream seed router once their uplink is active. `link_id`
    /// is an opaque label (e.g. the canvas edge id) attached to tapped frames.
    #[wasm_bindgen(js_name = connectTo)]
    pub fn connect_to(
        &self,
        target: &WasmNode,
        link_id: Option<String>,
    ) -> Result<WasmLink, JsError> {
        let parent = match &self.stack {
            Stack::Router(stack) | Stack::Bridge { stack, .. } => stack,
            Stack::Edge { .. } => {
                return Err(JsError::new(
                    "connectTo must be called on a router or bridge (the downlink side)",
                ));
            }
        };
        let parent_is_bridge = self.profile == NodeProfile::Bridge;
        if matches!(target.stack, Stack::Router(_)) {
            return Err(JsError::new(
                "connectTo target must be a bridge or edge node (routers have no uplink)",
            ));
        }
        if !target.uplink_free() {
            return Err(JsError::new("target node is already linked upstream"));
        }
        let kind = target.link_kind;

        let closer = Arc::new(WaitQueue::new());
        let impairment = Impairment::default();
        let generation = next_link_generation();
        let binding = link_id.map(|label| TapBinding { generation, label });
        let parent_tap_label = Rc::new(RefCell::new(binding.clone()));

        // Parent side: register a downlink interface. Routers assign a net id
        // immediately; bridge downlinks start pending until seed assignment.
        let parent_queue = new_std_queue(QUEUE_SIZE);
        let res = parent.manage_profile(|im| {
            let sink = new_sink(
                kind,
                &parent_queue,
                Tap {
                    label: parent_tap_label.clone(),
                    dir: "down",
                },
            );
            let ident = if parent_is_bridge {
                im.register_interface_pending(sink).ok()?
            } else {
                im.register_interface(sink).ok()?
            };
            let net_id = im.net_id_of(ident).unwrap_or(0);
            let state = im.interface_state(ident)?;
            im.set_interface_closer(ident, closer.clone());
            Some((ident, net_id, state))
        });
        let Some((ident, net_id, parent_state)) = res else {
            return Err(JsError::new("parent has no free interface slots"));
        };
        *target.uplink_tap_label.borrow_mut() = binding;

        // Child side: mark the uplink interface up.
        let child_setup = match &target.stack {
            Stack::Edge { stack, .. } => stack.manage_profile(|im| {
                im.set_closer(closer.clone());
                im.set_interface_state(
                    (),
                    InterfaceState::Active {
                        net_id: 0,
                        node_id: EDGE_NODE_ID,
                    },
                )
                .map_err(|e| JsError::new(&format!("failed to set interface state: {e:?}")))
            }),
            Stack::Bridge { stack, .. } => stack.manage_profile(|im| {
                im.set_interface_closer(UPSTREAM_IDENT, closer.clone());
                // The bridge upstream starts Inactive and discovers its net id
                // from the first frame the parent sends.
                im.set_interface_state(UPSTREAM_IDENT, InterfaceState::Inactive)
                    .map_err(|e| JsError::new(&format!("failed to set upstream state: {e:?}")))
            }),
            Stack::Router(_) => unreachable!("validated above"),
        };
        let child_state = match &target.stack {
            Stack::Edge { .. } => InterfaceState::Active {
                net_id: 0,
                node_id: EDGE_NODE_ID,
            },
            _ => InterfaceState::Inactive,
        };
        if let Err(e) = child_setup {
            closer.close();
            let owns_binding = target
                .uplink_tap_label
                .borrow()
                .as_ref()
                .is_some_and(|binding| binding.generation == generation);
            if owns_binding {
                *target.uplink_tap_label.borrow_mut() = None;
            }
            parent.manage_profile(|im| {
                let _ = im.deregister_interface(ident);
            });
            return Err(e);
        }

        let parent_rx = StackSide::RouterDown(parent.clone(), ident, net_id);
        let child_rx = match &target.stack {
            Stack::Edge { stack, .. } => StackSide::Edge(stack.clone()),
            Stack::Bridge { stack, .. } => StackSide::BridgeUp(stack.clone()),
            Stack::Router(_) => unreachable!("validated above"),
        };
        let child_queue = match &target.stack {
            Stack::Edge { queue, .. } | Stack::Bridge { queue, .. } => queue.clone(),
            Stack::Router(_) => unreachable!("validated above"),
        };

        match kind {
            LinkKind::Stream => {
                // Per direction: tx → pipe a → impairment forwarder → pipe b → rx.
                let (parent_writer, down_tap) = duplex::pipe(QUEUE_SIZE);
                let (down_out, child_reader) = duplex::pipe(QUEUE_SIZE);
                let (child_writer, up_tap) = duplex::pipe(QUEUE_SIZE);
                let (up_out, parent_reader) = duplex::pipe(QUEUE_SIZE);
                spawn_stream_impairment(down_tap, down_out, impairment.clone(), closer.clone());
                spawn_stream_impairment(up_tap, up_out, impairment.clone(), closer.clone());

                spawn_stream_rx(parent_rx, parent_reader, closer.clone());
                spawn_stream_tx(parent_writer, parent_queue.clone(), closer.clone());
                spawn_stream_rx(child_rx, child_reader, closer.clone());
                spawn_stream_tx(child_writer, child_queue, closer.clone());
            }
            LinkKind::Packet => {
                // Per direction: worker → channel a → impairment forwarder → channel b → worker.
                let (parent_tx, down_tap) = channel(PACKET_QUEUE_FRAMES);
                let (down_out, child_chan_rx) = channel(PACKET_QUEUE_FRAMES);
                let (child_tx, up_tap) = channel(PACKET_QUEUE_FRAMES);
                let (up_out, parent_chan_rx) = channel(PACKET_QUEUE_FRAMES);
                spawn_packet_impairment(down_tap, down_out, impairment.clone(), closer.clone());
                spawn_packet_impairment(up_tap, up_out, impairment.clone(), closer.clone());

                spawn_packet_worker(
                    parent_rx,
                    parent_chan_rx,
                    router_tx_half(parent_tx, &impairment),
                    parent_queue.clone(),
                    parent_state,
                    closer.clone(),
                );
                spawn_packet_worker(
                    child_rx,
                    child_chan_rx,
                    router_tx_half(child_tx, &impairment),
                    child_queue,
                    child_state,
                    closer.clone(),
                );
            }
        }

        // Bridge downlinks: lease a net id from the upstream seed router as
        // soon as the uplink is active, then warm the new net with one ping
        // so the child learns its address.
        if parent_is_bridge {
            spawn_seed_assign(parent.clone(), ident, closer.clone());
        }

        let state = Rc::new(LinkState {
            closer: closer.clone(),
            disconnected: Cell::new(false),
            ends: [Rc::downgrade(&self.links), Rc::downgrade(&target.links)],
            tap_labels: [parent_tap_label, target.uplink_tap_label.clone()],
            generation,
        });
        self.links.borrow_mut().push(state.clone());
        target.links.borrow_mut().push(state.clone());

        // Transport workers close the shared closer on EOF/error. Reflect
        // that asynchronous shutdown in both endpoint registries as well.
        let weak_state = Rc::downgrade(&state);
        spawn_local(async move {
            let _ = closer.wait().await;
            if let Some(state) = weak_state.upgrade() {
                state.disconnect();
            }
        });

        Ok(WasmLink {
            state,
            net_id,
            kind,
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
            Stack::Router(stack) | Stack::Bridge { stack, .. } => {
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
                Stack::Router(stack) | Stack::Bridge { stack, .. } => {
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

    /// Attach a sensor-topic subscriber. Received readings accumulate in a
    /// ring buffer drained by `takeSamples()`. Runs until the node is freed.
    #[wasm_bindgen(js_name = subscribeSensor)]
    pub async fn subscribe_sensor(&self) {
        let closer = self.services_closer.clone();
        let samples = self.samples.clone();
        match &self.stack {
            Stack::Router(stack) | Stack::Bridge { stack, .. } => {
                let stack = stack.clone();
                spawn_local(async move {
                    let recv = stack.topics().bounded_receiver::<SensorTopic, 16>(None);
                    let recv = pin!(recv);
                    let mut hdl = recv.subscribe();
                    let recv_loop = async {
                        loop {
                            let msg = hdl.recv().await;
                            push_sample(&samples, &msg);
                        }
                    };
                    let _ = select(recv_loop, closer.wait()).await;
                });
            }
            Stack::Edge { stack, .. } => {
                let stack = stack.clone();
                spawn_local(async move {
                    let recv = stack.topics().bounded_receiver::<SensorTopic, 16>(None);
                    let recv = pin!(recv);
                    let mut hdl = recv.subscribe();
                    let recv_loop = async {
                        loop {
                            let msg = hdl.recv().await;
                            push_sample(&samples, &msg);
                        }
                    };
                    let _ = select(recv_loop, closer.wait()).await;
                });
            }
        }
        yield_now().await;
    }

    /// Drain sensor readings received since the last call.
    #[wasm_bindgen(js_name = takeSamples)]
    pub fn take_samples(&self) -> SampleBatch {
        SampleBatch {
            samples: self.samples.borrow_mut().drain(..).collect(),
        }
    }

    /// Broadcast a single sensor reading to the whole network.
    #[wasm_bindgen(js_name = publishSensor)]
    pub fn publish_sensor(&self, value: f32) -> Result<(), JsError> {
        let res = match &self.stack {
            Stack::Router(stack) | Stack::Bridge { stack, .. } => {
                stack.topics().broadcast::<SensorTopic>(&value, None)
            }
            Stack::Edge { stack, .. } => stack.topics().broadcast::<SensorTopic>(&value, None),
        };
        res.map_err(|e| JsError::new(&format!("publish failed: {e:?}")))
    }

    /// Send a single sensor reading to one node (unicast, port 0 + topic key).
    #[wasm_bindgen(js_name = publishSensorTo)]
    pub fn publish_sensor_to(
        &self,
        network_id: u16,
        node_id: u8,
        value: f32,
    ) -> Result<(), JsError> {
        let dest = Address {
            network_id,
            node_id,
            port_id: 0,
        };
        let res = match &self.stack {
            Stack::Router(stack) | Stack::Bridge { stack, .. } => {
                stack.topics().unicast::<SensorTopic>(dest, &value)
            }
            Stack::Edge { stack, .. } => stack.topics().unicast::<SensorTopic>(dest, &value),
        };
        res.map_err(|e| JsError::new(&format!("publish failed: {e:?}")))
    }

    /// Start a periodic publisher broadcasting a sine wave with a little
    /// noise every `interval_ms`. Replaces any running publisher.
    #[wasm_bindgen(js_name = startPublisher)]
    pub fn start_publisher(&self, interval_ms: u32) {
        self.stop_publisher();
        let closer = Arc::new(WaitQueue::new());
        *self.publisher_closer.borrow_mut() = Some(closer.clone());
        let interval = interval_ms.max(20);

        macro_rules! run_publisher {
            ($stack:expr) => {{
                let stack = $stack.clone();
                spawn_local(async move {
                    let publish_loop = async {
                        loop {
                            TimeoutFuture::new(interval).await;
                            let t = js_sys::Date::now() / 1000.0;
                            let value = ((t * core::f64::consts::TAU * 0.3).sin()
                                + js_sys::Math::random() * 0.1)
                                as f32;
                            let _ = stack.topics().broadcast::<SensorTopic>(&value, None);
                        }
                    };
                    let _ = select(publish_loop, closer.wait()).await;
                });
            }};
        }
        match &self.stack {
            Stack::Router(stack) | Stack::Bridge { stack, .. } => run_publisher!(stack),
            Stack::Edge { stack, .. } => run_publisher!(stack),
        }
    }

    /// Stop the periodic publisher, if running.
    #[wasm_bindgen(js_name = stopPublisher)]
    pub fn stop_publisher(&self) {
        if let Some(closer) = self.publisher_closer.borrow_mut().take() {
            closer.close();
        }
    }

    /// Is the periodic publisher running?
    #[wasm_bindgen(getter)]
    pub fn publishing(&self) -> bool {
        self.publisher_closer.borrow().is_some()
    }
}

impl Drop for WasmNode {
    fn drop(&mut self) {
        self.services_closer.close();
        if let Some(closer) = self.publisher_closer.borrow_mut().take() {
            closer.close();
        }
        let links = core::mem::take(&mut *self.links.borrow_mut());
        for link in links {
            link.disconnect();
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
    state: Rc<LinkState>,
    net_id: u16,
    kind: LinkKind,
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

    /// Frames dropped because a bounded packet-link buffer was full.
    #[wasm_bindgen(getter, js_name = overflowDrops)]
    pub fn overflow_drops(&self) -> u32 {
        self.impairment.overflow_drops.get()
    }

    /// Set artificial latency and loss for both directions of this link.
    #[wasm_bindgen(js_name = setImpairment)]
    pub fn set_impairment(&self, latency_ms: u32, loss_pct: u8) {
        self.impairment
            .latency_ms
            .set(latency_ms.min(MAX_LATENCY_MS));
        self.impairment.loss_pct.set(loss_pct.min(100));
    }

    pub fn disconnect(&self) {
        self.state.disconnect();
    }
}

impl Drop for WasmLink {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// Yield to the microtask queue once, letting freshly spawned tasks run.
async fn yield_now() {
    let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL)).await;
}
