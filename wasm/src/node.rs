//! Handle-based WASM API: nodes, links, and per-node services.
//!
//! A [`WasmNode`] owns one ergot NetStack with a profile chosen at
//! construction: a `Router` (many downlinks, each its own network segment)
//! or a DirectEdge `Edge` (single uplink). [`WasmNode::connect_to`] wires a
//! router to an edge with an in-memory duplex pipe and spawns the transport
//! workers; the returned [`WasmLink`] tears everything down on
//! `disconnect()`/`free()`. Freeing a node stops its service tasks and all
//! attached links.

use std::cell::RefCell;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use gloo_timers::future::TimeoutFuture;
use maitake_sync::WaitQueue;
use serde::Serialize;
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use ergot::{
    Address,
    interface_manager::{
        Interface, InterfaceState, Profile,
        profiles::{
            direct_edge::{DirectEdge, EDGE_NODE_ID, EdgeFrameProcessor},
            router::{Router, RouterFrameProcessor},
        },
        transports::futures_io::{RxWorker, tx_worker},
        utils::{
            cobs_stream,
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

struct WasmInterface;
impl Interface for WasmInterface {
    type Sink = cobs_stream::Sink<StdQueue>;
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

/// Which ergot profile a node runs.
#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeProfile {
    /// Router profile: many downlinks, each assigned its own network id.
    Router,
    /// DirectEdge target: a single uplink to a router.
    Edge,
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

/// One ergot node (a full NetStack) living in this browser tab.
#[wasm_bindgen]
pub struct WasmNode {
    stack: Stack,
    profile: NodeProfile,
    /// Closes node-owned service tasks (ping server, ...) on drop.
    services_closer: Arc<WaitQueue>,
    /// Closers of currently attached links. Shared with [`WasmLink`]s.
    links: Rc<RefCell<Vec<Arc<WaitQueue>>>>,
}

#[wasm_bindgen]
impl WasmNode {
    #[wasm_bindgen(constructor)]
    pub fn new(profile: NodeProfile) -> WasmNode {
        let stack = match profile {
            NodeProfile::Router => {
                Stack::Router(RouterStack::new_with_profile(Router::new_std()))
            }
            NodeProfile::Edge => {
                let queue = new_std_queue(QUEUE_SIZE);
                let sink = cobs_stream::Sink::new_from_handle(queue.clone(), MTU);
                Stack::Edge {
                    stack: EdgeStack::new_with_profile(DirectEdge::new_target(sink)),
                    queue,
                }
            }
        };
        WasmNode {
            stack,
            profile,
            services_closer: Arc::new(WaitQueue::new()),
            links: Rc::new(RefCell::new(Vec::new())),
        }
    }

    #[wasm_bindgen(getter)]
    pub fn profile(&self) -> NodeProfile {
        self.profile
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

    /// Connect this node (router) to an edge node with an in-memory duplex
    /// pipe. The router assigns the link its own network id.
    #[wasm_bindgen(js_name = connectTo)]
    pub fn connect_to(&self, target: &WasmNode) -> Result<WasmLink, JsError> {
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

        let closer = Arc::new(WaitQueue::new());

        // Two unidirectional pipes: router→edge and edge→router
        let (router_writer, edge_reader) = duplex::pipe();
        let (edge_writer, router_reader) = duplex::pipe();

        // Router side: register a new interface; the profile assigns a net id.
        let router_queue = new_std_queue(QUEUE_SIZE);
        let sink = cobs_stream::Sink::new_from_handle(router_queue.clone(), MTU);
        let res = router.manage_profile(|im| {
            let ident = im.register_interface(sink).ok()?;
            let net_id = im.net_id_of(ident)?;
            im.set_interface_closer(ident, closer.clone());
            Some((ident, net_id))
        });
        let Some((ident, net_id)) = res else {
            return Err(JsError::new("router has no free interface slots"));
        };

        // Router-side workers
        let rx_router = router.clone();
        let rx_closer = closer.clone();
        spawn_local(async move {
            let mut rx_worker = RxWorker::new(
                rx_router.clone(),
                router_reader,
                RouterFrameProcessor::new(net_id),
                ident,
            )
            .with_closer(rx_closer.clone());
            let mut frame = vec![0u8; BUF_SIZE];
            let mut scratch = vec![0u8; BUF_SIZE];
            let _ = rx_worker.run(&mut frame, &mut scratch).await;
            rx_closer.close();
            drop(rx_worker);
            rx_router.manage_profile(|im| {
                let _ = im.deregister_interface(ident);
            });
        });
        spawn_tx_worker(router_writer, router_queue, closer.clone());

        // Edge side: single DirectEdge interface, discovers net id from frames.
        if let Err(e) = spawn_edge_side(
            edge.clone(),
            edge_reader,
            edge_writer,
            edge_queue.clone(),
            closer.clone(),
        ) {
            // Roll back the router-side registration.
            closer.close();
            router.manage_profile(|im| {
                let _ = im.deregister_interface(ident);
            });
            return Err(e);
        }

        self.links.borrow_mut().push(closer.clone());
        target.links.borrow_mut().push(closer.clone());

        Ok(WasmLink {
            closer,
            net_id,
            ends: [self.links.clone(), target.links.clone()],
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

/// A live link between a router and an edge node. `disconnect()` (or
/// `free()`) tears down the transport workers on both sides; the edge
/// returns to Down and the router frees the interface slot.
#[wasm_bindgen]
pub struct WasmLink {
    closer: Arc<WaitQueue>,
    net_id: u16,
    ends: [Rc<RefCell<Vec<Arc<WaitQueue>>>>; 2],
}

#[wasm_bindgen]
impl WasmLink {
    /// The network id the router assigned to this link.
    #[wasm_bindgen(getter, js_name = netId)]
    pub fn net_id(&self) -> u16 {
        self.net_id
    }

    pub fn disconnect(&self) {
        self.closer.close();
        for end in &self.ends {
            end.borrow_mut().retain(|c| !Arc::ptr_eq(c, &self.closer));
        }
    }
}

impl Drop for WasmLink {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// Set up the edge side of a link: mark the interface active and spawn the
/// RX/TX workers, all tied to `closer`.
fn spawn_edge_side(
    stack: EdgeStack,
    reader: duplex::PipeReader,
    writer: duplex::PipeWriter,
    queue: StdQueue,
    closer: Arc<WaitQueue>,
) -> Result<(), JsError> {
    stack.manage_profile(|im| {
        match im.interface_state(()) {
            Some(InterfaceState::Down) | None => {}
            _ => return Err(JsError::new("edge interface is already in use")),
        }
        im.set_closer(closer.clone());
        im.set_interface_state(
            (),
            InterfaceState::Active {
                net_id: 0,
                node_id: EDGE_NODE_ID,
            },
        )
        .map_err(|e| JsError::new(&format!("failed to set interface state: {e:?}")))?;
        Ok(())
    })?;

    let rx_closer = closer.clone();
    spawn_local(async move {
        let mut rx_worker = RxWorker::new(stack, reader, EdgeFrameProcessor::new(), ())
            .with_closer(rx_closer.clone());
        let mut frame = vec![0u8; BUF_SIZE];
        let mut scratch = vec![0u8; BUF_SIZE];
        let _ = rx_worker.run(&mut frame, &mut scratch).await;
        // Ensure the TX worker (and the peer's workers) stop too.
        rx_closer.close();
    });
    spawn_tx_worker(writer, queue, closer);

    Ok(())
}

fn spawn_tx_worker(writer: duplex::PipeWriter, queue: StdQueue, closer: Arc<WaitQueue>) {
    spawn_local(async move {
        let consumer = queue.stream_consumer();
        let mut writer = writer;
        let _ = select(tx_worker(&mut writer, consumer), closer.wait()).await;
        closer.close();
    });
}

/// Yield to the microtask queue once, letting freshly spawned tasks run.
async fn yield_now() {
    let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL)).await;
}
