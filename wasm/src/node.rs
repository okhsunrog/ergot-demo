//! Handle-based WASM API: nodes, links, and per-node services.
//!
//! A [`WasmNode`] owns one ergot NetStack. [`WasmNode::connect_to`] wires a
//! controller node to a target node with an in-memory duplex pipe and spawns
//! the transport workers; the returned [`WasmLink`] tears everything down on
//! `disconnect()`/`free()`. Freeing a node stops its service tasks and any
//! attached link.

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
        profiles::direct_edge::{CENTRAL_NODE_ID, DirectEdge, EDGE_NODE_ID, EdgeFrameProcessor},
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

struct WasmInterface;
impl Interface for WasmInterface {
    type Sink = cobs_stream::Sink<StdQueue>;
}

type EdgeStack = ArcNetStack<CriticalSectionRawMutex, DirectEdge<WasmInterface>>;

/// Which side of a DirectEdge link a node plays.
#[wasm_bindgen]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NodeRole {
    /// Link controller: owns the network id of the link (router/PC side).
    Controller,
    /// Link target: discovers its network id from the controller (device side).
    Target,
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

/// Current state of a node's (single) interface.
#[derive(Serialize, Tsify)]
#[tsify(into_wasm_abi)]
#[serde(tag = "status", rename_all = "lowercase", rename_all_fields = "camelCase")]
pub enum LinkStatus {
    Down,
    Inactive,
    Active { net_id: u16, node_id: u8 },
}

/// One ergot node (a full NetStack) living in this browser tab.
#[wasm_bindgen]
pub struct WasmNode {
    stack: EdgeStack,
    queue: StdQueue,
    role: NodeRole,
    /// Closes node-owned service tasks (ping server, ...) on drop.
    services_closer: Arc<WaitQueue>,
    /// Closer of the currently attached link, if any. Shared with [`WasmLink`].
    link: Rc<RefCell<Option<Arc<WaitQueue>>>>,
}

#[wasm_bindgen]
impl WasmNode {
    #[wasm_bindgen(constructor)]
    pub fn new(role: NodeRole) -> WasmNode {
        let queue = new_std_queue(QUEUE_SIZE);
        let sink = cobs_stream::Sink::new_from_handle(queue.clone(), MTU);
        let stack = match role {
            NodeRole::Controller => EdgeStack::new_with_profile(DirectEdge::new_controller(
                sink,
                InterfaceState::Down,
            )),
            NodeRole::Target => EdgeStack::new_with_profile(DirectEdge::new_target(sink)),
        };
        WasmNode {
            stack,
            queue,
            role,
            services_closer: Arc::new(WaitQueue::new()),
            link: Rc::new(RefCell::new(None)),
        }
    }

    #[wasm_bindgen(getter)]
    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// Is a link currently attached?
    #[wasm_bindgen(getter)]
    pub fn linked(&self) -> bool {
        self.link.borrow().is_some()
    }

    /// Current interface state of this node.
    #[wasm_bindgen(js_name = linkStatus)]
    pub fn link_status(&self) -> LinkStatus {
        match self.stack.manage_profile(|im| im.interface_state(())) {
            Some(InterfaceState::Active { net_id, node_id }) => {
                LinkStatus::Active { net_id, node_id }
            }
            Some(InterfaceState::Inactive) => LinkStatus::Inactive,
            _ => LinkStatus::Down,
        }
    }

    /// Connect this node (controller) to a target node with an in-memory
    /// duplex pipe. `net_id` is the network id of the link (default 1).
    #[wasm_bindgen(js_name = connectTo)]
    pub fn connect_to(&self, target: &WasmNode, net_id: Option<u16>) -> Result<WasmLink, JsError> {
        if self.role != NodeRole::Controller || target.role != NodeRole::Target {
            return Err(JsError::new(
                "connectTo must be called as controller.connectTo(target)",
            ));
        }
        if self.link.borrow().is_some() || target.link.borrow().is_some() {
            return Err(JsError::new("node is already linked"));
        }
        let net_id = net_id.unwrap_or(1);
        let closer = Arc::new(WaitQueue::new());

        // Two unidirectional pipes: ctrl→tgt and tgt→ctrl
        let (ctrl_writer, tgt_reader) = duplex::pipe();
        let (tgt_writer, ctrl_reader) = duplex::pipe();

        spawn_link_side(
            self.stack.clone(),
            ctrl_reader,
            ctrl_writer,
            self.queue.clone(),
            EdgeFrameProcessor::new_controller(net_id),
            InterfaceState::Active {
                net_id,
                node_id: CENTRAL_NODE_ID,
            },
            closer.clone(),
        )?;
        spawn_link_side(
            target.stack.clone(),
            tgt_reader,
            tgt_writer,
            target.queue.clone(),
            EdgeFrameProcessor::new(),
            InterfaceState::Active {
                net_id: 0,
                node_id: EDGE_NODE_ID,
            },
            closer.clone(),
        )?;

        *self.link.borrow_mut() = Some(closer.clone());
        *target.link.borrow_mut() = Some(closer.clone());

        Ok(WasmLink {
            closer,
            ends: [self.link.clone(), target.link.clone()],
        })
    }

    /// Attach a ping server (well-known `ErgotPingEndpoint`, name "ping").
    /// It serves until the node is freed. Resolves once the server is
    /// attached and ready.
    #[wasm_bindgen(js_name = servePing)]
    pub async fn serve_ping(&self) {
        let stack = self.stack.clone();
        let closer = self.services_closer.clone();
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
            self.stack
                .endpoints()
                .request::<ErgotPingEndpoint>(addr, &42u32, Some("ping"))
                .await
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
        if let Some(closer) = self.link.borrow_mut().take() {
            closer.close();
        }
    }
}

/// A live link between two nodes. `disconnect()` (or `free()`) tears down
/// the transport workers on both sides; the nodes return to Down and can
/// be reconnected.
#[wasm_bindgen]
pub struct WasmLink {
    closer: Arc<WaitQueue>,
    ends: [Rc<RefCell<Option<Arc<WaitQueue>>>>; 2],
}

#[wasm_bindgen]
impl WasmLink {
    pub fn disconnect(&self) {
        self.closer.close();
        for end in &self.ends {
            *end.borrow_mut() = None;
        }
    }
}

impl Drop for WasmLink {
    fn drop(&mut self) {
        self.disconnect();
    }
}

/// Set up one side of a link: mark the interface active and spawn the
/// RX/TX workers, all tied to `closer`.
fn spawn_link_side(
    stack: EdgeStack,
    reader: duplex::PipeReader,
    writer: duplex::PipeWriter,
    queue: StdQueue,
    processor: EdgeFrameProcessor,
    initial_state: InterfaceState,
    closer: Arc<WaitQueue>,
) -> Result<(), JsError> {
    stack.manage_profile(|im| {
        match im.interface_state(()) {
            Some(InterfaceState::Down) | None => {}
            _ => return Err(JsError::new("interface is already in use")),
        }
        im.set_closer(closer.clone());
        im.set_interface_state((), initial_state)
            .map_err(|e| JsError::new(&format!("failed to set interface state: {e:?}")))?;
        Ok(())
    })?;

    let rx_closer = closer.clone();
    spawn_local(async move {
        let mut rx_worker =
            RxWorker::new(stack, reader, processor, ()).with_closer(rx_closer.clone());
        let mut frame = vec![0u8; BUF_SIZE];
        let mut scratch = vec![0u8; BUF_SIZE];
        let _ = rx_worker.run(&mut frame, &mut scratch).await;
        // Ensure the TX worker (and the peer's workers) stop too.
        rx_closer.close();
    });

    spawn_local(async move {
        let consumer = queue.stream_consumer();
        let mut writer = writer;
        let _ = select(tx_worker(&mut writer, consumer), closer.wait()).await;
        closer.close();
    });

    Ok(())
}

/// Yield to the microtask queue once, letting freshly spawned tasks run.
async fn yield_now() {
    let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL)).await;
}
