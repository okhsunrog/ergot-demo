//! Shared packet bus for the browser demo.
//!
//! One root-router interface and multiple DirectEdge devices share a single
//! broadcast medium and therefore one network id. Edge devices claim unique
//! node ids through ergot's address-claim protocol.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use embassy_futures::select::{Either, select};
use ergot::interface_manager::transports::packet::PacketSender;
use ergot::interface_manager::{InterfaceState, Profile};
use ergot::net_stack::services::{bus_claim_refresh, bus_claim_with_retry};
use futures_channel::mpsc::{Sender as MpscSender, channel};
use gloo_timers::future::TimeoutFuture;
use maitake_sync::WaitQueue;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use super::frame_tap::{Tap, TapBinding, TapLabel, new_sink, next_link_generation};
use super::impairment::Impairment;
use super::transport::{LinkClosed, StackSide, spawn_packet_worker};
use super::{LinkKind, LinkState, PACKET_QUEUE_FRAMES, QUEUE_SIZE, Stack, WasmLink, WasmNode};

struct BusMember {
    id: u64,
    inbox: MpscSender<Vec<u8>>,
}

struct BusState {
    members: RefCell<Vec<BusMember>>,
    links: super::LinkList,
    next_member: Cell<u64>,
    router_member: Cell<Option<u64>>,
    net_id: Cell<u16>,
    impairment: Impairment,
}

impl BusState {
    fn new() -> Self {
        Self {
            members: RefCell::new(Vec::new()),
            links: Rc::new(RefCell::new(Vec::new())),
            next_member: Cell::new(1),
            router_member: Cell::new(None),
            net_id: Cell::new(0),
            impairment: Impairment::default(),
        }
    }

    fn add_member(&self, inbox: MpscSender<Vec<u8>>) -> u64 {
        let id = self.next_member.get();
        self.next_member.set(id.wrapping_add(1).max(1));
        self.members.borrow_mut().push(BusMember { id, inbox });
        id
    }

    fn remove_member(&self, id: u64) {
        self.members.borrow_mut().retain(|member| member.id != id);
        if self.router_member.get() == Some(id) {
            self.router_member.set(None);
            self.net_id.set(0);
        }
    }
}

struct BusTx {
    bus: Weak<BusState>,
    member_id: u64,
    impairment: Impairment,
    closer: Arc<WaitQueue>,
}

impl PacketSender for BusTx {
    type Error = LinkClosed;

    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        let Some(bus) = self.bus.upgrade() else {
            return Err(LinkClosed);
        };
        match self.impairment.delay_and_drop(&self.closer).await {
            Some(true) => return Ok(()),
            Some(false) => {}
            None => return Err(LinkClosed),
        }

        let mut sender_is_attached = false;
        bus.members.borrow_mut().retain_mut(|member| {
            if member.id == self.member_id {
                sender_is_attached = true;
                return true;
            }
            match member.inbox.try_send(data.to_vec()) {
                Ok(()) => true,
                Err(err) if err.is_full() => {
                    self.impairment
                        .overflow_drops
                        .set(self.impairment.overflow_drops.get().saturating_add(1));
                    true
                }
                Err(_) => false,
            }
        });

        sender_is_attached.then_some(()).ok_or(LinkClosed)
    }
}

/// A simulated shared packet medium. Attach one root router, then any number
/// of packet edge nodes. All members hear every frame sent by another member.
#[wasm_bindgen]
pub struct WasmBus {
    state: Rc<BusState>,
}

#[wasm_bindgen]
impl WasmBus {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            state: Rc::new(BusState::new()),
        }
    }

    #[wasm_bindgen(getter, js_name = routerAttached)]
    pub fn router_attached(&self) -> bool {
        self.state.router_member.get().is_some()
    }

    #[wasm_bindgen(getter, js_name = routerFree)]
    pub fn router_free(&self) -> bool {
        !self.router_attached()
    }

    #[wasm_bindgen(getter, js_name = netId)]
    pub fn net_id(&self) -> u16 {
        self.state.net_id.get()
    }

    #[wasm_bindgen(getter, js_name = memberCount)]
    pub fn member_count(&self) -> usize {
        self.state.members.borrow().len()
    }

    /// Register the bus as one packet interface of a root Router.
    #[wasm_bindgen(js_name = attachRouter)]
    pub fn attach_router(
        &self,
        parent: &WasmNode,
        link_id: Option<String>,
    ) -> Result<WasmLink, JsError> {
        if parent.profile != super::NodeProfile::Router {
            return Err(JsError::new(
                "a shared bus must be controlled by a root Router",
            ));
        }
        if self.router_attached() {
            return Err(JsError::new("this bus already has a router"));
        }
        let Stack::Router(stack) = &parent.stack else {
            return Err(JsError::new("bus controller is not a root Router"));
        };

        let closer = Arc::new(WaitQueue::new());
        let generation = next_link_generation();
        let binding = link_id.map(|label| TapBinding { generation, label });
        let tap_label: TapLabel = Rc::new(RefCell::new(binding));
        let queue = super::new_std_queue(QUEUE_SIZE);
        let registered = stack.manage_profile(|profile| {
            let sink = new_sink(
                LinkKind::Packet,
                &queue,
                Tap {
                    label: tap_label.clone(),
                    dir: "down",
                },
            );
            let ident = profile.register_interface(sink).ok()?;
            let net_id = profile.net_id_of(ident)?;
            let state = profile.interface_state(ident)?;
            profile.set_interface_closer(ident, closer.clone());
            Some((ident, net_id, state))
        });
        let Some((ident, net_id, initial_state)) = registered else {
            return Err(JsError::new("router has no free interface slots"));
        };

        let (inbox, rx) = channel(PACKET_QUEUE_FRAMES);
        let member_id = self.state.add_member(inbox);
        self.state.router_member.set(Some(member_id));
        self.state.net_id.set(net_id);

        let tx = BusTx {
            bus: Rc::downgrade(&self.state),
            member_id,
            impairment: self.state.impairment.clone(),
            closer: closer.clone(),
        };
        spawn_packet_worker(
            StackSide::RouterDown(stack.clone(), ident, net_id),
            rx,
            tx,
            queue,
            initial_state,
            closer.clone(),
        );

        Ok(self.finish_attachment(
            parent,
            member_id,
            closer,
            vec![tap_label],
            generation,
            net_id,
        ))
    }

    /// Attach one packet DirectEdge device and start its address-claim lease.
    #[wasm_bindgen(js_name = attachEdge)]
    pub fn attach_edge(
        &self,
        target: &WasmNode,
        link_id: Option<String>,
    ) -> Result<WasmLink, JsError> {
        if !self.router_attached() {
            return Err(JsError::new("connect a root Router to the bus first"));
        }
        if target.profile != super::NodeProfile::Edge {
            return Err(JsError::new("only Edge nodes can join a shared bus"));
        }
        if target.link_kind != LinkKind::Packet {
            return Err(JsError::new(
                "shared-bus devices must use the Packet transport",
            ));
        }
        if !target.uplink_free() {
            return Err(JsError::new("target node is already linked upstream"));
        }
        let Stack::Edge { stack, queue } = &target.stack else {
            return Err(JsError::new("bus target is not an Edge node"));
        };

        let closer = Arc::new(WaitQueue::new());
        let generation = next_link_generation();
        let binding = link_id.map(|label| TapBinding { generation, label });
        *target.uplink_tap_label.borrow_mut() = binding;
        let initial_state = InterfaceState::Active {
            net_id: 0,
            node_id: target.bus_candidate,
        };
        let setup = stack.manage_profile(|profile| {
            profile.set_closer(closer.clone());
            profile.set_interface_state((), initial_state)
        });
        if let Err(err) = setup {
            *target.uplink_tap_label.borrow_mut() = None;
            return Err(JsError::new(&format!(
                "failed to initialize bus device: {err:?}"
            )));
        }

        let (inbox, rx) = channel(PACKET_QUEUE_FRAMES);
        let member_id = self.state.add_member(inbox);
        let tx = BusTx {
            bus: Rc::downgrade(&self.state),
            member_id,
            impairment: self.state.impairment.clone(),
            closer: closer.clone(),
        };
        spawn_packet_worker(
            StackSide::BusEdge(stack.clone()),
            rx,
            tx,
            queue.clone(),
            initial_state,
            closer.clone(),
        );
        spawn_bus_claim(
            stack.clone(),
            target.bus_candidate,
            target.bus_nonce,
            closer.clone(),
        );

        Ok(self.finish_attachment(
            target,
            member_id,
            closer,
            vec![target.uplink_tap_label.clone()],
            generation,
            self.net_id(),
        ))
    }

    fn finish_attachment(
        &self,
        node: &WasmNode,
        member_id: u64,
        closer: Arc<WaitQueue>,
        tap_labels: Vec<TapLabel>,
        generation: u64,
        net_id: u16,
    ) -> WasmLink {
        let weak_bus = Rc::downgrade(&self.state);
        let state = Rc::new(LinkState {
            closer: closer.clone(),
            disconnected: Cell::new(false),
            ends: vec![Rc::downgrade(&self.state.links), Rc::downgrade(&node.links)],
            tap_labels,
            generation,
            on_disconnect: Some(Box::new(move || {
                if let Some(bus) = weak_bus.upgrade() {
                    bus.remove_member(member_id);
                }
            })),
        });
        self.state.links.borrow_mut().push(state.clone());
        node.links.borrow_mut().push(state.clone());

        let weak_state = Rc::downgrade(&state);
        spawn_local(async move {
            let _ = closer.wait().await;
            if let Some(state) = weak_state.upgrade() {
                state.disconnect();
            }
        });

        WasmLink {
            state,
            net_id,
            kind: LinkKind::Packet,
            impairment: self.state.impairment.clone(),
        }
    }
}

impl Default for WasmBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WasmBus {
    fn drop(&mut self) {
        let links = core::mem::take(&mut *self.state.links.borrow_mut());
        for link in links {
            link.disconnect();
        }
    }
}

fn spawn_bus_claim(stack: super::EdgeStack, candidate: u8, nonce: u64, closer: Arc<WaitQueue>) {
    async fn closed_within(closer: &WaitQueue, ms: u32) -> bool {
        matches!(
            select(TimeoutFuture::new(ms), closer.wait()).await,
            Either::Second(_)
        )
    }

    spawn_local(async move {
        'claim: loop {
            let candidates = (candidate..=254).chain(3..candidate);
            let claim = bus_claim_with_retry(&stack, (), candidates, nonce);
            let lease = match select(claim, select(TimeoutFuture::new(1_500), closer.wait())).await
            {
                Either::First(Ok(lease)) => lease,
                Either::First(Err(err)) => {
                    log::warn!("bus address claim failed: {err:?}");
                    if closed_within(&closer, 250).await {
                        return;
                    }
                    continue;
                }
                Either::Second(Either::First(())) => {
                    log::warn!("bus address claim timed out; retrying");
                    continue;
                }
                Either::Second(Either::Second(_)) => return,
            };

            let mut lease = lease;
            let mut failures = 0u8;
            loop {
                let delay_s = if failures == 0 {
                    u32::from(
                        lease
                            .expires_seconds
                            .saturating_sub(lease.min_refresh_seconds),
                    )
                    .max(1)
                } else {
                    2
                };
                if closed_within(&closer, delay_s * 1_000).await {
                    return;
                }

                let refresh = bus_claim_refresh(&stack, &lease);
                match select(refresh, select(TimeoutFuture::new(1_500), closer.wait())).await {
                    Either::First(Ok(refreshed)) => {
                        lease = refreshed;
                        failures = 0;
                    }
                    Either::Second(Either::Second(_)) => return,
                    Either::First(Err(err)) => {
                        failures += 1;
                        log::warn!("bus address refresh failed ({failures}x): {err:?}");
                    }
                    Either::Second(Either::First(())) => {
                        failures += 1;
                        log::warn!("bus address refresh timed out ({failures}x)");
                    }
                }
                if failures >= 3 {
                    let _ = stack.manage_profile(|profile| {
                        profile.set_interface_state(
                            (),
                            InterfaceState::Active {
                                net_id: 0,
                                node_id: candidate,
                            },
                        )
                    });
                    continue 'claim;
                }
            }
        }
    });
}
