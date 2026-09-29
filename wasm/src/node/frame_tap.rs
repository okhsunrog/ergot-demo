use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use ergot::{
    Address, FrameKind, Header, ProtocolError, TrafficClass,
    interface_manager::{
        Interface, InterfaceSink, LinkMeta,
        utils::{cobs_stream, framed_stream, std::StdQueue},
    },
};
use serde::{Serialize, Serialize as SerdeSerialize};
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;

use super::{LinkKind, MTU};

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
    /// "req" | "resp" | "topic" | "err".
    pub kind: String,
    /// Traffic class: "control" | "normal" | "bulk" | "background".
    pub class: String,
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
    static NEXT_LINK_GENERATION: Cell<u64> = const { Cell::new(1) };
}

pub(super) fn next_link_generation() -> u64 {
    NEXT_LINK_GENERATION.with(|next| {
        let generation = next.get();
        next.set(generation.wrapping_add(1).max(1));
        generation
    })
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

fn kind_name(kind: FrameKind) -> String {
    match kind {
        FrameKind::ENDPOINT_REQ => "req".into(),
        FrameKind::ENDPOINT_RESP => "resp".into(),
        FrameKind::TOPIC_MSG => "topic".into(),
        FrameKind::PROTOCOL_ERROR => "err".into(),
        FrameKind(other) => other.to_string(),
    }
}

fn class_name(class: TrafficClass) -> &'static str {
    match class {
        TrafficClass::Control => "control",
        TrafficClass::Normal => "normal",
        TrafficClass::Bulk => "bulk",
        TrafficClass::Background => "background",
    }
}

#[derive(Clone)]
pub(super) struct TapBinding {
    pub(super) generation: u64,
    pub(super) label: String,
}

pub(super) type TapLabel = Rc<RefCell<Option<TapBinding>>>;

/// A tap attached to one interface sink. `label` identifies the canvas edge
/// currently served by the interface (None while disconnected).
#[derive(Clone)]
pub(super) struct Tap {
    pub(super) label: TapLabel,
    pub(super) dir: &'static str,
}

impl Tap {
    fn record(&self, hdr: &Header) {
        let Some(binding) = self.label.borrow().clone() else {
            return;
        };
        let ev = FrameEvent {
            link_id: binding.label,
            dir: self.dir.into(),
            src: fmt_addr(&hdr.src),
            dst: fmt_addr(&hdr.dst),
            kind: kind_name(hdr.kind),
            class: class_name(hdr.class).into(),
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

enum SinkInner {
    Stream(cobs_stream::Sink<StdQueue>),
    Packet(framed_stream::Sink<StdQueue>),
}

pub(super) struct WasmSink {
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

    fn send_ty<T: SerdeSerialize>(
        &mut self,
        link: &LinkMeta,
        hdr: &Header,
        body: &T,
    ) -> Result<(), ()> {
        let result = match &mut self.inner {
            SinkInner::Stream(s) => s.send_ty(link, hdr, body),
            SinkInner::Packet(s) => s.send_ty(link, hdr, body),
        };
        if result.is_ok() {
            self.tap.record(hdr);
        }
        result
    }

    fn send_raw(&mut self, link: &LinkMeta, hdr: &Header, body: &[u8]) -> Result<(), ()> {
        let result = match &mut self.inner {
            SinkInner::Stream(s) => s.send_raw(link, hdr, body),
            SinkInner::Packet(s) => s.send_raw(link, hdr, body),
        };
        if result.is_ok() {
            self.tap.record(hdr);
        }
        result
    }

    fn send_err(&mut self, link: &LinkMeta, hdr: &Header, err: ProtocolError) -> Result<(), ()> {
        let result = match &mut self.inner {
            SinkInner::Stream(s) => s.send_err(link, hdr, err),
            SinkInner::Packet(s) => s.send_err(link, hdr, err),
        };
        if result.is_ok() {
            self.tap.record(hdr);
        }
        result
    }
}

pub(super) struct WasmInterface;
impl Interface for WasmInterface {
    type Sink = WasmSink;
}

pub(super) fn new_sink(kind: LinkKind, queue: &StdQueue, tap: Tap) -> WasmSink {
    let inner = match kind {
        LinkKind::Stream => {
            SinkInner::Stream(cobs_stream::Sink::new_from_handle(queue.clone(), MTU))
        }
        LinkKind::Packet => {
            SinkInner::Packet(framed_stream::Sink::new_from_handle(queue.clone(), MTU))
        }
    };
    WasmSink { inner, tap }
}
