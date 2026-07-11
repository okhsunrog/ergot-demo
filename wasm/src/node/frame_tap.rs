use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use ergot::{
    Address, HeaderSeq, ProtocolError,
    interface_manager::{
        Interface, InterfaceSink,
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

fn kind_name(kind: u8) -> String {
    match kind {
        1 => "req".into(),
        2 => "resp".into(),
        3 => "topic".into(),
        255 => "err".into(),
        other => other.to_string(),
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
    fn record(&self, hdr: &HeaderSeq) {
        let Some(binding) = self.label.borrow().clone() else {
            return;
        };
        let ev = FrameEvent {
            link_id: binding.label,
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

    fn send_ty<T: SerdeSerialize>(&mut self, hdr: &HeaderSeq, body: &T) -> Result<(), ()> {
        let result = match &mut self.inner {
            SinkInner::Stream(s) => s.send_ty(hdr, body),
            SinkInner::Packet(s) => s.send_ty(hdr, body),
        };
        if result.is_ok() {
            self.tap.record(hdr);
        }
        result
    }

    fn send_raw(&mut self, hdr: &HeaderSeq, body: &[u8]) -> Result<(), ()> {
        let result = match &mut self.inner {
            SinkInner::Stream(s) => s.send_raw(hdr, body),
            SinkInner::Packet(s) => s.send_raw(hdr, body),
        };
        if result.is_ok() {
            self.tap.record(hdr);
        }
        result
    }

    fn send_err(&mut self, hdr: &HeaderSeq, err: ProtocolError) -> Result<(), ()> {
        let result = match &mut self.inner {
            SinkInner::Stream(s) => s.send_err(hdr, err),
            SinkInner::Packet(s) => s.send_err(hdr, err),
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
