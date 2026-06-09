mod duplex;

use std::pin::pin;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use ergot::{
    Address,
    interface_manager::{
        InterfaceState,
        profiles::direct_edge::{CENTRAL_NODE_ID, DirectEdge, EDGE_NODE_ID, EdgeFrameProcessor},
        utils::{cobs_stream, std::new_std_queue},
    },
    net_stack::ArcNetStack,
    well_known::ErgotPingEndpoint,
};
use mutex::raw_impls::cs::CriticalSectionRawMutex;

use ergot::interface_manager::{Interface, Profile};

struct WasmInterface;
impl Interface for WasmInterface {
    type Sink = cobs_stream::Sink<ergot::interface_manager::utils::std::StdQueue>;
}

type EdgeStack = ArcNetStack<CriticalSectionRawMutex, DirectEdge<WasmInterface>>;

fn new_controller_stack(queue: &ergot::interface_manager::utils::std::StdQueue, mtu: u16) -> EdgeStack {
    EdgeStack::new_with_profile(DirectEdge::new_controller(
        cobs_stream::Sink::new_from_handle(queue.clone(), mtu),
        InterfaceState::Down,
    ))
}

fn new_target_stack(queue: &ergot::interface_manager::utils::std::StdQueue, mtu: u16) -> EdgeStack {
    EdgeStack::new_with_profile(DirectEdge::new_target(
        cobs_stream::Sink::new_from_handle(queue.clone(), mtu),
    ))
}

/// Register a futures-io transport on an edge stack.
/// Spawns RX and TX workers using wasm_bindgen_futures::spawn_local.
fn register_edge(
    stack: EdgeStack,
    reader: duplex::PipeReader,
    writer: duplex::PipeWriter,
    queue: ergot::interface_manager::utils::std::StdQueue,
    processor: EdgeFrameProcessor,
    initial_state: InterfaceState,
) {
    // Set interface state synchronously before spawning workers; the
    // RxWorker only manages the Down transition on exit.
    stack.manage_profile(|im| {
        im.set_interface_state((), initial_state).expect("failed to set state");
    });

    // RX worker
    let rx_stack = stack.clone();
    spawn_local(async move {
        log::info!("[rx] worker started");
        let mut rx_worker = ergot::interface_manager::transports::futures_io::RxWorker::new(
            rx_stack,
            reader,
            processor,
            (),
        );
        let mut frame_buf = vec![0u8; 2048];
        let mut scratch_buf = vec![0u8; 2048];
        let _ = rx_worker.run(&mut frame_buf, &mut scratch_buf).await;
    });

    // TX worker
    spawn_local(async move {
        log::info!("[tx] worker started");
        let consumer = queue.stream_consumer();
        let mut writer = writer;
        let _ = ergot::interface_manager::transports::futures_io::tx_worker(&mut writer, consumer).await;
        log::info!("[tx] worker exited");
    });
}

/// Smoke test: create two connected ergot nodes and ping between them.
#[wasm_bindgen]
pub async fn ergot_ping_test() -> String {
    let _ = console_log::init_with_level(log::Level::Trace);
    log::info!("ergot_ping_test: starting");

    let mtu: u16 = 512;

    // Create two unidirectional pipes (ctrl→tgt and tgt→ctrl)
    let (ctrl_writer, tgt_reader) = duplex::pipe();
    let (tgt_writer, ctrl_reader) = duplex::pipe();

    // Controller (router side)
    let ctrl_queue = new_std_queue(4096);
    let ctrl_stack = new_controller_stack(&ctrl_queue, mtu);

    // Target (device side)
    let tgt_queue = new_std_queue(4096);
    let tgt_stack = new_target_stack(&tgt_queue, mtu);

    log::info!("ergot_ping_test: registering transports");

    // Register transports
    register_edge(
        ctrl_stack.clone(),
        ctrl_reader,
        ctrl_writer,
        ctrl_queue,
        EdgeFrameProcessor::new_controller(1),
        InterfaceState::Active {
            net_id: 1,
            node_id: CENTRAL_NODE_ID,
        },
    );

    register_edge(
        tgt_stack.clone(),
        tgt_reader,
        tgt_writer,
        tgt_queue,
        EdgeFrameProcessor::new(),
        InterfaceState::Active {
            net_id: 0,
            node_id: EDGE_NODE_ID,
        },
    );

    log::info!("ergot_ping_test: transports registered, spawning ping server");

    // Spawn ping server on target
    spawn_local(async move {
        let server = tgt_stack
            .endpoints()
            .bounded_server::<ErgotPingEndpoint, 4>(Some("ping"));
        let server = pin!(server);
        let mut hdl = server.attach();
        loop {
            let _ = hdl
                .serve(|val: &u32| {
                    let v = *val;
                    async move { v }
                })
                .await;
        }
    });

    // Yield to let spawned tasks (ping server, workers) register
    wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL)).await.unwrap();

    log::info!("ergot_ping_test: sending ping...");

    // Ping the target
    let addr = Address {
        network_id: 1,
        node_id: 2,
        port_id: 0,
    };

    let result = ctrl_stack
        .endpoints()
        .request::<ErgotPingEndpoint>(addr, &42u32, Some("ping"))
        .await;

    match result {
        Ok(v) => format!("Ping OK: {v}"),
        Err(e) => format!("Ping failed: {e:?}"),
    }
}
