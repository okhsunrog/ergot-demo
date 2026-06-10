# ergot-demo

**Live demo: <https://okhsunrog.github.io/ergot-demo/>**

A browser playground for [ergot](https://github.com/jamesmunns/ergot) networks.
Draw a topology on a canvas and it runs for real: every node on the canvas is a
full ergot NetStack compiled to WebAssembly, links carry real ergot frames
(stream/COBS or packet flavor, per link), and pings route through actual
Router / DirectEdge profiles. A frame inspector shows every frame on the wire,
links animate with traffic, and per-link latency/loss knobs let you watch the
UDP-like semantics degrade honestly.

Built with Vue 3.6 (Vapor), Nuxt UI v4, Pinia, Vue Flow, and a Rust/WASM module
(`wasm/`), managed with [Vite+](https://viteplus.dev/).

## What works today

- **Live topology**: every canvas node is a full ergot NetStack (Router or
  DirectEdge profile) running in WASM; drawing/deleting nodes and links
  creates and tears down real stacks, transports, and routing state.
- **Two link kinds per ergot's interface flavors**: stream (byte pipe +
  COBS framing, serial/TCP-like, via the `futures_io` transport) and packet
  (one message = one frame, UDP/USB-like, via ergot's generic
  `PacketRxTxWorker`). One router can mix kinds across its downlinks.
- **Real routing**: each downlink gets its own network id from the Router
  profile; multi-hop edge → router → edge traffic works, addresses appear
  on node cards as edges learn them.
- **Bridge routers**: router ↔ bridge ↔ edge hierarchies with live
  seed-router net assignment — a bridge's downlinks stay pending until its
  uplink activates, then lease globally routed network ids from upstream
  and keep the lease refreshed (initial leases expire after 30 s; if
  refreshing fails repeatedly, the bridge re-leases from scratch).
- **Endpoints**: well-known ping with measured RTT between any two selected
  nodes.
- **Topics (pub/sub)**: nodes publish a sensor stream (broadcast or unicast);
  every node subscribes and charts received readings in a sparkline.
- **Frame inspector**: every frame on the wire is tapped at the sink (src →
  dst, kind, seq) and listed live; links animate while traffic flows.
- **Link impairment**: per-link latency and loss knobs. Packet links drop
  whole frames; stream links corrupt the COBS stream mid-frame and resync —
  the honest failure mode of each transport.
- **Runtime tests**: the WASM node API is tested in plain Node via `vp test`,
  no browser required; CI runs the full pipeline.

## Future plans

- **Web Serial**: wrap `SerialPort` streams into `futures_io::AsyncRead/Write`
  so the browser joins a _physical_ ergot network — an MCU on `/dev/ttyACM0`
  as a node on the canvas, zero install.
- **Web Bluetooth**: ergot over BLE (GATT characteristics as a packet
  transport) for wireless devices.
- **WebUSB**: bulk-endpoint transport in the spirit of ergot's `nusb` host
  support, for USB devices without a serial CDC interface.
- **Discovery panel**: `discover()` + `DeviceInfo` from any node, once
  ergot's discovery is freed from its tokio-only timeout (same
  sleeper-injection treatment the RX workers already got on the
  `wasm-support` branch).
- **Log viewer**: aggregate the well-known `ErgotFmtTx` log topic into a
  browser syslog panel.

## Prerequisites

- [Vite+](https://viteplus.dev/) (`vp`) and bun
- Rust with the `wasm32-unknown-unknown` target and `wasm-pack`
  (`cargo install wasm-pack`)

The ergot dependency is fetched straight from
[ergot main](https://github.com/jamesmunns/ergot) — no local checkout is
needed. To hack on ergot itself, use the `[patch]` override documented in
`wasm/Cargo.toml` (and set `cache: false` on the `wasm:build` task in
`vite.config.ts` so rebuilds pick up your local changes).

## Develop

```sh
vp install
vp run wasm:build   # build the Rust → WASM module (needed once; cached after)
vp dev              # dev server with wasm hot-rebuild
```

Editing `.rs` files under `wasm/` (or the ergot worktree) triggers a wasm
rebuild; the page reloads automatically.

## Checks

```sh
vp test             # runtime tests of the WASM node API (plain Node, no browser)
vp check            # format, lint, type checks
vp run typecheck    # vue-tsc only
vp build            # production build
```
