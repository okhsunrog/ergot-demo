# ergot-demo

A browser playground for [ergot](https://github.com/jamesmunns/ergot) networks.
Draw a topology on a canvas and it runs for real: every node on the canvas is a
full ergot NetStack compiled to WebAssembly, links carry real ergot frames
(stream/COBS or packet flavor, per link), and pings route through actual
Router / DirectEdge profiles. A frame inspector shows every frame on the wire,
links animate with traffic, and per-link latency/loss knobs let you watch the
UDP-like semantics degrade honestly.

Built with Vue 3.6 (Vapor), Nuxt UI v4, Pinia, Vue Flow, and a Rust/WASM module
(`wasm/`), managed with [Vite+](https://viteplus.dev/).

## Prerequisites

- [Vite+](https://viteplus.dev/) (`vp`) and bun
- Rust with the `wasm32-unknown-unknown` target and `wasm-pack`
  (`cargo install wasm-pack`)

The ergot dependency is fetched from the
[`wasm-support` branch](https://github.com/okhsunrog/ergot/tree/wasm-support)
of the ergot fork — no local ergot checkout is needed. To hack on ergot
itself, use the `[patch]` override documented in `wasm/Cargo.toml` (and set
`cache: false` on the `wasm:build` task in `vite.config.ts` so rebuilds pick
up your local changes).

## Develop

```sh
vp install
vp run dev      # wasm build + dev server with wasm hot-rebuild
```

Editing `.rs` files under `wasm/` (or the ergot worktree) triggers a wasm
rebuild; the page reloads automatically.

## Checks

```sh
vp test             # runtime tests of the WASM node API (plain Node, no browser)
vp check            # format, lint, type checks
vp run typecheck    # vue-tsc only
vp run build        # production build
```
