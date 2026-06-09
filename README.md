# ergot-demo

A browser playground for [ergot](https://github.com/jamesmunns/ergot) networks.
Draw a topology on a canvas and it runs for real: every node on the canvas is a
full ergot NetStack compiled to WebAssembly, links are in-memory byte pipes
carrying COBS-framed ergot frames, and pings route through actual Router /
DirectEdge profiles.

Built with Vue 3.6 (Vapor), Nuxt UI v4, Pinia, Vue Flow, and a Rust/WASM module
(`wasm/`), managed with [Vite+](https://viteplus.dev/).

## Prerequisites

- [Vite+](https://viteplus.dev/) (`vp`) and bun
- Rust with the `wasm32-unknown-unknown` target and `wasm-pack`
  (`cargo install wasm-pack`)
- The ergot checkout this demo builds against: a git worktree of
  [ergot](https://github.com/okhsunrog/ergot) on the `wasm-support` branch at
  `../../rust/ergot-wasm` (relative to this repo):

  ```sh
  git -C ~/code/rust/ergot worktree add ../ergot-wasm wasm-support
  ```

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
