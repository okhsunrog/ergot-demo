# Ergot Demo

Vue 3.5 + Nuxt UI v4 + Pinia + Vue Flow project with a Rust/WASM module: a browser playground where canvas nodes are real ergot NetStacks running in WASM.

## Project-Specific Notes

- **ergot dependency**: `wasm/Cargo.toml` uses a git dependency on upstream ergot main (https://github.com/jamesmunns/ergot) — the wasm support (futures-io transport, web-time) is merged upstream (PR #207). For local ergot development, enable the commented `[patch]` block in `wasm/Cargo.toml` AND set `cache: false` on the `wasm:build` task (vp input globs cannot see changes outside the workspace root, so the cache would serve stale builds with a path override). To pick up new upstream commits, run `cargo update -p ergot` in `wasm/`.
- **WASM build**: Defined as a cached Vite Task in `vite.config.ts`; the cache is sound because the ergot git dep is pinned by `wasm/Cargo.lock`, which the auto inputs hash. `wasm-pack` is a system binary (cargo-installed), not an npm package.
- **WASM hot reload**: A custom Vite plugin (`wasmHotRebuild`) watches `.rs` files, `Cargo.toml`, and `Cargo.lock` in `wasm/` and triggers wasm-pack rebuild before HMR.
- **Tests**: `vp test` runs `src/__tests__/wasm-api.test.ts` against the built wasm pkg in plain Node (no browser needed). Rebuild the pkg first (`vp run wasm:build`) when the Rust side changed.
- **Type checking**: `vp check` enables Vite+'s type-aware lint and TypeScript checking. Keep `vue-tsc --build` as the SFC-aware check; `vp run typecheck` runs it after the cached WASM build, and CI runs it explicitly.
- **Scripts**: `vp dev` / `vp build` run the dev server / production build directly; run `vp run wasm:build` first when the Rust side changed (src/wasm-pkg is generated, not tracked). The `package.json` dev/build scripts chain both: `vp run dev`, `vp run build`.
- **Vite+ toolchain**: `vite-plus` and the `vite` alias are pinned together. Upgrade them with `vp migrate --no-interactive`, then inspect and commit the lockfile changes.

<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

## Review Checklist

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check` and `vp test` to format, lint, type check and test changes.
- [ ] Check if there are `vite.config.ts` tasks or `package.json` scripts necessary for validation, run via `vp run <script>`.
- [ ] If setup, runtime, or package-manager behavior looks wrong, run `vp env doctor` and include its output when asking for help.

<!--VITE PLUS END-->
