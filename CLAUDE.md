# Ergot Demo

Vue 3.6 (beta) + Nuxt UI v4 + Pinia + Vue Flow project with a Rust/WASM module: a browser playground where canvas nodes are real ergot NetStacks running in WASM.

## Project-Specific Notes

- **ergot dependency**: `wasm/Cargo.toml` uses a git dependency on the `wasm-support` branch of https://github.com/okhsunrog/ergot (all wasm/futures-io ergot work happens on that branch; local worktree at `../../rust/ergot-wasm`). For local ergot development, enable the commented `[patch]` block in `wasm/Cargo.toml` AND set `cache: false` on the `wasm:build` task (vp input globs cannot see changes outside the workspace root, so the cache would serve stale builds with a path override). After pushing new ergot commits, run `cargo update -p ergot` in `wasm/` to bump the locked rev.
- **WASM build**: Defined as a cached Vite Task in `vite.config.ts`; the cache is sound because the ergot git dep is pinned by `wasm/Cargo.lock`, which the auto inputs hash. `wasm-pack` is a system binary (cargo-installed), not an npm package.
- **WASM hot reload**: A custom Vite plugin (`wasmHotRebuild`) watches `.rs` files, `Cargo.toml`, and `Cargo.lock` in `wasm/` and triggers wasm-pack rebuild before HMR.
- **Tests**: `vp test` runs `src/__tests__/wasm-api.test.ts` against the built wasm pkg in plain Node (no browser needed). Rebuild the pkg first (`vp run wasm:build`) when the Rust side changed.
- **Type checking**: Use `vue-tsc --build`, not `tsc`. tsgolint's `typeCheck` doesn't support `.vue` SFC imports yet. The lint config uses `typeAware: true` only (no `typeCheck`). Type checking runs in the pre-commit hook (`.vite-hooks/pre-commit` runs `vp staged` then `vp exec vue-tsc --build`) and is available manually via `vp run typecheck`.
- **Scripts**: `vp dev` / `vp build` run the dev server / production build directly; run `vp run wasm:build` first when the Rust side changed (src/wasm-pkg is generated, not tracked). The `package.json` dev/build scripts chain both: `vp run dev`, `vp run build`.
- **Vue beta overrides**: `package.json` has overrides pinning all `@vue/*` packages to `beta` channel.

<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, but it invokes Vite through `vp dev` and `vp build`.

## Vite+ Workflow

`vp` is a global binary that handles the full development lifecycle. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

### Start

- create - Create a new project from a template
- migrate - Migrate an existing project to Vite+
- config - Configure hooks and agent integration
- staged - Run linters on staged files
- install (`i`) - Install dependencies
- env - Manage Node.js versions

### Develop

- dev - Run the development server
- check - Run format, lint, and TypeScript type checks
- lint - Lint code
- fmt - Format code
- test - Run tests

### Execute

- run - Run monorepo tasks
- exec - Execute a command from local `node_modules/.bin`
- dlx - Execute a package binary without installing it as a dependency
- cache - Manage the task cache

### Build

- build - Build for production
- pack - Build libraries
- preview - Preview production build

### Manage Dependencies

Vite+ automatically detects and wraps the underlying package manager such as pnpm, npm, or Yarn through the `packageManager` field in `package.json` or package manager-specific lockfiles.

- add - Add packages to dependencies
- remove (`rm`, `un`, `uninstall`) - Remove packages from dependencies
- update (`up`) - Update packages to latest versions
- dedupe - Deduplicate dependencies
- outdated - Check for outdated packages
- list (`ls`) - List installed packages
- why (`explain`) - Show why a package is installed
- info (`view`, `show`) - View package information from the registry
- link (`ln`) / unlink - Manage local package links
- pm - Forward a command to the package manager

### Maintain

- upgrade - Update `vp` itself to the latest version

These commands map to their corresponding tools. For example, `vp dev --port 3000` runs Vite's dev server and works the same as Vite. `vp test` runs JavaScript tests through the bundled Vitest. The version of all tools can be checked using `vp --version`. This is useful when researching documentation, features, and bugs.

## Common Pitfalls

- **Using the package manager directly:** Do not use pnpm, npm, or Yarn directly. Vite+ can handle all package manager operations.
- **Always use Vite commands to run tools:** Don't attempt to run `vp vitest` or `vp oxlint`. They do not exist. Use `vp test` and `vp lint` instead.
- **Running scripts:** Vite+ built-in commands (`vp dev`, `vp build`, `vp test`, etc.) always run the Vite+ built-in tool, not any `package.json` script of the same name. To run a custom script that shares a name with a built-in command, use `vp run <script>`. For example, if you have a custom `dev` script that runs multiple services concurrently, run it with `vp run dev`, not `vp dev` (which always starts Vite's dev server).
- **Do not install Vitest, Oxlint, Oxfmt, or tsdown directly:** Vite+ wraps these tools. They must not be installed directly. You cannot upgrade these tools by installing their latest versions. Always use Vite+ commands.
- **Use Vite+ wrappers for one-off binaries:** Use `vp dlx` instead of package-manager-specific `dlx`/`npx` commands.
- **Import JavaScript modules from `vite-plus`:** Instead of importing from `vite` or `vitest`, all modules should be imported from the project's `vite-plus` dependency. For example, `import { defineConfig } from 'vite-plus';` or `import { expect, test, vi } from 'vite-plus/test';`. You must not install `vitest` to import test utilities.
- **Type-Aware Linting:** There is no need to install `oxlint-tsgolint`, `vp lint --type-aware` works out of the box.

## CI Integration

For GitHub Actions, consider using [`voidzero-dev/setup-vp`](https://github.com/voidzero-dev/setup-vp) to replace separate `actions/setup-node`, package-manager setup, cache, and install steps with a single action.

```yaml
- uses: voidzero-dev/setup-vp@v1
  with:
    cache: true
- run: vp check
- run: vp test
```

## Review Checklist for Agents

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check` and `vp test` to validate changes.
<!--VITE PLUS END-->
