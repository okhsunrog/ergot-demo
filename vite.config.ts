import { fileURLToPath, URL } from 'node:url'
import { execSync } from 'node:child_process'

import { defineConfig, type Plugin } from 'vite-plus'
import vue from '@vitejs/plugin-vue'
import ui from '@nuxt/ui/vite'

function wasmHotRebuild(): Plugin {
  let building = false
  return {
    name: 'wasm-hot-rebuild',
    configureServer(server) {
      server.watcher.on('change', (path) => {
        if (building) return
        if (
          !/wasm\/src\/.*\.rs$/.test(path) &&
          !path.endsWith('wasm/Cargo.toml') &&
          !path.endsWith('wasm/Cargo.lock')
        )
          return
        building = true
        server.config.logger.info('Rebuilding WASM...', { timestamp: true })
        try {
          execSync('wasm-pack build wasm --target web --out-dir ../src/wasm-pkg', {
            stdio: 'inherit',
          })
          server.config.logger.info('WASM rebuild complete', {
            timestamp: true,
          })
        } catch {
          server.config.logger.error('WASM rebuild failed', {
            timestamp: true,
          })
        } finally {
          building = false
        }
      })
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  staged: {
    '*': 'vp check --fix',
  },
  fmt: {
    semi: false,
    singleQuote: true,
  },
  lint: {
    options: {
      typeAware: true,
    },
  },
  run: {
    tasks: {
      typecheck: {
        command: 'vp exec vue-tsc --build',
      },
      'wasm:build': {
        command: 'wasm-pack build wasm --target web --out-dir ../src/wasm-pkg',
        // The ergot git dependency is pinned by wasm/Cargo.lock, which the
        // auto inputs hash — so caching is sound. If you [patch] ergot to a
        // local path (see wasm/Cargo.toml), set cache: false, since vp's
        // input globs cannot see changes outside the workspace root.
        cache: true,
        input: [{ auto: true }, '!src/wasm-pkg/**', '!wasm/target/**'],
      },
    },
  },
  plugins: [vue(), ui(), wasmHotRebuild()],
  server: {
    watch: {
      ignored: ['**/wasm/target/**'],
    },
  },
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
})
