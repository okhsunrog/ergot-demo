// Runtime tests for the WASM node API, running in plain Node (no browser):
// the wasm-bindgen module only needs Promise/Date/setTimeout/crypto here.
import { readFileSync } from 'node:fs'
import { expect, test } from 'vite-plus/test'
import { initLogging, initSync, NodeRole, WasmNode } from '../wasm-pkg/ergot_demo_wasm'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

initSync({
  module: readFileSync(new URL('../wasm-pkg/ergot_demo_wasm_bg.wasm', import.meta.url)),
})
initLogging('warn')

function nodePair() {
  return {
    ctrl: new WasmNode(NodeRole.Controller),
    tgt: new WasmNode(NodeRole.Target),
  }
}

test('nodes start down and become active on connect', () => {
  const { ctrl, tgt } = nodePair()
  expect(ctrl.linkStatus().status).toBe('down')
  expect(tgt.linkStatus().status).toBe('down')

  const link = ctrl.connectTo(tgt)
  expect(ctrl.linkStatus()).toEqual({ status: 'active', netId: 1, nodeId: 1 })
  expect(tgt.linkStatus()).toEqual({ status: 'active', netId: 0, nodeId: 2 })

  link.free()
  tgt.free()
  ctrl.free()
})

test('ping times out when no server is attached', async () => {
  const { ctrl, tgt } = nodePair()
  const link = ctrl.connectTo(tgt)

  await expect(ctrl.ping(1, 2, 200)).rejects.toThrow(/timed out/)

  link.free()
  tgt.free()
  ctrl.free()
})

test('ping echoes through a served target', async () => {
  const { ctrl, tgt } = nodePair()
  const link = ctrl.connectTo(tgt)
  await tgt.servePing()

  const res = await ctrl.ping(1, 2)
  expect(res.value).toBe(42)
  expect(res.latencyMs).toBeGreaterThanOrEqual(0)

  link.free()
  tgt.free()
  ctrl.free()
})

test('connecting an already linked node is rejected', () => {
  const { ctrl, tgt } = nodePair()
  const link = ctrl.connectTo(tgt)

  expect(() => ctrl.connectTo(tgt)).toThrow(/already linked/)
  const other = new WasmNode(NodeRole.Target)
  expect(() => ctrl.connectTo(other)).toThrow(/already linked/)

  other.free()
  link.free()
  tgt.free()
  ctrl.free()
})

test('role validation: only controller→target links are allowed', () => {
  const { ctrl, tgt } = nodePair()
  expect(() => tgt.connectTo(ctrl)).toThrow(/controller/)

  tgt.free()
  ctrl.free()
})

test('disconnect tears down both sides and reconnect works', async () => {
  const { ctrl, tgt } = nodePair()
  const link = ctrl.connectTo(tgt)
  await tgt.servePing()
  await ctrl.ping(1, 2)

  link.free()
  await sleep(50)
  expect(ctrl.linkStatus().status).toBe('down')
  expect(tgt.linkStatus().status).toBe('down')
  expect(ctrl.linked).toBe(false)

  const link2 = ctrl.connectTo(tgt)
  const res = await ctrl.ping(1, 2)
  expect(res.value).toBe(42)

  link2.free()
  tgt.free()
  ctrl.free()
})
