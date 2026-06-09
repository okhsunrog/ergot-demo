// Runtime tests for the WASM node API, running in plain Node (no browser):
// the wasm-bindgen module only needs Promise/Date/setTimeout/crypto here.
import { readFileSync } from 'node:fs'
import { expect, test } from 'vite-plus/test'
import { initLogging, initSync, NodeProfile, WasmNode } from '../wasm-pkg/ergot_demo_wasm'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

initSync({
  module: readFileSync(new URL('../wasm-pkg/ergot_demo_wasm_bg.wasm', import.meta.url)),
})
initLogging('warn')

function edgeStatus(node: WasmNode) {
  const status = node.status()
  if (status.profile !== 'edge') throw new Error('expected an edge node')
  return status
}

test('nodes start down and become active on connect', () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  expect(router.status()).toEqual({ profile: 'router', nets: [] })
  expect(edgeStatus(edge).status).toBe('down')

  const link = router.connectTo(edge)
  expect(router.status()).toEqual({ profile: 'router', nets: [link.netId] })
  // The edge discovers its net id from the first routed frame, so right
  // after connect it is active on net 0 until traffic flows.
  expect(edgeStatus(edge).status).toBe('active')

  link.free()
  edge.free()
  router.free()
})

test('ping times out when no server is attached', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const link = router.connectTo(edge)

  await expect(router.ping(link.netId, 2, 200)).rejects.toThrow(/timed out/)

  link.free()
  edge.free()
  router.free()
})

test('router pings a served edge', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const link = router.connectTo(edge)
  await edge.servePing()

  const res = await router.ping(link.netId, 2)
  expect(res.value).toBe(42)
  expect(res.latencyMs).toBeGreaterThanOrEqual(0)

  // After traffic, the edge knows its address.
  expect(edgeStatus(edge)).toEqual({
    profile: 'edge',
    status: 'active',
    netId: link.netId,
    nodeId: 2,
  })

  link.free()
  edge.free()
  router.free()
})

test('edge pings another edge across the router (multi-hop)', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const a = new WasmNode(NodeProfile.Edge)
  const b = new WasmNode(NodeProfile.Edge)
  const linkA = router.connectTo(a)
  const linkB = router.connectTo(b)
  expect(linkA.netId).not.toBe(linkB.netId)
  await b.servePing()

  const res = await a.ping(linkB.netId, 2)
  expect(res.value).toBe(42)

  linkA.free()
  linkB.free()
  a.free()
  b.free()
  router.free()
})

test('edge pings the router itself (node 1 on the link net)', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const link = router.connectTo(edge)
  await router.servePing()

  const res = await edge.ping(link.netId, 1)
  expect(res.value).toBe(42)

  link.free()
  edge.free()
  router.free()
})

test('connection validation', () => {
  const router = new WasmNode(NodeProfile.Router)
  const router2 = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const edge2 = new WasmNode(NodeProfile.Edge)

  expect(() => edge.connectTo(edge2)).toThrow(/router\.connectTo/)
  expect(() => router.connectTo(router2)).toThrow(/must be an edge/)

  const link = router.connectTo(edge)
  expect(() => router2.connectTo(edge)).toThrow(/already linked/)

  link.free()
  for (const n of [router, router2, edge, edge2]) n.free()
})

test('disconnect frees both sides and reconnect works', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const link = router.connectTo(edge)
  await edge.servePing()
  await router.ping(link.netId, 2)

  link.free()
  await sleep(50)
  expect(edgeStatus(edge).status).toBe('down')
  expect(router.status()).toEqual({ profile: 'router', nets: [] })
  expect(edge.linkCount).toBe(0)
  expect(router.linkCount).toBe(0)

  const link2 = router.connectTo(edge)
  const res = await router.ping(link2.netId, 2)
  expect(res.value).toBe(42)

  link2.free()
  edge.free()
  router.free()
})
