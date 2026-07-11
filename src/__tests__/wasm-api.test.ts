// Runtime tests for the WASM node API, running in plain Node (no browser):
// the wasm-bindgen module only needs Promise/Date/setTimeout/crypto here.
import { readFileSync } from 'node:fs'
import { expect, test } from 'vite-plus/test'
import {
  initLogging,
  initSync,
  LinkKind,
  NodeProfile,
  takeFrameEvents,
  WasmNode,
} from '../wasm-pkg/ergot_demo_wasm'

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

test('packet link: ping over a frame-channel uplink', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  expect(edge.linkKind).toBe(LinkKind.Packet)

  const link = router.connectTo(edge)
  expect(link.kind).toBe(LinkKind.Packet)
  await edge.servePing()

  const res = await router.ping(link.netId, 2)
  expect(res.value).toBe(42)

  link.free()
  edge.free()
  router.free()
})

test('packet link: bounded buffers drop overflow without closing the link', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  const link = router.connectTo(edge)
  await edge.servePing()
  await router.ping(link.netId, 2)

  link.setImpairment(1_000, 0)
  for (let i = 0; i < 64; i++) {
    router.publishSensor(i)
    await sleep(0)
  }
  expect(link.overflowDrops).toBeGreaterThan(0)
  expect(router.linkCount).toBe(1)
  expect(edge.linkCount).toBe(1)

  link.free()
  edge.free()
  router.free()
})

test('mixed kinds: stream edge pings packet edge across one router', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const streamy = new WasmNode(NodeProfile.Edge, LinkKind.Stream)
  const packety = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  const linkS = router.connectTo(streamy)
  const linkP = router.connectTo(packety)
  await packety.servePing()
  await streamy.servePing()

  // stream → router → packet
  const res1 = await streamy.ping(linkP.netId, 2)
  expect(res1.value).toBe(42)
  // packet → router → stream
  const res2 = await packety.ping(linkS.netId, 2)
  expect(res2.value).toBe(42)

  linkS.free()
  linkP.free()
  for (const n of [router, streamy, packety]) n.free()
})

test('packet link: disconnect and reconnect', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  const link = router.connectTo(edge)
  await edge.servePing()
  await router.ping(link.netId, 2)

  link.free()
  await sleep(50)
  expect(router.status()).toEqual({ profile: 'router', nets: [] })
  expect(edge.linkCount).toBe(0)

  const link2 = router.connectTo(edge)
  const res = await router.ping(link2.netId, 2)
  expect(res.value).toBe(42)

  link2.free()
  edge.free()
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

  expect(() => edge.connectTo(edge2)).toThrow(/router or bridge/)
  expect(() => router.connectTo(router2)).toThrow(/must be a bridge or edge/)

  const link = router.connectTo(edge)
  expect(() => router2.connectTo(edge)).toThrow(/already linked upstream/)

  link.free()
  for (const n of [router, router2, edge, edge2]) n.free()
})

test('frame tap records labelled request and response frames', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const link = router.connectTo(edge, 'edge-42')
  await edge.servePing()
  takeFrameEvents() // discard connect-time warm-up noise from other tests

  await router.ping(link.netId, 2)
  const { events } = takeFrameEvents()
  const onLink = events.filter((e) => e.linkId === 'edge-42')
  expect(onLink.some((e) => e.dir === 'down' && e.kind === 'req')).toBe(true)
  expect(onLink.some((e) => e.dir === 'up' && e.kind === 'resp')).toBe(true)
  const req = onLink.find((e) => e.kind === 'req')
  expect(req?.dst).toBe(`${link.netId}.2:0`)

  // After disconnect the tap label is cleared: no more events for this link.
  const netId = link.netId
  link.free()
  await sleep(20)
  takeFrameEvents()
  await router.ping(netId, 2, 100).catch(() => {})
  const after = takeFrameEvents().events.filter((e) => e.linkId === 'edge-42')
  expect(after).toEqual([])

  edge.free()
  router.free()
})

test('frame tap excludes frames rejected by a full interface queue', () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const link = router.connectTo(edge, 'backpressure-link')
  takeFrameEvents()

  let queued = 0
  let recorded = 0
  for (let i = 0; i < 1_000; i++) {
    try {
      router.publishSensor(i)
      queued++
    } catch {
      recorded += takeFrameEvents().events.filter(
        (event) => event.linkId === 'backpressure-link',
      ).length
      break
    }
    recorded += takeFrameEvents().events.filter(
      (event) => event.linkId === 'backpressure-link',
    ).length
  }

  expect(queued).toBeLessThan(1_000)
  expect(recorded).toBe(queued)

  link.free()
  edge.free()
  router.free()
})

test('impairment: latency raises RTT, full loss kills the link', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  const link = router.connectTo(edge)
  await edge.servePing()

  const fast = await router.ping(link.netId, 2)
  expect(fast.latencyMs).toBeLessThan(50)

  link.setImpairment(100_000, 0)
  expect(link.latencyMs).toBe(60_000)

  link.setImpairment(100, 0)
  expect(link.latencyMs).toBe(100)
  const slow = await router.ping(link.netId, 2)
  // 100 ms each way; allow slack for timer jitter.
  expect(slow.latencyMs).toBeGreaterThanOrEqual(150)

  link.setImpairment(0, 100)
  await expect(router.ping(link.netId, 2, 300)).rejects.toThrow(/timed out/)

  link.setImpairment(0, 0)
  const healed = await router.ping(link.netId, 2)
  expect(healed.value).toBe(42)

  link.free()
  edge.free()
  router.free()
})

test('impairment on a stream link: traffic recovers after loss', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge, LinkKind.Stream)
  const link = router.connectTo(edge)
  await edge.servePing()

  link.setImpairment(0, 100)
  await expect(router.ping(link.netId, 2, 200)).rejects.toThrow(/timed out/)

  link.setImpairment(0, 0)
  const healed = await router.ping(link.netId, 2)
  expect(healed.value).toBe(42)

  link.free()
  edge.free()
  router.free()
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

test('freeing one endpoint removes the link from its peer', async () => {
  const firstRouter = new WasmNode(NodeProfile.Router)
  const secondRouter = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  const oldLink = firstRouter.connectTo(edge, 'old-link')
  await edge.servePing()

  firstRouter.free()
  await sleep(20)
  expect(edge.linkCount).toBe(0)
  expect(edge.uplinkFree).toBe(true)

  const newLink = secondRouter.connectTo(edge, 'new-link')
  expect(edge.linkCount).toBe(1)
  // Releasing the stale JS handle must not affect the replacement link.
  oldLink.free()
  expect(edge.linkCount).toBe(1)
  expect((await secondRouter.ping(newLink.netId, 2)).value).toBe(42)

  newLink.free()
  edge.free()
  secondRouter.free()
})

test('an old disconnected handle cannot clear a replacement frame tap', async () => {
  const firstRouter = new WasmNode(NodeProfile.Router)
  const secondRouter = new WasmNode(NodeProfile.Router)
  const edge = new WasmNode(NodeProfile.Edge)
  await edge.servePing()
  const oldLink = firstRouter.connectTo(edge, 'old-link')

  oldLink.disconnect()
  await sleep(20)
  const newLink = secondRouter.connectTo(edge, 'new-link')
  await secondRouter.ping(newLink.netId, 2)
  takeFrameEvents()

  oldLink.disconnect()
  await secondRouter.ping(newLink.netId, 2)
  const events = takeFrameEvents().events.filter((event) => event.linkId === 'new-link')
  expect(events.some((event) => event.dir === 'down' && event.kind === 'req')).toBe(true)
  expect(events.some((event) => event.dir === 'up' && event.kind === 'resp')).toBe(true)

  oldLink.free()
  newLink.free()
  edge.free()
  firstRouter.free()
  secondRouter.free()
})

test('sensor topic: broadcast fans out to all subscribers', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const a = new WasmNode(NodeProfile.Edge)
  const b = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  const linkA = router.connectTo(a)
  const linkB = router.connectTo(b)
  await router.subscribeSensor()
  await a.subscribeSensor()
  await b.subscribeSensor()
  // Warm the links so the edges learn their addresses.
  await a.ping(linkA.netId, 1, 200).catch(() => {})
  await b.ping(linkB.netId, 1, 200).catch(() => {})

  a.publishSensor(1.25)
  await sleep(50)

  // Both the router and the *other* edge hear the broadcast.
  expect(router.takeSamples().samples.some((s) => s.value === 1.25)).toBe(true)
  expect(b.takeSamples().samples.some((s) => s.value === 1.25)).toBe(true)

  linkA.free()
  linkB.free()
  for (const n of [router, a, b]) n.free()
})

test('sensor topic: unicast reaches only the target', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const a = new WasmNode(NodeProfile.Edge)
  const b = new WasmNode(NodeProfile.Edge)
  const linkA = router.connectTo(a)
  const linkB = router.connectTo(b)
  await a.subscribeSensor()
  await b.subscribeSensor()
  await a.ping(linkA.netId, 1, 200).catch(() => {})
  await b.ping(linkB.netId, 1, 200).catch(() => {})

  a.publishSensorTo(linkB.netId, 2, 7.5)
  await sleep(50)

  expect(b.takeSamples().samples.some((s) => s.value === 7.5)).toBe(true)
  expect(a.takeSamples().samples).toEqual([])

  linkA.free()
  linkB.free()
  for (const n of [router, a, b]) n.free()
})

test('periodic publisher streams and full loss starves a subscriber', async () => {
  const router = new WasmNode(NodeProfile.Router)
  const pub = new WasmNode(NodeProfile.Edge)
  const sub = new WasmNode(NodeProfile.Edge, LinkKind.Packet)
  const linkPub = router.connectTo(pub)
  const linkSub = router.connectTo(sub)
  await sub.subscribeSensor()
  await pub.ping(linkPub.netId, 1, 200).catch(() => {})

  pub.startPublisher(20)
  expect(pub.publishing).toBe(true)
  await sleep(200)
  const flowing = sub.takeSamples().samples.length
  expect(flowing).toBeGreaterThan(3)

  linkSub.setImpairment(0, 100)
  await sleep(100)
  sub.takeSamples() // discard in-flight stragglers
  await sleep(200)
  expect(sub.takeSamples().samples).toEqual([])

  linkSub.setImpairment(0, 0)
  await sleep(200)
  expect(sub.takeSamples().samples.length).toBeGreaterThan(3)

  pub.stopPublisher()
  expect(pub.publishing).toBe(false)

  linkPub.free()
  linkSub.free()
  for (const n of [router, pub, sub]) n.free()
})

async function waitFor(cond: () => boolean, ms = 4000) {
  const t0 = Date.now()
  while (!cond()) {
    if (Date.now() - t0 > ms) throw new Error('waitFor timed out')
    await sleep(50)
  }
}

function bridgeStatus(node: WasmNode) {
  const status = node.status()
  if (status.profile !== 'bridge') throw new Error('expected a bridge node')
  return status
}

test('bridge: seed lease arrives and traffic routes root → bridge → edge', async () => {
  const root = new WasmNode(NodeProfile.Router)
  const bridge = new WasmNode(NodeProfile.Bridge)
  const edge = new WasmNode(NodeProfile.Edge)
  const linkRB = root.connectTo(bridge)
  const linkBE = bridge.connectTo(edge)
  expect(linkBE.netId).toBe(0) // pending until seed assignment
  await edge.servePing()

  // Any frame from the root activates the bridge uplink; the seed task
  // then leases a net for the pending downlink and warms it.
  await root.ping(linkRB.netId, 2, 300).catch(() => {})
  await waitFor(() => {
    const st = bridge.status()
    return st.profile === 'bridge' && st.upstream === 'active' && st.nets.length === 1
  })
  await waitFor(() => {
    const st = edgeStatus(edge)
    return st.status === 'active' && (st.netId ?? 0) > 0
  })

  const seedNet = edgeStatus(edge).netId!
  expect(bridgeStatus(bridge).nets).toEqual([seedNet])
  expect(seedNet).not.toBe(linkRB.netId)

  // Multi-hop across the bridge using the globally routed seed net.
  const res = await root.ping(seedNet, 2)
  expect(res.value).toBe(42)

  linkBE.free()
  linkRB.free()
  for (const n of [root, bridge, edge]) n.free()
})

test('bridge: orphan subtree gets its lease once the uplink appears', async () => {
  const root = new WasmNode(NodeProfile.Router)
  const bridge = new WasmNode(NodeProfile.Bridge)
  const edge = new WasmNode(NodeProfile.Edge)
  // Downlink first: stays pending with no upstream.
  const linkBE = bridge.connectTo(edge)
  await edge.servePing()
  await sleep(200)
  expect(bridgeStatus(bridge).nets).toEqual([])

  // Now attach the uplink: the pending downlink gets its lease.
  const linkRB = root.connectTo(bridge)
  await root.ping(linkRB.netId, 2, 300).catch(() => {})
  await waitFor(() => bridgeStatus(bridge).nets.length === 1)
  await waitFor(() => (edgeStatus(edge).netId ?? 0) > 0)

  const res = await root.ping(edgeStatus(edge).netId!, 2)
  expect(res.value).toBe(42)

  linkBE.free()
  linkRB.free()
  for (const n of [root, bridge, edge]) n.free()
})

test('bridge: connection validation', () => {
  const root = new WasmNode(NodeProfile.Router)
  const bridge = new WasmNode(NodeProfile.Bridge)
  const root2 = new WasmNode(NodeProfile.Router)

  expect(root.uplinkFree).toBe(false)
  expect(bridge.uplinkFree).toBe(true)
  expect(() => root.connectTo(root2)).toThrow(/must be a bridge or edge/)

  const link = root.connectTo(bridge)
  expect(bridge.uplinkFree).toBe(false)
  expect(() => root2.connectTo(bridge)).toThrow(/already linked upstream/)

  link.free()
  for (const n of [root, bridge, root2]) n.free()
})
