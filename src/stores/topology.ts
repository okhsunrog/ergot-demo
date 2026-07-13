import { reactive, ref } from 'vue'
import { defineStore } from 'pinia'
import init, {
  initLogging,
  LinkKind,
  NodeProfile,
  takeFrameEvents,
  WasmBus,
  WasmNode,
  WasmLink,
  type FrameEvent,
  type NodeStatus,
  type PingResult,
} from '../wasm-pkg/ergot_demo_wasm'

export type ProfileType = 'router' | 'bridge' | 'edge'
export type LinkKindType = 'stream' | 'packet'
export interface BusStatus {
  routerAttached: boolean
  netId: number
  deviceCount: number
}

// WASM handles are plain pointers — keep them out of Vue's reactivity.
const nodeHandles = new Map<string, WasmNode>()
const busHandles = new Map<string, WasmBus>()
const linkHandles = new Map<string, WasmLink>()
const linkEndpoints = new Map<string, { sourceId: string; targetId: string; busId?: string }>()

function toWasmProfile(profile: ProfileType): NodeProfile {
  if (profile === 'router') return NodeProfile.Router
  if (profile === 'bridge') return NodeProfile.Bridge
  return NodeProfile.Edge
}

function toWasmLinkKind(kind: LinkKindType): LinkKind {
  return kind === 'packet' ? LinkKind.Packet : LinkKind.Stream
}

/**
 * Owns the live ergot network: one WasmNode per canvas node, one WasmLink
 * per canvas edge. The canvas is the source of truth for topology; this
 * store keeps the WASM network in sync and exposes reactive node statuses.
 */
const MAX_FRAMES = 100

export const useTopologyStore = defineStore('topology', () => {
  const ready = ref(false)
  const statuses = reactive<Record<string, NodeStatus>>({})
  const busStatuses = reactive<Record<string, BusStatus>>({})
  /** Attached-link counts per node id (drives UI locking of selects). */
  const linkCounts = reactive<Record<string, number>>({})
  /** Recent tapped frames, newest last. */
  const frames = ref<FrameEvent[]>([])
  /** Last frame timestamp per canvas edge id, for edge activity animation. */
  const linkActivity = reactive<Record<string, number>>({})
  /** Recent sensor readings per node id, newest last. */
  const sensorData = reactive<Record<string, { ts: number; value: number }[]>>({})
  /** Which nodes run a periodic sensor publisher. */
  const publishing = reactive<Record<string, boolean>>({})

  async function initWasm() {
    if (ready.value) return
    await init()
    initLogging(import.meta.env.DEV ? 'debug' : 'info')
    ready.value = true
  }

  function refresh(id: string) {
    const handle = nodeHandles.get(id)
    if (!handle) return
    statuses[id] = handle.status()
    linkCounts[id] = handle.linkCount
  }

  function refreshAll() {
    for (const id of nodeHandles.keys()) refresh(id)
    for (const id of busHandles.keys()) refreshBus(id)
  }

  function refreshBus(id: string) {
    const bus = busHandles.get(id)
    if (!bus) return
    busStatuses[id] = {
      routerAttached: bus.routerAttached,
      netId: bus.netId,
      deviceCount: Math.max(0, bus.memberCount - (bus.routerAttached ? 1 : 0)),
    }
  }

  /** Drain tapped frames from the WASM side into the reactive log. */
  function pollFrames() {
    const { events } = takeFrameEvents()
    if (!events.length) return
    for (const e of events) {
      linkActivity[e.linkId] = e.ts
      const busId = linkEndpoints.get(e.linkId)?.busId
      if (!busId) continue
      for (const [edgeId, endpoints] of linkEndpoints) {
        if (endpoints.busId === busId) linkActivity[edgeId] = e.ts
      }
    }
    frames.value = [...frames.value, ...events].slice(-MAX_FRAMES)
  }

  /** Drain received sensor readings from every node into the reactive buffers. */
  function pollSamples() {
    for (const [id, handle] of nodeHandles) {
      const { samples } = handle.takeSamples()
      if (!samples.length) continue
      const buf = (sensorData[id] ??= [])
      for (const s of samples) buf.push({ ts: s.ts, value: s.value })
      if (buf.length > 50) buf.splice(0, buf.length - 50)
    }
  }

  /** Start/stop the periodic sensor publisher on a node. */
  function togglePublisher(id: string, intervalMs = 100) {
    const handle = nodeHandles.get(id)
    if (!handle) return
    if (publishing[id]) {
      handle.stopPublisher()
      publishing[id] = false
    } else {
      handle.startPublisher(intervalMs)
      publishing[id] = true
    }
  }

  function setImpairment(edgeId: string, latencyMs: number, lossPct: number) {
    linkHandles.get(edgeId)?.setImpairment(latencyMs, lossPct)
  }

  function getImpairment(edgeId: string): { latencyMs: number; lossPct: number } | undefined {
    const link = linkHandles.get(edgeId)
    return link ? { latencyMs: link.latencyMs, lossPct: link.lossPct } : undefined
  }

  function createNode(id: string, profile: ProfileType, kind: LinkKindType = 'stream') {
    const node = new WasmNode(toWasmProfile(profile), toWasmLinkKind(kind))
    nodeHandles.set(id, node)
    // Every node answers pings and listens to the sensor topic.
    void node.servePing()
    void node.subscribeSensor()
    refresh(id)
  }

  function createBus(id: string) {
    busHandles.set(id, new WasmBus())
    refreshBus(id)
  }

  function destroyNode(id: string) {
    for (const [edgeId, endpoints] of linkEndpoints) {
      if (endpoints.sourceId === id || endpoints.targetId === id) disconnect(edgeId)
    }
    nodeHandles.get(id)?.free()
    nodeHandles.delete(id)
    delete statuses[id]
    delete linkCounts[id]
    delete sensorData[id]
    delete publishing[id]
  }

  function destroyBus(id: string) {
    for (const [edgeId, endpoints] of linkEndpoints) {
      if (endpoints.sourceId === id || endpoints.targetId === id) disconnect(edgeId)
    }
    busHandles.get(id)?.free()
    busHandles.delete(id)
    delete busStatuses[id]
  }

  function isBus(id: string): boolean {
    return busHandles.has(id)
  }

  function connectionError(sourceId: string, targetId: string): string | undefined {
    const source = nodeHandles.get(sourceId)
    const target = nodeHandles.get(targetId)
    const sourceBus = busHandles.get(sourceId)
    const targetBus = busHandles.get(targetId)

    if (source && target) {
      if (source.profile === NodeProfile.Edge) return 'Links must start at a Router or Bridge.'
      if (target.profile === NodeProfile.Router) return 'Routers do not have an uplink.'
      if (!target.uplinkFree) return 'The target node already has an uplink.'
      return undefined
    }
    if (source && targetBus) {
      if (source.profile !== NodeProfile.Router) {
        return 'A shared bus must be controlled by a root Router.'
      }
      if (!targetBus.routerFree) return 'That bus already has a Router.'
      return undefined
    }
    if (sourceBus && target) {
      if (!sourceBus.routerAttached) return 'Connect a root Router to the bus first.'
      if (target.profile !== NodeProfile.Edge) return 'Only Edge nodes can join a shared bus.'
      if (target.linkKind !== LinkKind.Packet) {
        return 'Select Packet transport on the Edge before joining a shared bus.'
      }
      if (!target.uplinkFree) return 'The target node already has an uplink.'
      return undefined
    }
    return 'Connect Router → Bus → Packet Edge, or Router/Bridge → Node.'
  }

  /** Can `source` accept a new link to `target`? Used for canvas validation. */
  function canConnect(sourceId: string, targetId: string): boolean {
    return connectionError(sourceId, targetId) === undefined
  }

  /** Wire two canvas nodes together. Throws if the link is invalid.
   *  Returns the kind of the created link. */
  function connect(edgeId: string, sourceId: string, targetId: string): LinkKindType {
    const source = nodeHandles.get(sourceId)
    const target = nodeHandles.get(targetId)
    const sourceBus = busHandles.get(sourceId)
    const targetBus = busHandles.get(targetId)
    const validationError = connectionError(sourceId, targetId)
    if (validationError) throw new Error(validationError)

    let link: WasmLink
    let busId: string | undefined
    if (sourceBus && target) {
      link = sourceBus.attachEdge(target, edgeId)
      busId = sourceId
    } else if (source && targetBus) {
      link = targetBus.attachRouter(source, edgeId)
      busId = targetId
    } else if (source && target) {
      link = source.connectTo(target, edgeId)
    } else {
      throw new Error('unknown topology endpoint')
    }
    linkHandles.set(edgeId, link)
    linkEndpoints.set(edgeId, { sourceId, targetId, busId })
    // Warm the link with one ping so the child learns its address. Pending
    // bridge downlinks (netId 0) warm themselves after seed assignment.
    if (!busId && source && target && link.netId > 0) {
      void source
        .ping(link.netId, 2, 500)
        .catch(() => {})
        .then(() => {
          refresh(sourceId)
          refresh(targetId)
        })
    }
    refresh(sourceId)
    refresh(targetId)
    if (busId) refreshBus(busId)
    return link.kind === LinkKind.Packet ? 'packet' : 'stream'
  }

  function disconnectOne(edgeId: string) {
    const endpoints = linkEndpoints.get(edgeId)
    linkHandles.get(edgeId)?.free()
    linkHandles.delete(edgeId)
    linkEndpoints.delete(edgeId)
    delete linkActivity[edgeId]
    if (!endpoints) return
    refresh(endpoints.sourceId)
    refresh(endpoints.targetId)
    if (endpoints.busId) refreshBus(endpoints.busId)
  }

  /** Disconnect one canvas edge. Removing a Router→Bus edge tears down the
   * whole bus segment so no visually connected device keeps a stale handle. */
  function disconnect(edgeId: string): string[] {
    const endpoints = linkEndpoints.get(edgeId)
    const ids =
      endpoints?.busId && busHandles.has(endpoints.targetId)
        ? [...linkEndpoints]
            .filter(([, candidate]) => candidate.busId === endpoints.busId)
            .map(([id]) => id)
        : [edgeId]
    for (const id of ids) disconnectOne(id)
    return ids
  }

  /** Tear down every live WASM handle and clear all topology state. */
  function dispose() {
    for (const edgeId of linkHandles.keys()) disconnectOne(edgeId)
    for (const bus of busHandles.values()) bus.free()
    for (const node of nodeHandles.values()) node.free()
    busHandles.clear()
    nodeHandles.clear()
    for (const id of Object.keys(statuses)) delete statuses[id]
    for (const id of Object.keys(linkCounts)) delete linkCounts[id]
    for (const id of Object.keys(busStatuses)) delete busStatuses[id]
    for (const id of Object.keys(sensorData)) delete sensorData[id]
    for (const id of Object.keys(publishing)) delete publishing[id]
    for (const id of Object.keys(linkActivity)) delete linkActivity[id]
    frames.value = []
  }

  /** Replace a node's stack with a different profile. Only valid when unlinked. */
  function setProfile(id: string, profile: ProfileType, kind: LinkKindType = 'stream') {
    const handle = nodeHandles.get(id)
    if (!handle) return
    if (handle.linkCount > 0) throw new Error('disconnect the node before changing its profile')
    destroyNode(id)
    createNode(id, profile, kind)
  }

  /** Replace a node's uplink kind. Only valid when unlinked. */
  function setLinkKind(id: string, kind: LinkKindType) {
    const handle = nodeHandles.get(id)
    if (!handle) return
    if (handle.linkCount > 0) throw new Error('disconnect the node before changing its link kind')
    if (handle.profile === NodeProfile.Router) return
    destroyNode(id)
    createNode(id, handle.profile === NodeProfile.Bridge ? 'bridge' : 'edge', kind)
  }

  /** Ping from one canvas node to another, using the target's address. */
  async function ping(sourceId: string, targetId: string): Promise<PingResult> {
    const source = nodeHandles.get(sourceId)
    const target = nodeHandles.get(targetId)
    if (!source || !target) throw new Error('unknown node')
    const status = target.status()
    let networkId: number
    let nodeId: number
    if (status.profile === 'edge') {
      if (status.status !== 'active' || status.netId === undefined || status.netId === 0) {
        throw new Error('target edge has no address yet (is it connected?)')
      }
      networkId = status.netId
      nodeId = status.nodeId ?? 2
    } else if (status.profile === 'bridge') {
      if (status.upstream === 'active' && status.upstreamNetId) {
        networkId = status.upstreamNetId
        nodeId = 2
      } else if (status.nets[0] !== undefined) {
        networkId = status.nets[0]
        nodeId = 1
      } else {
        throw new Error('target bridge has no active links')
      }
    } else {
      const net = status.nets[0]
      if (net === undefined) throw new Error('target router has no active links')
      networkId = net
      nodeId = 1
    }
    return await source.ping(networkId, nodeId)
  }

  return {
    ready,
    statuses,
    busStatuses,
    linkCounts,
    frames,
    linkActivity,
    sensorData,
    publishing,
    initWasm,
    pollFrames,
    pollSamples,
    togglePublisher,
    setImpairment,
    getImpairment,
    createNode,
    createBus,
    destroyNode,
    destroyBus,
    isBus,
    connectionError,
    canConnect,
    connect,
    disconnect,
    dispose,
    setProfile,
    setLinkKind,
    ping,
    refresh,
    refreshAll,
  }
})
