import { reactive, ref } from 'vue'
import { defineStore } from 'pinia'
import init, {
  initLogging,
  NodeProfile,
  WasmNode,
  WasmLink,
  type NodeStatus,
  type PingResult,
} from '../wasm-pkg/ergot_demo_wasm'

export type ProfileType = 'router' | 'edge'

// WASM handles are plain pointers — keep them out of Vue's reactivity.
const nodeHandles = new Map<string, WasmNode>()
const linkHandles = new Map<string, WasmLink>()

function toWasmProfile(profile: ProfileType): NodeProfile {
  return profile === 'router' ? NodeProfile.Router : NodeProfile.Edge
}

/**
 * Owns the live ergot network: one WasmNode per canvas node, one WasmLink
 * per canvas edge. The canvas is the source of truth for topology; this
 * store keeps the WASM network in sync and exposes reactive node statuses.
 */
export const useTopologyStore = defineStore('topology', () => {
  const ready = ref(false)
  const statuses = reactive<Record<string, NodeStatus>>({})

  async function initWasm() {
    if (ready.value) return
    await init()
    initLogging(import.meta.env.DEV ? 'debug' : 'info')
    ready.value = true
  }

  function refresh(id: string) {
    const handle = nodeHandles.get(id)
    if (handle) statuses[id] = handle.status()
  }

  function refreshAll() {
    for (const id of nodeHandles.keys()) refresh(id)
  }

  function createNode(id: string, profile: ProfileType) {
    const node = new WasmNode(toWasmProfile(profile))
    nodeHandles.set(id, node)
    // Every node answers pings, so anything on the canvas is a ping target.
    void node.servePing()
    refresh(id)
  }

  function destroyNode(id: string) {
    // Freeing the node closes its links; drop our link handles for them too.
    nodeHandles.get(id)?.free()
    nodeHandles.delete(id)
    delete statuses[id]
  }

  /** Can `source` accept a new link to `target`? Used for canvas validation. */
  function canConnect(sourceId: string, targetId: string): boolean {
    const source = nodeHandles.get(sourceId)
    const target = nodeHandles.get(targetId)
    return (
      source !== undefined &&
      target !== undefined &&
      source.profile === NodeProfile.Router &&
      target.profile === NodeProfile.Edge &&
      target.linkCount === 0
    )
  }

  /** Wire two canvas nodes together. Throws if the link is invalid. */
  function connect(edgeId: string, sourceId: string, targetId: string) {
    const source = nodeHandles.get(sourceId)
    const target = nodeHandles.get(targetId)
    if (!source || !target) throw new Error('unknown node')
    const link = source.connectTo(target)
    linkHandles.set(edgeId, link)
    // Warm the link with one ping so the edge node learns its address.
    void source
      .ping(link.netId, 2, 500)
      .catch(() => {})
      .then(() => {
        refresh(sourceId)
        refresh(targetId)
      })
    refresh(sourceId)
    refresh(targetId)
  }

  function disconnect(edgeId: string) {
    linkHandles.get(edgeId)?.free()
    linkHandles.delete(edgeId)
  }

  /** Replace a node's stack with a different profile. Only valid when unlinked. */
  function setProfile(id: string, profile: ProfileType) {
    const handle = nodeHandles.get(id)
    if (!handle) return
    if (handle.linkCount > 0) throw new Error('disconnect the node before changing its profile')
    destroyNode(id)
    createNode(id, profile)
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
    initWasm,
    createNode,
    destroyNode,
    canConnect,
    connect,
    disconnect,
    setProfile,
    ping,
    refresh,
    refreshAll,
  }
})
