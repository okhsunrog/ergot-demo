<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { VueFlow, useVueFlow } from '@vue-flow/core'
import type { Connection } from '@vue-flow/core'
import ErgotNode, { nodeName, type ErgotNodeData } from './components/ErgotNode.vue'
import BusNode, { busName, type BusNodeData } from './components/BusNode.vue'
import FrameInspector from './components/FrameInspector.vue'
import HelpModal from './components/HelpModal.vue'
import { useTopologyStore, type LinkKindType, type ProfileType } from '@/stores/topology'
import { isTextEditingTarget } from '@/utils/keyboard'

const store = useTopologyStore()
const toast = useToast()
const MAX_LATENCY_MS = 60_000

const {
  fitView,
  project,
  addNodes,
  addEdges,
  edges,
  findNode,
  getSelectedNodes,
  getSelectedEdges,
  removeNodes,
  removeEdges,
} = useVueFlow({ nodes: [], edges: [] })

let nodeSeq = 0
let busSeq = 0
const newNodeId = () => `n-${crypto.randomUUID()}`
const newEdgeId = () => `e-${crypto.randomUUID()}`

function addNode(
  profile: ProfileType,
  position?: { x: number; y: number },
  kind: LinkKindType = 'stream',
) {
  const id = newNodeId()
  store.createNode(id, profile, kind)
  addNodes({
    id,
    type: 'ergot',
    position: position ?? project({ x: 250 + Math.random() * 100, y: 150 + Math.random() * 100 }),
    data: { seq: ++nodeSeq, profile, kind },
  })
  return id
}

function addRouter() {
  addNode('router')
}

function addBridge() {
  addNode('bridge')
}

function addEdge() {
  addNode('edge')
}

function addBus(position?: { x: number; y: number }) {
  const id = newNodeId()
  store.createBus(id)
  addNodes({
    id,
    type: 'bus',
    position: position ?? project({ x: 250 + Math.random() * 100, y: 150 + Math.random() * 100 }),
    data: { seq: ++busSeq },
  })
  return id
}

function addSharedBus() {
  addBus()
}

function connectNodes(source: string, target: string): boolean {
  if (!store.canConnect(source, target)) return false
  const edgeId = newEdgeId()
  let kind: LinkKindType
  try {
    kind = store.connect(edgeId, source, target)
  } catch (e) {
    toast.add({ title: 'Connection failed', description: String(e), color: 'error' })
    return false
  }
  addEdges({
    id: edgeId,
    source,
    sourceHandle: 'bottom',
    target,
    targetHandle: 'top',
    label: store.isBus(source) || store.isBus(target) ? 'bus' : kind === 'packet' ? 'pkt' : 'cobs',
    style: kind === 'packet' ? { strokeDasharray: '6 3' } : undefined,
  })
  return true
}

function wouldLoop(source: string, target: string): boolean {
  // Walk up the parent chain from the source; every node has at most one
  // uplink, so this terminates. Hitting the target means a ring.
  const parentOf = new Map(edges.value.map((e) => [e.target, e.source]))
  let cur: string | undefined = source
  while (cur) {
    if (cur === target) return true
    cur = parentOf.get(cur)
  }
  return false
}

function onConnect(connection: Connection) {
  if (wouldLoop(connection.source, connection.target)) {
    toast.add({
      title: 'Invalid connection',
      description: 'That link would create a ring with no root router.',
      color: 'warning',
    })
    return
  }
  const validationError = store.connectionError(connection.source, connection.target)
  if (validationError) {
    toast.add({
      title: 'Invalid connection',
      description: validationError,
      color: 'warning',
    })
    return
  }
  connectNodes(connection.source, connection.target)
}

function deleteSelected() {
  const selectedNodes = getSelectedNodes.value
  const selectedEdges = getSelectedEdges.value

  // Tear down explicitly selected links plus all links of deleted nodes.
  const nodeIds = new Set(selectedNodes.map((n) => n.id))
  const edgesToDrop = edges.value.filter(
    (e) =>
      selectedEdges.some((s) => s.id === e.id) || nodeIds.has(e.source) || nodeIds.has(e.target),
  )
  const droppedEdgeIds = new Set<string>()
  for (const edge of edgesToDrop) {
    for (const droppedId of store.disconnect(edge.id)) droppedEdgeIds.add(droppedId)
  }
  if (droppedEdgeIds.size) removeEdges([...droppedEdgeIds])

  for (const id of nodeIds) {
    if (store.isBus(id)) store.destroyBus(id)
    else store.destroyNode(id)
  }
  if (selectedNodes.length) removeNodes(selectedNodes)
}

function onKeyDown(e: KeyboardEvent) {
  if (e.key !== 'Delete' && e.key !== 'Backspace') return
  if (isTextEditingTarget(e.target)) return
  e.preventDefault()
  deleteSelected()
}

// Help modal: shown automatically on the first visit
const HELP_SEEN_KEY = 'ergot-demo-help-seen'
const showHelp = ref(false)

function openHelp() {
  showHelp.value = true
  localStorage.setItem(HELP_SEEN_KEY, '1')
}

// Frame inspector and per-link activity animation
const showInspector = ref(true)

function toggleInspector() {
  showInspector.value = !showInspector.value
}

function resolveLink(edgeId: string): string {
  const edge = edges.value.find((e) => e.id === edgeId)
  if (!edge) return edgeId.slice(0, 8)
  return `${canvasNodeName(edge.source)} ⇄ ${canvasNodeName(edge.target)}`
}

function canvasNodeName(id: string): string {
  const node = findNode(id)
  if (!node) return id
  return node.type === 'bus'
    ? busName(node.data as BusNodeData)
    : nodeName(node.data as ErgotNodeData)
}

function animateActiveEdges() {
  const now = Date.now()
  for (const edge of edges.value) {
    const last = store.linkActivity[edge.id] ?? 0
    const active = now - last < 600
    if (edge.animated !== active) edge.animated = active
  }
}

// Impairment controls for the selected link
const latencyMs = ref(0)
const lossPct = ref(0)
const selectedEdge = computed(() => {
  const sel = getSelectedEdges.value
  return sel.length === 1 ? sel[0] : null
})

watch(selectedEdge, (edge) => {
  if (!edge) return
  const imp = store.getImpairment(edge.id)
  latencyMs.value = imp?.latencyMs ?? 0
  lossPct.value = imp?.lossPct ?? 0
})

function applyImpairment() {
  const edge = selectedEdge.value
  if (!edge) return
  latencyMs.value = Math.min(MAX_LATENCY_MS, Math.max(0, Math.floor(latencyMs.value) || 0))
  lossPct.value = Math.min(100, Math.max(0, Math.floor(lossPct.value) || 0))
  store.setImpairment(edge.id, latencyMs.value, lossPct.value)
}

// Ping between the two selected nodes
const selectedPair = computed(() => {
  const sel = getSelectedNodes.value.filter((node) => node.type === 'ergot')
  return sel.length === 2 ? sel : null
})

async function pingSelected() {
  const pair = selectedPair.value
  if (!pair) return
  const [a, b] = pair
  if (!a || !b) return
  const names = `${nodeName(a.data as ErgotNodeData)} → ${nodeName(b.data as ErgotNodeData)}`
  try {
    const res = await store.ping(a.id, b.id)
    toast.add({
      title: names,
      description: `pong in ${res.latencyMs.toFixed(1)} ms`,
      color: 'success',
      icon: 'i-lucide-radio',
    })
  } catch (e) {
    toast.add({
      title: names,
      description: e instanceof Error ? e.message : String(e),
      color: 'error',
      icon: 'i-lucide-radio',
    })
  }
}

let refreshTimer: ReturnType<typeof setInterval> | undefined
let frameTimer: ReturnType<typeof setInterval> | undefined

onMounted(async () => {
  if (!localStorage.getItem(HELP_SEEN_KEY)) openHelp()
  await store.initWasm()

  // Show both transport shapes immediately: two packet devices share one
  // claimed-address bus while a stream node keeps a point-to-point link.
  const router = addNode('router', { x: 300, y: 40 })
  const bus = addBus({ x: 150, y: 200 })
  const busA = addNode('edge', { x: 40, y: 350 }, 'packet')
  const busB = addNode('edge', { x: 260, y: 350 }, 'packet')
  const point = addNode('edge', { x: 500, y: 220 })
  connectNodes(router, bus)
  connectNodes(bus, busA)
  connectNodes(bus, busB)
  connectNodes(router, point)
  void fitView()

  refreshTimer = setInterval(() => store.refreshAll(), 1000)
  frameTimer = setInterval(() => {
    store.pollFrames()
    store.pollSamples()
    animateActiveEdges()
  }, 150)
})

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
  if (frameTimer) clearInterval(frameTimer)
  store.dispose()
})
</script>

<template>
  <UApp>
    <div class="h-screen flex flex-col" @keydown="onKeyDown" tabindex="0">
      <header
        class="flex items-center justify-between px-4 py-2 border-b border-(--ui-border-muted) bg-(--ui-bg-elevated)"
      >
        <h1 class="text-lg font-semibold text-(--ui-text-highlighted)">Ergot Network Topology</h1>
        <div class="flex gap-2 items-center">
          <UButton icon="i-lucide-router" :disabled="!store.ready" @click="addRouter"
            >Add Router</UButton
          >
          <UButton icon="i-lucide-git-fork" :disabled="!store.ready" @click="addBridge"
            >Add Bridge</UButton
          >
          <UButton icon="i-lucide-network" :disabled="!store.ready" @click="addSharedBus"
            >Add Bus</UButton
          >
          <UButton icon="i-lucide-plus" :disabled="!store.ready" @click="addEdge">Add Node</UButton>
          <UButton color="error" variant="outline" icon="i-lucide-trash-2" @click="deleteSelected"
            >Delete</UButton
          >
          <UButton v-if="selectedPair" variant="outline" icon="i-lucide-radio" @click="pingSelected"
            >Ping</UButton
          >
          <div
            v-if="selectedEdge"
            class="flex items-center gap-1 text-xs text-(--ui-text-muted) border-l border-(--ui-border-muted) pl-2"
          >
            <span>lat</span>
            <UTooltip text="Artificial one-way latency added to each direction of the link">
              <UInput
                v-model.number="latencyMs"
                type="number"
                :max="MAX_LATENCY_MS"
                min="0"
                size="xs"
                class="w-16"
                @change="applyImpairment"
              />
            </UTooltip>
            <span>ms · loss</span>
            <UTooltip text="Probability of dropping each chunk/frame, per direction">
              <UInput
                v-model.number="lossPct"
                type="number"
                size="xs"
                class="w-14"
                @change="applyImpairment"
              />
            </UTooltip>
            <span>%</span>
          </div>
          <UButton
            :variant="showInspector ? 'solid' : 'outline'"
            icon="i-lucide-list"
            @click="toggleInspector"
            >Frames</UButton
          >
          <UButton
            variant="ghost"
            icon="i-lucide-circle-help"
            aria-label="Help"
            @click="openHelp"
          />
          <UColorModeButton />
        </div>
      </header>
      <div class="flex-1 min-h-0">
        <VueFlow :default-viewport="{ zoom: 1 }" :min-zoom="0.2" :max-zoom="4" @connect="onConnect">
          <template #node-ergot="nodeProps">
            <ErgotNode :id="nodeProps.id" :data="nodeProps.data" />
          </template>
          <template #node-bus="nodeProps">
            <BusNode :id="nodeProps.id" :data="nodeProps.data" />
          </template>
        </VueFlow>
      </div>
      <FrameInspector v-if="showInspector" :frames="store.frames" :resolve-link="resolveLink" />
      <HelpModal v-model:open="showHelp" />
    </div>
  </UApp>
</template>

<style>
@import '@vue-flow/core/dist/style.css';

.vue-flow {
  background: var(--ui-bg-default);
}

.vue-flow__node-ergot {
  background: var(--ui-bg-accented);
  color: var(--ui-text-highlighted);
  border: 1px solid var(--ui-border-default);
  border-radius: var(--ui-radius);
  padding: 0;
  font-size: 0.75rem;
  box-shadow: var(--tw-shadow, 0 1px 2px rgb(0 0 0 / 0.05));
}

.vue-flow__node-bus {
  background: color-mix(in srgb, var(--ui-secondary) 10%, var(--ui-bg-accented));
  color: var(--ui-text-highlighted);
  border: 1px solid color-mix(in srgb, var(--ui-secondary) 55%, var(--ui-border-default));
  border-radius: var(--ui-radius);
  padding: 0;
  font-size: 0.75rem;
  box-shadow: var(--tw-shadow, 0 1px 2px rgb(0 0 0 / 0.05));
}

.vue-flow__node-ergot.selected {
  border-color: var(--ui-primary);
  box-shadow: 0 0 0 2px var(--ui-primary);
}

.vue-flow__node-bus.selected {
  border-color: var(--ui-secondary);
  box-shadow: 0 0 0 2px var(--ui-secondary);
}

.vue-flow__edge-path {
  stroke: var(--ui-border-accented);
  stroke-width: 2;
}

.vue-flow__edge.selected .vue-flow__edge-path {
  stroke: var(--ui-primary);
}

.vue-flow__edge-text {
  fill: var(--ui-text-muted);
  font-size: 10px;
}

.vue-flow__edge-textbg {
  fill: var(--ui-bg-elevated);
}

.vue-flow__handle {
  width: 8px;
  height: 8px;
  background: var(--ui-border-accented);
  border: 2px solid var(--ui-bg-elevated);
}

.vue-flow__handle:hover {
  background: var(--ui-primary);
}

.vue-flow__connection-line path {
  stroke: var(--ui-primary);
  stroke-width: 2;
}
</style>
