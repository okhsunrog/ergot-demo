<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { VueFlow, useVueFlow } from '@vue-flow/core'
import type { Connection } from '@vue-flow/core'
import ErgotNode from './components/ErgotNode.vue'
import { useTopologyStore, type ProfileType } from '@/stores/topology'

const store = useTopologyStore()
const toast = useToast()

const {
  fitView,
  project,
  addNodes,
  addEdges,
  edges,
  getSelectedNodes,
  getSelectedEdges,
  removeNodes,
  removeEdges,
} = useVueFlow({ nodes: [], edges: [] })

let nodeSeq = 0
const newNodeId = () => `n-${crypto.randomUUID()}`
const newEdgeId = () => `e-${crypto.randomUUID()}`

function addNode(profile: ProfileType, position?: { x: number; y: number }) {
  const id = newNodeId()
  store.createNode(id, profile)
  addNodes({
    id,
    type: 'ergot',
    position: position ?? project({ x: 250 + Math.random() * 100, y: 150 + Math.random() * 100 }),
    data: { label: `${profile === 'router' ? 'Router' : 'Node'} ${++nodeSeq}`, profile },
  })
  return id
}

function connectNodes(source: string, target: string): boolean {
  if (!store.canConnect(source, target)) return false
  const edgeId = newEdgeId()
  try {
    store.connect(edgeId, source, target)
  } catch (e) {
    toast.add({ title: 'Connection failed', description: String(e), color: 'error' })
    return false
  }
  addEdges({ id: edgeId, source, sourceHandle: 'bottom', target, targetHandle: 'top' })
  return true
}

function onConnect(connection: Connection) {
  if (!store.canConnect(connection.source, connection.target)) {
    toast.add({
      title: 'Invalid connection',
      description: 'Links go from a router to an unlinked edge node.',
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
  for (const edge of edgesToDrop) store.disconnect(edge.id)
  if (edgesToDrop.length) removeEdges(edgesToDrop.map((e) => e.id))

  for (const id of nodeIds) store.destroyNode(id)
  if (selectedNodes.length) removeNodes(selectedNodes)
}

function onKeyDown(e: KeyboardEvent) {
  if (e.key === 'Delete' || e.key === 'Backspace') {
    deleteSelected()
  }
}

// Ping between the two selected nodes
const pingResult = ref('')
const selectedPair = computed(() => {
  const sel = getSelectedNodes.value
  return sel.length === 2 ? sel : null
})

async function pingSelected() {
  const pair = selectedPair.value
  if (!pair) return
  const [a, b] = pair
  if (!a || !b) return
  pingResult.value = `${a.data.label} → ${b.data.label}: ...`
  try {
    const res = await store.ping(a.id, b.id)
    pingResult.value = `${a.data.label} → ${b.data.label}: ${res.latencyMs.toFixed(1)} ms`
  } catch (e) {
    pingResult.value = `${a.data.label} → ${b.data.label}: ${e instanceof Error ? e.message : e}`
  }
}

let refreshTimer: ReturnType<typeof setInterval> | undefined

onMounted(async () => {
  await store.initWasm()

  // Seed a small default topology: one router with two edge nodes.
  const router = addNode('router', { x: 250, y: 50 })
  const nodeB = addNode('edge', { x: 100, y: 250 })
  const nodeC = addNode('edge', { x: 400, y: 250 })
  connectNodes(router, nodeB)
  connectNodes(router, nodeC)
  void fitView()

  refreshTimer = setInterval(() => store.refreshAll(), 1000)
})

onUnmounted(() => {
  if (refreshTimer) clearInterval(refreshTimer)
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
          <UButton icon="i-lucide-router" :disabled="!store.ready" @click="addNode('router')"
            >Add Router</UButton
          >
          <UButton icon="i-lucide-plus" :disabled="!store.ready" @click="addNode('edge')"
            >Add Node</UButton
          >
          <UButton color="error" variant="outline" icon="i-lucide-trash-2" @click="deleteSelected"
            >Delete</UButton
          >
          <UButton v-if="selectedPair" variant="outline" icon="i-lucide-radio" @click="pingSelected"
            >Ping</UButton
          >
          <span v-if="pingResult" class="text-xs text-(--ui-text-muted)">{{ pingResult }}</span>
          <UColorModeButton />
        </div>
      </header>
      <div class="flex-1">
        <VueFlow :default-viewport="{ zoom: 1 }" :min-zoom="0.2" :max-zoom="4" @connect="onConnect">
          <template #node-ergot="nodeProps">
            <ErgotNode :id="nodeProps.id" :data="nodeProps.data" />
          </template>
        </VueFlow>
      </div>
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

.vue-flow__node-ergot.selected {
  border-color: var(--ui-primary);
  box-shadow: 0 0 0 2px var(--ui-primary);
}

.vue-flow__edge-path {
  stroke: var(--ui-border-accented);
  stroke-width: 2;
}

.vue-flow__edge.selected .vue-flow__edge-path {
  stroke: var(--ui-primary);
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
