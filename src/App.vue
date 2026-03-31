<script setup lang="ts">
import { ref } from 'vue'
import { VueFlow, useVueFlow } from '@vue-flow/core'
import type { Node, Edge, Connection } from '@vue-flow/core'

let nextId = 4

const nodes = ref<Node[]>([
  {
    id: '1',
    type: 'default',
    label: 'Router A',
    position: { x: 250, y: 50 },
  },
  {
    id: '2',
    type: 'default',
    label: 'Edge B',
    position: { x: 100, y: 250 },
  },
  {
    id: '3',
    type: 'default',
    label: 'Edge C',
    position: { x: 400, y: 250 },
  },
])

const edges = ref<Edge[]>([
  { id: 'e1-2', source: '1', target: '2' },
  { id: 'e1-3', source: '1', target: '3' },
])

const { fitView, project, getSelectedNodes, getSelectedEdges, removeNodes, removeEdges } = useVueFlow()

function addNode() {
  const id = String(nextId++)
  nodes.value.push({
    id,
    type: 'default',
    label: `Node ${id}`,
    position: project({ x: 300, y: 200 }),
  })
}

function onConnect(connection: Connection) {
  edges.value.push({
    id: `e${connection.source}-${connection.target}`,
    source: connection.source,
    target: connection.target,
  })
}

function deleteSelected() {
  const selectedNodes = getSelectedNodes.value
  const selectedEdges = getSelectedEdges.value
  if (selectedNodes.length) removeNodes(selectedNodes)
  if (selectedEdges.length) removeEdges(selectedEdges)
}

function onKeyDown(e: KeyboardEvent) {
  if (e.key === 'Delete' || e.key === 'Backspace') {
    deleteSelected()
  }
}
</script>

<template>
  <UApp>
    <div class="h-screen flex flex-col" @keydown="onKeyDown" tabindex="0">
      <header class="flex items-center justify-between px-4 py-2 border-b border-(--ui-border-muted) bg-(--ui-bg-elevated)">
        <h1 class="text-lg font-semibold text-(--ui-text-highlighted)">Ergot Network Topology</h1>
        <div class="flex gap-2">
          <UButton icon="i-lucide-plus" @click="addNode">Add Node</UButton>
          <UButton color="error" variant="outline" icon="i-lucide-trash-2" @click="deleteSelected">Delete Selected</UButton>
          <UColorModeButton />
        </div>
      </header>
      <div class="flex-1">
        <VueFlow
          :nodes="nodes"
          :edges="edges"
          :default-viewport="{ zoom: 1 }"
          :min-zoom="0.2"
          :max-zoom="4"
          fit-view-on-init
          @nodes-initialized="fitView"
          @connect="onConnect"
        />
      </div>
    </div>
  </UApp>
</template>

<style>
@import '@vue-flow/core/dist/style.css';

.vue-flow {
  background: var(--ui-bg-default);
}

.vue-flow__node-default {
  background: var(--ui-bg-accented);
  color: var(--ui-text-highlighted);
  border: 1px solid var(--ui-border-default);
  border-radius: var(--ui-radius);
  padding: 10px 16px;
  font-size: 0.875rem;
  box-shadow: var(--tw-shadow, 0 1px 2px rgb(0 0 0 / 0.05));
}

.vue-flow__node-default.selected {
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
