<script lang="ts">
export interface BusNodeData {
  seq: number
}

export function busName(data: BusNodeData): string {
  return `Bus ${data.seq}`
}
</script>

<script setup lang="ts">
import { computed } from 'vue'
import { Handle, Position } from '@vue-flow/core'
import { useTopologyStore } from '@/stores/topology'

const props = defineProps<{ id: string; data: BusNodeData }>()
const store = useTopologyStore()
const status = computed(() => store.busStatuses[props.id])
const name = computed(() => busName(props.data))
const statusLabel = computed(() => {
  const current = status.value
  if (!current?.routerAttached) return 'connect a Router'
  return `net ${current.netId} · ${current.deviceCount} device${current.deviceCount === 1 ? '' : 's'}`
})
</script>

<template>
  <div class="bus-node">
    <Handle id="top" type="target" :position="Position.Top" />
    <div class="flex items-center gap-1.5">
      <span class="bus-track" />
      <span class="font-medium text-(--ui-text-highlighted) truncate flex-1">{{ name }}</span>
    </div>
    <div class="text-[9px] text-(--ui-text-muted) mt-1 truncate">{{ statusLabel }}</div>
    <div class="text-[9px] text-(--ui-text-dimmed) truncate">shared packet medium</div>
    <Handle id="bottom" type="source" :position="Position.Bottom" />
  </div>
</template>

<style scoped>
.bus-node {
  width: 130px;
  padding: 7px 9px;
  font-size: 10px;
}

.bus-track {
  width: 20px;
  height: 4px;
  border-radius: 999px;
  background: var(--ui-secondary);
  box-shadow: 0 0 0 1px color-mix(in srgb, var(--ui-secondary) 60%, transparent);
}
</style>
