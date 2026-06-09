<script setup lang="ts">
import { computed } from 'vue'
import { Handle, Position, useVueFlow, type Connection } from '@vue-flow/core'
import { useTopologyStore, type LinkKindType, type ProfileType } from '@/stores/topology'

export interface ErgotNodeData {
  label: string
  profile: ProfileType
  kind: LinkKindType
}

const props = defineProps<{ id: string; data: ErgotNodeData }>()

const store = useTopologyStore()
const { updateNodeData } = useVueFlow()

const profileOptions = [
  { label: 'Router', value: 'router' },
  { label: 'Edge', value: 'edge' },
]

const kindOptions = [
  { label: 'Stream (COBS)', value: 'stream' },
  { label: 'Packet', value: 'packet' },
]

const profileColors: Record<ProfileType, string> = {
  router: 'var(--ui-warning)',
  edge: 'var(--ui-success)',
}

const status = computed(() => store.statuses[props.id])

const linked = computed(() => {
  const s = status.value
  if (!s) return false
  return s.profile === 'router' ? s.nets.length > 0 : s.status !== 'down'
})

const addressLabel = computed(() => {
  const s = status.value
  if (!s) return '—'
  if (s.profile === 'router') {
    return s.nets.length ? `nets ${s.nets.join(', ')}` : 'no links'
  }
  if (s.status === 'active' && s.netId) return `${s.netId}.${s.nodeId}`
  return s.status
})

function onProfileChange(value: string) {
  const profile = value as ProfileType
  store.setProfile(props.id, profile, props.data.kind)
  updateNodeData(props.id, { profile })
}

function onKindChange(value: string) {
  const kind = value as LinkKindType
  store.setLinkKind(props.id, kind)
  updateNodeData(props.id, { kind })
}

const isValidConnection = (conn: Connection) => store.canConnect(conn.source, conn.target)
</script>

<template>
  <div class="ergot-node">
    <!-- Uplink handle (edge nodes connect up to a router) -->
    <Handle
      v-if="data.profile === 'edge'"
      id="top"
      type="target"
      :position="Position.Top"
      :is-valid-connection="isValidConnection"
    />

    <div class="flex items-center gap-1 mb-0.5">
      <span
        class="inline-block w-1.5 h-1.5 rounded-full shrink-0"
        :style="{ background: profileColors[data.profile] }"
      />
      <span class="font-medium text-(--ui-text-highlighted) truncate leading-tight">
        {{ data.label }}
      </span>
    </div>

    <div class="text-[9px] text-(--ui-text-muted) mb-0.5 truncate">{{ addressLabel }}</div>

    <USelect
      :model-value="data.profile"
      :items="profileOptions"
      :disabled="linked"
      size="xs"
      class="w-full"
      @update:model-value="onProfileChange"
    />

    <USelect
      v-if="data.profile === 'edge'"
      :model-value="data.kind"
      :items="kindOptions"
      :disabled="linked"
      size="xs"
      class="w-full mt-0.5"
      @update:model-value="onKindChange"
    />

    <!-- Downlink handle (routers fan out to edges) -->
    <Handle
      v-if="data.profile === 'router'"
      id="bottom"
      type="source"
      :position="Position.Bottom"
      :is-valid-connection="isValidConnection"
    />
  </div>
</template>

<style scoped>
.ergot-node {
  width: 110px;
  padding: 4px 6px;
  font-size: 10px;
}
</style>
