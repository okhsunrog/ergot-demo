<script lang="ts">
import type { LinkKindType, ProfileType } from '@/stores/topology'

export interface ErgotNodeData {
  seq: number
  profile: ProfileType
  kind: LinkKindType
}

export function nodeName(data: ErgotNodeData): string {
  return `${data.profile === 'router' ? 'Router' : 'Node'} ${data.seq}`
}
</script>

<script setup lang="ts">
import { computed } from 'vue'
import { Handle, Position, useVueFlow } from '@vue-flow/core'
import { useTopologyStore } from '@/stores/topology'

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

const samples = computed(() => store.sensorData[props.id] ?? [])
const isPublishing = computed(() => store.publishing[props.id] ?? false)

/** Polyline points for a 100×20 sparkline of the recent readings. */
const sparkline = computed(() => {
  const data = samples.value
  if (data.length < 2) return ''
  let min = Infinity
  let max = -Infinity
  for (const s of data) {
    if (s.value < min) min = s.value
    if (s.value > max) max = s.value
  }
  const span = max - min || 1
  return data
    .map((s, i) => {
      const x = (i / (data.length - 1)) * 100
      const y = 18 - ((s.value - min) / span) * 16
      return `${x.toFixed(1)},${y.toFixed(1)}`
    })
    .join(' ')
})

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

function togglePublish() {
  store.togglePublisher(props.id)
}

const name = computed(() => nodeName(props.data))
</script>

<template>
  <div class="ergot-node">
    <!-- Uplink handle (edge nodes connect up to a router) -->
    <Handle v-if="data.profile === 'edge'" id="top" type="target" :position="Position.Top" />

    <div class="flex items-center gap-1 mb-0.5">
      <span
        class="inline-block w-1.5 h-1.5 rounded-full shrink-0"
        :style="{ background: profileColors[data.profile] }"
      />
      <span class="font-medium text-(--ui-text-highlighted) truncate leading-tight flex-1">
        {{ name }}
      </span>
      <button
        class="publish-toggle"
        :class="{ active: isPublishing }"
        :title="isPublishing ? 'Stop publishing' : 'Publish sensor stream'"
        @click.stop="togglePublish"
      >
        {{ isPublishing ? '■' : '▶' }}
      </button>
    </div>

    <svg v-if="sparkline" viewBox="0 0 100 20" class="w-full h-5 mb-0.5" preserveAspectRatio="none">
      <polyline
        :points="sparkline"
        fill="none"
        stroke="var(--ui-primary)"
        stroke-width="1.5"
        vector-effect="non-scaling-stroke"
      />
    </svg>

    <div class="text-[9px] text-(--ui-text-muted) mb-0.5 truncate">{{ addressLabel }}</div>

    <div :title="linked ? 'Disconnect the node to change its profile' : 'Node profile'">
      <USelect
        :model-value="data.profile"
        :items="profileOptions"
        :disabled="linked"
        size="xs"
        class="w-full"
        @update:model-value="onProfileChange"
      />
    </div>

    <div
      v-if="data.profile === 'edge'"
      :title="
        linked
          ? 'Disconnect the node to change its uplink transport'
          : 'Uplink transport: COBS byte stream or one-message-one-frame packets'
      "
    >
      <USelect
        :model-value="data.kind"
        :items="kindOptions"
        :disabled="linked"
        size="xs"
        class="w-full mt-0.5"
        @update:model-value="onKindChange"
      />
    </div>

    <!-- Downlink handle (routers fan out to edges) -->
    <Handle
      v-if="data.profile === 'router'"
      id="bottom"
      type="source"
      :position="Position.Bottom"
    />
  </div>
</template>

<style scoped>
.ergot-node {
  width: 110px;
  padding: 4px 6px;
  font-size: 10px;
}

.publish-toggle {
  font-size: 8px;
  line-height: 1;
  padding: 2px 3px;
  border-radius: 3px;
  color: var(--ui-text-muted);
  cursor: pointer;
}

.publish-toggle:hover {
  color: var(--ui-text-highlighted);
  background: var(--ui-bg-elevated);
}

.publish-toggle.active {
  color: var(--ui-error);
}
</style>
