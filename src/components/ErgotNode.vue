<script setup lang="ts">
import { Handle, Position } from '@vue-flow/core'

export type ProfileType = 'root-router' | 'bridge-router' | 'edge'

export interface ErgotNodeData {
  label: string
  profile: ProfileType
}

const props = defineProps<{ data: ErgotNodeData }>()

const profileOptions = [
  { label: 'Root', value: 'root-router' },
  { label: 'Bridge', value: 'bridge-router' },
  { label: 'Edge', value: 'edge' },
]

const profileColors: Record<ProfileType, string> = {
  'root-router': 'var(--ui-warning)',
  'bridge-router': 'var(--ui-info)',
  edge: 'var(--ui-success)',
}

function onProfileChange(value: string) {
  props.data.profile = value as ProfileType
}
</script>

<template>
  <div class="ergot-node">
    <!-- Upstream handle -->
    <Handle v-if="data.profile !== 'root-router'" id="top" type="target" :position="Position.Top" />

    <div class="flex items-center gap-1 mb-0.5">
      <span
        class="inline-block w-1.5 h-1.5 rounded-full shrink-0"
        :style="{ background: profileColors[data.profile] }"
      />
      <span class="font-medium text-(--ui-text-highlighted) truncate leading-tight">
        {{ data.label }}
      </span>
    </div>

    <USelect
      :model-value="data.profile"
      :items="profileOptions"
      size="xs"
      class="w-full"
      @update:model-value="onProfileChange"
    />

    <!-- Downstream handle -->
    <Handle v-if="data.profile !== 'edge'" id="bottom" type="source" :position="Position.Bottom" />
  </div>
</template>

<style scoped>
.ergot-node {
  width: 100px;
  padding: 4px 6px;
  font-size: 10px;
}
</style>
