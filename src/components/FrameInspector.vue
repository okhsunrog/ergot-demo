<script setup lang="ts">
import { computed } from 'vue'
import type { FrameEvent } from '@/wasm-pkg/ergot_demo_wasm'

const props = defineProps<{
  frames: FrameEvent[]
  resolveLink: (edgeId: string) => string
}>()

const rows = computed(() => [...props.frames].reverse())

const kindColors: Record<string, string> = {
  req: 'var(--ui-info)',
  resp: 'var(--ui-success)',
  topic: 'var(--ui-primary)',
  err: 'var(--ui-error)',
}

function fmtTime(ts: number): string {
  const d = new Date(ts)
  const ms = String(d.getMilliseconds()).padStart(3, '0')
  return `${d.toLocaleTimeString('en-GB')}.${ms}`
}
</script>

<template>
  <div class="h-44 overflow-y-auto border-t border-(--ui-border-muted) bg-(--ui-bg-elevated)">
    <table class="w-full text-xs font-mono">
      <thead class="sticky top-0 bg-(--ui-bg-elevated) text-(--ui-text-muted)">
        <tr class="text-left">
          <th class="px-3 py-1 font-normal">time</th>
          <th class="px-3 py-1 font-normal">link</th>
          <th class="px-3 py-1 font-normal">dir</th>
          <th class="px-3 py-1 font-normal">src → dst</th>
          <th class="px-3 py-1 font-normal">kind</th>
          <th class="px-3 py-1 font-normal">seq</th>
        </tr>
      </thead>
      <tbody>
        <tr v-if="!rows.length">
          <td colspan="6" class="px-3 py-2 text-(--ui-text-muted)">no frames yet — try a ping</td>
        </tr>
        <tr
          v-for="(f, i) in rows"
          :key="`${f.ts}-${f.seq}-${i}`"
          class="border-t border-(--ui-border-muted)/40 text-(--ui-text-default)"
        >
          <td class="px-3 py-0.5 text-(--ui-text-muted)">{{ fmtTime(f.ts) }}</td>
          <td class="px-3 py-0.5">{{ resolveLink(f.linkId) }}</td>
          <td class="px-3 py-0.5">
            <span :style="{ color: f.dir === 'down' ? 'var(--ui-info)' : 'var(--ui-warning)' }">{{
              f.dir === 'down' ? '→' : '←'
            }}</span>
          </td>
          <td class="px-3 py-0.5">{{ f.src }} → {{ f.dst }}</td>
          <td class="px-3 py-0.5">
            <span :style="{ color: kindColors[f.kind] }">{{ f.kind }}</span>
          </td>
          <td class="px-3 py-0.5 text-(--ui-text-muted)">{{ f.seq }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
