<script setup lang="ts">
const open = defineModel<boolean>('open', { required: true })
</script>

<template>
  <UModal v-model:open="open" title="Ergot Playground" description="A real network in your tab">
    <template #body>
      <div class="space-y-3 text-sm text-(--ui-text-default)">
        <p>
          Every node on this canvas is a real
          <a
            href="https://github.com/jamesmunns/ergot"
            target="_blank"
            class="text-(--ui-primary) underline"
            >ergot</a
          >
          networking stack compiled to WebAssembly. Links carry real frames, pings route through
          real Router profiles — nothing is mocked. Things to try:
        </p>
        <ul class="space-y-2 list-disc pl-5">
          <li>
            <b>Ping:</b> select two nodes (click, then ctrl-click — ⌘ on Mac), hit <b>Ping</b> — the
            request hops edge → router → edge and shows the round trip.
          </li>
          <li>
            <b>Broadcast:</b> press <b>▶</b> on any node card to publish a sensor stream. Every link
            animates as the broadcast fans out, and sparklines appear on all subscribers.
          </li>
          <li>
            <b>Break a link:</b> select a link, set <b>loss</b> to 100 — that node's sparkline
            starves while its siblings keep flowing. Set <b>lat</b> to 200 and ping through it.
          </li>
          <li>
            <b>Inspect frames:</b> the <b>Frames</b> panel lists every frame on the wire —
            addresses, kind, sequence numbers.
          </li>
          <li>
            <b>Rewire:</b> drag from a router's bottom handle to a node's top handle. Uplinked nodes
            take one uplink; the transport (COBS byte stream vs packet) is selectable while
            unlinked.
          </li>
          <li>
            <b>Bridge:</b> add a Bridge between a router and its nodes — watch it lease network ids
            for its downlinks from the upstream seed router (the <b>nets</b> on its card appear only
            after its uplink comes alive).
          </li>
          <li>
            <b>Shared bus:</b> add a Bus, connect <b>Router → Bus</b>, then fan out
            <b>Bus → Packet Edge</b>. Every device shares one network id and claims a unique node id
            at runtime, just like devices on CAN FD, RS-485, or a simple radio medium.
          </li>
        </ul>
        <p class="text-(--ui-text-muted)">
          Addresses appear on cards as nodes learn them (<span class="font-mono">net.node</span>).
          Dashed links are packet transports. Latency/loss selected on any bus leg applies to the
          whole shared medium. Source:
          <a
            href="https://github.com/okhsunrog/ergot-demo"
            target="_blank"
            class="text-(--ui-primary) underline"
            >okhsunrog/ergot-demo</a
          >.
        </p>
      </div>
    </template>
  </UModal>
</template>
