<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import init, {
  greet,
  fibonacci,
  check_prime,
  start_prime_search,
  type BackgroundTask,
} from './wasm-pkg/ergot_demo_wasm'

const wasmReady = ref(false)
const greetName = ref('World')
const greetResult = ref('')
const fibN = ref(10)
const fibResult = ref('')
const primeN = ref(97)
const primeResult = ref('')

// Background task state
let task: BackgroundTask | null = null
const taskRunning = ref(false)
const checked = ref(0)
const primesFound = ref(0)
const lastPrime = ref(0)

onMounted(async () => {
  await init()
  wasmReady.value = true
})

onUnmounted(() => {
  stopSearch()
})

function runGreet() {
  greetResult.value = greet(greetName.value)
}

function runFibonacci() {
  const result = fibonacci(fibN.value)
  fibResult.value = `fibonacci(${fibN.value}) = ${result}`
}

function runIsPrime() {
  const result = check_prime(BigInt(primeN.value))
  primeResult.value = `${primeN.value} is ${result ? '' : 'not '}prime`
}

function startSearch() {
  if (task) return
  checked.value = 0
  primesFound.value = 0
  lastPrime.value = 0
  task = start_prime_search((c: number, f: number, lp: number) => {
    checked.value = c
    primesFound.value = f
    lastPrime.value = lp
  })
  taskRunning.value = true
}

function stopSearch() {
  if (!task) return
  task.stop()
  task.free()
  task = null
  taskRunning.value = false
}
</script>

<template>
  <div class="container">
    <h1>Ergot Demo</h1>
    <p class="subtitle">Rust WASM + Vue</p>

    <div v-if="!wasmReady" class="loading">Loading WASM module...</div>

    <template v-else>
      <section>
        <h2>Greet</h2>
        <div class="row">
          <input v-model="greetName" placeholder="Enter a name" @keyup.enter="runGreet" />
          <button @click="runGreet">Greet</button>
        </div>
        <p v-if="greetResult" class="result">{{ greetResult }}</p>
      </section>

      <section>
        <h2>Fibonacci</h2>
        <div class="row">
          <input v-model.number="fibN" type="number" min="0" max="93" @keyup.enter="runFibonacci" />
          <button @click="runFibonacci">Compute</button>
        </div>
        <p v-if="fibResult" class="result">{{ fibResult }}</p>
      </section>

      <section>
        <h2>Prime check</h2>
        <div class="row">
          <input v-model.number="primeN" type="number" min="0" @keyup.enter="runIsPrime" />
          <button @click="runIsPrime">Check</button>
        </div>
        <p v-if="primeResult" class="result">{{ primeResult }}</p>
      </section>

      <section>
        <h2>Background prime search</h2>
        <p class="description">
          Spawns an async Rust task (via tokio_with_wasm) that searches for
          primes and reports progress back to Vue.
        </p>
        <div class="row">
          <button v-if="!taskRunning" @click="startSearch">Start</button>
          <button v-else class="stop" @click="stopSearch">Stop</button>
        </div>
        <div v-if="checked > 0" class="stats">
          <div class="stat">
            <span class="stat-label">Checked</span>
            <span class="stat-value">{{ checked.toLocaleString() }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Primes found</span>
            <span class="stat-value">{{ primesFound.toLocaleString() }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">Latest prime</span>
            <span class="stat-value">{{ lastPrime.toLocaleString() }}</span>
          </div>
        </div>
      </section>
    </template>
  </div>
</template>

<style scoped>
.container {
  max-width: 600px;
  margin: 2rem auto;
  font-family: system-ui, sans-serif;
}

.subtitle {
  color: #888;
  margin-top: -0.5rem;
}

.loading {
  color: #999;
  font-style: italic;
}

.description {
  color: #666;
  font-size: 0.9rem;
  margin-top: -0.25rem;
}

section {
  margin: 1.5rem 0;
  padding: 1rem;
  border: 1px solid #ddd;
  border-radius: 8px;
}

h2 {
  margin-top: 0;
}

.row {
  display: flex;
  gap: 0.5rem;
}

input {
  flex: 1;
  padding: 0.5rem;
  border: 1px solid #ccc;
  border-radius: 4px;
  font-size: 1rem;
}

button {
  padding: 0.5rem 1rem;
  background: #42b883;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 1rem;
}

button:hover {
  background: #38a373;
}

button.stop {
  background: #e74c3c;
}

button.stop:hover {
  background: #c0392b;
}

.result {
  margin-top: 0.75rem;
  padding: 0.5rem;
  background: #f5f5f5;
  border-radius: 4px;
  font-family: monospace;
}

.stats {
  margin-top: 0.75rem;
  display: flex;
  gap: 1rem;
}

.stat {
  flex: 1;
  padding: 0.5rem;
  background: #f5f5f5;
  border-radius: 4px;
  text-align: center;
}

.stat-label {
  display: block;
  font-size: 0.75rem;
  color: #888;
  margin-bottom: 0.25rem;
}

.stat-value {
  display: block;
  font-family: monospace;
  font-size: 1.1rem;
  font-weight: 600;
}
</style>
