<script setup lang="ts">
import { ref, onMounted } from 'vue'
import init, { greet, fibonacci, is_prime } from './wasm-pkg/ergot_demo_wasm'

const wasmReady = ref(false)
const greetName = ref('World')
const greetResult = ref('')
const fibN = ref(10)
const fibResult = ref('')
const primeN = ref(97)
const primeResult = ref('')

onMounted(async () => {
  await init()
  wasmReady.value = true
})

function runGreet() {
  greetResult.value = greet(greetName.value)
}

function runFibonacci() {
  const result = fibonacci(fibN.value)
  fibResult.value = `fibonacci(${fibN.value}) = ${result}`
}

function runIsPrime() {
  const result = is_prime(BigInt(primeN.value))
  primeResult.value = `${primeN.value} is ${result ? '' : 'not '}prime`
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

.result {
  margin-top: 0.75rem;
  padding: 0.5rem;
  background: #f5f5f5;
  border-radius: 4px;
  font-family: monospace;
}
</style>
