import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// In dev, forward API calls (including the SSE stream) to the trace-producer on :8081.
// Override with VITE_API_TARGET if the backend runs elsewhere.
const target = process.env.VITE_API_TARGET ?? 'http://localhost:8081'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target, changeOrigin: true },
    },
  },
})
