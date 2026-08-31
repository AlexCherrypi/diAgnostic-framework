import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // State + telemetry WebSockets are served by the core on 8765.
      '/ws': {
        target: 'http://127.0.0.1:8765',
        ws: true,
      },
      '/api': {
        target: 'http://127.0.0.1:8765',
      },
    },
  },
})
