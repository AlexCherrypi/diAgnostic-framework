import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { stateSocket } from './lib/ws.ts'

// Open the /ws/state connection once at boot; it reconnects on its own.
stateSocket.start()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
