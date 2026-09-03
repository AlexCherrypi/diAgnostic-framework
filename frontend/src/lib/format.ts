/**
 * Date/time formatting shared by the vehicle views: absolute local
 * timestamps, coarse relative phrases ("3 min ago") and a ticking `now` so
 * relative labels stay honest without re-rendering per second.
 */

import { useEffect, useState } from 'react'

function pad2(v: number): string {
  return String(v).padStart(2, '0')
}

/** "2026-09-02" in local time. */
export function formatDate(tsMs: number): string {
  if (!Number.isFinite(tsMs)) return '—'
  const d = new Date(tsMs)
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
}

/** "2026-09-02 14:03:11" in local time. */
export function formatDateTime(tsMs: number): string {
  if (!Number.isFinite(tsMs)) return '—'
  const d = new Date(tsMs)
  return `${formatDate(tsMs)} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`
}

/**
 * Coarse relative phrase for `tsMs` seen from `now` ("just now", "3 min ago",
 * "2 h ago", "5 days ago"); older than a month falls back to the date.
 */
export function formatRelative(tsMs: number, now: number): string {
  if (!Number.isFinite(tsMs)) return '—'
  const diff = now - tsMs
  const past = diff >= 0
  const s = Math.round(Math.abs(diff) / 1000)
  let text: string
  if (s < 45) return past ? 'just now' : 'in a moment'
  if (s < 90) text = '1 min'
  else if (s < 3600) text = `${Math.round(s / 60)} min`
  else if (s < 7200) text = '1 h'
  else if (s < 86_400) text = `${Math.round(s / 3600)} h`
  else if (s < 172_800) text = '1 day'
  else if (s < 30 * 86_400) text = `${Math.round(s / 86_400)} days`
  else return formatDate(tsMs)
  return past ? `${text} ago` : `in ${text}`
}

/** Wall-clock ms, refreshed every `intervalMs` (default 30 s) while mounted. */
export function useNow(intervalMs = 30_000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs)
    return () => window.clearInterval(timer)
  }, [intervalMs])
  return now
}
