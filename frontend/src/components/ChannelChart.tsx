/**
 * ChannelChart (SPEC section 8): renderer for `telemetry.channel` nodes.
 *
 * - subscribes to /ws/telemetry on mount, unsubscribes (refcounted) + destroys
 *   the uPlot instance on unmount
 * - renders a rolling 60 s window from the client-side ring buffer
 * - big current-value readout with unit, always shown with its recent history
 * - redraws are rAF-throttled; a slow tick keeps the window sliding when the
 *   channel goes quiet
 */

import { useEffect, useRef } from 'react'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { telemetry } from '../lib/telemetry'
import type { NodeComponentProps } from '../registry/registry'

const WINDOW_S = 60
const WINDOW_MS = WINDOW_S * 1000
const CHART_HEIGHT = 170
const IDLE_TICK_MS = 500

const FONT =
  '10px ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif'

function formatValue(v: number): string {
  const a = Math.abs(v)
  if (!Number.isFinite(v)) return '—'
  if (a >= 1000) return v.toFixed(0)
  if (a >= 100) return v.toFixed(1)
  if (a >= 1) return v.toFixed(2)
  if (a === 0) return '0.00'
  return v.toPrecision(3)
}

function cssVar(el: Element, name: string, fallback: string): string {
  const v = getComputedStyle(el).getPropertyValue(name).trim()
  return v !== '' ? v : fallback
}

function numProp(props: Record<string, unknown>, key: string): number | undefined {
  const v = props[key]
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined
}

export function ChannelChart({ node }: NodeComponentProps) {
  const plotRef = useRef<HTMLDivElement | null>(null)
  const valueRef = useRef<HTMLSpanElement | null>(null)

  const unit = typeof node.props['unit'] === 'string' ? node.props['unit'] : ''
  const sampleRateHz = numProp(node.props, 'sampleRateHz')
  const yMin = numProp(node.props, 'min')
  const yMax = numProp(node.props, 'max')
  const channelId = node.id

  useEffect(() => {
    const el = plotRef.current
    if (el === null) return

    const line = cssVar(el, '--chart-line', '#58a6ff')
    const grid = cssVar(el, '--chart-grid', 'rgba(140,150,165,0.12)')
    const axis = cssVar(el, '--chart-axis', '#8b95a3')

    const axisCommon: uPlot.Axis = {
      stroke: axis,
      font: FONT,
      grid: { stroke: grid, width: 1 },
      ticks: { stroke: grid, width: 1, size: 4 },
    }

    const opts: uPlot.Options = {
      width: Math.max(el.clientWidth, 240),
      height: CHART_HEIGHT,
      legend: { show: false },
      cursor: { show: false },
      scales: {
        x: {
          time: false,
          range: [-WINDOW_S, 0],
        },
        y: {
          auto: yMin === undefined || yMax === undefined,
          range: (_u, dataMin, dataMax) => {
            if (yMin !== undefined && yMax !== undefined) return [yMin, yMax]
            const lo = dataMin ?? 0
            const hi = dataMax ?? 1
            if (lo === hi) return [lo - 1, hi + 1]
            const pad = (hi - lo) * 0.1
            return [lo - pad, hi + pad]
          },
        },
      },
      series: [
        {},
        {
          stroke: line,
          width: 1,
          points: { show: false },
        },
      ],
      axes: [
        {
          ...axisCommon,
          incrs: [5, 10, 15, 30, 60],
          values: (_u, ticks) => ticks.map((v) => (v === 0 ? 'now' : `${v}s`)),
        },
        {
          ...axisCommon,
          size: 48,
        },
      ],
    }

    const u = new uPlot(opts, [[], []], el)

    let rafId = 0
    let destroyed = false

    const draw = () => {
      rafId = 0
      if (destroyed) return
      const buf = telemetry.getBuffer(channelId)
      const now = Date.now()
      if (buf !== undefined) {
        const { ts, vs } = buf.since(now - WINDOW_MS)
        const xs = ts.map((t) => (t - now) / 1000)
        u.setData([xs, vs])
        const last = buf.last()
        if (valueRef.current !== null) {
          valueRef.current.textContent = last !== undefined ? formatValue(last[1]) : '—'
        }
      }
    }

    // rAF throttle: many sample batches within one frame → one redraw.
    const scheduleDraw = () => {
      if (rafId === 0 && !destroyed) rafId = requestAnimationFrame(draw)
    }

    const unsubscribe = telemetry.subscribe(channelId, scheduleDraw)

    // Keep the 60 s window sliding even when no samples arrive.
    const idleTimer = window.setInterval(scheduleDraw, IDLE_TICK_MS)

    const resizeObserver = new ResizeObserver(() => {
      if (!destroyed) {
        u.setSize({ width: Math.max(el.clientWidth, 240), height: CHART_HEIGHT })
        scheduleDraw()
      }
    })
    resizeObserver.observe(el)

    return () => {
      destroyed = true
      window.clearInterval(idleTimer)
      if (rafId !== 0) cancelAnimationFrame(rafId)
      resizeObserver.disconnect()
      unsubscribe()
      u.destroy()
    }
  }, [channelId, yMin, yMax])

  return (
    <div className="chart">
      <div className="chart-head">
        <span className="chart-value" ref={valueRef}>
          —
        </span>
        <span className="chart-unit">{unit}</span>
        <span className="chart-meta">
          {sampleRateHz !== undefined ? `${sampleRateHz} Hz · ` : ''}
          last {WINDOW_S} s
        </span>
      </div>
      <div className="chart-plot" ref={plotRef} />
    </div>
  )
}
