/**
 * Evidence for #9727 (UX round): the pane header's regenerate control is no
 * longer an unlabeled glyph — the hover-revealed state carries the visible
 * "Auto-title" text next to the Sparkles, in every pane of a split.
 *
 * Runs the REAL built SPA (website/dist) behind the shared in-process static
 * server with all /api/** answered from fixtures (gateway-free) — the pod path
 * in capture-pane-header-rename.mjs needs the user systemd bus, which is not
 * reachable from every agent sandbox. Split view is reached the way a user
 * reaches it: `mc-split-layouts` holds a two-session layout anchored at the
 * left slot, and landing on that anchor auto-enters split; `session_grid` is
 * turned on by the config fixture.
 *
 * Frames:
 *   1. pane-header-autotitle-hover.png — pointer on the left pane's title bar:
 *      Pen + Sparkles + "Auto-title" revealed; the right pane stays at rest.
 *   2. pane-header-autotitle-hover-zh.png — the same in zh-CN, so the label is
 *      shown to come from the catalog, not a hardcoded string.
 *
 * Asserts, before shooting, that the regenerate button in the hovered pane is
 * visible and contains the catalog text, and that the pane at rest does not
 * show it.
 *
 * Usage: node scripts/capture-pane-header-autotitle.mjs [outDir]
 */
import { chromium } from 'playwright'
import { mkdirSync } from 'node:fs'
import { serveDist } from './lib/serve-dist.mjs'
import { logPageProblems, stubDashboardApi, json } from './lib/stub-dashboard-api.mjs'

const OUT = process.argv[2] || '../temp-screenshots/pane-header-rename'
mkdirSync(OUT, { recursive: true })

const LEFT = 'chat-left'
const RIGHT = 'chat-right'

const slots = [
  {
    key: LEFT, title: 'Release checklist review', messages: 4, running: false,
    agent: 'kirocrew', created: '2026-06-01T09:00:00Z', last_ts: '2026-08-13T10:00:00Z', folder_id: '',
  },
  {
    key: RIGHT, title: 'Pipeline triage', messages: 2, running: false,
    agent: 'oncall', created: '2026-08-12T09:00:00Z', last_ts: '2026-08-13T09:30:00Z', folder_id: '',
  },
]

const LAYOUT = {
  [LEFT]: {
    type: 'split', id: 'sp-1', dir: 'row', sizes: [0.5, 0.5],
    children: [
      { type: 'leaf', id: 'lf-1', kind: 'session', slot: LEFT },
      { type: 'leaf', id: 'lf-2', kind: 'session', slot: RIGHT },
    ],
  },
}

const leftMsgs = [
  { role: 'user', content: 'Which checklist items are still open?', ts: '2026-08-13T09:56:00Z', meta: { mid: 'm-1' } },
  { role: 'assistant', content: 'The changelog entry, the migration note, and the smoke run.', ts: '2026-08-13T09:57:00Z', meta: { mid: 'm-2' } },
]
const rightMsgs = [
  { role: 'user', content: 'Anything paging overnight?', ts: '2026-08-13T09:28:00Z', meta: { mid: 's-1' } },
  { role: 'assistant', content: 'Nothing paged. One warning cleared itself at 03:10.', ts: '2026-08-13T09:29:00Z', meta: { mid: 's-2' } },
]

const LABEL = { en: 'Auto-title', 'zh-CN': '自动命名' }

async function shoot(browser, base, locale, file) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, locale })
  const page = await context.newPage()
  await stubDashboardApi(page, {
    folders: [], slots,
    extra: async (path, route) => {
      if (path === '/api/dashboard/config') {
        await json(route, {
          restore_sessions: false, restore_window_minutes: 30,
          merge_queued_messages: false, widget_density: 'more', session_grid: true,
        })
        return true
      }
      if (path === `/api/chat/slots/${LEFT}`) { await json(route, { messages: leftMsgs, has_more: false, total: leftMsgs.length }); return true }
      if (path === `/api/chat/slots/${RIGHT}`) { await json(route, { messages: rightMsgs, has_more: false, total: rightMsgs.length }); return true }
      if (path === '/api/sessions') { await json(route, { sessions: [], has_more: false }); return true }
      if (path === '/api/chat/pins') { await json(route, { pins: [] }); return true }
      return false
    },
  })
  // Added after stubDashboardApi so it runs after that script's localStorage.clear().
  await page.addInitScript(({ layout, lang }) => {
    localStorage.setItem('mc-split-layouts', JSON.stringify(layout))
    localStorage.setItem('mc-lang', lang)
  }, { layout: LAYOUT, lang: locale })
  logPageProblems(page)

  await page.goto(`${base}/chat/${LEFT}`, { waitUntil: 'domcontentloaded' })
  await page.getByText('Pipeline triage').first().waitFor({ timeout: 20_000 })
  await page.waitForTimeout(2000)

  // Two pane toolbars, each hosting the shared control.
  const bars = page.locator('.group\\/header')
  const count = await bars.count()
  if (count < 2) throw new Error(`expected 2 pane headers, found ${count}`)
  const leftBar = bars.nth(0)
  const rightBar = bars.nth(1)

  await leftBar.hover()
  await page.waitForTimeout(400)
  const leftLabel = leftBar.getByText(LABEL[locale], { exact: true })
  await leftLabel.waitFor({ state: 'visible', timeout: 5_000 })
  const leftOpacity = await leftLabel.evaluate(el => {
    const btn = el.closest('button')
    return btn ? Number(getComputedStyle(btn).opacity) : -1
  })
  if (!(leftOpacity > 0.2)) throw new Error(`hovered pane's regenerate button is not revealed (opacity=${leftOpacity})`)
  const rightOpacity = await rightBar.getByText(LABEL[locale], { exact: true }).evaluate(el => {
    const btn = el.closest('button')
    return btn ? Number(getComputedStyle(btn).opacity) : -1
  })
  if (rightOpacity !== 0) throw new Error(`pane at rest still shows the regenerate control (opacity=${rightOpacity})`)

  await page.screenshot({ path: `${OUT}/${file}`, fullPage: false })
  console.log(`wrote ${OUT}/${file} (hovered label "${LABEL[locale]}", opacity ${leftOpacity}; rest pane opacity ${rightOpacity})`)
  await context.close()
}

async function main() {
  const { srv, base } = await serveDist()
  const browser = await chromium.launch()
  try {
    await shoot(browser, base, 'en', 'pane-header-autotitle-hover.png')
    await shoot(browser, base, 'zh-CN', 'pane-header-autotitle-hover-zh.png')
  } finally {
    await browser.close()
    srv.close()
  }
}

main().catch(e => { console.error(e); process.exit(1) })
