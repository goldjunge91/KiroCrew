/**
 * System > Services > "Tasks & capacity".
 *
 * The card is the only dashboard surface that shows the durable task queue
 * and the effective concurrency the gateway is running under. These cases pin
 * the render to the `/api/tasks/summary` payload: depth by state, the oldest
 * wait, the effective cap against the user's ceiling with the degrade reason,
 * one row per waiting/recovering task or slot with its reason and age, the
 * empty state, the no-store notice, and the failure notice.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen, waitFor } from '@testing-library/react'

import { renderWithProviders } from './helpers'
import type { TasksSummary } from '../api/tasks'

const tasksSummary = vi.fn<() => Promise<TasksSummary>>()

vi.mock('../api/client', () => ({
  api: {
    tasksSummary: () => tasksSummary(),
  },
}))

import TasksCapacityCard, { cardHealth, fmtAge } from '../pages/system/TasksCapacityCard'

function summary(overrides: Partial<TasksSummary> = {}): TasksSummary {
  return {
    generated_at: 1_000,
    available: true,
    depth: { by_state: {}, queued: 0, waiting: 0, recovering: 0, running: 0, total: 0 },
    oldest_wait_secs: 0,
    lanes: {},
    degrade_reason: null,
    adaptive: null,
    slots: [],
    waiting: [],
    recovering: { tasks: [], task_attempts: 0, slots: [], ladder: [] },
    stalled: {},
    counts: {},
    stall_after_secs: 600,
    ...overrides,
  }
}

function row(id: string, state: string, extra: Record<string, unknown> = {}) {
  return {
    id, kind: 'subagent', state, lane: 'web-a', session_key: 'web-a', parent_id: null, root_id: id,
    attempts: 1, generation: 1, next_run_at: null, deadline_at: null, lease_owner: null,
    lease_expires_at: null, wait: null, wait_reason: null, wait_since: null, wait_deadline_at: null,
    age_secs: 0, created_at: 0, updated_at: 0, terminal: false, ...extra,
  }
}

beforeEach(() => {
  tasksSummary.mockReset()
})

describe('TasksCapacityCard', () => {
  it('renders the empty state with a healthy badge and zero counts', async () => {
    tasksSummary.mockResolvedValue(summary())
    renderWithProviders(<TasksCapacityCard />)

    await screen.findByTestId('tasks-capacity-empty')
    expect(screen.getByText('Tasks & capacity')).toBeTruthy()
    expect(screen.getByText('Healthy')).toBeTruthy()
    expect(screen.getByText('Nothing is waiting or recovering.')).toBeTruthy()
    // No store notice when the store IS available.
    expect(screen.queryByText(/without a task store/)).toBeNull()
  })

  it('says so when the gateway has no task store instead of showing zeros as facts', async () => {
    tasksSummary.mockResolvedValue(summary({ available: false }))
    renderWithProviders(<TasksCapacityCard />)
    await screen.findByText(/runs without a task store/)
  })

  it('shows depth, oldest wait, cap vs ceiling, the degrade reason and each wait with its age', async () => {
    tasksSummary.mockResolvedValue(summary({
      depth: {
        by_state: { queued: 12, running: 3, waiting_dependency: 1, waiting_input: 1, recovering: 2 },
        queued: 12, waiting: 2, recovering: 2, running: 3, total: 19,
      },
      oldest_wait_secs: 754,
      lanes: {
        subagents: { effective: 4, user_max: 8, running: 3 },
        spawn_gate: { effective: 2, user_max: 8 },
      },
      degrade_reason: 'loop_lag=410ms',
      waiting: [
        row('dep-1', 'waiting_dependency', {
          wait_reason: 'github:api rate limited', age_secs: 42,
          wait: { reason: 'github:api rate limited', since: 900, deadline_at: null, resume_kind: 'at_time', dependency_scope: 'github:api', cancel_semantics: 'task', tool_call_id: '' },
        }),
        row('inp-1', 'waiting_input', { wait_reason: 'sudo wants a password', age_secs: 3_700 }),
      ],
      recovering: {
        tasks: [row('rec-1', 'recovering', { attempts: 3, age_secs: 5, next_run_at: 1_000 + 90 })],
        task_attempts: 3,
        slots: [{ key: 'web-z', age_secs: 12, evidence: ['retry in flight: infra'] }],
        ladder: [],
      },
      slots: [
        { key: 'web-z', classification: 'recovering', age_secs: 12, evidence: ['retry in flight: infra'] },
        { key: 'web-ok', classification: 'running', age_secs: 1, evidence: [] },
      ],
      stalled: { 'web-s': { reason: 'no_progress', since_ts: 0, age_secs: 700, evidence: [] } },
    }))
    renderWithProviders(<TasksCapacityCard />)

    // A stalled run outranks the degrade reason: the badge goes red, not amber.
    const badge = await screen.findByTestId('tasks-capacity-health')
    expect(badge.textContent).toBe('Stalled')
    expect(badge.className).toContain('text-danger')
    // Queue depth.
    const queued = screen.getByText('Queued', { selector: 'span' }).closest('div')!
    expect(queued.textContent).toContain('12')
    expect(screen.getByText('Oldest wait').closest('div')!.textContent).toContain(fmtAge(754))
    // Effective cap against the ceiling, per lane, with the in-use count as a
    // separately labelled fact rather than a third bare number.
    const [subagents, spawnGate] = screen.getAllByTestId('tasks-capacity-lane')
    const subagentLines = Array.from(subagents.querySelectorAll('span')).map(s => s.textContent)
    expect(subagentLines).toEqual(['4 of 8 slots', '3 in use'])
    expect(spawnGate.textContent).toBe('2 of 8 slots')
    expect(screen.getByText('Subagent runs').closest('div')!.textContent).not.toContain('running:')
    expect(screen.getByText('loop_lag=410ms')).toBeTruthy()
    // Recovery column: attempts across recovering rows, recovering slots, stalled.
    expect(screen.getByText('Task retries').closest('div')!.textContent).toContain('3')
    expect(screen.getByText('Recovering sessions').closest('div')!.textContent).toContain('1')
    expect(screen.getByText('Stalled', { selector: 'span:not([data-testid])' }).closest('div')!.textContent).toContain('1')
    // Waits list: one row per task/slot with its reason, age and state badge;
    // the running slot is NOT a wait.
    const list = screen.getByRole('list', { name: 'Waiting & recovering' })
    const items = list.querySelectorAll('li')
    expect(items).toHaveLength(4)
    expect(list.textContent).toContain('github:api rate limited')
    expect(list.textContent).toContain('sudo wants a password')
    expect(list.textContent).toContain('Waiting for dependency')
    expect(list.textContent).toContain('Waiting for input')
    expect(list.textContent).toContain('attempts: 3')
    expect(list.textContent).toContain(`next retry in ${fmtAge(90)}`)
    // The age column names its clock: "for 5s" beside "next retry in 1m 30s",
    // never two bare durations on one row.
    const ages = screen.getAllByTestId('tasks-capacity-wait-age')
    expect(ages).toHaveLength(4)
    expect(ages.map(a => a.textContent)).toContain(`${fmtAge(3_700)} so far`)
    const recRow = Array.from(items).find(li => li.textContent!.includes('rec-1'))!
    expect(recRow.textContent).toContain(`next retry in ${fmtAge(90)}`)
    expect(recRow.querySelector('[data-testid="tasks-capacity-wait-age"]')!.textContent).toBe(`${fmtAge(5)} so far`)
    for (const a of ages) expect(a.getAttribute('title')).toBe('Time spent in the state the badge names.')
    expect(list.textContent).not.toContain('web-ok')
    // Longest wait first.
    expect(items[0].textContent).toContain('inp-1')
    expect(screen.queryByTestId('tasks-capacity-empty')).toBeNull()
  })

  it('folds a long wait list and names how many are hidden', async () => {
    tasksSummary.mockResolvedValue(summary({
      waiting: Array.from({ length: 11 }, (_, i) => row(`w-${i}`, 'waiting_children', { age_secs: i })),
    }))
    renderWithProviders(<TasksCapacityCard />)
    await screen.findByText('3 more not shown')
    expect(screen.getByRole('list', { name: 'Waiting & recovering' }).querySelectorAll('li')).toHaveLength(8)
  })

  it('derives the badge from the numbers the card shows, not from degrade_reason alone', async () => {
    // 14 queued, an oldest wait past the stall threshold and a stalled run
    // with NO degrade reason: the reviewer's case, which used to read Healthy.
    tasksSummary.mockResolvedValue(summary({
      depth: { by_state: {}, queued: 14, waiting: 4, recovering: 2, running: 2, total: 22 },
      oldest_wait_secs: 1_308,
      lanes: { subagents: { effective: 4, user_max: 14, running: 0 } },
      stalled: { 'web-s': { reason: 'no_progress', since_ts: 0, age_secs: 700, evidence: [] } },
    }))
    renderWithProviders(<TasksCapacityCard />)
    const badge = await screen.findByTestId('tasks-capacity-health')
    expect(badge.textContent).toBe('Stalled')
    expect(badge.className).toContain('text-danger')
    expect(screen.queryByText('Healthy')).toBeNull()
  })

  it('renders the backlog badge in amber when the queue is late but nothing is stalled', async () => {
    tasksSummary.mockResolvedValue(summary({
      depth: { by_state: {}, queued: 14, waiting: 0, recovering: 0, running: 2, total: 16 },
      oldest_wait_secs: 1_308,
      lanes: { subagents: { effective: 4, user_max: 14, running: 2 } },
    }))
    renderWithProviders(<TasksCapacityCard />)
    const badge = await screen.findByTestId('tasks-capacity-health')
    expect(badge.textContent).toBe('Backed up')
    expect(badge.className).toContain('text-warn')
  })

  it('keeps a long reason un-truncated at the narrow layout and only clips from sm up', async () => {
    const reason = 'GitHub API rate limited for repo kirodotdev/KiroCrew; the coordinator resumes this run once the reset window at the top of the hour has passed'
    tasksSummary.mockResolvedValue(summary({
      waiting: [row('dep-long', 'waiting_dependency', { wait_reason: reason, age_secs: 70, attempts: 3 })],
    }))
    renderWithProviders(<TasksCapacityCard />)
    const el = await screen.findByTestId('tasks-capacity-wait-reason')
    // The full sentence is in the DOM (not elided) ...
    expect(el.textContent).toBe(reason)
    // ... and truncation is a `sm:`-scoped utility only: at narrow widths the
    // span wraps (`break-words`) instead of relying on a hover-only title.
    const classes = el.className.split(/\s+/)
    expect(classes).toContain('break-words')
    expect(classes).toContain('sm:truncate')
    expect(classes).not.toContain('truncate')
    // The row stacks narrow-first and becomes a single row from sm up.
    const li = screen.getByTestId('tasks-capacity-wait-row')
    expect(li.className.split(/\s+/)).toEqual(expect.arrayContaining(['flex-col', 'sm:flex-row']))
  })

  it('renders the failure through ErrorNotice, not a bare div', async () => {
    tasksSummary.mockRejectedValue(new Error('503 task store unavailable'))
    renderWithProviders(<TasksCapacityCard />)
    await waitFor(() => expect(screen.getByTestId('tasks-capacity-error')).toBeTruthy())
    expect(screen.getByTestId('tasks-capacity-error').textContent).toContain('Could not load the task queue')
  })
})

describe('cardHealth', () => {
  const base = () => summary({
    depth: { by_state: {}, queued: 0, waiting: 0, recovering: 0, running: 0, total: 0 },
    lanes: { subagents: { effective: 4, user_max: 8, running: 0 } },
    stall_after_secs: 600,
  })

  it('is healthy on an empty queue with nothing degraded', () => {
    expect(cardHealth(base())).toEqual({ variant: 'ok', state: 'healthy' })
  })

  it('ranks stalled above degraded above backlog', () => {
    const all = summary({
      ...base(),
      degrade_reason: 'loop_lag=410ms',
      oldest_wait_secs: 5_000,
      depth: { by_state: {}, queued: 40, waiting: 0, recovering: 0, running: 0, total: 40 },
      stalled: { s: { reason: 'no_progress', since_ts: 0, age_secs: 1, evidence: [] } },
    })
    expect(cardHealth(all)).toEqual({ variant: 'err', state: 'stalled' })
    expect(cardHealth({ ...all, stalled: {} })).toEqual({ variant: 'warn', state: 'degraded' })
    expect(cardHealth({ ...all, stalled: {}, degrade_reason: null })).toEqual({ variant: 'warn', state: 'backlog' })
  })

  it('calls the queue backed up at the stall threshold, and one second under it healthy', () => {
    expect(cardHealth({ ...base(), oldest_wait_secs: 600 }).state).toBe('backlog')
    expect(cardHealth({ ...base(), oldest_wait_secs: 599 }).state).toBe('healthy')
    // No published threshold → the 10-minute fallback.
    expect(cardHealth({ ...base(), stall_after_secs: null, oldest_wait_secs: 600 }).state).toBe('backlog')
    expect(cardHealth({ ...base(), stall_after_secs: null, oldest_wait_secs: 599 }).state).toBe('healthy')
  })

  it('calls the queue backed up past 2× the widest effective lane cap, and never without a cap', () => {
    const depth = (queued: number) => ({ by_state: {}, queued, waiting: 0, recovering: 0, running: 0, total: queued })
    expect(cardHealth({ ...base(), depth: depth(9) }).state).toBe('backlog')
    expect(cardHealth({ ...base(), depth: depth(8) }).state).toBe('healthy')
    // The widest lane sets the bar, not the narrowest.
    const lanes = { subagents: { effective: 4, user_max: 8 }, spawn_gate: { effective: 2, user_max: 8 } }
    expect(cardHealth({ ...base(), lanes, depth: depth(8) }).state).toBe('healthy')
    // No lane reports an effective cap: depth alone cannot say backlog.
    expect(cardHealth({ ...base(), lanes: {}, depth: depth(500) }).state).toBe('healthy')
  })
})

describe('fmtAge', () => {
  it('keeps seconds under an hour, drops them past it, and dashes bad input', () => {
    expect(fmtAge(42)).toContain('42')
    expect(fmtAge(3_700)).not.toContain('40')   // 1h 1m 40s → seconds dropped
    expect(fmtAge(3_700)).toContain('1')
    expect(fmtAge(-1)).toBe('—')
    expect(fmtAge(null)).toBe('—')
  })
})
