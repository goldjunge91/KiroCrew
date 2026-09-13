/**
 * Tasks & capacity — the durable task queue and the concurrency it runs under.
 *
 * Answers the questions the Sessions plane cannot: how much accepted work is
 * WAITING for capacity (and for how long), what the effective concurrency is
 * right now against the ceiling the user configured, why it was lowered, and
 * which live runs have yielded their slot and for what reason (children,
 * permission, dependency cooldown, real user input) or are being recovered.
 *
 * Read from `/api/tasks/summary` (`dashboard/handlers/tasks.py`), which folds
 * the task store, the structured session health and the adaptive controller
 * into one payload. Polled at the same cadence as the spawn panels: the store
 * read is cheap and the numbers move on the scale of seconds when a burst
 * lands.
 *
 * Purely additive to the Services plane: the card composes the plane's own
 * `Card` / section-row shape and the design tokens; it introduces no new
 * theme variables. It self-describes when the gateway runs without a task
 * store rather than rendering zeros as if the queue were empty.
 */
import { useQuery } from '@tanstack/react-query'
import { Card, CardTitle, Badge } from '../../components/ui'
import ErrorNotice from '../../components/ErrorNotice'
import InfoTip from '../../components/InfoTip'
import { api } from '../../api/client'
import type { LaneCap, TaskRow, TasksSummary } from '../../api/tasks'
import { fmtDuration, fmtNumber, type FormatUnit } from '../../i18n/format'
import { i18nT } from '../../i18n/t'

/** Same cadence as the spawn/activity panels (`/api/spawn` every 5s). */
export const TASKS_SUMMARY_REFETCH_MS = 5_000
/** Rows shown in the waits list before it folds into "N more not shown". */
const WAIT_ROWS_SHOWN = 8

/**
 * Catalog key per wait/health classification. A flat map of FULL literal keys
 * indexed inline at the `i18nT()` call, so `check-i18n-keys.mjs` validates the
 * union of the values; a key assembled from the wire's state string would be
 * invisible to that gate and render its raw dotted path the day a state is
 * renamed. Unknown states fall back to the raw token (a fact, not copy).
 */
const STATE_LABEL_KEY: Record<string, string> = {
  queued: 'pages.tasksCapacityCard.state_queued',
  admitted: 'pages.tasksCapacityCard.state_queued',
  starting: 'pages.tasksCapacityCard.state_running',
  running: 'pages.tasksCapacityCard.state_running',
  waiting_children: 'pages.tasksCapacityCard.state_waiting_children',
  waiting_permission: 'pages.tasksCapacityCard.state_waiting_permission',
  waiting_dependency: 'pages.tasksCapacityCard.state_waiting_dependency',
  waiting_input: 'pages.tasksCapacityCard.state_waiting_input',
  waiting_infra: 'pages.tasksCapacityCard.state_waiting_infra',
  retry_wait: 'pages.tasksCapacityCard.state_retry_wait',
  recovering: 'pages.tasksCapacityCard.state_recovering',
  stalled: 'pages.tasksCapacityCard.state_stalled',
}

/** Lane names the gateway publishes; anything else renders as its own key. */
const LANE_LABEL_KEY: Record<string, string> = {
  subagents: 'pages.tasksCapacityCard.lane_subagents',
  spawn_gate: 'pages.tasksCapacityCard.lane_spawn_gate',
  system: 'pages.tasksCapacityCard.lane_system',
}

type BadgeVariant = 'ok' | 'err' | 'warn' | 'aim' | 'muted'

/**
 * When the gateway publishes no stall threshold, the queue is "backed up"
 * once its oldest row has waited this long — the same 10 minutes
 * `session_health` uses before it calls a silent run stalled.
 */
const BACKLOG_WAIT_SECS_FALLBACK = 600
/**
 * Queued rows per effective slot of the widest lane before the backlog
 * counts as a warning: at 2× every slot already has a full turn of work
 * behind it, so a new request waits at least two dispatch cycles.
 */
const BACKLOG_DEPTH_FACTOR = 2

export type CardHealthState = 'healthy' | 'degraded' | 'backlog' | 'stalled'

export interface CardHealth {
  variant: BadgeVariant
  state: CardHealthState
}

/** Literal keys per health state, indexed inline (see `STATE_LABEL_KEY`). */
const HEALTH_LABEL_KEY: Record<CardHealthState, string> = {
  healthy: 'pages.tasksCapacityCard.badge_healthy',
  degraded: 'pages.tasksCapacityCard.badge_degraded',
  backlog: 'pages.tasksCapacityCard.badge_backlog',
  stalled: 'pages.tasksCapacityCard.badge_stalled',
}

/**
 * The badge state, derived from the numbers the card already shows so the
 * two can never disagree. Severity order, first match wins:
 *
 *  1. `err`  — any stalled run (`stalled` non-empty): the same red the
 *              "Stalled" row badge uses, because a run with no progress and
 *              no wait reason is the one thing nothing here recovers on its own.
 *  2. `warn` — the controller lowered the effective cap (`degrade_reason`
 *              set): the strongest signal about capacity, kept ahead of the
 *              backlog rules since it explains why the backlog exists.
 *  3. `warn` — backed up: the oldest queued row has waited at least
 *              `stall_after_secs` (fallback `BACKLOG_WAIT_SECS_FALLBACK`), or
 *              `queued` exceeds `BACKLOG_DEPTH_FACTOR` × the largest effective
 *              lane cap (skipped when no lane reports one). Amber to match the
 *              "Retry wait" / "Recovering" row badges: work is late, not lost.
 *  4. `ok`   — otherwise.
 */
export function cardHealth(d: TasksSummary): CardHealth {
  if (Object.keys(d.stalled).length > 0) return { variant: 'err', state: 'stalled' }
  if (d.degrade_reason) return { variant: 'warn', state: 'degraded' }
  const waitLimit = d.stall_after_secs ?? BACKLOG_WAIT_SECS_FALLBACK
  const widestCap = Math.max(0, ...Object.values(d.lanes).map(cap => cap.effective ?? 0))
  const longWait = d.oldest_wait_secs >= waitLimit
  const deepQueue = widestCap > 0 && d.depth.queued > BACKLOG_DEPTH_FACTOR * widestCap
  if (longWait || deepQueue) return { variant: 'warn', state: 'backlog' }
  return { variant: 'ok', state: 'healthy' }
}

function stateVariant(state: string): BadgeVariant {
  if (state === 'stalled') return 'err'
  if (state === 'recovering' || state === 'retry_wait' || state === 'waiting_infra') return 'warn'
  if (state.startsWith('waiting_')) return 'aim'
  if (state === 'running' || state === 'starting') return 'ok'
  return 'muted'
}

export function stateLabel(state: string): string {
  const key = STATE_LABEL_KEY[state]
  return key ? i18nT(key) : state
}

function laneLabel(lane: string): string {
  const key = LANE_LABEL_KEY[lane]
  return key ? i18nT(key) : lane
}

/**
 * An age in seconds as a compound duration. Seconds are kept: a 40-second
 * dependency cooldown is the common case here, and "0 min" would read as a
 * frozen counter.
 */
export function fmtAge(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—'
  const s = Math.floor(seconds)
  const parts: Array<[number, FormatUnit]> = [
    [Math.floor(s / 86400), 'day'],
    [Math.floor((s % 86400) / 3600), 'hour'],
    [Math.floor((s % 3600) / 60), 'minute'],
    [s % 60, 'second'],
  ]
  // Past an hour the seconds are noise; past a day so are the minutes.
  const trimmed = s >= 86400 ? parts.slice(0, 2) : s >= 3600 ? parts.slice(0, 3) : parts
  return fmtDuration(trimmed, { dropZero: true, maximumFractionDigits: 0 })
}

/** `effective / ceiling` for a lane; a lone number when no ceiling is known. */
function fmtCap(cap: LaneCap): string {
  const eff = cap.effective
  if (eff == null) return '—'
  const max = cap.user_max
  return max == null
    ? fmtNumber(eff)
    : i18nT('pages.tasksCapacityCard.cap_of_max', { effective: fmtNumber(eff), max: fmtNumber(max) })
}

/** One waiting/recovering entry, whether it came from the store or a live slot. */
interface WaitEntry {
  key: string
  id: string
  state: string
  reason: string
  ageSecs: number
  nextRunAt: number | null
  attempts: number
}

function entriesOf(data: TasksSummary): WaitEntry[] {
  // A parked row (retry_wait / recovering) carries no wait record; its
  // "reason" is when the dispatcher will pick it up again.
  const retryReason = (row: TaskRow): string =>
    row.next_run_at != null
      ? i18nT('pages.tasksCapacityCard.next_retry_in', {
        age: fmtAge(Math.max(0, row.next_run_at - data.generated_at)),
      })
      : ''
  const fromTask = (row: TaskRow): WaitEntry => ({
    key: `task:${row.id}`,
    id: row.id,
    state: row.state,
    reason: row.wait_reason ?? row.wait?.dependency_scope ?? retryReason(row),
    ageSecs: row.age_secs,
    nextRunAt: row.next_run_at,
    attempts: row.attempts,
  })
  const taskIds = new Set<string>()
  const out: WaitEntry[] = []
  for (const row of data.waiting) { out.push(fromTask(row)); taskIds.add(row.id) }
  for (const row of data.recovering.tasks) { out.push(fromTask(row)); taskIds.add(row.id) }
  // Live slots the store does not know (main chat sessions): their own
  // classification is the reason.
  for (const slot of data.slots) {
    if (slot.classification === 'running' || taskIds.has(slot.key)) continue
    out.push({
      key: `slot:${slot.key}`,
      id: slot.key,
      state: slot.classification,
      reason: slot.evidence.join(' · '),
      ageSecs: slot.age_secs,
      nextRunAt: null,
      attempts: 0,
    })
  }
  // Longest wait first: that is the one the operator is asking about.
  out.sort((a, b) => b.ageSecs - a.ageSecs)
  return out
}

/* ── Rows (the Services plane's own label/value shape) ── */

interface Row {
  label: string
  value: React.ReactNode
  tip?: string
}

function SectionBlock({ title, rows }: { title: string; rows: Row[] }) {
  return (
    <div className="mb-4" style={{ breakInside: 'avoid' }}>
      <h4 className="text-[11.5px] font-semibold text-muted uppercase tracking-wide mb-2">{title}</h4>
      {rows.map(row => (
        <div
          key={row.label}
          className="flex justify-between gap-3 py-1.5 border-b border-border text-[12.5px] last:border-b-0"
        >
          <span className="text-muted shrink-0 inline-flex items-center gap-1">
            {row.label}
            {row.tip && <InfoTip text={row.tip} />}
          </span>
          <span className="text-text-strong font-mono tabular-nums text-right break-words">{row.value}</span>
        </div>
      ))}
    </div>
  )
}

/* ── Main component ── */

export default function TasksCapacityCard() {
  const { data, error, isError } = useQuery<TasksSummary>({
    queryKey: ['tasksSummary'],
    queryFn: () => api.tasksSummary(),
    refetchInterval: TASKS_SUMMARY_REFETCH_MS,
  })

  const d = data ?? null
  const depth = d?.depth
  const lanes = d ? Object.entries(d.lanes) : []
  const entries = d ? entriesOf(d) : []
  const shown = entries.slice(0, WAIT_ROWS_SHOWN)
  const hidden = entries.length - shown.length
  const health = d ? cardHealth(d) : null

  const queueRows: Row[] = [
    { label: i18nT('pages.tasksCapacityCard.queued'), value: depth ? fmtNumber(depth.queued) : '—',
      tip: i18nT('pages.tasksCapacityCard.queued_tip') },
    { label: i18nT('pages.tasksCapacityCard.running'), value: depth ? fmtNumber(depth.running) : '—' },
    { label: i18nT('pages.tasksCapacityCard.waiting'), value: depth ? fmtNumber(depth.waiting) : '—',
      tip: i18nT('pages.tasksCapacityCard.waiting_tip') },
    { label: i18nT('pages.tasksCapacityCard.recovering'), value: depth ? fmtNumber(depth.recovering) : '—',
      tip: i18nT('pages.tasksCapacityCard.recovering_tip') },
    { label: i18nT('pages.tasksCapacityCard.oldest_wait'), value: d ? fmtAge(d.oldest_wait_secs) : '—',
      tip: i18nT('pages.tasksCapacityCard.oldest_wait_tip') },
  ]

  const laneRows: Row[] = lanes.length > 0
    ? lanes.map(([lane, cap]) => ({
      label: laneLabel(lane),
      tip: i18nT('pages.tasksCapacityCard.lane_tip'),
      // Two labelled facts, not one run of numbers: "4 of 14 slots" is the
      // cap this lane may fill against the ceiling the user set, and
      // "0 in use" is how many of those slots a live run holds right now.
      // Stacked on purpose: the column is a third of the card, so one line
      // would wrap anyway, and the muted second line reads as the detail.
      value: (
        <span className="inline-flex flex-col items-end leading-snug" data-testid="tasks-capacity-lane">
          <span>{fmtCap(cap)}</span>
          {cap.running != null && (
            <span className="text-muted">
              {i18nT('pages.tasksCapacityCard.lane_running', { count: fmtNumber(cap.running) })}
            </span>
          )}
        </span>
      ),
    }))
    : [{ label: i18nT('pages.tasksCapacityCard.effective_cap'), value: '—' }]
  laneRows.push({
    label: i18nT('pages.tasksCapacityCard.degrade_reason'),
    value: d?.degrade_reason
      ? <span style={{ color: 'var(--warn)' }}>{d.degrade_reason}</span>
      : <span className="text-muted">{i18nT('pages.tasksCapacityCard.not_degraded')}</span>,
    tip: i18nT('pages.tasksCapacityCard.degrade_reason_tip'),
  })

  const recovery = d?.recovering
  const recoveryRows: Row[] = [
    { label: i18nT('pages.tasksCapacityCard.task_retries'), value: recovery ? fmtNumber(recovery.task_attempts) : '—',
      tip: i18nT('pages.tasksCapacityCard.task_retries_tip') },
    { label: i18nT('pages.tasksCapacityCard.recovering_runs'), value: recovery ? fmtNumber(recovery.slots.length) : '—',
      tip: i18nT('pages.tasksCapacityCard.recovering_runs_tip') },
    { label: i18nT('pages.tasksCapacityCard.stalled'), value: d ? fmtNumber(Object.keys(d.stalled).length) : '—',
      tip: d?.stall_after_secs != null
        ? i18nT('pages.tasksCapacityCard.stalled_tip', { age: fmtAge(d.stall_after_secs) })
        : undefined },
  ]

  return (
    <Card data-testid="tasks-capacity-card">
      <CardTitle>
        {i18nT('pages.tasksCapacityCard.title')}
        {health && (
          <Badge variant={health.variant} data-testid="tasks-capacity-health">
            {i18nT(HEALTH_LABEL_KEY[health.state])}
          </Badge>
        )}
      </CardTitle>
      {isError && (
        <ErrorNotice
          message={error instanceof Error ? error.message : String(error)}
          title={i18nT('pages.tasksCapacityCard.load_failed')}
          askAgent
          className="mb-3"
          testId="tasks-capacity-error"
        />
      )}
      {d && !d.available && (
        <p className="text-[12.5px] text-muted mb-3">{i18nT('pages.tasksCapacityCard.no_store')}</p>
      )}
      <div className="columns-3 gap-6 max-[900px]:columns-2 max-[600px]:columns-1">
        <SectionBlock title={i18nT('pages.tasksCapacityCard.section_queue')} rows={queueRows} />
        <SectionBlock title={i18nT('pages.tasksCapacityCard.section_capacity')} rows={laneRows} />
        <SectionBlock title={i18nT('pages.tasksCapacityCard.section_recovery')} rows={recoveryRows} />
      </div>

      <h4 className="text-[11.5px] font-semibold text-muted uppercase tracking-wide mb-2 mt-1 flex items-center gap-1">
        {i18nT('pages.tasksCapacityCard.section_waits')}
        <InfoTip text={i18nT('pages.tasksCapacityCard.section_waits_tip')} />
      </h4>
      {d && shown.length === 0 && (
        <p className="text-[12.5px] text-muted py-1.5" data-testid="tasks-capacity-empty">
          {i18nT('pages.tasksCapacityCard.waits_empty')}
        </p>
      )}
      {shown.length > 0 && (
        <ul className="m-0 p-0 list-none" aria-label={i18nT('pages.tasksCapacityCard.section_waits')}>
          {shown.map(entry => (
            <li
              key={entry.key}
              data-testid="tasks-capacity-wait-row"
              className="flex flex-col gap-1 py-1.5 border-b border-border text-[12.5px] last:border-b-0 sm:flex-row sm:items-center sm:gap-3"
            >
              {/* Narrow-first: three stacked lines (badge + id / reason / attempts +
                  age) so the reason keeps its full width on a phone, where a
                  truncated span has no hover to reveal it. From `sm` up the two
                  wrappers dissolve (`contents`) and everything sits on one row. */}
              <div className="flex items-center gap-2 min-w-0 sm:contents">
                <Badge variant={stateVariant(entry.state)}>{stateLabel(entry.state)}</Badge>
                <span className="font-mono text-text-strong truncate min-w-0 max-w-[220px]" title={entry.id}>
                  {entry.id}
                </span>
              </div>
              <span
                className="text-muted break-words sm:min-w-0 sm:flex-1 sm:truncate"
                data-testid="tasks-capacity-wait-reason"
                title={entry.reason || undefined}
              >
                {entry.reason}
              </span>
              <div className="flex items-center gap-3 sm:contents">
                {entry.attempts > 1 && (
                  <span className="text-muted shrink-0">
                    {i18nT('pages.tasksCapacityCard.attempts', { count: fmtNumber(entry.attempts) })}
                  </span>
                )}
                {/* "5s so far", not a bare "5s": on a retry row the reason column
                    already carries a clock ("next retry in 1m 29s"), so the age
                    says which one it is. Not "for 5s": a value opening with a
                    connector word reads as half a sentence to the source-string
                    gate, and to a translator. */}
                <span
                  className="font-mono tabular-nums text-text-strong shrink-0"
                  data-testid="tasks-capacity-wait-age"
                  title={i18nT('pages.tasksCapacityCard.age_tip')}
                >
                  {i18nT('pages.tasksCapacityCard.age_for', { age: fmtAge(entry.ageSecs) })}
                </span>
              </div>
            </li>
          ))}
        </ul>
      )}
      {hidden > 0 && (
        <p className="text-[12.5px] text-muted pt-1.5">
          {i18nT('pages.tasksCapacityCard.and_more', { count: fmtNumber(hidden) })}
        </p>
      )}
    </Card>
  )
}
