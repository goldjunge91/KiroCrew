import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ArrowDownLeft,
  ArrowUpRight,
  ChevronDown,
  ChevronRight,
  ClipboardList,
  Info,
  Mail,
  MessageSquareShare,
  Timer,
  Users,
} from 'lucide-react'
import { api, type MemberProjectionRow } from '../../api/client'
import {
  MEMBERS_ROSTER_QUERY_KEY,
  MEMBER_PROJECTION_POLL_MS,
  memberProjectionQueryKey,
} from '../../api/membersQuery'
import ErrorNotice from '../../components/ErrorNotice'
import UserMessage from '../chat/UserMessage'
import AssistantMessage from '../chat/AssistantMessage'
import { renderUserContent } from '../chat/ChatPageMessageContent'
import { fmtMessageTime, fmtMessageTimeFull } from '../chat/messageTime'
import { cn } from '../../lib/utils'
import {
  groupProjectionRows,
  isNewerThanMarker,
  newestRow,
  peerLabel,
  previewLine,
  type ProjectionItem,
} from './projection'

/** How close to the bottom (px) still counts as reading the tail. */
const FOLLOW_TAIL_PX = 48

/**
 * The inbox-model thread (RFC member-inbox-model, M1): a flagged member's
 * conversation rendered from GET /api/members/{slug}/projection instead of
 * the DM slot's transcript.
 *
 * `user_dm` rows the person typed and the member's outbox `reply` rows are the
 * conversation and draw with the SAME bubble components the transcript uses
 * (`UserMessage`, `AssistantMessage`), so switching a member onto the inbox
 * model changes what feeds the thread, not how it reads. Every other envelope
 * kind — `peer_dm` either way, `worker_report`, `wake_timer`, `system`, a
 * `session_dm` another session relayed — renders as a compact badged row, and
 * consecutive rows of one kind fold into a group collapsed to a single line
 * with an expand control. The person sees THAT the member talked to a peer or
 * got a report, and can read it, without it looking like either of them said
 * it to the other.
 *
 * Viewing is reading: while the page is visible and the newest row is newer
 * than the read marker, the marker advances (POST /read) and the roster's
 * unread badge follows. The composer stays the host's — typing still posts to
 * api_chat, which mints the `user_dm`.
 */
const EMPTY_ROWS: readonly MemberProjectionRow[] = []

export default function MemberProjection({
  slug,
  visible,
  memberName,
}: {
  slug: string
  /** The host's page-visible-and-focused reading; a hidden tab never marks read. */
  visible: boolean
  /** The crew name, for the avatar seed and the member bubble's identity. */
  memberName: string
}) {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: memberProjectionQueryKey(slug),
    queryFn: () => api.memberProjection(slug),
    refetchInterval: MEMBER_PROJECTION_POLL_MS,
    refetchIntervalInBackground: false,
  })
  const rows = useMemo(() => query.data?.rows ?? EMPTY_ROWS, [query.data?.rows])
  const marker = query.data?.marker ?? null
  const items = useMemo(() => groupProjectionRows(rows), [rows])

  // Mark read: the newest rendered row, once per row id, only while visible.
  // `lastMarked` guards the loop the poll would otherwise run (refetch → same
  // newest row → POST again). On success the roster is invalidated so the
  // row's badge clears without waiting for its own 30s staleness.
  // A failed POST is shown through the shared error notice and the guard is
  // KEPT for the row that failed: clearing it in onError would re-run this
  // effect on the state change and POST again at render speed for as long as
  // the failure persists. The guard is released when the thread goes hidden,
  // so the next visibility change (or a newer row) retries it -- a durable
  // unread never stays stale behind a silently swallowed failure.
  const lastMarked = useRef('')
  const markReadFailedRef = useRef(false)
  const [markReadFailed, setMarkReadFailed] = useState(false)
  const markRead = useMutation({
    mutationFn: (id: string) => api.memberMarkRead(slug, id),
    onSuccess: (data) => {
      markReadFailedRef.current = false
      setMarkReadFailed(false)
      queryClient.setQueryData(memberProjectionQueryKey(slug), (prev: typeof query.data) =>
        prev ? { ...prev, unread: data.unread, marker: data.marker } : prev,
      )
      void queryClient.invalidateQueries({ queryKey: MEMBERS_ROSTER_QUERY_KEY })
    },
    onError: () => {
      markReadFailedRef.current = true
      setMarkReadFailed(true)
    },
  })
  // Where the reader is. `atTail` gates the read marker (state, so scrolling
  // back down re-runs the effect); the ref is the synchronous copy the tail
  // follow below samples before a new row renders.
  const scrollerRef = useRef<HTMLDivElement>(null)
  const atTailRef = useRef(true)
  const [atTail, setAtTail] = useState(true)
  const onScroll = useCallback(() => {
    const el = scrollerRef.current
    if (!el) return
    const at = el.scrollHeight - el.scrollTop - el.clientHeight <= FOLLOW_TAIL_PX
    atTailRef.current = at
    setAtTail(at)
  }, [])

  const newest = newestRow(rows)
  // "Read" means SEEN: the newest row is marked only while the thread is
  // visible AND the reader is at the tail where that row renders. Scrolled up
  // into the history during an active relay, a row arriving off-screen must
  // not advance the marker -- it only moves forward, so a badge cleared for a
  // row nobody saw stays cleared.
  useEffect(() => {
    if (!visible || !atTail || !newest || !query.data?.inbox_model) return
    if (!isNewerThanMarker(newest, marker)) return
    if (lastMarked.current === newest.id || markRead.isPending) return
    lastMarked.current = newest.id
    markRead.mutate(newest.id)
  }, [visible, atTail, newest, marker, markRead, query.data?.inbox_model])
  // Hidden after a failure: release the guard so the next visible retries.
  useEffect(() => {
    if (!visible && markReadFailedRef.current) lastMarked.current = ''
  }, [visible])
  // A slug change is a different member: the guard must not carry over.
  useEffect(() => {
    lastMarked.current = ''
    markReadFailedRef.current = false
  }, [slug])

  // Follow the tail like a transcript: a new newest row scrolls into view --
  // but only while the reader IS at the tail. A person scrolled up into the
  // history during an active relay (a new row every few seconds) must not be
  // snapped back down on every poll, so the position is sampled before the row
  // renders and the tail is followed only from within FOLLOW_TAIL_PX of it.
  useEffect(() => {
    const el = scrollerRef.current
    if (!el || !atTailRef.current) return
    el.scrollTop = el.scrollHeight
  }, [newest?.id])

  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const toggle = useCallback((key: string) => {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  // The notice replaces the thread only when there is nothing else to show.
  // react-query keeps the last good rows through a failed background refetch,
  // and with a 4 s poll one network blip would otherwise blank the whole
  // conversation until the next success; with stale rows the notice sits
  // above them and the poll keeps retrying.
  const loadNotice = query.isError && (
    <div className="px-4 py-2">
      {/* No hand-off: MemberProjection is ChatPane's transcript; a nav would discard
          the pane's unsaved composer draft (`input`). */}
      <ErrorNotice
        message={t('pages.membersPage.inbox_load_failed')}
        variant="inline"
        askAgent={false}
        testId="member-projection-error"
      />
    </div>
  )
  if (query.isError && !query.data) {
    return <>{loadNotice}</>
  }

  return (
    <div
      ref={scrollerRef}
      onScroll={onScroll}
      className="chat-container flex-1 min-h-0 overflow-y-auto"
      style={{ paddingTop: 12, paddingBottom: 12 }}
      data-testid="member-projection"
      aria-label={t('pages.membersPage.inbox_thread', { name: memberName })}
    >
      {loadNotice}
      {markReadFailed && (
        <div className="px-4 pb-2">
          {/* No hand-off: MemberProjection is ChatPane's transcript; a nav would discard
              the pane's unsaved composer draft (`input`). */}
          <ErrorNotice
            message={t('pages.membersPage.inbox_mark_read_failed')}
            variant="inline"
            askAgent={false}
            testId="member-projection-mark-read-error"
          />
        </div>
      )}
      {query.isSuccess && rows.length === 0 && (
        <div className="text-center text-muted text-[13px] py-8" data-testid="member-projection-empty">
          {t('pages.membersPage.inbox_empty')}
        </div>
      )}
      {items.map((item) => (
        <ProjectionItemView
          key={item.key}
          item={item}
          expanded={expanded.has(item.key)}
          onToggle={() => toggle(item.key)}
          marker={marker}
        />
      ))}
    </div>
  )
}

/** Lucide glyph per envelope kind. One place, so a new kind gets a face here
 *  and a label in the catalog and nowhere else. */
const KIND_ICON: Record<string, typeof Mail> = {
  user_dm: Mail,
  session_dm: MessageSquareShare,
  peer_dm: Users,
  worker_report: ClipboardList,
  wake_timer: Timer,
  system: Info,
}

const KIND_LABEL_KEY: Record<string, string> = {
  user_dm: 'pages.membersPage.inbox_kind_user_dm',
  session_dm: 'pages.membersPage.inbox_kind_session_dm',
  reply: 'pages.membersPage.inbox_kind_reply',
  peer_dm: 'pages.membersPage.inbox_kind_peer_dm',
  worker_report: 'pages.membersPage.inbox_kind_worker_report',
  wake_timer: 'pages.membersPage.inbox_kind_wake_timer',
  system: 'pages.membersPage.inbox_kind_system',
}

const KIND_TIP_KEY: Record<string, string> = {
  peer_dm: 'pages.membersPage.inbox_kind_peer_dm_tip',
  worker_report: 'pages.membersPage.inbox_kind_worker_report_tip',
  wake_timer: 'pages.membersPage.inbox_kind_wake_timer_tip',
  session_dm: 'pages.membersPage.inbox_kind_session_dm_tip',
  system: 'pages.membersPage.inbox_kind_system_tip',
}

function KindBadge({ kind }: { kind: string }) {
  const { t } = useTranslation()
  const Icon = KIND_ICON[kind] ?? Info
  const labelKey = KIND_LABEL_KEY[kind]
  const tipKey = KIND_TIP_KEY[kind]
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full border border-border bg-bg-elevated px-1.5 py-px text-[10px] font-medium uppercase tracking-wide text-muted shrink-0"
      data-testid="member-envelope-kind"
      data-kind={kind}
      title={tipKey ? t(tipKey) : undefined}
    >
      <Icon size={11} aria-hidden />
      {labelKey ? t(labelKey) : kind}
    </span>
  )
}

/** The wrapper `ChatMessageList` gives a bubble, so a projection bubble sits
 *  on the same measure and edge as a transcript bubble. */
function Bubble({ children, isUser }: { children: React.ReactNode; isUser?: boolean }) {
  return (
    <div className="px-4 mx-auto w-full py-1" style={{ maxWidth: 'var(--mc-content-width, 100%)' }}>
      <div className={cn('group flex flex-col min-w-0', isUser && 'items-end')}>
        <div className={cn('flex flex-col gap-0.5 min-w-0 overflow-hidden max-w-full', isUser && 'items-end')}>
          {children}
        </div>
      </div>
    </div>
  )
}

function ProjectionItemView({
  item,
  expanded,
  onToggle,
  marker,
}: {
  item: ProjectionItem
  expanded: boolean
  onToggle: () => void
  marker: { last_read_id: string; last_read_at: string } | null
}) {
  if (item.kind === 'human') {
    return (
      <Bubble isUser>
        <UserMessage
          content={item.row.body}
          timestamp={fmtMessageTime(item.row.created_at) || undefined}
          timestampTitle={fmtMessageTimeFull(item.row.created_at)}
          renderContent={(c, mt) => renderUserContent({ content: c, meta: mt })}
          hideSteerBadge
        />
      </Bubble>
    )
  }
  if (item.kind === 'member') {
    return (
      <Bubble>
        <AssistantMessage
          content={item.row.body}
          isStreaming={false}
          timestamp={fmtMessageTime(item.row.created_at) || undefined}
          timestampTitle={fmtMessageTimeFull(item.row.created_at)}
          showFooter={false}
        />
      </Bubble>
    )
  }
  return <EnvelopeGroup item={item} expanded={expanded} onToggle={onToggle} marker={marker} />
}

function EnvelopeGroup({
  item,
  expanded,
  onToggle,
  marker,
}: {
  item: Extract<ProjectionItem, { kind: 'group' }>
  expanded: boolean
  onToggle: () => void
  marker: { last_read_id: string; last_read_at: string } | null
}) {
  const { t } = useTranslation()
  const last = item.rows[item.rows.length - 1]
  const count = item.rows.length
  const Chevron = expanded ? ChevronDown : ChevronRight
  const hasNew = item.rows.some((r) => r.direction === 'out' && isNewerThanMarker(r, marker))
  return (
    <div
      className="px-4 mx-auto w-full py-0.5"
      style={{ maxWidth: 'var(--mc-content-width, 100%)' }}
      data-testid="member-envelope-group"
      data-kind={item.envelopeKind}
      data-count={count}
    >
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={expanded}
        className={cn(
          'flex w-full items-center gap-2 rounded-md border border-border/60 bg-bg-elevated/40 px-2 py-1 text-left text-[12px] text-muted',
          'hover:bg-bg-hover hover:text-text focus-ring transition-colors cursor-pointer',
        )}
        data-testid="member-envelope-group-toggle"
      >
        <Chevron size={13} className="shrink-0" aria-hidden />
        <KindBadge kind={item.envelopeKind} />
        {item.envelopeKind === 'peer_dm' && <DirectionArrow direction={last.direction} />}
        {/* Direction in words, not only in the arrow: "To fixer" / "From radar",
            the same strings the expanded row uses, so the collapsed strip reads
            without opening it. */}
        <span className="shrink-0 font-medium text-text/80" data-testid="member-envelope-counterpart">
          {t(last.direction === 'out' ? 'pages.membersPage.inbox_to' : 'pages.membersPage.inbox_from', {
            name: peerLabel(last.direction === 'out' ? last.to : last.from),
          })}
        </span>
        {!expanded && (
          <span className="min-w-0 flex-1 truncate" data-testid="member-envelope-preview">
            {previewLine(last.body)}
          </span>
        )}
        {expanded && <span className="min-w-0 flex-1" />}
        {count > 1 && (
          <span className="shrink-0 rounded-full bg-bg-elevated px-1.5 text-[10px] tabular-nums" data-testid="member-envelope-count">
            {t('pages.membersPage.inbox_rows', { count })}
          </span>
        )}
        {hasNew && (
          <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{ background: 'var(--accent)' }} aria-hidden />
        )}
        <span className="shrink-0 text-[11px] tabular-nums" title={fmtMessageTimeFull(last.created_at)}>
          {fmtMessageTime(last.created_at)}
        </span>
      </button>
      {expanded && (
        <ul className="mt-1 flex flex-col gap-1 pl-5" data-testid="member-envelope-rows">
          {item.rows.map((row) => (
            <EnvelopeRow key={row.id} row={row} />
          ))}
        </ul>
      )}
    </div>
  )
}

function DirectionArrow({ direction }: { direction: 'in' | 'out' }) {
  const { t } = useTranslation()
  const Icon = direction === 'out' ? ArrowUpRight : ArrowDownLeft
  const label = t(direction === 'out' ? 'pages.membersPage.inbox_direction_out' : 'pages.membersPage.inbox_direction_in')
  return (
    <Icon
      size={13}
      className={cn('shrink-0', direction === 'out' ? 'text-accent' : 'text-ok')}
      role="img"
      aria-label={label}
      data-testid="member-envelope-direction"
      data-direction={direction}
    />
  )
}

function EnvelopeRow({ row }: { row: MemberProjectionRow }) {
  const { t } = useTranslation()
  const counterpart = row.direction === 'out' ? row.to : row.from
  return (
    <li
      className="rounded-md border border-border/60 bg-bg px-2.5 py-1.5 text-[12.5px] leading-5"
      data-testid="member-envelope-row"
      data-state={row.state}
    >
      <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] text-muted mb-0.5">
        {row.kind === 'peer_dm' && <DirectionArrow direction={row.direction} />}
        <span className="font-medium text-text/80 whitespace-nowrap shrink-0">
          {t(row.direction === 'out' ? 'pages.membersPage.inbox_to' : 'pages.membersPage.inbox_from', {
            name: peerLabel(counterpart),
          })}
        </span>
        {row.hop > 0 && (
          <span
            className="tabular-nums whitespace-nowrap shrink-0"
            title={t('pages.membersPage.inbox_hop_tip')}
            data-testid="member-envelope-hop"
          >
            {t('pages.membersPage.inbox_hop', { hop: row.hop })}
          </span>
        )}
        {row.state === 'pending' && (
          <span className="text-warn whitespace-nowrap shrink-0" title={t('pages.membersPage.inbox_state_pending_tip', { name: peerLabel(row.to) })}>
            {t('pages.membersPage.inbox_state_pending')}
          </span>
        )}
        {row.state === 'dead' && (
          <span className="text-danger whitespace-nowrap shrink-0" title={t('pages.membersPage.inbox_state_dead_tip')}>
            {t('pages.membersPage.inbox_state_dead')}
          </span>
        )}
        <span className="ml-auto tabular-nums whitespace-nowrap shrink-0" title={fmtMessageTimeFull(row.created_at)}>
          {fmtMessageTime(row.created_at)}
        </span>
      </div>
      <div className="whitespace-pre-wrap break-words text-text">{row.body}</div>
    </li>
  )
}
