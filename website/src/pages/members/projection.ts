import type { MemberProjectionRow } from '../../api/client'

/**
 * Pure shaping of an inbox-model member's projection (RFC member-inbox-model,
 * M1) into what the Members page renders. No React here so the grouping,
 * collapse and unread rules are unit-testable on plain data.
 *
 * The projection is the time-ordered merge of the member's inbox (every state)
 * and outbox. Two kinds of row ARE the conversation and render as bubbles:
 *
 * - `user_dm` from `user` — the person typed it (the human's bubble);
 * - outbox `reply` — the member answered (the member's bubble).
 *
 * Everything else is machinery the person should be able to SEE happened
 * without it reading as something they or the member said to each other: a
 * `peer_dm` to or from another member, a `worker_report` from a session the
 * member dispatched, a `wake_timer` tick, a `system` notice, or a `session_dm`
 * that another SESSION relayed through `session_send`. Consecutive
 * rows of the same kind fold into one badged group, collapsed by default —
 * patrol noise stays one line until the person asks for it.
 */

export type ProjectionItem =
  | { kind: 'human'; key: string; row: MemberProjectionRow }
  | { kind: 'member'; key: string; row: MemberProjectionRow }
  | { kind: 'group'; key: string; envelopeKind: string; rows: MemberProjectionRow[] }

/** The person's own words: the only `from` value the intake handler writes
 *  for a message typed into the thread. A session-relayed message is its own
 *  kind (`session_dm`), so the check on `from` is defence in depth for a
 *  `user_dm` some other writer stamped with a foreign sender. */
export const FROM_USER = 'user'

export function isHumanRow(row: MemberProjectionRow): boolean {
  return row.kind === 'user_dm' && row.from === FROM_USER
}

export function isMemberReplyRow(row: MemberProjectionRow): boolean {
  return row.kind === 'reply' && row.direction === 'out'
}

/** Fold projection rows into bubbles and per-kind groups, in order. */
export function groupProjectionRows(rows: readonly MemberProjectionRow[]): ProjectionItem[] {
  const out: ProjectionItem[] = []
  for (const row of rows) {
    if (isHumanRow(row)) {
      out.push({ kind: 'human', key: row.id, row })
      continue
    }
    if (isMemberReplyRow(row)) {
      out.push({ kind: 'member', key: row.id, row })
      continue
    }
    const last = out[out.length - 1]
    // Fold on kind AND direction: the collapsed header describes the group by
    // its newest row (arrow, From/To, preview), so a received message folded
    // under a sent one would read as sent until expanded.
    if (
      last &&
      last.kind === 'group' &&
      last.envelopeKind === row.kind &&
      last.rows[last.rows.length - 1].direction === row.direction
    ) {
      last.rows.push(row)
      continue
    }
    // The group is keyed by its FIRST row: stable across refetches that append
    // to the group, so an expanded group stays expanded when a new row lands.
    out.push({ kind: 'group', key: `g:${row.id}`, envelopeKind: row.kind, rows: [row] })
  }
  return out
}

/** A projection row's `from` / `to` as the roster names it: `member:fixer` →
 *  `fixer`, `session:chat-1-9` → `chat-1-9`, `user` and `system` unchanged. */
export function peerLabel(address: string): string {
  const idx = address.indexOf(':')
  return idx === -1 ? address : address.slice(idx + 1)
}

/** Rows newer than the read marker, by the server's own `(created_at, id)`
 *  order. Used to decide whether the newest rendered row is news (and so
 *  whether viewing it should advance the marker). */
export function isNewerThanMarker(
  row: Pick<MemberProjectionRow, 'id' | 'created_at'>,
  marker: { last_read_id: string; last_read_at: string } | null | undefined,
): boolean {
  if (!marker || (!marker.last_read_at && !marker.last_read_id)) return true
  if (row.created_at !== marker.last_read_at) return row.created_at > marker.last_read_at
  return row.id > marker.last_read_id
}

/** The newest row the projection holds, or undefined. Rows arrive sorted. */
export function newestRow(rows: readonly MemberProjectionRow[]): MemberProjectionRow | undefined {
  return rows.length ? rows[rows.length - 1] : undefined
}

/** One-line preview for a collapsed group header: the newest row's first
 *  non-empty line, bounded so a long report cannot stretch the header. */
export function previewLine(body: string, max = 140): string {
  const line = body.split('\n').map((l) => l.trim()).find((l) => l.length > 0) ?? ''
  return line.length > max ? `${line.slice(0, max - 1)}…` : line
}
