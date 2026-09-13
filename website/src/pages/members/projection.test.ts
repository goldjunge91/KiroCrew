import { describe, expect, it } from 'vitest'
import type { MemberProjectionRow } from '../../api/client'
import {
  groupProjectionRows,
  isNewerThanMarker,
  newestRow,
  peerLabel,
  previewLine,
} from './projection'

let seq = 0
function row(over: Partial<MemberProjectionRow>): MemberProjectionRow {
  seq += 1
  const id = over.id ?? `env_${String(1000 + seq).padStart(13, '0')}_${seq.toString(16).padStart(8, '0')}`
  return {
    id,
    from: 'user',
    to: 'member:radar',
    kind: 'user_dm',
    body: 'hello',
    hop: 0,
    created_at: `2026-09-13T10:00:${String(seq).padStart(2, '0')}.000000Z`,
    refs: {},
    attempts: 0,
    acked_at: null,
    direction: 'in',
    state: 'acked',
    ...over,
  }
}

describe('groupProjectionRows', () => {
  it('renders the person and the member as bubbles, everything else as kind groups', () => {
    const rows = [
      row({ kind: 'user_dm', from: 'user', body: 'triage #42' }),
      row({ kind: 'wake_timer', from: 'system', body: 'Scheduled wake' }),
      row({ kind: 'wake_timer', from: 'system', body: 'Scheduled wake' }),
      row({ kind: 'peer_dm', from: 'member:radar', to: 'member:fixer', direction: 'out', state: 'sent' }),
      row({ kind: 'peer_dm', from: 'member:fixer', direction: 'in', state: 'pending' }),
      row({ kind: 'reply', from: 'member:radar', direction: 'out', state: 'sent', body: 'done' }),
    ]
    const items = groupProjectionRows(rows)
    expect(items.map((i) => i.kind)).toEqual(['human', 'group', 'group', 'group', 'member'])
    const [, timers, sent, received] = items
    expect(timers.kind === 'group' && timers.envelopeKind).toBe('wake_timer')
    expect(timers.kind === 'group' && timers.rows.length).toBe(2)
    // Consecutive peer_dm rows fold only while they go the same way: the
    // collapsed header names one direction, so a received message never hides
    // under a "sent" strip.
    expect(sent.kind === 'group' && sent.rows.map((r) => r.direction)).toEqual(['out'])
    expect(received.kind === 'group' && received.rows.map((r) => r.direction)).toEqual(['in'])
  })

  it('folds same-direction peer rows and breaks the group when the direction flips', () => {
    const rows = [
      row({ kind: 'peer_dm', from: 'member:radar', to: 'member:fixer', direction: 'out', state: 'sent' }),
      row({ kind: 'peer_dm', from: 'member:radar', to: 'member:scout', direction: 'out', state: 'sent' }),
      row({ kind: 'peer_dm', from: 'member:fixer', direction: 'in', state: 'acked' }),
      row({ kind: 'peer_dm', from: 'member:scout', direction: 'in', state: 'acked' }),
    ]
    const items = groupProjectionRows(rows)
    expect(items.map((i) => (i.kind === 'group' ? i.rows.length : 0))).toEqual([2, 2])
  })

  it('does not fold different kinds together, and re-opens a group after a bubble', () => {
    const rows = [
      row({ kind: 'worker_report', from: 'session:chat-1-1' }),
      row({ kind: 'system', from: 'system' }),
      row({ kind: 'reply', direction: 'out', state: 'sent' }),
      row({ kind: 'system', from: 'system' }),
    ]
    const items = groupProjectionRows(rows)
    expect(items.map((i) => (i.kind === 'group' ? i.envelopeKind : i.kind))).toEqual([
      'worker_report',
      'system',
      'member',
      'system',
    ])
  })

  it('treats a session_dm (and a foreign-sender user_dm) as machinery, not as the human speaking', () => {
    const items = groupProjectionRows([
      row({ kind: 'session_dm', from: 'session:chat-7-2' }),
      row({ kind: 'user_dm', from: 'session:chat-7-2' }),
    ])
    expect(items.map((i) => i.kind)).toEqual(['group', 'group'])
    expect(items[0].kind === 'group' && items[0].envelopeKind).toBe('session_dm')
  })

  it('keys a group by its first row so an appended row keeps the same key', () => {
    const first = row({ kind: 'wake_timer', from: 'system' })
    const before = groupProjectionRows([first])
    const after = groupProjectionRows([first, row({ kind: 'wake_timer', from: 'system' })])
    expect(after[0].key).toBe(before[0].key)
    expect(after[0].key).toBe(`g:${first.id}`)
  })

  it('a reply the member wrote into its own INBOX is not a member bubble', () => {
    // Only an OUTBOX row is the member's answer; a stray inbound `reply`
    // (there is no producer of one, but the kind is data) is machinery.
    const items = groupProjectionRows([row({ kind: 'reply', direction: 'in' })])
    expect(items[0].kind).toBe('group')
  })
})

describe('marker and labels', () => {
  it('isNewerThanMarker orders by created_at then id, and an empty marker is "all new"', () => {
    const marker = { last_read_at: '2026-09-13T10:00:05.000000Z', last_read_id: 'env_b' }
    expect(isNewerThanMarker({ created_at: '2026-09-13T10:00:06.000000Z', id: 'env_a' }, marker)).toBe(true)
    expect(isNewerThanMarker({ created_at: '2026-09-13T10:00:04.000000Z', id: 'env_z' }, marker)).toBe(false)
    expect(isNewerThanMarker({ created_at: '2026-09-13T10:00:05.000000Z', id: 'env_c' }, marker)).toBe(true)
    expect(isNewerThanMarker({ created_at: '2026-09-13T10:00:05.000000Z', id: 'env_b' }, marker)).toBe(false)
    expect(isNewerThanMarker({ created_at: 'x', id: 'y' }, null)).toBe(true)
    expect(isNewerThanMarker({ created_at: 'x', id: 'y' }, { last_read_at: '', last_read_id: '' })).toBe(true)
  })

  it('newestRow is the last row (rows arrive sorted)', () => {
    const a = row({})
    const b = row({})
    expect(newestRow([a, b])).toBe(b)
    expect(newestRow([])).toBeUndefined()
  })

  it('peerLabel strips the address namespace', () => {
    expect(peerLabel('member:fixer')).toBe('fixer')
    expect(peerLabel('session:chat-1-9')).toBe('chat-1-9')
    expect(peerLabel('user')).toBe('user')
    expect(peerLabel('system')).toBe('system')
  })

  it('previewLine takes the first non-empty line and bounds it', () => {
    expect(previewLine('\n\n  first line  \nsecond')).toBe('first line')
    expect(previewLine('x'.repeat(200), 10)).toBe('xxxxxxxxx…')
    expect(previewLine('')).toBe('')
  })
})
