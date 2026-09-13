import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen, fireEvent, waitFor, within } from '@testing-library/react'
import { renderWithProviders } from '../../test/helpers'
import type { MemberProjection as Projection, MemberProjectionRow } from '../../api/client'

/* The inbox-model thread (RFC member-inbox-model, M1). These pin what the
 * person sees of a flagged member's projection: the human's `user_dm` and the
 * member's `reply` draw as bubbles, machinery folds into per-kind groups that
 * are collapsed until asked, a peer row says which way it went, and viewing a
 * projection whose newest row is newer than the read marker advances the
 * marker exactly once. */

vi.mock('../../api/client', () => ({
  api: {
    memberProjection: vi.fn(),
    memberMarkRead: vi.fn(),
  },
}))
// The bubbles are the transcript's own components; their internals (markdown,
// file cards) are pinned by their own suites. Here they only need to show up.
vi.mock('../chat/UserMessage', () => ({
  default: ({ content }: { content: string }) => <div data-testid="user-bubble">{content}</div>,
}))
vi.mock('../chat/AssistantMessage', () => ({
  default: ({ content }: { content: string }) => <div data-testid="member-bubble">{content}</div>,
}))
vi.mock('../chat/ChatPageMessageContent', () => ({ renderUserContent: ({ content }: { content: string }) => content }))

import { api } from '../../api/client'
import MemberProjection from './MemberProjection'

let seq = 0
function row(over: Partial<MemberProjectionRow>): MemberProjectionRow {
  seq += 1
  return {
    id: `env_${String(1700000000000 + seq).padStart(13, '0')}_${seq.toString(16).padStart(8, '0')}`,
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

function projection(rows: MemberProjectionRow[], marker: Projection['marker'] = null): Projection {
  return { slug: 'radar', inbox_model: true, rows, unread: 0, marker }
}

const ROWS = [
  row({ kind: 'user_dm', from: 'user', body: 'triage #42 please' }),
  row({ kind: 'wake_timer', from: 'system', body: 'Scheduled wake (every 600s).' }),
  row({ kind: 'wake_timer', from: 'system', body: 'Scheduled wake (every 600s).' }),
  row({ kind: 'peer_dm', from: 'member:radar', to: 'member:fixer', direction: 'out', state: 'sent', body: 'fixer, take #42' }),
  row({ kind: 'peer_dm', from: 'member:fixer', to: 'member:radar', direction: 'in', state: 'pending', body: 'on it', hop: 1 }),
  row({ kind: 'reply', from: 'member:radar', direction: 'out', state: 'sent', body: 'Handed #42 to fixer.' }),
]

beforeEach(() => {
  vi.mocked(api.memberProjection).mockReset()
  vi.mocked(api.memberMarkRead).mockReset()
  vi.mocked(api.memberMarkRead).mockImplementation(async (_slug, id) => ({
    ok: true,
    slug: 'radar',
    unread: 0,
    marker: { last_read_id: id ?? '', last_read_at: '2026-09-13T10:00:06.000000Z' },
  }))
})

describe('MemberProjection', () => {
  it('draws the conversation as bubbles and folds machinery into collapsed kind groups', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible={false} />)
    expect(await screen.findByTestId('user-bubble')).toHaveTextContent('triage #42 please')
    expect(screen.getByTestId('member-bubble')).toHaveTextContent('Handed #42 to fixer.')

    const groups = screen.getAllByTestId('member-envelope-group')
    // The two peer rows went opposite ways, so they do not share a strip.
    expect(groups.map((g) => g.dataset.kind)).toEqual(['wake_timer', 'peer_dm', 'peer_dm'])
    expect(groups.map((g) => g.dataset.count)).toEqual(['2', '1', '1'])
    // Collapsed: the header shows a one-line preview and the count pill; the
    // rows themselves are not rendered until expanded.
    expect(screen.queryAllByTestId('member-envelope-row')).toHaveLength(0)
    expect(within(groups[0]).getByTestId('member-envelope-preview')).toHaveTextContent('Scheduled wake (every 600s).')
    expect(within(groups[0]).getByTestId('member-envelope-count')).toHaveTextContent('2 items')
    expect(within(groups[0]).getByRole('button')).toHaveAttribute('aria-expanded', 'false')
  })

  it('expands a group on click, with a direction arrow per peer row and the hop', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible={false} />)
    await screen.findByTestId('user-bubble')
    const [, sent, received] = screen.getAllByTestId('member-envelope-group')
    // Each collapsed strip carries ONE direction's arrow: the sent note and the
    // received one never share a header.
    expect(within(sent).getByTestId('member-envelope-direction')).toHaveAttribute('data-direction', 'out')
    expect(within(received).getByTestId('member-envelope-direction')).toHaveAttribute('data-direction', 'in')
    fireEvent.click(within(sent).getByTestId('member-envelope-group-toggle'))
    expect(within(sent).getByRole('button')).toHaveAttribute('aria-expanded', 'true')
    const [sentRow] = within(sent).getAllByTestId('member-envelope-row')
    expect(within(sentRow).getByTestId('member-envelope-direction')).toHaveAttribute('data-direction', 'out')
    expect(sentRow).toHaveTextContent('To fixer')
    expect(sentRow).toHaveTextContent('fixer, take #42')
    fireEvent.click(within(received).getByTestId('member-envelope-group-toggle'))
    const [receivedRow] = within(received).getAllByTestId('member-envelope-row')
    expect(within(receivedRow).getByTestId('member-envelope-direction')).toHaveAttribute('data-direction', 'in')
    expect(receivedRow).toHaveTextContent('From fixer')
    expect(receivedRow).toHaveTextContent('hop 1')
    expect(within(receivedRow).getByTestId('member-envelope-hop')).toHaveAttribute('title', 'How many member-to-member relays this thread has made')
    expect(receivedRow).toHaveAttribute('data-state', 'pending')
    // Collapses again.
    fireEvent.click(within(sent).getByTestId('member-envelope-group-toggle'))
    expect(within(sent).queryAllByTestId('member-envelope-row')).toHaveLength(0)
  })

  it('marks the newest row read once while visible, and never while hidden', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    const { rerender } = renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible={false} />)
    await screen.findByTestId('member-bubble')
    expect(api.memberMarkRead).not.toHaveBeenCalled()
    rerender(<MemberProjection slug="radar" memberName="radar" visible />)
    await waitFor(() => expect(api.memberMarkRead).toHaveBeenCalledTimes(1))
    expect(api.memberMarkRead).toHaveBeenCalledWith('radar', ROWS[ROWS.length - 1].id)
    // Re-rendering with the same newest row does not POST again.
    rerender(<MemberProjection slug="radar" memberName="radar" visible />)
    await new Promise((r) => setTimeout(r, 20))
    expect(api.memberMarkRead).toHaveBeenCalledTimes(1)
  })

  it('surfaces a failed mark-read and retries it on the next visibility change', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    vi.mocked(api.memberMarkRead).mockRejectedValueOnce(new Error('offline'))
    const { rerender } = renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible />)
    expect(await screen.findByTestId('member-projection-mark-read-error')).toBeInTheDocument()
    expect(api.memberMarkRead).toHaveBeenCalledTimes(1)
    // Hidden then visible again: the guard was cleared by the failure, so the
    // same newest row is retried; success clears the notice.
    rerender(<MemberProjection slug="radar" memberName="radar" visible={false} />)
    rerender(<MemberProjection slug="radar" memberName="radar" visible />)
    await waitFor(() => expect(api.memberMarkRead).toHaveBeenCalledTimes(2))
    await waitFor(() => expect(screen.queryByTestId('member-projection-mark-read-error')).toBeNull())
  })

  it('does not retry a persistently failing mark-read while the thread stays visible', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    vi.mocked(api.memberMarkRead).mockRejectedValue(new Error('offline'))
    renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible />)
    expect(await screen.findByTestId('member-projection-mark-read-error')).toBeInTheDocument()
    // The failure must not release the guard while visible: that would POST
    // again on the state change, at render speed, for as long as it fails.
    await new Promise((r) => setTimeout(r, 50))
    expect(api.memberMarkRead).toHaveBeenCalledTimes(1)
  })

  it('does not mark read when the marker already covers the newest row', async () => {
    const last = ROWS[ROWS.length - 1]
    vi.mocked(api.memberProjection).mockResolvedValue(
      projection(ROWS, { last_read_id: last.id, last_read_at: last.created_at }),
    )
    renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible />)
    await screen.findByTestId('member-bubble')
    await new Promise((r) => setTimeout(r, 20))
    expect(api.memberMarkRead).not.toHaveBeenCalled()
  })

  it('shows the empty state and the load failure', async () => {
    vi.mocked(api.memberProjection).mockResolvedValueOnce(projection([]))
    const first = renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible />)
    expect(await screen.findByTestId('member-projection-empty')).toBeInTheDocument()
    expect(api.memberMarkRead).not.toHaveBeenCalled()
    first.unmount()

    vi.mocked(api.memberProjection).mockRejectedValueOnce(new Error('boom'))
    renderWithProviders(<MemberProjection slug="fixer" memberName="fixer" visible />)
    expect(await screen.findByTestId('member-projection-error')).toBeInTheDocument()
    expect(screen.getByTestId('member-projection-error')).toHaveTextContent('Retrying')
    expect(screen.queryByTestId('member-projection')).toBeNull()
  })

  it('keeps the stale rows through a failed background poll and shows the notice above them', async () => {
    // The thread polls every 4 s; one blip must not blank a conversation that
    // was already on screen. react-query keeps the last good data through a
    // failed refetch, and the view renders it with the notice on top.
    vi.mocked(api.memberProjection).mockResolvedValueOnce(projection(ROWS))
    const { queryClient } = renderWithProviders(
      <MemberProjection slug="radar" memberName="radar" visible={false} />,
    )
    await screen.findByTestId('member-bubble')
    vi.mocked(api.memberProjection).mockRejectedValueOnce(new Error('blip'))
    await queryClient.refetchQueries()
    expect(await screen.findByTestId('member-projection-error')).toBeInTheDocument()
    expect(screen.getByTestId('member-projection')).toBeInTheDocument()
    expect(screen.getByTestId('member-bubble')).toHaveTextContent('Handed #42 to fixer.')
    // wakes, sent peer note, received peer note -- the same three groups as before the blip
    expect(screen.getAllByTestId('member-envelope-group')).toHaveLength(3)
  })

  it('follows the tail only while the reader is at the tail', async () => {
    // jsdom has no layout: model a 200px viewport over 1000px of rows.
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    const { queryClient } = renderWithProviders(
      <MemberProjection slug="radar" memberName="radar" visible={false} />,
    )
    await screen.findByTestId('member-bubble')
    const el = screen.getByTestId('member-projection')
    Object.defineProperty(el, 'scrollHeight', { value: 1000, configurable: true })
    Object.defineProperty(el, 'clientHeight', { value: 200, configurable: true })
    // The reader scrolled up into the history...
    el.scrollTop = 100
    fireEvent.scroll(el)
    // ...and a new row lands on the next poll: the position must NOT snap to the tail.
    const more = [...ROWS, row({ kind: 'reply', from: 'member:radar', direction: 'out', state: 'sent', body: 'Later.' })]
    vi.mocked(api.memberProjection).mockResolvedValue(projection(more))
    await queryClient.refetchQueries()
    await screen.findByText('Later.')
    expect(el.scrollTop).toBe(100)
    // Back at the tail, the next row is followed.
    el.scrollTop = 800
    fireEvent.scroll(el)
    const evenMore = [...more, row({ kind: 'reply', from: 'member:radar', direction: 'out', state: 'sent', body: 'Latest.' })]
    vi.mocked(api.memberProjection).mockResolvedValue(projection(evenMore))
    await queryClient.refetchQueries()
    await screen.findByText('Latest.')
    expect(el.scrollTop).toBe(1000)
  })

  it('does not mark a row read that arrived while the reader was scrolled up', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    const { queryClient } = renderWithProviders(
      <MemberProjection slug="radar" memberName="radar" visible />,
    )
    await screen.findByTestId('member-bubble')
    // The rows already on screen at the tail are marked read once...
    await waitFor(() => expect(api.memberMarkRead).toHaveBeenCalledTimes(1))
    const el = screen.getByTestId('member-projection')
    Object.defineProperty(el, 'scrollHeight', { value: 1000, configurable: true })
    Object.defineProperty(el, 'clientHeight', { value: 200, configurable: true })
    // ...then the reader scrolls up into the history and a row lands off-screen.
    el.scrollTop = 100
    fireEvent.scroll(el)
    const more = [...ROWS, row({ kind: 'reply', from: 'member:radar', direction: 'out', state: 'sent', body: 'Later.' })]
    vi.mocked(api.memberProjection).mockResolvedValue(projection(more))
    await queryClient.refetchQueries()
    await screen.findByText('Later.')
    await new Promise((r) => setTimeout(r, 20))
    // The marker did not move for a row nobody saw.
    expect(api.memberMarkRead).toHaveBeenCalledTimes(1)
    // Scrolling back to the tail is what marks it read.
    el.scrollTop = 800
    fireEvent.scroll(el)
    await waitFor(() => expect(api.memberMarkRead).toHaveBeenCalledTimes(2))
    expect(api.memberMarkRead).toHaveBeenLastCalledWith('radar', more[more.length - 1].id)
  })

  it('says which way the collapsed peer strip went, in words', async () => {
    vi.mocked(api.memberProjection).mockResolvedValue(projection(ROWS))
    renderWithProviders(<MemberProjection slug="radar" memberName="radar" visible={false} />)
    await screen.findByTestId('user-bubble')
    const [wakes, sent, received] = screen.getAllByTestId('member-envelope-group')
    // The sent note says To, the received one From; the wake group is from the scheduler.
    expect(within(sent).getByTestId('member-envelope-counterpart')).toHaveTextContent('To fixer')
    expect(within(received).getByTestId('member-envelope-counterpart')).toHaveTextContent('From fixer')
    expect(within(wakes).getByTestId('member-envelope-counterpart')).toHaveTextContent('From system')
  })
})
