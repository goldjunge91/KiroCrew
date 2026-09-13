import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { http, HttpResponse } from 'msw'
import { server } from '../../integration/mocks/server'
import '../api/client'
import { crewCapabilitiesApi, type CapabilityDraft, type CapabilityView } from '../api/crewCapabilities'
import CrewCapabilitiesPane from '../components/crew/CrewCapabilitiesPane'

const endpoint = '/api/agents/crewA/capabilities'
let view: CapabilityView
let previewBodies: CapabilityDraft[]
let saveBodies: (CapabilityDraft & { preview_token: string })[]
const baseView = (): CapabilityView => ({
  schema_version: 1, member: 'crewA', mode: 'inherited', revision: 'r1',
  template: { name: 'atlas', source: 'custom', scope: 'global', available: true },
  rows: [
    { section: 'tools', id: '@search/read', label: 'Search read', state: 'inherited', present: true, value: true, editable: true },
    { section: 'allowedTools', id: '@search/read', label: 'Search read', state: 'local', present: true, value: true, editable: true },
    { section: 'autoApprove', id: '@search/list', label: 'Search list', state: 'inherited', present: true, value: true, editable: true },
    { section: 'mcpServers', id: 'search', label: 'Search', state: 'local', present: true, value: { command: 'search-cli', args: ['--read'], env: {} }, editable: true },
    { section: 'mcpServers', id: 'protected', label: 'Protected', state: 'inherited', present: true, value: { command: 'protected-cli', args: ['--read', '[REDACTED]'], env: { TOKEN: '[REDACTED]' } }, editable: true },
    { section: 'mcpServers', id: 'managed', label: 'Managed', state: 'inherited', present: true, value: { command: 'managed-cli' }, editable: false, locked_reason: 'managed_transport_locked' },
    { section: 'skills', id: 'catalog/review', label: 'Review skill', state: 'removed', present: false, value: null, editable: true },
    { section: 'resources', id: 'file:///manual/*.md', label: 'Manual reference', state: 'local', present: true, value: 'file:///manual/*.md', editable: true, shared_reference: true },
  ],
  skills: [{ id: 'catalog/review', label: 'Review skill', shared_reference: true }, { id: 'catalog/test', label: 'Test skill', shared_reference: true }],
  connections: [{ id: 'configured', label: 'Configured search', managed: false }],
  parent_changes: [{ section: 'tools', id: '@search/read', kind: 'changed', conflict: true, requires_approval: true, before: true, after: true }],
  runtime: { status: 'unverified', apply_mode: 'new_runtime', saved_revision: 'r1', sessions: [] }, warnings: [],
})

beforeEach(() => {
  view = baseView(); previewBodies = []; saveBodies = []
  server.use(
    http.get(endpoint, () => HttpResponse.json(view)),
    http.post(`${endpoint}/preview`, async ({ request }) => {
      const body = await request.json() as CapabilityDraft
      previewBodies.push(body)
      return HttpResponse.json({ ...view, preview_token: 'signed-preview', impact: body.operations.map(op => ({ ...op, member: 'crewA', change: 'changed', approval_expanded: op.section === 'allowedTools' })) })
    }),
    http.put(endpoint, async ({ request }) => {
      saveBodies.push(await request.json() as CapabilityDraft & { preview_token: string })
      view = { ...view, revision: 'r2', runtime: { ...view.runtime, status: 'pending', saved_revision: 'r2' } }
      return HttpResponse.json(view)
    }),
  )
})

function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  const dirty = vi.fn(), busy = vi.fn(), saved = vi.fn()
  const tree = (hidden: boolean) => <QueryClientProvider client={client}><CrewCapabilitiesPane member="crewA" hidden={hidden} onDirtyChange={dirty} onBusyChange={busy} onSaved={saved} /></QueryClientProvider>
  const result = render(tree(false))
  return { ...result, client, dirty, busy, saved, hide: (hidden: boolean) => result.rerender(tree(hidden)) }
}
async function ready() { await screen.findByText('Parent template: atlas') }
async function pickState(name: string, state: string) {
  fireEvent.click(screen.getByRole('combobox', { name: `Source for ${name}` }))
  fireEvent.click(await screen.findByRole('option', { name: state, exact: true }))
}
async function review() {
  fireEvent.click(screen.getByRole('button', { name: 'Review/save', exact: true }))
  await screen.findByRole('button', { name: 'Save reviewed changes' })
}

describe('crew capability draft editor with mocked HTTP', () => {
  it('keeps category navigation non-shrinking beside an expanded transport', async () => {
    mount(); await ready()
    const row = within(screen.getByTestId('capability-mcpServers-search'))
    fireEvent.click(row.getByText('Transport settings'))
    fireEvent.change(row.getByLabelText('Command'), { target: { value: 'draft-command' } })
    const tabs = screen.getByRole('tablist', { name: 'Capabilities' })
    expect(tabs.parentElement).toHaveClass('shrink-0', 'overflow-x-auto')
    fireEvent.click(within(tabs).getByRole('tab', { name: 'Tools', exact: true }))
    expect(screen.getByText('Search read')).toBeInTheDocument()
    fireEvent.click(within(tabs).getByRole('tab', { name: 'MCP Servers', exact: true }))
    expect(within(screen.getByTestId('capability-mcpServers-search')).getByLabelText('Command')).toHaveValue('draft-command')
  })

  it('keeps the save controls outside the focused input scroll region', async () => {
    mount(); await ready()
    const scroller = screen.getByTestId('capability-scroll-region')
    fireEvent.click(within(screen.getByTestId('capability-mcpServers-search')).getByText('Transport settings'))
    const command = within(screen.getByTestId('capability-mcpServers-search')).getByLabelText('Command')
    command.focus()
    expect(scroller.contains(command)).toBe(true)
    expect(scroller.contains(screen.getByRole('button', { name: 'Review/save' }))).toBe(false)
  })

  it('renders proposed values only from the sanitized preview response', async () => {
    server.use(http.post(`${endpoint}/preview`, () => HttpResponse.json({ ...view,
      rows: [{ ...view.rows[3], value: { command: 'server-approved-command', env: { TOKEN: '[REDACTED]' } } }],
      preview_token: 'signed-preview', impact: [{ member: 'crewA', section: 'mcpServers', id: 'search', change: 'changed', approval_expanded: false }],
    })))
    mount(); await ready()
    const row = within(screen.getByTestId('capability-mcpServers-search'))
    fireEvent.click(row.getByText('Transport settings'))
    fireEvent.change(row.getByLabelText('Command'), { target: { value: 'unsanitized-draft-command' } })
    await review()
    const preview = within(screen.getByRole('region', { name: 'Changes to be saved' }))
    expect(preview.getByText(/server-approved-command/)).toBeInTheDocument()
    expect(preview.queryByText(/unsanitized-draft-command/)).not.toBeInTheDocument()
  })

  it('reports an unavailable parent as a source error, not a provider load failure', async () => {
    view.template.available = false
    view.runtime = { ...view.runtime, status: 'failed', error_code: 'parent_missing' }
    view.warnings = ['parent_missing']
    mount(); await ready()
    expect(screen.getAllByRole('alert')).toHaveLength(1)
    expect(screen.queryByText('parent_missing')).not.toBeInTheDocument()
    expect(screen.queryByText('Server notices')).not.toBeInTheDocument()
    expect(screen.getByRole('alert')).toHaveTextContent('The parent template is missing or cannot be verified. Saving is blocked.')
    expect(screen.queryByText('Runtime loading failed. The saved version remains.')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Refresh from server (draft kept)' })).toBeEnabled()
    expect(screen.getByRole('button', { name: 'Review/save' })).toBeDisabled()
  })

  it.each([
    { code: 'source_changed', message: 'The saved configuration could not be validated. Refresh from server to retry.', other: 'Runtime loading failed. The saved version remains.' },
    { code: undefined, message: 'Runtime loading failed. The saved version remains.', other: 'The saved configuration could not be validated. Refresh from server to retry.' },
  ])('keeps validation and provider failures distinct ($code)', async ({ code, message, other }) => {
    view.runtime = { ...view.runtime, status: 'failed', error_code: code, sessions: code ? [] : [{ session_key: 'test-session', status: 'failed', error_code: 'capability_mcp_failed' }] }
    mount(); await ready()
    expect(screen.getByRole('alert')).toHaveTextContent(message)
    expect(screen.queryByText(other)).not.toBeInTheDocument()
  })

  it('previews before saving and sends the identical draft with its signed token', async () => {
    const result = mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Tools', exact: true }))
    await pickState('Search read', 'Removed')
    expect(saveBodies).toEqual([])
    await review()
    expect(previewBodies).toEqual([{ revision: 'r1', enroll: false, operations: [{ section: 'tools', id: '@search/read', action: 'remove' }], accept_parent: [], accept_members: [] }])
    fireEvent.click(screen.getByRole('button', { name: 'Save reviewed changes' }))
    await waitFor(() => expect(result.saved).toHaveBeenCalledOnce())
    expect(saveBodies).toEqual([{ ...previewBodies[0], preview_token: 'signed-preview' }])
    expect(screen.getByText('Saved. Waiting for a new runtime.')).toBeInTheDocument()
    expect(screen.queryByText('The server reports this version loaded.')).not.toBeInTheDocument()
    expect(result.dirty).toHaveBeenLastCalledWith(false)
  })

  it('keeps drafts and previews through pane hiding and invalidates preview after an edit', async () => {
    const result = mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Tools', exact: true }))
    await pickState('Search read', 'Removed'); await review()
    result.hide(true); result.hide(false)
    expect(screen.getByRole('button', { name: 'Save reviewed changes' })).toBeInTheDocument()
    await pickState('Search read', 'Inherited')
    expect(screen.queryByRole('button', { name: 'Save reviewed changes' })).not.toBeInTheDocument()
    await review()
    expect(previewBodies[1].operations).toEqual([{ section: 'tools', id: '@search/read', action: 'inherit' }])
  })

  it('requires explicit enrollment for an independent snapshot', async () => {
    view.mode = 'legacy_snapshot'
    mount(); await ready()
    expect(screen.getByRole('combobox', { name: 'Source for Search' })).toBeDisabled()
    const enroll = screen.getByRole('checkbox', { name: 'Follow the parent template' })
    // The helper is the checkbox's accessible description, so a reader hears
    // what stays and how Restore from parent works before deciding.
    expect(enroll).toHaveAccessibleDescription(/stays as it is, including fields left empty/)
    expect(enroll).toHaveAccessibleDescription(/Restore from parent/)
    fireEvent.click(enroll)
    expect(screen.getByRole('combobox', { name: 'Source for Search' })).not.toBeDisabled()
    await review()
    expect(previewBodies[0].enroll).toBe(true)
    expect(previewBodies[0].operations).toEqual([])
  })

  it('keeps tool exposure, agent approvals and MCP approvals separate', async () => {
    mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Auto-Approved' }))
    await pickState('Search read', 'Removed')
    await pickState('Search list', 'Local')
    await review()
    expect(previewBodies[0].operations).toEqual([
      { section: 'allowedTools', id: '@search/read', action: 'remove' },
      { section: 'autoApprove', id: '@search/list', action: 'set', value: true },
    ])
    expect(screen.getByText('This change grants more automatic approval.')).toBeInTheDocument()
  })

  it('selects configured connections without copying transport secrets', async () => {
    mount(); await ready()
    fireEvent.click(screen.getByRole('button', { name: 'Add configured MCP connection' }))
    fireEvent.click(await screen.findByRole('option', { name: 'Configured search' }))
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'configured', action: 'set', connection_id: 'configured' }])
  })

  it('offers searchable skills and preserves manual resource references', async () => {
    mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Skills', exact: true }))
    fireEvent.click(screen.getByRole('button', { name: 'Add a skill from the catalog' }))
    fireEvent.click(await screen.findByRole('option', { name: 'Test skill' }))
    await pickState('Review skill', 'Local')
    fireEvent.click(screen.getByText('Advanced references and template fields'))
    expect(screen.getByText('file:///manual/*.md')).toBeInTheDocument()
    await review()
    expect(previewBodies[0].operations).toEqual([
      { section: 'skills', id: 'catalog/test', action: 'set', value: true },
      { section: 'skills', id: 'catalog/review', action: 'set', value: true },
    ])
  })

  it('edits whole transports and never serializes a redaction mask', async () => {
    mount(); await ready()
    const protectedRow = within(screen.getByTestId('capability-mcpServers-protected'))
    fireEvent.click(protectedRow.getByText('Transport settings'))
    expect(protectedRow.getByLabelText('Command')).not.toBeDisabled()
    fireEvent.click(protectedRow.getByText('Replace transport', { selector: 'summary' }))
    fireEvent.click(protectedRow.getByRole('button', { name: 'Replace transport' }))
    fireEvent.change(protectedRow.getByLabelText('Command'), { target: { value: 'new-cli' } })
    fireEvent.click(protectedRow.getByRole('button', { name: 'Add argument' }))
    fireEvent.change(protectedRow.getByLabelText('Argument 1'), { target: { value: '--quiet' } })
    fireEvent.click(protectedRow.getByRole('button', { name: 'Add argument' }))
    fireEvent.change(protectedRow.getByLabelText('Argument 2'), { target: { value: '--read' } })
    fireEvent.click(protectedRow.getByRole('button', { name: 'Add variable' }))
    fireEvent.change(protectedRow.getByLabelText('Variable name'), { target: { value: 'REGION' } })
    fireEvent.change(protectedRow.getByLabelText('Variable value'), { target: { value: 'test-region' } })
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'protected', action: 'set', value: { command: 'new-cli', args: ['--quiet', '--read'], env: { REGION: 'test-region' } } }])
    expect(JSON.stringify(previewBodies)).not.toContain('REDACTED')
    expect(screen.getByRole('combobox', { name: 'Source for Managed' })).toBeDisabled()
    expect(screen.getByText('Managed by the system. This row cannot be edited here.')).toBeInTheDocument()
  })

  it('sends selected parent changes and explicit member scope together', async () => {
    mount(); await ready()
    fireEvent.click(screen.getByText('Review parent changes'))
    // The hint must not promise that accepting replaces a local value: the
    // resolver keeps explicit rows until Inherited or Restore from parent.
    expect(screen.getByText(/Rows you set yourself keep your value; choose Inherited or Restore from parent/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('checkbox', { name: /Local conflict/ }))
    fireEvent.change(screen.getByLabelText(/Also apply selected parent changes/), { target: { value: 'crewB, crewC, crewB' } })
    await review()
    expect(previewBodies[0].accept_parent).toEqual([{ section: 'tools', id: '@search/read' }])
    expect(previewBodies[0].accept_members).toEqual(['crewB', 'crewC'])
  })

  it('shows each row state once, in the source select, never as a duplicate badge', async () => {
    mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Tools', exact: true }))
    const row = within(screen.getByTestId('capability-tools-@search/read'))
    expect(row.getByRole('combobox', { name: 'Source for Search read' })).toHaveTextContent('Inherited')
    expect(row.getAllByText('Inherited')).toHaveLength(1)
  })

  it('keeps the draft after stale save, reloads and requires a fresh preview', async () => {
    server.use(http.put(endpoint, () => HttpResponse.json({ code: 'stale_revision' }, { status: 409 })))
    mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Tools', exact: true }))
    await pickState('Search read', 'Removed'); await review()
    fireEvent.click(screen.getByRole('button', { name: 'Save reviewed changes' }))
    await screen.findByText(/The saved version changed/)
    expect(screen.queryByRole('button', { name: 'Save reviewed changes' })).not.toBeInTheDocument()
    view = { ...view, revision: 'r3' }
    fireEvent.click(screen.getByRole('button', { name: 'Refresh from server (draft kept)' }))
    await screen.findByText('Review version: r3')
    fireEvent.click(screen.getByRole('button', { name: 'Review draft on new version' }))
    await review()
    expect(previewBodies[1].revision).toBe('r3')
    expect(previewBodies[1].operations).toEqual(previewBodies[0].operations)
  })

  it('discards every draft change and leaves the server untouched', async () => {
    const result = mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Tools', exact: true }))
    await pickState('Search read', 'Removed')
    fireEvent.click(screen.getByRole('button', { name: 'Discard draft' }))
    expect(result.dirty).toHaveBeenLastCalledWith(false)
    expect(screen.getByRole('button', { name: 'Review/save' })).toBeDisabled()
    expect(previewBodies).toEqual([]); expect(saveBodies).toEqual([])
  })

  it('blocks a missing parent and reports an unsupported backend honestly', async () => {
    view.template.available = false
    const result = mount(); await ready()
    expect(screen.getByText(/The parent template is missing/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Review/save' })).toBeDisabled()
    result.unmount()
    server.use(http.get(endpoint, () => new HttpResponse(null, { status: 501 })))
    mount()
    await screen.findByText('This server does not support the capabilities editor.')
  })

  it('locks managed transport fields while allowing the supported enable switch', async () => {
    view.rows = view.rows.map(row => row.id === 'managed' ? { ...row, editable: true, managed: true } : row)
    mount(); await ready()
    const managed = within(screen.getByTestId('capability-mcpServers-managed'))
    fireEvent.click(managed.getByText('Transport settings'))
    expect(managed.getByLabelText('Command')).toBeDisabled()
    expect(managed.getByRole('combobox', { name: 'Transport settings' })).toBeDisabled()
    expect(managed.queryByRole('button', { name: 'Replace transport' })).not.toBeInTheDocument()
    fireEvent.click(managed.getByRole('checkbox', { name: 'Enabled' }))
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'managed', action: 'set', value: { disabled: true } }])
  })

  it.each([
    { url: '[REDACTED]', args: '[REDACTED]', env: '[REDACTED]', headers: '[REDACTED]' },
    { url: 'https://example.test/mcp', args: ['--read'], env: { TOKEN: '[REDACTED]' }, headers: { Authorization: '[REDACTED]' } },
  ])('keeps masked remote transports untouched when selecting their configured connection (%#)', async transport => {
    view.rows.push({ section: 'mcpServers', id: 'remote', label: 'Remote', state: 'local', present: true, value: transport, editable: true })
    view.connections.push({ id: 'remote', label: 'Configured remote', managed: false })
    mount(); await ready()
    const remote = within(screen.getByTestId('capability-mcpServers-remote'))
    fireEvent.click(remote.getByText('Transport settings'))
    expect(remote.getByLabelText('Server URL')).not.toBeDisabled()
    expect(remote.getByLabelText('Server URL')).toHaveValue(transport.url === '[REDACTED]' ? '' : transport.url)
    const headerValues = remote.queryAllByLabelText('Header value')
    expect(headerValues).toHaveLength(typeof transport.headers === 'string' ? 0 : 1)
    for (const input of headerValues) { expect(input).not.toBeDisabled(); expect(input).toHaveValue(''); expect(input).toHaveAttribute('placeholder', 'Keep existing value') }
    expect(previewBodies).toEqual([])
    fireEvent.click(screen.getByRole('button', { name: 'Add configured MCP connection' }))
    fireEvent.click(await screen.findByRole('option', { name: 'Configured remote' }))
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'remote', action: 'set', connection_id: 'remote' }])
    expect(JSON.stringify(previewBodies)).not.toContain('REDACTED')
  })

  it('blocks a reviewed save when a background read sees a newer version', async () => {
    const result = mount(); await ready()
    fireEvent.click(screen.getByRole('tab', { name: 'Tools', exact: true }))
    await pickState('Search read', 'Removed'); await review()
    view = { ...view, revision: 'r4' }
    await act(async () => { await result.client.invalidateQueries({ queryKey: ['crew-capabilities', 'crewA'] }) })
    await screen.findByText('Review version: r4')
    expect(screen.getByRole('button', { name: 'Save reviewed changes' })).toBeDisabled()
    expect(saveBodies).toEqual([])
    fireEvent.click(screen.getByRole('button', { name: 'Review draft on new version' }))
    expect(screen.queryByRole('button', { name: 'Save reviewed changes' })).not.toBeInTheDocument()
    await review()
    expect(previewBodies[1].revision).toBe('r4')
    expect(previewBodies[1].operations).toEqual(previewBodies[0].operations)
  })

  it('retains unchanged masked leaves on a command-only edit through preview and save', async () => {
    view.rows = view.rows.map(row => row.id === 'protected' ? { ...row, value: { command: 'old-cli', args: ['--token', '[REDACTED]'], env: { 'TOKEN/~key': '[REDACTED]' }, timeout: 30 } } : row)
    mount(); await ready()
    const row = within(screen.getByTestId('capability-mcpServers-protected'))
    fireEvent.click(row.getByText('Transport settings'))
    expect(row.getByLabelText('Variable value')).toHaveValue('')
    expect(row.getByLabelText('Argument 2')).toHaveAttribute('placeholder', 'Keep existing value')
    fireEvent.change(row.getByLabelText('Command'), { target: { value: 'new-cli' } })
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'protected', action: 'set', value: { command: 'new-cli', args: ['--token', '[REDACTED]'], env: { 'TOKEN/~key': '[REDACTED]' }, timeout: 30 }, retain_paths: ['/args/1', '/env/TOKEN~1~0key'] }])
    fireEvent.click(screen.getByRole('button', { name: 'Save reviewed changes' }))
    await waitFor(() => expect(saveBodies).toHaveLength(1))
    expect(saveBodies[0]).toEqual({ ...previewBodies[0], preview_token: 'signed-preview' })
  })

  it('never reuses retention when an argument moves or a secret key is renamed', async () => {
    mount(); await ready()
    const row = within(screen.getByTestId('capability-mcpServers-protected'))
    fireEvent.click(row.getByText('Transport settings'))
    expect(row.getAllByRole('button', { name: 'Remove argument' })[0]).toBeDisabled()
    expect(row.getByLabelText('Variable name')).toBeDisabled()
    fireEvent.change(row.getByLabelText('Argument 2'), { target: { value: 'replacement' } })
    fireEvent.click(row.getAllByRole('button', { name: 'Remove argument' })[0])
    fireEvent.change(row.getByLabelText('Variable value'), { target: { value: 'replacement-value' } })
    fireEvent.change(row.getByLabelText('Variable name'), { target: { value: 'RENAMED' } })
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'protected', action: 'set', value: { command: 'protected-cli', args: ['replacement'], env: { RENAMED: 'replacement-value' } } }])
    expect(screen.queryByText('replacement-value')).not.toBeInTheDocument()
  })

  it('does not treat a newly typed mask as permission to retain the original value', async () => {
    mount(); await ready()
    const row = within(screen.getByTestId('capability-mcpServers-protected'))
    fireEvent.click(row.getByText('Transport settings'))
    fireEvent.change(row.getByLabelText('Variable value'), { target: { value: 'replacement' } })
    fireEvent.change(row.getByLabelText('Variable value'), { target: { value: '[REDACTED]' } })
    expect(screen.getByRole('button', { name: 'Review/save' })).toBeDisabled()
    expect(screen.getByText('Complete the connection fields and replace any hidden values that are not marked to keep.')).toBeInTheDocument()
    expect(previewBodies).toEqual([])
  })

  it('edits HTTP headers while keeping an unchanged authorization header', async () => {
    view.rows.push({ section: 'mcpServers', id: 'http', label: 'HTTP server', state: 'local', present: true, editable: true, value: { url: 'https://example.test/mcp', headers: { Authorization: '[REDACTED]' } } })
    mount(); await ready()
    const row = within(screen.getByTestId('capability-mcpServers-http'))
    fireEvent.click(row.getByText('Transport settings'))
    fireEvent.change(row.getByLabelText('Server URL'), { target: { value: 'https://example.test/new' } })
    fireEvent.click(row.getByRole('button', { name: 'Add header' }))
    fireEvent.change(row.getAllByLabelText('Header name')[1], { target: { value: 'X-Region' } })
    fireEvent.change(row.getAllByLabelText('Header value')[1], { target: { value: 'test-region' } })
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'http', action: 'set', value: { url: 'https://example.test/new', headers: { Authorization: '[REDACTED]', 'X-Region': 'test-region' } }, retain_paths: ['/headers/Authorization'] }])
  })

  it('creates an unlisted member connection as a complete transport', async () => {
    mount(); await ready()
    fireEvent.change(screen.getByLabelText('Custom connection name'), { target: { value: 'member-only' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add custom MCP connection' }))
    expect(screen.getByRole('button', { name: 'Review/save' })).toBeDisabled()
    const row = within(screen.getByTestId('capability-mcpServers-member-only'))
    fireEvent.click(row.getByText('Transport settings'))
    fireEvent.change(row.getByLabelText('Command'), { target: { value: 'member-cli' } })
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'mcpServers', id: 'member-only', action: 'set', value: { command: 'member-cli', args: [], env: {} } }])
  })

  it('sets absent prompt and model using the advertised model catalog', async () => {
    const modelsRead = vi.fn()
    server.use(http.get('/api/models', () => { modelsRead(); return HttpResponse.json([{ model_name: 'catalog-model' }]) }))
    view.rows.push(...(['prompt', 'model'] as const).map(section => ({ section, id: section, label: section, state: 'inherited' as const, present: false, editable: true, value: null })))
    mount(); await ready()
    fireEvent.click(screen.getByText('Advanced references and template fields'))
    fireEvent.change(screen.getByLabelText('System Prompt'), { target: { value: 'First line\nSecond line' } })
    await waitFor(() => expect(modelsRead).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('combobox', { name: 'Model', exact: true }))
    fireEvent.click(await screen.findByRole('option', { name: 'catalog-model' }))
    await review()
    expect(previewBodies[0].operations).toEqual([{ section: 'prompt', id: 'prompt', action: 'set', value: 'First line\nSecond line' }, { section: 'model', id: 'model', action: 'set', value: 'catalog-model' }])
    expect(screen.queryByRole('textbox', { name: 'Model', exact: true })).not.toBeInTheDocument()
  })

  it('encodes the exact member identity in the HTTP path', async () => {
    const observed = vi.fn()
    server.use(http.get('/api/agents/:member/capabilities', ({ params }) => { observed(params.member); return HttpResponse.json(view) }))
    await crewCapabilitiesApi.get('crew space')
    expect(observed).toHaveBeenCalledWith('crew space')
  })
})
