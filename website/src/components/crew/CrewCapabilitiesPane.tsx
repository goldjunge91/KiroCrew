import { useEffect, useLayoutEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { ApiError } from '../../api/apiError'
import { capabilityKey, crewCapabilitiesApi, crewCapabilitiesKey, type CapabilityDraft, type CapabilityOperation, type CapabilityPreview, type CapabilityRef, type CapabilityRow, type CapabilitySection } from '../../api/crewCapabilities'
import { Badge, Btn, Checkbox, Input, PanelSectionHeader, SendBtn } from '../ui'
import ErrorNotice from '../ErrorNotice'
import SearchableSelect from '../SearchableSelect'
import SimpleSelect from '../SimpleSelect'
import Tablist from '../Tablist'
import CapabilityRowEditor from './CapabilityRowEditor'
import { capabilityLabels } from './capabilityLabels'
import { useAvailableModels } from '../../hooks/useAvailableModels'
import { retentionValid, transportValue } from './capabilityTransport'

const CATEGORIES = ['mcpServers', 'tools', 'autoApprove', 'skills'] as const
type Category = typeof CATEGORIES[number]
const CATEGORY_KEYS = { mcpServers: 'pages.agentsPage.mcp_servers', tools: 'crewCapabilities.tools', autoApprove: 'pages.agentsPage.auto_approved', skills: 'pages.agentsPage.skills' }

/** Mounted for the entire editor opening, even when a different rail pane is
 * visible. Drafts and signed previews must not follow the active-pane lifetime. */
export default function CrewCapabilitiesPane({ member, hidden, onDirtyChange, onBusyChange, onSaved }: {
  member: string
  hidden: boolean
  onDirtyChange: (dirty: boolean) => void
  onBusyChange: (busy: boolean) => void
  onSaved: () => void
}) {
  const { t } = useTranslation()
  const qc = useQueryClient()
  const query = useQuery({ queryKey: crewCapabilitiesKey(member), queryFn: () => crewCapabilitiesApi.get(member), retry: false })
  const [draft, setDraft] = useState<CapabilityDraft | null>(null)
  const [preview, setPreview] = useState<CapabilityPreview | null>(null)
  const [category, setCategory] = useState<Category>('mcpServers')
  const [reference, setReference] = useState('')
  const [approvalSection, setApprovalSection] = useState<'allowedTools' | 'autoApprove'>('allowedTools')
  const [members, setMembers] = useState('')
  const [connectionName, setConnectionName] = useState('')
  const models = useAvailableModels({ enabled: !hidden }).map(model => model.name)
  const [reviewedRequest, setReviewedRequest] = useState<CapabilityDraft | null>(null)
  const dirty = draft !== null || reference !== '' || members !== '' || connectionName !== ''
  const prepare = useMutation({ mutationFn: (body: CapabilityDraft) => crewCapabilitiesApi.preview(member, body), onSuccess: (view, body) => { setPreview(view); setReviewedRequest(body) } })
  const save = useMutation({
    mutationFn: (body: CapabilityDraft & { preview_token: string }) => crewCapabilitiesApi.save(member, body),
    onSuccess: async view => {
      qc.setQueryData(crewCapabilitiesKey(member), view)
      setDraft(null); setPreview(null); setReviewedRequest(null); setReference(''); setMembers(''); setConnectionName('')
      await Promise.all([
        qc.invalidateQueries({ queryKey: ['agentDetail'] }),
        qc.invalidateQueries({ queryKey: ['agents-installed'] }),
        qc.invalidateQueries({ queryKey: ['kirocrew-agents'] }),
      ])
      onSaved()
    },
    onError: () => { setPreview(null); setReviewedRequest(null) },
  })
  const busy = prepare.isPending || save.isPending
  // Closing reads these in the parent. Publish before paint so paste followed
  // by Escape cannot reach a close handler that still considers the pane clean.
  useLayoutEffect(() => { onDirtyChange(dirty) }, [dirty, onDirtyChange])
  useLayoutEffect(() => { onBusyChange(busy) }, [busy, onBusyChange])
  useEffect(() => {
    if (!dirty) return
    const guard = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = '' }
    window.addEventListener('beforeunload', guard)
    return () => window.removeEventListener('beforeunload', guard)
  }, [dirty])
  const view = query.data
  const emptyDraft = (): CapabilityDraft => ({ revision: view!.revision, enroll: false, operations: [], accept_parent: [], accept_members: [] })
  const edit = (change: (current: CapabilityDraft) => CapabilityDraft) => {
    setDraft(current => change(current ?? emptyDraft()))
    setPreview(null); setReviewedRequest(null); prepare.reset(); save.reset()
  }
  const changeRow = (operation: CapabilityOperation) => edit(current => ({ ...current, operations: [...current.operations.filter(op => capabilityKey(op) !== capabilityKey(operation)), operation] }))
  const discard = () => { setDraft(null); setPreview(null); setReviewedRequest(null); setReference(''); setMembers(''); setConnectionName(''); prepare.reset(); save.reset() }
  const error = query.error || prepare.error || save.error
  const errorKey = error instanceof ApiError && error.status === 409 ? 'crewCapabilities.stale' : error instanceof ApiError && [404, 405, 501].includes(error.status) ? 'crewCapabilities.unsupported' : 'crewCapabilities.failed'
  const enabled = !!view && view.schema_version === 1 && view.template.available && !query.isError && (view.mode === 'inherited' || draft?.enroll === true)
  const operations = draft?.operations ?? []
  const invalidTransport = operations.some(op => {
    if (!retentionValid(op)) return true
    if (op.section !== 'mcpServers' || op.action !== 'set' || !('value' in op)) return false
    const value = transportValue(op.value)
    if (!value) return true
    if (['env', 'headers'].some(field => Object.keys(transportValue(value[field]) ?? {}).some(key => !key.trim()))) return true
    if (Object.keys(value).length === 1 && typeof value.disabled === 'boolean') return false
    return typeof (value.command ?? value.url) !== 'string' || !(value.command ?? value.url)
  })
  const rows: CapabilityRow[] = [...(view?.rows ?? [])]
  for (const op of operations) {
    if (!rows.some(row => capabilityKey(row) === capabilityKey(op))) rows.push({ ...op, label: (op.section === 'skills' ? view?.skills : view?.connections)?.find(item => item.id === op.id)?.label ?? op.id, state: 'local', present: true, value: op.action === 'set' && 'value' in op ? op.value : true, editable: true })
  }
  const visibleSections: CapabilitySection[] = category === 'autoApprove' ? ['allowedTools', 'autoApprove'] : [category]
  const addReference = () => {
    const id = reference.trim()
    if (!id) return
    changeRow({ section: category === 'autoApprove' ? approvalSection : 'tools', id, action: 'set', value: true })
    setReference('')
  }
  const accept = (ref: CapabilityRef, checked: boolean) => edit(current => ({ ...current, accept_parent: checked ? [...current.accept_parent, ref] : current.accept_parent.filter(item => capabilityKey(item) !== capabilityKey(ref)) }))

  return (
    <div hidden={hidden} className={hidden ? 'hidden' : 'flex min-h-0 min-w-0 flex-1 flex-col'} data-testid="crew-capabilities-pane">
      <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto px-5 py-4" data-testid="capability-scroll-region">
        <PanelSectionHeader label={t('crewCapabilities.title')} />
        {/* No hand-off: capability drafts and signed previews are not saved. */}
        {error && <ErrorNotice message={t(errorKey)} variant="inline" />}
        <Btn disabled={busy || query.isFetching} onClick={() => { setPreview(null); setReviewedRequest(null); void query.refetch() }}>{t('crewCapabilities.reload')}</Btn>
        {query.isLoading && <p className="text-muted">{t('components.agentTemplateDetail.loading')}</p>}
        {view && view.schema_version !== 1 && <p className="text-muted">{t('crewCapabilities.unsupported')}</p>}
        {view?.schema_version === 1 && <>
          <div className="rounded-md border border-border bg-bg-accent p-3 text-[13px]">
            <p className="break-words">{t('crewCapabilities.parent', { name: view.template.name })}</p>
            <p>{t(capabilityLabels.source[view.template.source])} · {t(capabilityLabels.scope[view.template.scope])}</p>
            <Badge variant="muted">{t(capabilityLabels.mode[view.mode])}</Badge>
            <Badge variant="muted">{t(dirty ? 'components.crewEditor.unsaved_changes' : 'crewCapabilityEditing.saved')}</Badge>
            <details className="mt-2 text-[12px] text-muted">
              <summary>{t('crewCapabilityEditing.versions')}</summary>
              <p className="mt-2 break-all">{t('crewCapabilities.version', { revision: view.revision })}</p>
              <p className="mt-1 break-all">{t('crewCapabilities.savedVersion', { revision: view.runtime.saved_revision })}</p>
            </details>
            {/* No hand-off: validation and runtime errors must preserve the draft. */}
            {!view.template.available
              ? <ErrorNotice message={t('crewCapabilities.parentMissing')} variant="inline" />
              : view.runtime.status === 'failed'
                ? <ErrorNotice message={t(view.runtime.error_code ? 'crewCapabilityEditing.sourceFailed' : 'crewCapabilities.runtime_failed')} variant="inline" />
                : <p role="status" className="mt-2">{t(capabilityLabels.runtime[view.runtime.status])}</p>}
            <p className="mt-1 text-muted">{t('crewCapabilities.newRuntime')}</p>
            {view.mode !== 'inherited' && <div className="mt-3">
              <label className="flex items-start gap-2"><Checkbox aria-describedby="crew-capabilities-enroll-help" checked={draft?.enroll ?? false} disabled={busy || !view.template.available} onChange={e => edit(current => ({ ...current, enroll: e.target.checked }))} /><span>{t('crewCapabilities.enroll')}</span></label>
              <p id="crew-capabilities-enroll-help" className="mt-1 text-[12px] text-muted">{t('crewCapabilities.enrollHelp')}</p>
            </div>}
          </div>
          <div className="shrink-0 overflow-x-auto border-b border-border pb-2"><Tablist tabs={CATEGORIES.map(key => ({ key, label: t(CATEGORY_KEYS[key]) }))} value={category} onChange={setCategory} ariaLabel={t('crewCapabilities.title')} /></div>
          <fieldset disabled={!enabled || busy} className="min-w-0">
            {category === 'mcpServers' && <SearchableSelect aria-label={t('crewCapabilities.connection')} triggerFallback={t('crewCapabilities.connection')} value="" disabled={!enabled || busy} options={view.connections.map(item => ({ value: item.id, label: item.label, disabled: item.managed, sublabel: item.managed ? t('crewCapabilities.managed') : undefined }))} onChange={id => changeRow({ section: 'mcpServers', id, action: 'set', connection_id: id })} />}
            {category === 'mcpServers' && <div className="mt-3 flex flex-col gap-2">
              <label className="text-[13px]">{t('crewCapabilityEditing.connectionName')}<Input value={connectionName} onChange={e => { setConnectionName(e.target.value); setPreview(null); setReviewedRequest(null) }} /></label>
              <Btn disabled={!connectionName.trim() || rows.some(row => row.section === 'mcpServers' && row.id === connectionName.trim())} onClick={() => {
                changeRow({ section: 'mcpServers', id: connectionName.trim(), action: 'set', value: { command: '', args: [], env: {} } })
                setConnectionName('')
              }}>{t('crewCapabilityEditing.customConnection')}</Btn>
            </div>}
            {category === 'skills' && <SearchableSelect aria-label={t('crewCapabilities.skill')} triggerFallback={t('crewCapabilities.skill')} value="" disabled={!enabled || busy} options={view.skills.map(item => ({ value: item.id, label: item.label }))} onChange={id => changeRow({ section: 'skills', id, action: 'set', value: true })} />}
            {(category === 'tools' || category === 'autoApprove') && <div className="flex flex-col gap-2">
              <p className="text-[12px] text-muted">{t(category === 'tools' ? 'crewCapabilities.exposureHint' : 'crewCapabilities.approvalHint')}</p>
              {category === 'autoApprove' && <SimpleSelect aria-label={t('crewCapabilities.approvalKind')} options={['allowedTools', 'autoApprove']} optionLabels={[t('crewCapabilities.allowedTools'), t('crewCapabilities.serverApproval')]} value={approvalSection} onChange={v => setApprovalSection(v as 'allowedTools' | 'autoApprove')} />}
              <label className="text-[13px]">{t('crewCapabilities.reference')}<Input value={reference} onChange={e => { setReference(e.target.value); setPreview(null); setReviewedRequest(null) }} /></label>
              <Btn disabled={!reference.trim()} onClick={addReference}>{t('crewCapabilities.add')}</Btn>
            </div>}
            {visibleSections.map(section => <div key={section}>
              {category === 'autoApprove' && <PanelSectionHeader label={t(section === 'allowedTools' ? 'crewCapabilities.allowedTools' : 'crewCapabilities.serverApproval')} />}
              {rows.filter(row => row.section === section).map(row => <CapabilityRowEditor key={capabilityKey(row)} row={row} models={models} operation={operations.find(op => capabilityKey(op) === capabilityKey(row))} disabled={!enabled || busy} onChange={changeRow} />)}
            </div>)}
          </fieldset>
          <details className="text-[13px]">
            <summary className="cursor-pointer text-accent">{t('crewCapabilities.advanced')}</summary>
            {rows.filter(row => ['prompt', 'model', 'resources'].includes(row.section)).map(row => <CapabilityRowEditor key={capabilityKey(row)} row={row} models={models} operation={operations.find(op => capabilityKey(op) === capabilityKey(row))} disabled={!enabled || busy} onChange={changeRow} />)}
          </details>
          {view.parent_changes.length > 0 && <details className="rounded border border-border p-3 text-[13px]">
            <summary className="cursor-pointer text-accent">{t('crewCapabilities.parentReview')}</summary>
            <p className="my-2 text-muted">{t('crewCapabilities.parentHint')}</p>
            {view.parent_changes.map(change => <label key={capabilityKey(change)} htmlFor={`parent-change-${encodeURIComponent(capabilityKey(change))}`} className="mb-2 flex items-start gap-2">
              <Checkbox id={`parent-change-${encodeURIComponent(capabilityKey(change))}`} disabled={!enabled || busy} checked={draft?.accept_parent.some(ref => capabilityKey(ref) === capabilityKey(change)) ?? false} onChange={e => accept({ section: change.section, id: change.id }, e.target.checked)} />
              <span className="min-w-0 flex-1 break-all"><code translate="no">{change.section}: {change.id}</code> <Badge variant={change.conflict ? 'warn' : 'muted'}>{t(change.conflict ? 'crewCapabilities.conflict' : capabilityLabels.change[change.kind])}</Badge>
                <span className="mt-1 block text-muted">{t('components.agentTemplateDetail.change_was', { value: JSON.stringify(change.before) })}</span>
                <code className="mt-1 block whitespace-pre-wrap" translate="no">{JSON.stringify(change.after, null, 2)}</code>
              </span>
            </label>)}
            <label>{t('crewCapabilities.members')}<Input disabled={!enabled || busy} value={members} onChange={e => { setMembers(e.target.value); edit(current => ({ ...current, accept_members: [...new Set(e.target.value.split(',').map(name => name.trim()).filter(Boolean))] })) }} /></label>
          </details>}
          {draft && draft.revision !== view.revision && <p className="text-[13px] text-warn">{t('crewCapabilities.stale')}</p>}
          {draft && draft.revision !== view.revision && <Btn disabled={busy} onClick={() => edit(current => ({ ...current, revision: view.revision }))}>{t('crewCapabilities.rebase')}</Btn>}
          {preview && <section className="rounded border border-border bg-bg-accent p-3 text-[13px]" aria-label={t('crewCapabilities.previewHeading')}>
            <PanelSectionHeader label={t('crewCapabilities.previewHeading')} />
            {preview.impact.length === 0 && <p>{t('crewCapabilities.noChanges')}</p>}
            <ul className="space-y-2">{preview.impact.map((item, index) => {
              // Only the current member has a projected row in this response.
              // Never substitute a local draft, which can contain new secrets.
              const projected = item.member === member ? preview.rows.find(row => capabilityKey(row) === capabilityKey(item)) : undefined
              return <li key={`${capabilityKey(item)}-${item.member}-${index}`} className="break-all">
                <code translate="no">{item.member}: {item.section}: {item.id}</code>
                <p>{t(capabilityLabels.change[item.change])}</p>
                {projected?.present && <pre className="mt-2 overflow-auto whitespace-pre-wrap break-all rounded border border-border bg-bg p-2 text-[12px]" translate="no">{JSON.stringify(projected.value, null, 2)}</pre>}
                {item.approval_expanded && <p className="text-warn">{t('crewCapabilities.expanded')}</p>}
              </li>
            })}</ul>
            <p className="mt-2 text-muted">{t('crewCapabilities.newRuntime')}</p>
          </section>}
          {invalidTransport && <p className="text-[13px] text-warn">{t('crewCapabilityEditing.invalid')}</p>}
        </>}
      </div>
      {view?.schema_version === 1 && <div className="flex shrink-0 flex-wrap justify-end gap-2 border-t border-border bg-card px-5 py-3" data-testid="capability-save-footer">
            <Btn disabled={!dirty || busy} onClick={discard}>{t('crewCapabilities.discard')}</Btn>
            <SendBtn disabled={!enabled || !draft || busy || invalidTransport || !!connectionName.trim() || !!reference.trim() || draft.revision !== view.revision || (draft.enroll === false && view.mode !== 'inherited')} onClick={() => {
              if (preview && reviewedRequest) save.mutate({ ...reviewedRequest, preview_token: preview.preview_token })
              else if (draft) prepare.mutate(draft)
            }}>{busy ? t('crewCapabilities.working') : preview ? t('crewCapabilities.save') : t('crewCapabilities.review')}</SendBtn>
      </div>}
    </div>
  )
}
