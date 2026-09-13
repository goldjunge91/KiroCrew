import { afterEach, describe, expect, it } from 'vitest'
import { i18next, initI18n } from '../i18n/all'
import { CATALOGS } from '../i18n/catalogs'
import {
  embeddingSetupError, embeddingSetupWarning, embeddingRepairMessage, embeddingSetupDiagnostic, isModelPathErrorCode,
} from '../pages/overview/embeddingStatusText'
import { reembedBar, reembedBusy } from '../pages/overview/EmbeddingModelCard'

afterEach(async () => { await i18next.changeLanguage('en') })

const RAW = 'memory.embed_model_path could not be read: [Errno 5] Input/output error'

describe('embedding status codes', () => {
  it('keeps old and unknown backend English messages unchanged', () => {
    expect(embeddingSetupWarning({ setup_warning: 'Old warning' })).toBe('Old warning')
    expect(embeddingSetupWarning({ setup_warning: 'New warning', setup_warning_code: 'new_code' })).toBe('New warning')
    expect(embeddingSetupError({ setup_error: 'Old error' })).toBe('Old error')
    expect(embeddingSetupError({ setup_error: 'New error', setup_error_code: 'new_code' })).toBe('New error')
    expect(embeddingSetupError(null)).toBe('')
    // An unknown code renders its prose as the body, so there is no second copy to fold.
    expect(embeddingSetupDiagnostic({ setup_error: 'New error', setup_error_code: 'new_code' })).toBe('')
    expect(embeddingSetupDiagnostic(null)).toBe('')
  })

  it('translates every known code in every shipped language and preserves parameters', async () => {
    await initI18n()
    for (const language of Object.keys(CATALOGS).filter(code => code !== 'en-XA')) {
      await i18next.changeLanguage(language)
      const warning = embeddingSetupWarning({ setup_warning_code: 'legacy_embedding_vectors', setup_warning: 'backend fallback' })
      expect(warning).not.toContain('backend fallback')
      expect(warning).not.toContain('pages.overview')
      // With the model file missing, the warning must not tell the user to reapply the missing path.
      const pathWarning = embeddingSetupWarning({ setup_warning_code: 'legacy_embedding_vectors', setup_warning: 'backend fallback', setup_error_code: 'model_path_not_found' })
      expect(pathWarning).not.toContain('backend fallback')
      expect(pathWarning).not.toContain('pages.overview')
      expect(pathWarning).not.toBe(warning)
      for (const code of ['model_path_not_absolute', 'model_path_not_found', 'model_path_not_a_file', 'model_path_too_small', 'model_path_protected', 'model_path_unreadable', 'model_verification_failed']) {
        const error = embeddingSetupError({ setup_error_code: code, setup_error: 'backend fallback', setup_error_params: { path: '/models/文件.gguf', error: RAW } })
        expect(error).toContain('/models/文件.gguf')
        expect(error).not.toContain('pages.overview')
        expect(error).not.toContain('{{')
        // Known codes are fully localized: the backend's English exception never lands in the body.
        expect(error).not.toContain(RAW)
        expect(error).not.toContain('Errno')
      }
      expect(embeddingSetupError({ setup_error_code: 'model_identity_unverified' })).not.toContain('pages.overview')
      const download = embeddingSetupError({ setup_error_code: 'model_download_failed', setup_error_params: { error: RAW } })
      expect(download).not.toContain(RAW)
      expect(download).not.toContain('pages.overview')
      expect(download).not.toContain('{{')
      const repair = embeddingRepairMessage({ repair: { generation: 'request', pending_invalidation: 3, pending_vectors: 17, deferred_stores: 9 } })
      expect(repair).toContain('17')
      expect(repair).toContain('9')
      expect(repair).toContain('3')
      expect(repair).not.toContain('{{')
      // Not a raw field echo: none of the API's field names leak into the copy.
      for (const jargon of ['pending_invalidation', 'pending_vectors', 'deferred_stores', 'invalidation:', 'Invalidation:']) {
        expect(repair).not.toContain(jargon)
      }
    }
  })

  it('keeps the raw exception reachable through the diagnostic, not the body', () => {
    const status = { setup_error_code: 'model_verification_failed', setup_error: RAW, setup_error_params: { path: '/models/a.gguf', error: RAW } }
    expect(embeddingSetupError(status)).not.toContain(RAW)
    expect(embeddingSetupDiagnostic(status)).toBe(RAW)
    const download = { setup_error_code: 'model_download_failed', setup_error: 'HTTP 503', setup_error_params: { error: 'HTTP 503' } }
    expect(embeddingSetupError(download)).not.toContain('503')
    expect(embeddingSetupDiagnostic(download)).toBe('HTTP 503')
    // Path codes say everything the raw text says, so no diagnostic is offered.
    expect(embeddingSetupDiagnostic({ setup_error_code: 'model_path_not_found', setup_error: 'no file', setup_error_params: { path: '/x', error: 'no file' } })).toBe('')
    // Falls back to setup_error when the params object omits the text.
    expect(embeddingSetupDiagnostic({ setup_error_code: 'model_download_failed', setup_error: 'HTTP 503' })).toBe('HTTP 503')
  })

  it('recognises exactly the model path codes', () => {
    for (const code of ['model_path_not_absolute', 'model_path_not_found', 'model_path_not_a_file', 'model_path_too_small', 'model_path_protected', 'model_path_unreadable']) {
      expect(isModelPathErrorCode(code)).toBe(true)
    }
    expect(isModelPathErrorCode('model_verification_failed')).toBe(false)
    expect(isModelPathErrorCode('model_download_failed')).toBe(false)
    expect(isModelPathErrorCode('')).toBe(false)
    expect(isModelPathErrorCode(undefined)).toBe(false)
  })

  it('does not interpret English text as a code', async () => {
    await i18next.changeLanguage('zh-CN')
    expect(embeddingSetupError({ setup_error: 'model_path_not_found' })).toBe('model_path_not_found')
    expect(embeddingSetupWarning({ setup_warning_code: 'legacy_embedding_vectors' })).toContain('向量')
    expect(embeddingSetupWarning({ setup_warning_code: 'legacy_embedding_vectors', setup_error_code: 'model_path_not_found' })).toContain('先修正模型路径')
    expect(embeddingSetupError({ setup_error_code: 'model_identity_unverified' })).toContain('校验')
  })

  it('keeps unknown repair scope and deferred progress distinct from completion', () => {
    expect(embeddingRepairMessage({ repair: { generation: 'r', unknown_scope: true } })).toContain('not confirmed')
    expect(embeddingRepairMessage({ repair: { generation: 'r' } })).toBe('')
    expect(embeddingRepairMessage(null)).toBe('')
    expect(reembedBar({ step: 'deferred' })).toEqual({ widthPct: 0, indeterminate: true })
    expect(reembedBusy({ step: 'deferred' })).toBe(false)
  })

  it('never sums the store count into the vector count, and shows an invalidation-only state', () => {
    // 2 open stores still to clear, 0 vectors, 0 deferred: still pending, still shown.
    const invalidationOnly = embeddingRepairMessage({ repair: { generation: 'r', pending_invalidation: 2, pending_vectors: 0, deferred_stores: 0 } })
    expect(invalidationOnly).not.toBe('')
    expect(invalidationOnly).toContain('2')
    // 2 stores + 5 vectors must not read as 7 of anything.
    const both = embeddingRepairMessage({ repair: { generation: 'r', pending_invalidation: 2, pending_vectors: 5, deferred_stores: 0 } })
    expect(both).toContain('2')
    expect(both).toContain('5')
    expect(both).not.toMatch(/\b7\b/)
  })
})
