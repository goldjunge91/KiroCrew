import { i18nT } from '../../i18n/t'
import { embedModelErrorMessage } from './EmbeddingModelCard'

export interface EmbeddingSetupFields {
  setup_error?: string
  setup_error_code?: string
  setup_error_params?: { path?: string; error?: string }
  setup_warning?: string
  setup_warning_code?: string
  setup_warning_params?: Record<string, string>
  model_active?: boolean
  repair?: {
    generation?: string
    scope?: string
    pending_invalidation?: number
    pending_vectors?: number
    deferred_stores?: number
    unknown_scope?: boolean
  }
}

/** True for every backend code that means "the configured model path is unusable".
 *
 * These are the codes a user can fix from the Embedding Model card by editing
 * the path, so the card gates Apply on them and the legacy-vectors warning
 * swaps its "reapply" clause for "fix the path first". */
export function isModelPathErrorCode(code: string | undefined): boolean {
  return !!code && code.startsWith('model_path_')
}

export function embeddingSetupWarning(status: EmbeddingSetupFields | null): string {
  if (status?.setup_warning_code === 'legacy_embedding_vectors') {
    // "Reapply it" is a dead end while the file the path names is missing: the
    // Apply button would submit the very path the error is about. Point at the
    // path fix first, then the reapply.
    if (isModelPathErrorCode(status.setup_error_code)) {
      return i18nT('pages.overview.vectorMemoryCard.legacy_vectors_warning_path_error')
    }
    return i18nT('pages.overview.vectorMemoryCard.legacy_vectors_warning')
  }
  return status?.setup_warning || ''
}

/** Localized, actionable notice for a coded setup error.
 *
 * Known codes render fully localized copy with a next step and never
 * interpolate the backend's English exception text — that text stays available
 * through {@link embeddingSetupDiagnostic} for a collapsed details block.
 * Missing and unknown codes fall back to the diagnostic prose so a new backend
 * code is never silently swallowed. */
export function embeddingSetupError(status: EmbeddingSetupFields | null): string {
  if (!status) return ''
  const code = status.setup_error_code
  const path = status.setup_error_params?.path || ''
  const error = status.setup_error_params?.error ?? status.setup_error ?? ''
  switch (code) {
    case 'model_identity_unverified':
      return i18nT('pages.overview.vectorMemoryCard.verification_pending')
    case 'model_verification_failed':
      return i18nT('pages.overview.vectorMemoryCard.verification_failed', { path })
    case 'model_download_failed':
      return i18nT('pages.overview.vectorMemoryCard.download_error_detail')
    case 'model_path_not_absolute':
    case 'model_path_not_found':
    case 'model_path_not_a_file':
    case 'model_path_too_small':
    case 'model_path_protected':
    case 'model_path_unreadable': {
      const message = embedModelErrorMessage({ code, error })
      return path ? i18nT('pages.overview.vectorMemoryCard.model_error_path', { message, path }) : message
    }
    default: return status.setup_error || ''
  }
}

/** The raw backend exception text behind a known-code notice, or ''.
 *
 * Only the two codes whose localized body dropped it: verification and download
 * failures carry an OSError / downloader message that a log reader needs and a
 * user does not. Path codes already say everything the raw text says, and an
 * unknown code renders its prose as the body, so neither needs a second copy. */
export function embeddingSetupDiagnostic(status: EmbeddingSetupFields | null): string {
  if (!status) return ''
  const code = status.setup_error_code
  if (code !== 'model_verification_failed' && code !== 'model_download_failed') return ''
  return status.setup_error_params?.error ?? status.setup_error ?? ''
}

/** User-vocabulary summary of the standing rebuild, or '' when nothing is pending.
 *
 * Three counts, three meanings, never summed: `pending_vectors` is memory
 * entries still waiting for a new vector, `pending_invalidation` is open STORES
 * that still hold old vectors to clear, `deferred_stores` is closed or
 * unavailable stores that are rebuilt when they next open. A state where only
 * the invalidation count is non-zero is still pending and still rendered. */
export function embeddingRepairMessage(status: EmbeddingSetupFields | null): string {
  const repair = status?.repair
  if (!repair?.generation) return ''
  if (repair.unknown_scope) return i18nT('pages.overview.vectorMemoryCard.repair_unknown')
  const invalidation = repair.pending_invalidation ?? 0
  const vectors = repair.pending_vectors ?? 0
  const deferred = repair.deferred_stores ?? 0
  if (!invalidation && !vectors && !deferred) return ''
  return i18nT('pages.overview.vectorMemoryCard.repair_pending', { invalidation, vectors, deferred })
}
