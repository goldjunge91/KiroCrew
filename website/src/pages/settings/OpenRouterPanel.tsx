import React, { useState, useEffect } from 'react'
import { SettingsCard, SettingsSection, SettingsInput, SettingsSelect } from '../../components/settings'
import Clickable from '../../components/Clickable'

interface OpenRouterKey {
  id: string
  name: string
  masked_key: string
  created_at: number
}

interface ModelPreset {
  id: string
  name: string
  key_id: string
  model_name: string
}

export function OpenRouterBYOKPanel() {
  const [keys, setKeys] = useState<OpenRouterKey[]>([])
  const [presets, setPresets] = useState<ModelPreset[]>([])
  const [newKeyName, setNewKeyName] = useState('')
  const [newApiKey, setNewApiKey] = useState('')
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null)
  const [testing, setTesting] = useState(false)

  const [presetName, setPresetName] = useState('')
  const [presetKeyId, setPresetKeyId] = useState('')
  const [presetModelName, setPresetModelName] = useState('')

  useEffect(() => {
    fetchKeys()
    fetchPresets()
  }, [])

  const fetchKeys = async () => {
    try {
      const res = await fetch('/api/openrouter/keys')
      const data = await res.json()
      if (data.success) setKeys(data.keys || [])
    } catch (e) {
      console.error(e)
    }
  }

  const fetchPresets = async () => {
    try {
      const res = await fetch('/api/openrouter/presets')
      const data = await res.json()
      if (data.success) setPresets(data.presets || [])
    } catch (e) {
      console.error(e)
    }
  }

  const handleTestConnection = async () => {
    if (!newApiKey) return
    setTesting(true)
    setTestResult(null)
    try {
      const res = await fetch('/api/openrouter/keys/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: newApiKey }),
      })
      const data = await res.json()
      setTestResult({
        success: data.success,
        message: data.success ? 'Connection verified successfully!' : (data.error || 'Connection failed'),
      })
    } catch (e: any) {
      setTestResult({ success: false, message: e.message || 'Connection error' })
    } finally {
      setTesting(false)
    }
  }

  const handleAddKey = async () => {
    if (!newApiKey) return
    try {
      const res = await fetch('/api/openrouter/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newKeyName || 'OpenRouter Key', api_key: newApiKey }),
      })
      const data = await res.json()
      if (data.success) {
        setNewKeyName('')
        setNewApiKey('')
        setTestResult(null)
        fetchKeys()
      }
    } catch (e) {
      console.error(e)
    }
  }

  const handleDeleteKey = async (id: string) => {
    try {
      await fetch(`/api/openrouter/keys/${id}`, { method: 'DELETE' })
      fetchKeys()
    } catch (e) {
      console.error(e)
    }
  }

  const handleAddPreset = async () => {
    if (!presetName || !presetModelName) return
    try {
      const res = await fetch('/api/openrouter/presets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: presetName, key_id: presetKeyId, model_name: presetModelName }),
      })
      const data = await res.json()
      if (data.success) {
        setPresetName('')
        setPresetModelName('')
        fetchPresets()
      }
    } catch (e) {
      console.error(e)
    }
  }

  const handleDeletePreset = async (id: string) => {
    try {
      await fetch(`/api/openrouter/presets/${id}`, { method: 'DELETE' })
      fetchPresets()
    } catch (e) {
      console.error(e)
    }
  }

  return (
    <div className="flex flex-col gap-4">
      <SettingsSection title="OpenRouter API Keys (BYOK)">
        <SettingsCard>
          <div className="text-sm font-semibold mb-2">Configured API Keys</div>
          {keys.length === 0 ? (
            <div className="text-xs text-muted">No OpenRouter API keys configured yet.</div>
          ) : (
            <div className="flex flex-col gap-2">
              {keys.map((k) => (
                <div key={k.id} className="flex items-center justify-between p-2 border border-border rounded bg-bg">
                  <div>
                    <div className="text-xs font-semibold">{k.name}</div>
                    <div className="text-xs font-mono text-muted">{k.masked_key}</div>
                  </div>
                  <Clickable className="text-xs text-red-500 hover:underline" onClick={() => handleDeleteKey(k.id)}>
                    Delete
                  </Clickable>
                </div>
              ))}
            </div>
          )}

          <div className="border-t border-border mt-4 pt-3 flex flex-col gap-2">
            <div className="text-sm font-semibold">Add OpenRouter Key</div>
            <SettingsInput label="Key Name" value={newKeyName} onChange={setNewKeyName} placeholder="e.g. Production Key" />
            <SettingsInput label="API Key" type="text" value={newApiKey} onChange={setNewApiKey} placeholder="sk-or-v1-..." />

            {testResult && (
              <div className={`text-xs p-2 rounded ${testResult.success ? 'bg-green-500/10 text-green-600' : 'bg-red-500/10 text-red-600'}`}>
                {testResult.message}
              </div>
            )}

            <div className="flex items-center gap-2 mt-2">
              <Clickable
                className="px-3 py-1.5 text-xs font-semibold bg-bg-elevated border border-border rounded hover:bg-bg-hover"
                onClick={handleTestConnection}
                disabled={testing || !newApiKey}
              >
                {testing ? 'Testing...' : 'Test Connection'}
              </Clickable>
              <Clickable
                className="px-3 py-1.5 text-xs font-semibold bg-accent text-white rounded hover:opacity-90"
                onClick={handleAddKey}
                disabled={!newApiKey}
              >
                Save Key
              </Clickable>
            </div>
          </div>
        </SettingsCard>
      </SettingsSection>

      <SettingsSection title="Workspace Model Presets">
        <SettingsCard>
          <div className="text-sm font-semibold mb-2">Saved Presets</div>
          {presets.length === 0 ? (
            <div className="text-xs text-muted">No presets saved yet.</div>
          ) : (
            <div className="flex flex-col gap-2">
              {presets.map((p) => (
                <div key={p.id} className="flex items-center justify-between p-2 border border-border rounded bg-bg">
                  <div>
                    <div className="text-xs font-semibold">{p.name}</div>
                    <div className="text-xs font-mono text-muted">{p.model_name}</div>
                  </div>
                  <Clickable className="text-xs text-red-500 hover:underline" onClick={() => handleDeletePreset(p.id)}>
                    Delete
                  </Clickable>
                </div>
              ))}
            </div>
          )}

          <div className="border-t border-border mt-4 pt-3 flex flex-col gap-2">
            <div className="text-sm font-semibold">Create Model Preset</div>
            <SettingsInput label="Preset Name" value={presetName} onChange={setPresetName} placeholder="e.g. Fast Cron Model" />
            <SettingsSelect
              label="Assigned Key"
              value={presetKeyId}
              options={['', ...keys.map((k) => k.id)]}
              optionLabels={['Default / None', ...keys.map((k) => `${k.name} (${k.masked_key})`)]}
              onChange={setPresetKeyId}
            />
            <SettingsInput label="Model Name Identifier" value={presetModelName} onChange={setPresetModelName} placeholder="e.g. anthropic/claude-3.5-sonnet" />
            <div className="mt-2">
              <Clickable
                className="px-3 py-1.5 text-xs font-semibold bg-accent text-white rounded hover:opacity-90"
                onClick={handleAddPreset}
                disabled={!presetName || !presetModelName}
              >
                Save Preset
              </Clickable>
            </div>
          </div>
        </SettingsCard>
      </SettingsSection>
    </div>
  )
}
