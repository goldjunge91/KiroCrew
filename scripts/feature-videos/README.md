# feature-videos

Sign a release folder of feature-intro clips for the CDN, and check one before upload.

| File | Does |
|------|------|
| `publish.py` | Reads the folder you assembled, hashes every clip and poster where it is, signs the result, and writes `manifest.json` and `SHA256SUMS` into that folder. |
| `verify.py` | Re-checks a signed folder: every hash against the bytes on disk, and the signature against the committed release key. |
| `_manifest.py` | The shared schema, the canonical byte form both signing and verification hash, and the validation rules. |

Neither script copies, uploads, or reaches the network. `publish.py` prints the
`aws s3 sync` and CloudFront invalidation commands and stops, so the credentials
that can write to a public origin stay with the human running them.

## Layout

Two things, side by side:

```
catalog.json                      one entry per clip (kept OUTSIDE the folder)
dist/feature-videos/<release>/
    monitor-loops.mp4             <id>.mp4 for every entry
    monitor-loops.jpg             <id>.jpg for every entry
```

The catalog stays outside the release folder on purpose: `aws s3 sync` uploads the
whole folder, and the folder must hold nothing the manifest does not sign.
Publishing refuses a folder with any other file in it.

```json
{
  "entries": [
    {
      "id": "monitor-loops",
      "feature": "monitor-loops",
      "title": "Let one session watch a pull request",
      "description": "One or two plain sentences on what the feature does.",
      "doc": "monitor-loops.md",
      "used_when": ["sel_event_seen:monitor_start"],
      "min_version": "",
      "duration_s": 22.0
    }
  ]
}
```

`duration_s` is required. Nothing here inspects the media, so the catalog is the
only place the clip's length can come from, and the dashboard shows it. The
catalog fields themselves are described in
[feature-videos](../../src/kiro_crew/docs/feature-videos.md).

Clips should be H.264 video with AAC or no audio, the formats a browser `<video>`
plays everywhere the dashboard runs. The tool does not check this; the dashboard
plays what it is given.

## Sign

```bash
python3 scripts/feature-videos/publish.py \
  --catalog catalog.json \
  --cdn-host videos.example.com \
  --kms-key-arn "$RELEASE_SIGNING_KEY_ARN"
```

`--release` defaults to the version in `pyproject.toml`, and `--release-dir` to
`dist/feature-videos/<release>`. After it runs, the folder holds the media plus a
signed `manifest.json` and a `SHA256SUMS`, and nothing was moved.

A release folder is immutable. If the folder already holds a `manifest.json` or
`SHA256SUMS`, publishing refuses: that folder is a release someone may be serving.
Changing a clip means cutting a new release, not re-signing this one.

Signing reuses the CLI artifact manifest's trust root: the same offline key,
`RSASSA_PKCS1_V1_5_SHA_256`, and the same canonical JSON. One release carries one
trust root rather than two. Access to that key is one grant: a principal allowed
to sign feature videos can sign a CLI update manifest with the same key, so
video-signing access is the identical grant as CLI-release signing, never a
looser one.

Keys are separated by purpose. `--kms-key-arn` is the production path — the
private half is a non-exportable AWS KMS key held by the release workflow, so it
exists on no disk — and the tool checks that key's public half against the
committed one before it signs. The manifest then records `key_id` as a hint about
which pinned key was used. `--signing-key <path>` signs with a local key for
staging and tests, omits `key_id`, and warns that the folder is not a release.
The dashboard verifies against the pinned key either way, so a staging folder
stays a staging folder wherever it is uploaded.

`signature` is base64 at the manifest's top level and covers canonical JSON of
every other top-level field, nested values included. Editing one byte of the
manifest breaks it. That canonical rule is copied into `_manifest.py` rather than
imported, so the tool runs from a bare checkout and never executes runtime code;
`test/test_feature_videos_publish.py` pins the copy by signing with it and
verifying with the runtime's own verifier.

What publishing refuses, each with its reason on stderr:

| Refused | Why |
|---------|-----|
| An `id` that is not a lowercase hyphenated slug | The id becomes the asset basename and the display-state key. |
| A missing `<id>.mp4` or `<id>.jpg` | A release folder with a hole in it is not publishable. |
| A file the catalog does not name | It would be uploaded and served under a signature that never covered it. |
| A folder already holding `manifest.json` or `SHA256SUMS` | A release is never re-signed. If an earlier run was interrupted and nothing was uploaded, delete those two files and run again; the media is never touched. |
| A `doc` outside `src/kiro_crew/tips_allowlist.py` | The allowlist tips use, so a clip cannot point at an internal design note. |
| A clip or poster that is a symlink, a pipe or anything but a regular file | The bytes that get hashed must be the bytes on disk. |
| An empty file, or one over its cap (`--max-clip-bytes`, `--max-poster-bytes`) | Every dashboard that has not seen a clip fetches it once, and a poster is fetched before the clip. |
| A release that is not one to four numeric components, or an `id` over 92 characters | The dashboard's own grammar: it refuses a manifest whose release it cannot parse, and drops an entry whose `<id>.mp4` is over its 96-character basename bound. |
| A cap flag raised past the dashboard's hard limit | The release would sign cleanly here and be refused on every dashboard. |
| A missing `duration_s`, or one that is not a finite number | The dashboard shows the length; nothing else supplies it. |

## Verify

```bash
python3 scripts/feature-videos/verify.py dist/feature-videos/0.7.0
```

This recomputes every hash from the bytes on disk, verifies the signature against
the committed release key, and refuses a folder carrying a file nobody signed. It
only reads. Run it before every upload.

Pass `--public-key <pem>` to check a folder signed with a staging key.

## Upload

Publishing prints the commands and stops. Run them yourself:

```bash
aws s3 sync --dryrun dist/feature-videos/0.7.0/ s3://BUCKET/feature-videos/0.7.0/
aws s3 sync dist/feature-videos/0.7.0/ s3://BUCKET/feature-videos/0.7.0/
aws cloudfront create-invalidation --distribution-id DISTRIBUTION \
  --paths '/feature-videos/0.7.0/*'
```

The tool enforces immutability on the folder it signs; the bucket side is yours.
`aws s3 sync` overwrites objects freely, so put the guard where the bytes live: a
bucket policy that denies `s3:PutObject` on an existing key, or S3 Object Lock on
the `feature-videos/` prefix. The invalidation is for `manifest.json`, the one
file a dashboard re-reads.

## Size caps

These are PUBLISHER limits, and every one is deliberately stricter than what the
dashboard accepts. A release that only just fits today's consumer has no headroom
for a consumer that tightens. Publishing refuses and exits non-zero on any of
them, and writes nothing, so the failure lands while a person is watching.

| Cap | Publisher default | Runtime accepts | Flag |
|-----|-------------------|-----------------|------|
| Signed payload — the canonical JSON the signature covers, compact | 65536 (64 KiB) | 262144 (256 KiB) | `--max-payload-bytes` |
| Manifest document — `manifest.json` as published, indented so larger than the payload | 262144 (256 KiB) | 1048576 (1 MiB) | `--max-document-bytes` |
| Entry count | 500 | 1000 | `--max-entries` |
| One clip | 26214400 (25 MB) | 67108864 (64 MiB) | `--max-clip-bytes` |
| One poster | 4194304 (4 MiB) | 8388608 (8 MiB) | `--max-poster-bytes` |

The runtime column is `_SIGNED_PAYLOAD_MAX_BYTES`, `_MANIFEST_MAX_BYTES`,
`_MAX_ENTRIES` and `_MAX_ENTRY_BYTES` in `src/kiro_crew/feature_videos_manifest.py`
and `MAX_POSTER_BYTES` in `src/kiro_crew/feature_videos_cache.py`. The tool
restates them as `RUNTIME_LIMITS`, a test pins each restated number to the source,
and no flag may be raised past its runtime number: a release over any of them is
refused whole or has its media dropped by every dashboard, so publishing refuses
it first. Raise a flag when a release genuinely needs the headroom, up to that
limit. `verify.py` takes the same flags, so a folder can be re-checked against a
different ceiling without republishing.
