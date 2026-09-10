#!/usr/bin/env python3
"""Sign a feature-videos release folder for the CDN, in place.

The operator assembles the folder: ``dist/feature-videos/<release>/`` holding an
``<id>.mp4`` and an ``<id>.jpg`` for every entry in a ``catalog.json`` kept
beside it. This tool reads that folder, checks every file against the catalog,
hashes it, signs the result and writes ``manifest.json`` and ``SHA256SUMS`` into
the same folder. It copies nothing and it never uploads: it prints the exact
``aws s3 sync --dryrun`` and CloudFront invalidation commands and stops, so the
credentials that can write to a public origin stay with the human who owns them.

Signing reuses the CLI artifact manifest's trust root: same key, same
``RSASSA_PKCS1_V1_5_SHA_256``, same canonical-JSON bytes. Keys are separated by
purpose. Production signs with ``--kms-key-arn``, where the private half is a
non-exportable AWS KMS key that no human can read and the manifest records
``key_id`` as the hint that it was used. ``--signing-key`` takes a local private
key for staging and tests, omits ``key_id``, and says out loud that the result
is not a production artifact. Either way openssl verifies the signature before
anything reaches disk, so an unverifiable folder is never produced.

Usage:

    python3 scripts/feature-videos/publish.py \\
        --catalog catalog.json --cdn-host videos.example.com --kms-key-arn <arn>
"""

from __future__ import annotations

import argparse
import ast
import base64
import hashlib
import hmac
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _manifest import (  # noqa: E402
    ALGORITHM,
    DEFAULT_MAX_CLIP_BYTES,
    DEFAULT_MAX_DOCUMENT_BYTES,
    DEFAULT_MAX_ENTRIES,
    DEFAULT_MAX_PAYLOAD_BYTES,
    DEFAULT_MAX_POSTER_BYTES,
    PUBLIC_KEY_PATH,
    SCHEMA,
    ManifestError,
    canonical_bytes,
    check_cap,
    check_document_size,
    check_entry_count,
    check_signable,
    hash_regular_file,
    key_id_of,
    load_json_object,
    parse_generated_at,
    public_key_der,
    require_text,
    run_openssl,
    validate_cdn_base,
    validate_doc,
    validate_duration,
    validate_release,
    validate_slug,
    verify_signature,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: The runtime's tips doc allowlist, as a source file. Read as text, never
#: imported: a publishing tool must run from a bare checkout and must not
#: execute runtime code. Parsing the single source of truth is what keeps this
#: from becoming a second copy of the list that silently drifts, and a test
#: asserts the parse equals what the runtime exposes.
_ALLOWLIST_SOURCE = _REPO_ROOT / "src" / "kiro_crew" / "tips_allowlist.py"
_ALLOWLIST_NAME = "TIP_DOC_ALLOWLIST"

#: Ceiling on ``catalog.json`` itself. A release ships a handful of clips, so a
#: file past this is a mistake rather than a large catalog.
_MAX_CATALOG_BYTES = 256 * 1024

#: A floor is a bare release, matching what the runtime's version compare reads.
_MIN_VERSION_RE = re.compile(r"[0-9]+(?:\.[0-9]+)*\Z")

#: The two files this tool writes. Their presence means the folder is already a
#: release, and a release is never re-signed.
GENERATED_FILES = ("manifest.json", "SHA256SUMS")


def _warn(message: str) -> None:
    print(f"publish: warning: {message}", file=sys.stderr)


def read_tip_doc_allowlist() -> frozenset[str]:
    """The runtime's allowed tip docs, parsed out of its source.

    Finds the module-level ``TIP_DOC_ALLOWLIST`` assignment and evaluates only
    its literal set. A shape this cannot read is an error rather than an empty
    allowlist: an empty one would refuse every doc, and silently permitting
    everything is the failure this gate exists to prevent.
    """
    tree = ast.parse(_ALLOWLIST_SOURCE.read_text(encoding="utf-8"))
    for node in tree.body:
        targets = (
            node.targets
            if isinstance(node, ast.Assign)
            else [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        names = {t.id for t in targets if isinstance(t, ast.Name)}
        if _ALLOWLIST_NAME not in names or getattr(node, "value", None) is None:
            continue
        value = node.value
        # frozenset({...}) — take the call's single literal argument.
        if isinstance(value, ast.Call) and isinstance(value.func, ast.Name):
            if value.func.id != "frozenset" or len(value.args) != 1:
                break
            value = value.args[0]
        try:
            literal = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            break
        if not isinstance(literal, (set, frozenset, list, tuple)) or not literal:
            break
        if not all(isinstance(item, str) for item in literal):
            break
        return frozenset(literal)
    raise ManifestError(
        f"could not read {_ALLOWLIST_NAME} from {_ALLOWLIST_SOURCE}; "
        "the allowlist's shape changed and this parser needs updating"
    )


def _repo_version() -> str:
    """The version in ``pyproject.toml``, used when ``--release`` is omitted."""
    text = (_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    if match is None:
        raise ManifestError("could not read version from pyproject.toml; pass --release")
    return match.group(1)


def _validate_catalog_entry(
    raw: Any, index: int, seen: set[str], allowlist: frozenset[str]
) -> dict[str, Any]:
    where = f"catalog entry {index}"
    if not isinstance(raw, dict):
        raise ManifestError(f"{where}: must be a JSON object")
    unknown = set(raw) - {
        "id",
        "feature",
        "title",
        "description",
        "doc",
        "used_when",
        "min_version",
        "duration_s",
    }
    if unknown:
        raise ManifestError(f"{where}: unknown field(s): {', '.join(sorted(unknown))}")

    entry_id = validate_slug(require_text(raw, "id", where=where), where=where)
    if entry_id in seen:
        raise ManifestError(f"{where}: duplicate id {entry_id!r}")
    seen.add(entry_id)

    entry: dict[str, Any] = {
        "id": entry_id,
        "feature": require_text(raw, "feature", where=where),
        "title": require_text(raw, "title", where=where),
        "description": require_text(raw, "description", where=where),
        "doc": validate_doc(
            require_text(raw, "doc", where=where), where=where, allowlist=allowlist
        ),
    }

    used_when = raw.get("used_when", [])
    if not isinstance(used_when, list) or not all(
        isinstance(signal, str) and signal and len(signal) <= 200 for signal in used_when
    ):
        raise ManifestError(f"{where}: used_when must be a list of non-empty strings")
    # Signal NAMES are not checked against the runtime's probe registry: doing
    # so would mean executing runtime code from a publishing tool, and a second
    # copy of the registry here would drift from its answer. The runtime treats
    # an unregistered signal as "feature not used" and logs it, so a typo shows
    # the clip rather than hiding it.
    entry["used_when"] = list(used_when)

    min_version = raw.get("min_version", "")
    if not isinstance(min_version, str):
        raise ManifestError(f"{where}: min_version must be a string")
    if min_version and _MIN_VERSION_RE.fullmatch(min_version) is None:
        raise ManifestError(f"{where}: min_version must be a bare release like 0.7.0")
    entry["min_version"] = min_version

    # Required: nothing here inspects the media, so the catalog is the only
    # source of a duration, and the runtime shows a clip's length from it.
    if "duration_s" not in raw:
        raise ManifestError(f"{where}: duration_s is required")
    entry["duration_s"] = round(validate_duration(raw["duration_s"], where=where), 3)
    return entry


def load_catalog(path: Path) -> list[dict[str, Any]]:
    document = load_json_object(path, limit=_MAX_CATALOG_BYTES)
    entries = document.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ManifestError(f"{path.name} must carry a non-empty 'entries' array")
    allowlist = read_tip_doc_allowlist()
    seen: set[str] = set()
    return [
        _validate_catalog_entry(raw, index, seen, allowlist) for index, raw in enumerate(entries)
    ]


def check_release_dir(release_dir: Path, catalog: list[dict[str, Any]]) -> None:
    """Refuse a folder that is not exactly the catalog's media, and nothing else.

    Three rules, all cheap. The folder must be a real directory. It must not
    already hold a generated file: a release is immutable, so a folder with a
    ``manifest.json`` in it is a release someone may already be serving, and
    changing a clip means cutting a new release rather than re-signing this one.
    And it must hold no file the catalog does not name: ``aws s3 sync`` uploads
    the whole tree, so a stray file here would be served from the release prefix
    under a signature that never covered it. Refusing here is what lets the
    operator fix the folder before anything is signed.
    """
    if release_dir.is_symlink() or not release_dir.is_dir():
        raise ManifestError(f"release folder is not a directory: {release_dir}")
    expected = {f"{entry['id']}.mp4" for entry in catalog} | {
        f"{entry['id']}.jpg" for entry in catalog
    }
    present = {item.name for item in release_dir.iterdir()}
    generated = sorted(present & set(GENERATED_FILES))
    if generated:
        raise ManifestError(
            f"{release_dir} already holds {', '.join(generated)}: it is a release, and a "
            "release is never re-signed. Cut a new release. If an earlier run of this tool was "
            "interrupted and nothing was uploaded, delete manifest.json and SHA256SUMS and run "
            "it again."
        )
    stray = sorted(present - expected)
    if stray:
        raise ManifestError(
            f"{release_dir} holds file(s) the catalog does not name: {', '.join(stray[:5])}"
            f"{' and more' if len(stray) > 5 else ''}. A release folder carries only the "
            "media the manifest signs."
        )
    missing = sorted(expected - present)
    if missing:
        raise ManifestError(f"{release_dir} is missing: {', '.join(missing[:5])}")


def build_entries(
    catalog: list[dict[str, Any]],
    release_dir: Path,
    *,
    max_clip_bytes: int,
    max_poster_bytes: int,
) -> list[dict[str, Any]]:
    """Hash every entry's media in place and return the manifest entries."""
    built: list[dict[str, Any]] = []
    for entry in catalog:
        clip_name = f"{entry['id']}.mp4"
        poster_name = f"{entry['id']}.jpg"
        clip_sha, clip_size = hash_regular_file(release_dir / clip_name, where="release folder")
        poster_sha, poster_size = hash_regular_file(
            release_dir / poster_name, where="release folder"
        )
        # Separate caps: the runtime bounds a clip and a poster differently, and
        # one shared number would have to be the smaller of the two.
        for name, size, cap in (
            (clip_name, clip_size, max_clip_bytes),
            (poster_name, poster_size, max_poster_bytes),
        ):
            if size == 0:
                raise ManifestError(f"{name} is empty")
            if size > cap:
                raise ManifestError(f"{name} is {size} bytes, over the {cap} byte cap")
        built.append(
            {
                "id": entry["id"],
                "feature": entry["feature"],
                "title": entry["title"],
                "description": entry["description"],
                "file": clip_name,
                "poster": poster_name,
                "sha256": clip_sha,
                "poster_sha256": poster_sha,
                "bytes": clip_size,
                "duration_s": entry["duration_s"],
                "doc": entry["doc"],
                "used_when": entry["used_when"],
                "min_version": entry["min_version"],
            }
        )
    return built


def _run_aws_json(args: list[str]) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["aws", *args, "--output", "json", "--no-cli-pager"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ManifestError("the AWS CLI is required for KMS signing") from exc
    if proc.returncode != 0 or len(proc.stdout) > 64 * 1024:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        raise ManifestError(f"AWS KMS rejected the request: {detail or 'no detail'}")
    try:
        value = json.loads(proc.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestError("AWS KMS returned a malformed response") from exc
    if not isinstance(value, dict):
        raise ManifestError("AWS KMS returned a malformed response")
    return value


def _sign_with_kms(payload: bytes, key_arn: str) -> bytes:
    """Sign *payload* with the release KMS key, pinned to the committed pubkey.

    The KMS key's public half must byte-match the one in the repository. Without
    that check a mistyped ARN would sign with some other key and produce a folder
    every dashboard silently refuses.
    """
    public_response = _run_aws_json(["kms", "get-public-key", "--key-id", key_arn])
    if public_response.get("KeyUsage") != "SIGN_VERIFY":
        raise ManifestError("release KMS key must have SIGN_VERIFY usage")
    if public_response.get("KeySpec") not in {"RSA_3072", "RSA_4096"}:
        raise ManifestError("release KMS key must be RSA_3072 or RSA_4096")
    algorithms = public_response.get("SigningAlgorithms")
    if not isinstance(algorithms, list) or ALGORITHM not in algorithms:
        raise ManifestError("release KMS key does not allow the required algorithm")
    encoded = public_response.get("PublicKey")
    if not isinstance(encoded, str):
        raise ManifestError("AWS KMS did not return a public key")
    try:
        kms_der = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ManifestError("AWS KMS returned an invalid public key") from exc
    if not hmac.compare_digest(kms_der, public_key_der(PUBLIC_KEY_PATH)):
        raise ManifestError("configured KMS key does not match the committed public key")

    digest = hashlib.sha256(payload).digest()
    sign_response = _run_aws_json(
        [
            "kms",
            "sign",
            "--key-id",
            key_arn,
            "--message",
            base64.b64encode(digest).decode("ascii"),
            "--message-type",
            "DIGEST",
            "--signing-algorithm",
            ALGORITHM,
            "--cli-binary-format",
            "base64",
        ]
    )
    encoded_signature = sign_response.get("Signature")
    if not isinstance(encoded_signature, str):
        raise ManifestError("AWS KMS did not return a signature")
    try:
        signature = base64.b64decode(encoded_signature, validate=True)
    except ValueError as exc:
        raise ManifestError("AWS KMS returned an invalid signature") from exc
    return signature


def _sign_with_key(payload: bytes, private_key: Path, scratch: Path) -> bytes:
    """Sign *payload* with a local private key, for staging and tests."""
    if not private_key.is_file():
        raise ManifestError(f"signing key is missing: {private_key}")
    payload_path = scratch / "payload.json"
    payload_path.write_bytes(payload)
    return run_openssl(["dgst", "-sha256", "-sign", str(private_key), str(payload_path)])


def sign_document(
    document: dict[str, Any],
    *,
    scratch: Path,
    signing_key: Path | None,
    kms_key_arn: str | None,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
) -> dict[str, Any]:
    """Return *document* with ``key_id`` where applicable and a verified signature.

    Verification is not a courtesy: the signature is checked against the public
    half before the manifest is assembled, so a folder that reaches disk is one
    whose signature openssl already accepted over exactly these bytes.
    """
    if signing_key is not None:
        public_key = scratch / "public.pem"
        run_openssl(["pkey", "-in", str(signing_key), "-pubout", "-out", str(public_key)])
        # key_id is omitted for a local key. It is a hint about WHICH pinned key
        # signed, and a staging key is not one — claiming an id the runtime does
        # not pin would be a false hint, and claiming the pinned one would be a lie.
        signed = dict(document)
    else:
        public_key = PUBLIC_KEY_PATH
        signed = {**document, "key_id": key_id_of(PUBLIC_KEY_PATH)}

    payload = check_signable(signed, max_payload_bytes=max_payload_bytes)
    if signing_key is not None:
        signature = _sign_with_key(payload, signing_key, scratch)
    elif kms_key_arn:
        signature = _sign_with_kms(payload, kms_key_arn)
    else:  # pragma: no cover - argparse requires one of the two
        raise ManifestError("no signing method given")
    if not signature:
        raise ManifestError("signing produced no signature")

    manifest = {**signed, "signature": base64.b64encode(signature).decode("ascii")}
    verify_signature(manifest, public_key=public_key, max_payload_bytes=max_payload_bytes)
    return manifest


def _write_new_file(path: Path, data: bytes) -> None:
    """Create *path* and write *data*, refusing to write through anything existing.

    Exclusive creation is the point. The folder was checked to hold no generated
    file, and ``"x"`` keeps that true even if one appears in the gap: a plain
    write would follow a symlink planted at the name and overwrite its target.
    """
    try:
        handle = open(path, "xb")
    except FileExistsError as exc:
        raise ManifestError(f"{path.name} already exists in the release folder") from exc
    try:
        with handle:
            handle.write(data)
    except BaseException:
        # The name became this run's the moment the exclusive open succeeded, and
        # a file holding part of the data (disk full, interrupt) is worse than
        # none: the next run would refuse the folder as an existing release.
        _unlink_quietly(path)
        raise


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def write_generated_files(
    release_dir: Path,
    manifest: dict[str, Any],
    entries: list[dict[str, Any]],
    *,
    max_document_bytes: int = DEFAULT_MAX_DOCUMENT_BYTES,
) -> None:
    """Write ``manifest.json`` and ``SHA256SUMS`` beside the media they describe.

    Both or neither. A failure after the first file landed removes it before
    re-raising, so the folder is left as the operator assembled it rather than
    as a half-release the next run refuses. Only the two generated names are
    ever created or removed here; the media is never written.
    """
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    check_document_size(manifest_bytes, max_document_bytes=max_document_bytes)

    # manifest.json is listed too: SHA256SUMS is what an operator checks the
    # uploaded folder against, and a manifest missing from it would be the one
    # file the check could not see.
    lines = []
    for entry in entries:
        lines.append(f"{entry['sha256']}  {entry['file']}")
        lines.append(f"{entry['poster_sha256']}  {entry['poster']}")
    lines.append(f"{hashlib.sha256(manifest_bytes).hexdigest()}  manifest.json")
    sums_bytes = ("\n".join(sorted(lines)) + "\n").encode("utf-8")

    created: list[Path] = []
    try:
        for name, data in (("manifest.json", manifest_bytes), ("SHA256SUMS", sums_bytes)):
            path = release_dir / name
            _write_new_file(path, data)
            created.append(path)
    except BaseException:
        for path in created:
            _unlink_quietly(path)
        raise


def _print_upload_plan(
    release_dir: Path, release: str, bucket: str | None, distribution_id: str | None
) -> None:
    prefix = f"feature-videos/{release}/"
    target = f"s3://{bucket or '<BUCKET>'}/{prefix}"
    print()
    print("Nothing was uploaded. Run these yourself, in this order:")
    print()
    print(f"  aws s3 sync --dryrun {release_dir}/ {target}")
    print(f"  aws s3 sync {release_dir}/ {target}")
    print(
        f"  aws cloudfront create-invalidation --distribution-id "
        f"{distribution_id or '<DISTRIBUTION_ID>'} --paths '/{prefix}*'"
    )
    print()
    print(f"Verify the folder first: python3 scripts/feature-videos/verify.py {release_dir}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sign a feature-videos release folder in place.")
    parser.add_argument(
        "--catalog",
        type=Path,
        required=True,
        help="catalog.json describing the entries; kept OUTSIDE the release folder",
    )
    parser.add_argument(
        "--release-dir",
        type=Path,
        help=(
            "the folder holding <id>.mp4 and <id>.jpg per entry; manifest.json and "
            "SHA256SUMS are written into it (default dist/feature-videos/<release>)"
        ),
    )
    parser.add_argument(
        "--cdn-host", required=True, help="CDN host serving the release, e.g. videos.example.com"
    )
    parser.add_argument("--release", help="release version; defaults to pyproject.toml's version")
    parser.add_argument(
        "--max-clip-bytes",
        type=int,
        default=DEFAULT_MAX_CLIP_BYTES,
        help=f"clip size cap in bytes (default {DEFAULT_MAX_CLIP_BYTES})",
    )
    parser.add_argument(
        "--max-poster-bytes",
        type=int,
        default=DEFAULT_MAX_POSTER_BYTES,
        help=f"poster size cap in bytes (default {DEFAULT_MAX_POSTER_BYTES})",
    )
    parser.add_argument(
        "--max-payload-bytes",
        type=int,
        default=DEFAULT_MAX_PAYLOAD_BYTES,
        help=(
            "publishing cap on the canonical signed payload; stricter than the "
            f"runtime's own limit (default {DEFAULT_MAX_PAYLOAD_BYTES})"
        ),
    )
    parser.add_argument(
        "--max-document-bytes",
        type=int,
        default=DEFAULT_MAX_DOCUMENT_BYTES,
        help=(
            "publishing cap on manifest.json as fetched; stricter than the "
            f"runtime's own limit (default {DEFAULT_MAX_DOCUMENT_BYTES})"
        ),
    )
    parser.add_argument(
        "--max-entries",
        type=int,
        default=DEFAULT_MAX_ENTRIES,
        help=(
            "publishing cap on entry count; stricter than the runtime's own "
            f"limit (default {DEFAULT_MAX_ENTRIES})"
        ),
    )
    parser.add_argument("--s3-bucket", help="bucket name, used only to print the upload command")
    parser.add_argument(
        "--distribution-id", help="CloudFront id, used only to print the invalidation command"
    )
    signing = parser.add_mutually_exclusive_group(required=True)
    signing.add_argument(
        "--kms-key-arn", help="release KMS key ARN; the production path, key never leaves KMS"
    )
    signing.add_argument(
        "--signing-key",
        type=Path,
        help="local RSA private key; for staging and tests, not for a public release",
    )
    args = parser.parse_args(argv)

    # Every cap may be raised, none past the runtime's own limit: a release that
    # needs more than a dashboard accepts must not leave here signed.
    for name in (
        "max_clip_bytes",
        "max_poster_bytes",
        "max_payload_bytes",
        "max_document_bytes",
        "max_entries",
    ):
        check_cap(name, getattr(args, name))

    release = validate_release(args.release or _repo_version())
    # The base names the CDN's feature-videos ROOT, not this release's folder:
    # the runtime builds every asset URL as ``<cdn_base>/<release>/<name>``
    # (``VideoManifest.asset_url``), so a base already carrying the release would
    # double it in every URL and no clip would load.
    cdn_base = validate_cdn_base(f"https://{args.cdn_host}/feature-videos/")
    generated_at = parse_generated_at(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    release_dir = args.release_dir or (_REPO_ROOT / "dist" / "feature-videos" / release)

    catalog = load_catalog(args.catalog)
    check_entry_count(len(catalog), max_entries=args.max_entries)
    check_release_dir(release_dir, catalog)
    entries = build_entries(
        catalog,
        release_dir,
        max_clip_bytes=args.max_clip_bytes,
        max_poster_bytes=args.max_poster_bytes,
    )

    document: dict[str, Any] = {
        "schema": SCHEMA,
        "release": release,
        "cdn_base": cdn_base,
        "generated_at": generated_at,
        "entries": entries,
    }
    with tempfile.TemporaryDirectory(prefix="feature-videos-sign-") as scratch:
        manifest = sign_document(
            document,
            scratch=Path(scratch),
            signing_key=args.signing_key,
            kms_key_arn=args.kms_key_arn,
            max_payload_bytes=args.max_payload_bytes,
        )
    write_generated_files(
        release_dir, manifest, entries, max_document_bytes=args.max_document_bytes
    )

    payload_bytes = len(canonical_bytes({k: v for k, v in manifest.items() if k != "signature"}))
    print(f"signed {release_dir}")
    print(f"  {len(entries)} entry/entries, signed payload {payload_bytes} bytes")
    if args.signing_key is not None:
        _warn(
            "signed with a local key and no key_id: this is a staging artifact. "
            "A production release is signed with --kms-key-arn."
        )
    else:
        print(f"  key_id {manifest['key_id']}")
    _print_upload_plan(release_dir, release, args.s3_bucket, args.distribution_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ManifestError as exc:
        print(f"publish: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
