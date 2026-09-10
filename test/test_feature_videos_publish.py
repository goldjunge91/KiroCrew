"""The feature-videos publishing tool must produce a folder the runtime trusts.

Three properties carry this suite:

* **The manifest describes the bytes.** Every hash and size in ``manifest.json``
  is recomputed from the files on disk, so a manifest that agrees with itself but
  not with its media is a failure, not a pass.
* **Tampering is detected.** Each way a published folder can be altered — a
  media byte, a manifest field, a signature from another key, an unsigned extra
  file — is exercised and must be rejected.
* **The tool's copy of the rules matches the runtime's.** The tool owns its own
  canonical-JSON encoder and its own copy of nothing else: the doc allowlist is
  parsed from the runtime's source, and a signature the tool produces is fed to
  the runtime's own verifier. Those two cross-checks are what make a local copy
  safe, so a drift fails here rather than on a CDN.

The tool works in place: the operator assembles a release folder holding an
``<id>.mp4`` and an ``<id>.jpg`` per catalog entry, and the tool hashes them
where they sit and writes ``manifest.json`` and ``SHA256SUMS`` beside them. So
the fixtures here are a folder plus a ``catalog.json`` kept outside it, and the
signed folder is the same folder the fixture built.

The clip fixture is the recorded placeholder already in the tree rather than a
synthesized container: a hand-built MP4 would hash and size fine while being
something no browser plays, which is the one thing a publishing tool must not
ship.
"""

from __future__ import annotations

import ast
import base64
import errno
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import feature_video_fixture as fixture
import pytest

from kiro_crew import feature_videos_manifest as fvm
from kiro_crew.platform import feed_trust
from kiro_crew.tips_allowlist import TIP_DOC_ALLOWLIST

ROOT = Path(__file__).resolve().parents[1]
TOOL_DIR = ROOT / "scripts" / "feature-videos"
PLACEHOLDER_CLIP = ROOT / "website" / "capture" / "assets" / "placeholder.mp4"
PLACEHOLDER_POSTER = ROOT / "website" / "capture" / "assets" / "placeholder.jpg"

#: A doc that is really in the tips allowlist, so the allowlist gate passes for
#: the happy path and a made-up name can test the refusal.
ALLOWED_DOC = "monitor-loops.md"


def _load(name: str) -> Any:
    """Import one of the tool's modules by path.

    The tool lives under ``scripts/`` and is not an importable package, which is
    deliberate — it must run from a checkout with no install.
    """
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, TOOL_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


manifest_mod = _load("_manifest")
publish_mod = _load("publish")
verify_mod = _load("verify")
ManifestError = manifest_mod.ManifestError


#: The consumer module whose limits this tool must stay under. Its numbers are
#: read as SOURCE rather than imported so the comparison sees the literals the
#: runtime declares, with nothing computed in between.
CONSUMER_SOURCE = ROOT / "src" / "kiro_crew" / "feature_videos_manifest.py"
CACHE_SOURCE = ROOT / "src" / "kiro_crew" / "feature_videos_cache.py"


def _int_literal(node: ast.expr) -> int | None:
    """Evaluate an int constant or a product/sum of them, e.g. ``64 * 1024``.

    Deliberately narrow: the consumer writes its limits as plain arithmetic, and
    anything else should read as "cannot determine" rather than be executed.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Mult, ast.Add, ast.Sub)):
        left = _int_literal(node.left)
        right = _int_literal(node.right)
        if left is None or right is None:
            return None
        if isinstance(node.op, ast.Mult):
            return left * right
        return left + right if isinstance(node.op, ast.Add) else left - right
    return None


def _module_assignments(source: Path) -> dict[str, ast.expr]:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    out: dict[str, ast.expr] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                out[target.id] = node.value
    return out


def _consumer_limits() -> dict[str, int]:
    """The runtime's module-level integer limits, read from its source.

    Two modules: the manifest parser bounds the document and each clip, the
    cache bounds a poster transfer.
    """
    limits: dict[str, int] = {}
    for source in (CONSUMER_SOURCE, CACHE_SOURCE):
        for name, value in _module_assignments(source).items():
            literal = _int_literal(value)
            if literal is not None:
                limits[name] = literal
    return limits


def _consumer_pattern(name: str) -> str:
    """The pattern string of a ``re.compile(r"...")`` assignment in the consumer."""
    value = _module_assignments(CONSUMER_SOURCE)[name]
    assert isinstance(value, ast.Call) and value.args, f"{name} is not a re.compile(...) call"
    pattern = value.args[0]
    assert isinstance(pattern, ast.Constant) and isinstance(pattern.value, str)
    return pattern.value


def _load_cli_manifest_signer() -> Any:
    """The CLI feed's signer, loaded by path — its filename is not importable."""
    path = ROOT / "packaging" / "signing" / "cli-manifest.py"
    if not path.is_file():  # pragma: no cover - present in every checkout
        pytest.skip(f"{path} is missing")
    spec = importlib.util.spec_from_file_location("cli_manifest_signer", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def key_pair(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A throwaway RSA-3072 pair. Module-scoped: keygen is the slow part.

    3072 bits rather than 2048 because the tool refuses a weaker release key,
    the same floor the CLI manifest signer enforces. A development key, never
    the production one — the production private half lives in KMS and cannot be
    read by anyone.
    """
    return fixture.mint_throwaway_key(tmp_path_factory.mktemp("feature-videos-key"))


@pytest.fixture(scope="module")
def second_key(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A second RSA-3072 private key, for forging a signature by the wrong key."""
    private, _public = fixture.mint_throwaway_key(
        tmp_path_factory.mktemp("feature-videos-second-key")
    )
    return private


def _entry(**overrides: Any) -> dict[str, Any]:
    entry = {
        "id": "monitor-loops",
        "feature": "monitor-loops",
        "title": "Let one session watch a pull request",
        "description": "A monitor loop re-injects your check instructions on an interval.",
        "doc": ALLOWED_DOC,
        "used_when": ["sel_event_seen:monitor_start"],
        "min_version": "",
        "duration_s": 22.0,
    }
    entry.update(overrides)
    return entry


@pytest.fixture()
def release_dir(tmp_path: Path) -> Path:
    """A release folder holding one real clip and its poster, ready to be signed."""
    folder = tmp_path / "release"
    folder.mkdir()
    shutil.copyfile(PLACEHOLDER_CLIP, folder / "monitor-loops.mp4")
    shutil.copyfile(PLACEHOLDER_POSTER, folder / "monitor-loops.jpg")
    return folder


@pytest.fixture()
def catalog(tmp_path: Path) -> Path:
    """The catalog describing that folder, kept outside it.

    Outside because the tool refuses a release folder carrying any file the
    catalog does not name, and the catalog is not one of them.
    """
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"entries": [_entry()]}, indent=2) + "\n", encoding="utf-8")
    return path


def _rewrite_catalog(catalog_path: Path, entry: dict[str, Any]) -> None:
    catalog_path.write_text(json.dumps({"entries": [entry]}, indent=2) + "\n", encoding="utf-8")


def _publish(
    release_dir: Path, catalog: Path, private_key: Path, *extra: str, release: str = "0.7.0"
) -> Path:
    exit_code = publish_mod.main(
        [
            "--catalog",
            str(catalog),
            "--release-dir",
            str(release_dir),
            "--cdn-host",
            "videos.example.com",
            "--release",
            release,
            "--signing-key",
            str(private_key),
            *extra,
        ]
    )
    assert exit_code == 0
    return release_dir


def _read(folder: Path) -> dict[str, Any]:
    return json.loads((folder / "manifest.json").read_text(encoding="utf-8"))


def _rewrite(folder: Path, manifest: dict[str, Any]) -> None:
    (folder / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _sign_bytes(payload: bytes, private_key: Path, tmp_path: Path) -> str:
    path = tmp_path / "payload-to-sign.json"
    path.write_bytes(payload)
    signature = subprocess.run(
        [fixture.openssl_or_skip(), "dgst", "-sha256", "-sign", str(private_key), str(path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    ).stdout
    return base64.b64encode(signature).decode("ascii")


class TestProducedFolder:
    def test_manifest_has_the_contract_shape(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        private, _ = key_pair
        manifest = _read(_publish(release_dir, catalog, private))

        # key_id is absent for a local key: it is a hint about which PINNED key
        # signed, and a staging key is not one.
        assert set(manifest) == {
            "schema",
            "release",
            "cdn_base",
            "generated_at",
            "entries",
            "signature",
        }
        assert manifest["schema"] == "kirocrew-feature-videos-manifest-v1"
        assert manifest["release"] == "0.7.0"
        assert manifest["cdn_base"] == "https://videos.example.com/feature-videos/"
        assert manifest["generated_at"].endswith("Z")
        assert isinstance(manifest["signature"], str) and manifest["signature"]
        assert set(manifest["entries"][0]) == {
            "id",
            "feature",
            "title",
            "description",
            "file",
            "poster",
            "sha256",
            "poster_sha256",
            "bytes",
            "duration_s",
            "doc",
            "used_when",
            "min_version",
        }
        entry = manifest["entries"][0]
        assert entry["file"] == "monitor-loops.mp4"
        assert entry["poster"] == "monitor-loops.jpg"
        assert entry["used_when"] == ["sel_event_seen:monitor_start"]
        assert entry["min_version"] == ""
        assert entry["duration_s"] == 22.0

    def test_folder_carries_the_media_and_a_complete_sha256sums(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """The signed folder is exactly the media plus the two generated files."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        names = {path.name for path in folder.iterdir()}
        assert names == {"monitor-loops.mp4", "monitor-loops.jpg", "manifest.json", "SHA256SUMS"}

        listed = {
            line.split("  ", 1)[1]: line.split("  ", 1)[0]
            for line in (folder / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
        }
        assert set(listed) == {"monitor-loops.mp4", "monitor-loops.jpg", "manifest.json"}
        for name, digest in listed.items():
            assert digest == hashlib.sha256((folder / name).read_bytes()).hexdigest()

        assert verify_mod.verify_folder(folder, public_key=public)["entries"] == 1

    def test_hashes_and_size_describe_the_real_bytes(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        folder = _publish(release_dir, catalog, key_pair[0])
        entry = _read(folder)["entries"][0]
        clip = folder / "monitor-loops.mp4"
        poster = folder / "monitor-loops.jpg"
        assert entry["sha256"] == hashlib.sha256(clip.read_bytes()).hexdigest()
        assert entry["poster_sha256"] == hashlib.sha256(poster.read_bytes()).hexdigest()
        assert entry["bytes"] == clip.stat().st_size

    def test_the_upload_is_printed_and_never_run(
        self,
        release_dir: Path,
        catalog: Path,
        key_pair: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Credentials stay with the human: the tool may only print the commands."""

        def _refuse(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("publish.py must never invoke the AWS CLI")

        monkeypatch.setattr(publish_mod, "_run_aws_json", _refuse)
        _publish(
            release_dir,
            catalog,
            key_pair[0],
            "--s3-bucket",
            "example-bucket",
            "--distribution-id",
            "E123456789",
        )
        out = capsys.readouterr().out
        assert "aws s3 sync --dryrun" in out
        assert "s3://example-bucket/feature-videos/0.7.0/" in out
        assert "aws cloudfront create-invalidation --distribution-id E123456789" in out
        assert "'/feature-videos/0.7.0/*'" in out

    def test_the_runtime_derives_the_url_the_upload_plan_publishes_to(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path], capsys: Any
    ) -> None:
        """The URL a dashboard fetches must be the object the operator uploaded.

        The runtime builds ``<cdn_base>/<release>/<name>`` itself, and the upload
        plan puts the folder under ``feature-videos/<release>/``. The manifest's
        ``cdn_base`` is the only thing joining the two, so it is checked end to
        end here: a base already carrying the release would double it and every
        clip would 404 on every dashboard.
        """
        folder = _publish(release_dir, catalog, key_pair[0], "--s3-bucket", "example-bucket")
        prefix = "feature-videos/0.7.0/"
        assert f"s3://example-bucket/{prefix}" in capsys.readouterr().out
        parsed = fvm.parse_manifest(_read(folder))
        assert parsed is not None and parsed.entries
        entry = parsed.entries[0]
        assert parsed.asset_url(entry.file) == f"https://videos.example.com/{prefix}{entry.file}"
        assert parsed.asset_url(entry.poster) == (
            f"https://videos.example.com/{prefix}{entry.poster}"
        )

    def test_a_local_key_says_the_artifact_is_not_a_release(
        self,
        release_dir: Path,
        catalog: Path,
        key_pair: tuple[Path, Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Separate keys for staging and production, and the output must say which."""
        _publish(release_dir, catalog, key_pair[0])
        assert "staging artifact" in capsys.readouterr().err

    def test_the_signing_key_is_never_printed(
        self,
        release_dir: Path,
        catalog: Path,
        key_pair: tuple[Path, Path],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """No signing-tool output may carry key material or its bytes."""
        private, _ = key_pair
        _publish(release_dir, catalog, private)
        captured = capsys.readouterr()
        secret = private.read_text(encoding="utf-8")
        body = "".join(secret.splitlines()[1:-1])[:64]
        for stream in (captured.out, captured.err):
            assert "PRIVATE KEY" not in stream
            assert body not in stream


class TestRulesMatchTheRuntime:
    """The tool's local copies must not drift from what the runtime does."""

    def test_every_publisher_cap_sits_under_the_runtime_limit(self) -> None:
        """A publishing ceiling above the runtime's would ship an unreadable release.

        The runtime's numbers are read from its source rather than restated here,
        so tightening one on that side fails this test instead of silently making
        this tool the looser of the two. Equality is allowed; exceeding is not.
        """
        limits = _consumer_limits()
        pairs = (
            (
                "max_payload_bytes",
                manifest_mod.DEFAULT_MAX_PAYLOAD_BYTES,
                "_SIGNED_PAYLOAD_MAX_BYTES",
            ),
            ("max_document_bytes", manifest_mod.DEFAULT_MAX_DOCUMENT_BYTES, "_MANIFEST_MAX_BYTES"),
            ("max_entries", manifest_mod.DEFAULT_MAX_ENTRIES, "_MAX_ENTRIES"),
            ("max_clip_bytes", manifest_mod.DEFAULT_MAX_CLIP_BYTES, "_MAX_ENTRY_BYTES"),
            ("max_poster_bytes", manifest_mod.DEFAULT_MAX_POSTER_BYTES, "MAX_POSTER_BYTES"),
        )
        assert set(manifest_mod.RUNTIME_LIMITS) == {flag for flag, _, _ in pairs}
        for flag, publisher, constant in pairs:
            runtime = limits.get(constant)
            assert runtime is not None, f"{constant} is missing from the consumer"
            assert publisher <= runtime, (
                f"the {flag} publishing cap ({publisher}) exceeds the runtime's "
                f"{constant} ({runtime})"
            )
            # The ceiling a flag may be raised to is the runtime's number itself,
            # so a runtime that tightens fails here rather than letting a raised
            # cap sign a release it refuses.
            assert manifest_mod.RUNTIME_LIMITS[flag] == runtime, (
                f"RUNTIME_LIMITS[{flag!r}] restates {constant} as "
                f"{manifest_mod.RUNTIME_LIMITS[flag]}, the consumer says {runtime}"
            )

    def test_a_cap_raised_past_the_runtime_ceiling_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A flag may loosen a cap up to the runtime's limit and not one byte past.

        A 65 MiB clip signs and hashes cleanly; every dashboard then drops the
        entry. The refusal has to happen where the operator can see it.
        """
        over = manifest_mod.RUNTIME_LIMITS["max_clip_bytes"] + 1
        with pytest.raises(ManifestError, match="over the runtime's .* ceiling"):
            _publish(release_dir, catalog, key_pair[0], "--max-clip-bytes", str(over))
        assert not (release_dir / "manifest.json").exists()
        # At the ceiling exactly is allowed: the runtime's bound is inclusive.
        at = manifest_mod.RUNTIME_LIMITS["max_poster_bytes"]
        _publish(release_dir, catalog, key_pair[0], "--max-poster-bytes", str(at))

    def test_the_longest_id_still_fits_the_runtime_basename_bound(self) -> None:
        """``<id>.mp4`` is checked by the runtime's basename regex, id included.

        The id cap is derived from that bound, so it is checked against the
        runtime's own pattern: the longest id this tool signs must pass, and one
        character more must not, or the derivation has drifted.
        """
        basename_re = re.compile(_consumer_pattern("_SAFE_BASENAME_RE"))
        longest = "a" * manifest_mod.MAX_ID_CHARS
        assert manifest_mod.validate_slug(longest, where="test") == longest
        for suffix in (".mp4", ".jpg"):
            assert basename_re.match(longest + suffix), f"{len(longest)}-char id + {suffix}"
        assert not basename_re.match("a" * (manifest_mod.MAX_ID_CHARS + 1) + ".mp4")
        with pytest.raises(ManifestError, match="longer than"):
            manifest_mod.validate_slug("a" * (manifest_mod.MAX_ID_CHARS + 1), where="test")

    @pytest.mark.parametrize(
        ("release", "signable"),
        [
            ("1", True),
            ("0.7.0", True),
            ("1.2.3.4", True),
            ("99999.99999.99999.99999", True),
            ("1.2.3.4.5", False),
            ("123456", False),
            ("0..1", False),
            ("0.7.0-rc1", False),
        ],
    )
    def test_the_release_grammar_is_the_runtime_s(self, release: str, signable: bool) -> None:
        """The publisher accepts exactly the release shapes the runtime does.

        The runtime refuses a manifest whose release it cannot parse, whole, so a
        shape it refuses must be refused here before it is signed. Checked on
        shapes at and just past each bound of the runtime's own pattern.
        """
        runtime_re = re.compile(_consumer_pattern("_RELEASE_RE"))
        assert bool(runtime_re.match(release)) is signable, f"runtime disagrees on {release!r}"
        if signable:
            assert manifest_mod.validate_release(release) == release
        else:
            with pytest.raises(ManifestError, match="one to four components"):
                manifest_mod.validate_release(release)

    def test_a_duplicate_id_is_refused(self, catalog: Path) -> None:
        catalog.write_text(
            json.dumps({"entries": [_entry(), _entry()]}, indent=2) + "\n", encoding="utf-8"
        )
        with pytest.raises(ManifestError, match="duplicate id"):
            publish_mod.load_catalog(catalog)

    def test_an_unknown_catalog_field_is_refused(self, catalog: Path) -> None:
        _rewrite_catalog(catalog, _entry(src="/app-assets/feature-videos/x.mp4"))
        with pytest.raises(ManifestError, match="unknown field"):
            publish_mod.load_catalog(catalog)

    def test_a_non_finite_duration_is_refused(self, catalog: Path) -> None:
        """`> 0` admits infinity, and json.dumps writes the bare token Infinity.

        The signature over those bytes verifies perfectly while the document is
        not JSON, so the check has to be finiteness, not positivity.
        """
        _rewrite_catalog(catalog, _entry(duration_s=1e999))
        with pytest.raises(ManifestError, match="must be finite"):
            publish_mod.load_catalog(catalog)

    def test_an_out_of_range_duration_refuses_instead_of_crashing(self, catalog: Path) -> None:
        """A JSON integer has no width limit, and float() on a huge one raises.

        OverflowError is neither ValueError nor TypeError, so an uncaught one
        leaves a traceback where a refusal belongs.
        """
        catalog.write_text(
            '{"entries": [{"id": "monitor-loops", "feature": "monitor-loops", '
            '"title": "t", "description": "d", "doc": "monitor-loops.md", '
            '"used_when": [], "min_version": "", "duration_s": ' + "9" * 400 + "}]}\n",
            encoding="utf-8",
        )
        with pytest.raises(ManifestError, match="out of range"):
            publish_mod.load_catalog(catalog)

    def test_a_duration_that_rounds_to_zero_is_refused(self, catalog: Path) -> None:
        """The SIGNED value is what must be positive, not the value before rounding.

        0.0004 passes a bare `> 0` and then rounds to 0.0, which this tool's own
        verifier refuses — so publishing it produces a folder nobody can validate.
        """
        _rewrite_catalog(catalog, _entry(duration_s=0.0004))
        with pytest.raises(ManifestError, match="positive after rounding"):
            publish_mod.load_catalog(catalog)

    def test_a_duration_at_one_millisecond_is_kept(self, catalog: Path) -> None:
        """The boundary the rounding rule allows, so the refusal is not overbroad."""
        _rewrite_catalog(catalog, _entry(duration_s=0.001))
        assert publish_mod.load_catalog(catalog)[0]["duration_s"] == 0.001

    def test_an_oversize_catalog_is_refused_without_being_read_whole(self, catalog: Path) -> None:
        """The limit must gate the read, not follow it.

        Reading the file and measuring it afterwards makes the cap decorative: a
        multi-gigabyte input exhausts memory before the check that should have
        refused it. Asserted through the bounded reader directly, with a limit far
        below the file, so a regression to read-then-measure fails here.
        """
        with pytest.raises(ManifestError, match="larger than 8 bytes"):
            manifest_mod.read_bounded(catalog, limit=8)

    def test_the_bounded_reader_accepts_a_file_at_the_limit(self, tmp_path: Path) -> None:
        """Exactly at the cap is allowed; one byte over is not."""
        path = tmp_path / "payload.bin"
        path.write_bytes(b"0123456789")
        assert manifest_mod.read_bounded(path, limit=10) == b"0123456789"
        with pytest.raises(ManifestError, match="larger than 9 bytes"):
            manifest_mod.read_bounded(path, limit=9)

    def test_a_prerelease_min_version_is_refused(self, catalog: Path) -> None:
        _rewrite_catalog(catalog, _entry(min_version="0.8.0rc1"))
        with pytest.raises(ManifestError, match="bare release"):
            publish_mod.load_catalog(catalog)

    def test_a_non_https_cdn_base_is_refused(self) -> None:
        with pytest.raises(ManifestError, match="must be an https URL"):
            manifest_mod.validate_cdn_base("http://videos.example.com/feature-videos/0.7.0/")

    def test_a_cdn_base_without_a_trailing_slash_is_refused(self) -> None:
        with pytest.raises(ManifestError, match="must end with a slash"):
            manifest_mod.validate_cdn_base("https://videos.example.com/feature-videos/0.7.0")

    def test_a_malformed_cdn_base_is_refused_not_crashed(self) -> None:
        """urlsplit raises ValueError on an unbalanced bracket; the CLI must not."""
        with pytest.raises(ManifestError, match="not a well-formed URL"):
            manifest_mod.validate_cdn_base("https://[videos.example.com/feature-videos/")

    def test_a_deeply_nested_catalog_is_refused_not_crashed(self, catalog: Path) -> None:
        """json.loads raises RecursionError past the interpreter's depth; refuse it."""
        depth = 100_000
        catalog.write_text("[" * depth + "]" * depth, encoding="utf-8")
        with pytest.raises(ManifestError, match="nested too deeply"):
            manifest_mod.load_json_object(catalog, limit=1024 * 1024)

    def test_a_weak_signing_key_is_refused(self, tmp_path: Path) -> None:
        """Below 3072 bits is not a release key, the CLI signer's own floor."""
        openssl = fixture.openssl_or_skip()
        private = tmp_path / "weak.pem"
        subprocess.run(
            [
                openssl,
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
                str(private),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        public = tmp_path / "weak-public.pem"
        subprocess.run(
            [openssl, "pkey", "-in", str(private), "-pubout", "-out", str(public)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with pytest.raises(ManifestError, match="at least 3072 bits"):
            manifest_mod.key_id_of(public)

    def test_the_canonical_form_matches_the_cli_manifest_signer(self) -> None:
        """The sibling signer's canonicalization is the same rule, so pin it.

        `packaging/signing/cli-manifest.py` signs the CLI feed with the same key
        and the same encoding. Two independent copies of one rule drift silently,
        and a drift here means a release this tool signs verifies nowhere — so the
        two are compared byte-for-byte rather than trusted to stay identical.
        """
        signer = _load_cli_manifest_signer()
        for payload in (
            {"schema": "x", "version": "1"},
            {"b": "2", "a": "1"},
            {"unicode": "caf\u00e9", "quote": 'a"b'},
        ):
            assert manifest_mod.canonical_bytes(payload) == signer._canonical_json(payload)

    def test_the_key_id_derivation_matches_the_cli_manifest_signer(
        self, key_pair: tuple[Path, Path]
    ) -> None:
        """One key, one identity: both tools must name it the same way."""
        signer = _load_cli_manifest_signer()
        _, public = key_pair
        assert manifest_mod.key_id_of(public) == signer.public_key_id(public)

    def test_the_algorithm_matches_the_cli_manifest_signer(self) -> None:
        signer = _load_cli_manifest_signer()
        assert manifest_mod.ALGORITHM == signer.ALGORITHM

    def test_the_parsed_doc_allowlist_equals_the_runtime_one(self) -> None:
        """Parsed from source, never imported — but it must be the same set."""
        assert publish_mod.read_tip_doc_allowlist() == TIP_DOC_ALLOWLIST

    def test_the_runtime_verifier_accepts_what_the_tool_signs(
        self,
        release_dir: Path,
        catalog: Path,
        key_pair: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The canonical-JSON copy is pinned to the runtime's own verifier.

        The tool encodes the signed bytes itself, so the only thing that proves
        the copy agrees with the consumer is feeding a folder the tool signed to
        the consumer's verifier. A one-byte edit must flip the verdict.
        """
        private, public = key_pair
        manifest = _read(_publish(release_dir, catalog, private))
        fixture.pin_fixture_key(monkeypatch, public)

        cap = manifest_mod.DEFAULT_MAX_PAYLOAD_BYTES
        assert feed_trust.verify_document_signature(manifest, max_payload_bytes=cap) is True

        manifest["entries"][0]["title"] = "A title nobody signed"
        assert feed_trust.verify_document_signature(manifest, max_payload_bytes=cap) is False

    def test_the_runtime_parser_keeps_what_the_tool_signs(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A signed entry must survive the consumer's structural parse intact.

        The consumer drops an entry it cannot validate and logs the reason, so a
        field shape this tool signs but the runtime rejects would publish a
        release whose clips silently never appear.
        """
        folder = _publish(release_dir, catalog, key_pair[0])
        parsed = fvm.parse_manifest(_read(folder))
        assert parsed is not None
        assert len(parsed.entries) == 1
        assert parsed.entries[0].bytes == (folder / "monitor-loops.mp4").stat().st_size

    def test_the_canonical_form_sorts_nested_keys(self) -> None:
        """Nested payloads are allowed, so nested key order must not matter."""
        one = manifest_mod.canonical_bytes({"a": [{"y": 1, "x": 2}], "b": 3})
        two = manifest_mod.canonical_bytes({"b": 3, "a": [{"x": 2, "y": 1}]})
        assert one == two
        assert one == b'{"a":[{"x":2,"y":1}],"b":3}\n'


class TestTamperDetection:
    def test_verify_passes_on_a_freshly_produced_folder(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        report = verify_mod.verify_folder(folder, public_key=public)
        assert report["release"] == "0.7.0"
        assert report["entries"] == 1
        assert report["claims_key_id"] is False

    def test_a_flipped_media_byte_is_caught(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        clip = folder / "monitor-loops.mp4"
        raw = bytearray(clip.read_bytes())
        raw[-1] ^= 0xFF
        clip.write_bytes(bytes(raw))
        with pytest.raises(ManifestError, match="hash to"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_an_edited_manifest_field_is_caught(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """The signature covers every top-level field, so any edit breaks it."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        manifest["entries"][0]["title"] = "A title nobody signed"
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="does not verify against the release key"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_a_redirected_cdn_base_is_caught(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        manifest["cdn_base"] = "https://evil.example.com/feature-videos/"
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="does not verify against the release key"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_a_signature_from_another_key_is_refused(
        self,
        release_dir: Path,
        catalog: Path,
        tmp_path: Path,
        key_pair: tuple[Path, Path],
        second_key: Path,
    ) -> None:
        """A well-formed signature over the right bytes, by the wrong key."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        payload = manifest_mod.canonical_bytes(manifest_mod.signed_payload(manifest))
        manifest["signature"] = _sign_bytes(payload, second_key, tmp_path)
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="does not verify against the release key"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_a_stripped_signature_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        del manifest["signature"]
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="missing its signature"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_an_added_top_level_field_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A field outside the schema is refused, not ignored and left unsigned."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        manifest["extra"] = "smuggled"
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="unknown field"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_a_key_id_naming_another_key_is_refused(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """key_id is optional, but a present one must name the verifying key."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        manifest["key_id"] = f"sha256:{'0' * 64}"
        payload = manifest_mod.canonical_bytes(manifest_mod.signed_payload(manifest))
        manifest["signature"] = _sign_bytes(payload, private, tmp_path)
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="but the verifying key is"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_a_key_id_naming_the_verifying_key_is_accepted(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """The production shape: key_id present, inside the signed payload."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        manifest["key_id"] = manifest_mod.key_id_of(public)
        payload = manifest_mod.canonical_bytes(manifest_mod.signed_payload(manifest))
        manifest["signature"] = _sign_bytes(payload, private, tmp_path)
        _rewrite(folder, manifest)

        # SHA256SUMS covers manifest.json, so it has to be refreshed alongside.
        sums = folder / "SHA256SUMS"
        digest, _size = manifest_mod.hash_regular_file(
            folder / "manifest.json", where="release folder"
        )
        lines = [
            line
            for line in sums.read_text(encoding="utf-8").splitlines()
            if not line.endswith("  manifest.json")
        ]
        sums.write_text("\n".join(sorted([*lines, f"{digest}  manifest.json"])) + "\n", "utf-8")

        report = verify_mod.verify_folder(folder, public_key=public)
        assert report["claims_key_id"] is True

    def test_a_dropped_required_field_is_refused(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A validly signed but incomplete manifest is still refused.

        Re-signing after dropping a field makes the signature correct over what
        remains, so the schema's required-field set is the only thing left to
        catch it.
        """
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        del manifest["generated_at"]
        payload = manifest_mod.canonical_bytes(manifest_mod.signed_payload(manifest))
        manifest["signature"] = _sign_bytes(payload, private, tmp_path)
        _rewrite(folder, manifest)
        with pytest.raises(ManifestError, match="missing field"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_an_unsigned_extra_file_is_caught(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        (folder / "extra.mp4").write_bytes(b"not part of this release")
        with pytest.raises(ManifestError, match="unsigned path"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_an_unsigned_file_nested_in_a_subdirectory_is_caught(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """aws s3 sync uploads the whole tree, so a nested file reaches the CDN.

        A top-level-only scan sees a directory, not a file, and passes — which
        would serve unsigned bytes from the release prefix.
        """
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        nested = folder / "assets" / "deep"
        nested.mkdir(parents=True)
        (nested / "payload.js").write_bytes(b"nobody signed this")
        with pytest.raises(ManifestError, match="assets/deep/payload.js"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_republishing_into_a_used_release_folder_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A folder holding a manifest is a release, and a release is immutable.

        A re-recorded clip signed over the same release leaves any consumer that
        cached the old digest unable to validate the new file. The presence of a
        generated file is the whole rule, so there is no content comparison to
        need an exemption for the files this tool writes.
        """
        private, _ = key_pair
        folder = _publish(release_dir, catalog, private)
        published = (folder / "monitor-loops.mp4").read_bytes()

        (folder / "monitor-loops.mp4").write_bytes(published + b"one more frame")
        with pytest.raises(ManifestError, match="never re-signed"):
            _publish(folder, catalog, private)
        # Refused before anything is written: the manifest still describes the
        # bytes it signed, not the edited clip.
        assert _read(folder)["entries"][0]["bytes"] == len(published)

    def test_a_generated_file_is_never_written_through_something_existing(
        self, tmp_path: Path
    ) -> None:
        """The folder check fires first, so this guards the gap after it.

        Nothing end-to-end can reach it -- which is exactly why the primitive is
        exercised directly: it closes a real window between the check and the
        write, unlike a guard that merely restates a structural impossibility.
        """
        occupied = tmp_path / "manifest.json"
        occupied.write_text("keep me\n", encoding="utf-8")

        with pytest.raises(ManifestError, match="already exists"):
            publish_mod._write_new_file(occupied, b"overwritten\n")
        assert occupied.read_text(encoding="utf-8") == "keep me\n"

        fresh = tmp_path / "brand-new.json"
        publish_mod._write_new_file(fresh, b"written\n")
        assert fresh.read_bytes() == b"written\n"

    def test_a_symlinked_manifest_in_the_destination_cannot_be_written_through(
        self, release_dir: Path, tmp_path: Path
    ) -> None:
        """A link planted at manifest.json must not have its target overwritten.

        Exclusive creation is what makes the refusal about the NAME rather than
        about what it resolves to: a plain write would follow the link and
        replace a file outside the release folder entirely.
        """
        victim = tmp_path / "precious.json"
        victim.write_text('{"keep": "me"}\n', encoding="utf-8")
        planted = release_dir / "manifest.json"
        planted.symlink_to(victim)

        with pytest.raises(ManifestError, match="already exists"):
            publish_mod._write_new_file(planted, b"overwritten\n")
        assert victim.read_text(encoding="utf-8") == '{"keep": "me"}\n'

    def test_an_oversize_sha256sums_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """The verifier's checksum read is bounded too, for the same reason."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        (folder / "SHA256SUMS").write_bytes(b"x" * (1024 * 1024 + 1))
        with pytest.raises(ManifestError, match="larger than"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_verification_holds_durations_to_the_publisher_s_rule(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """Both sides must agree on a valid duration, or one blesses what the other bans.

        A bare `> 0` in the verifier admits infinity, which the publisher refuses —
        an asymmetry where a release could pass one side and fail the other.
        """
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        manifest = _read(folder)
        manifest["entries"][0]["duration_s"] = float("inf")
        payload = manifest_mod.canonical_bytes(manifest_mod.signed_payload(manifest))
        manifest["signature"] = _sign_bytes(payload, private, tmp_path)
        _rewrite(folder, manifest)

        with pytest.raises(ManifestError, match="must be finite"):
            verify_mod.verify_folder(folder, public_key=public)

    def test_a_symlink_in_a_release_folder_is_refused(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """Verification must not follow a link swapped in for signed media."""
        private, public = key_pair
        folder = _publish(release_dir, catalog, private)
        real = (folder / "monitor-loops.mp4").read_bytes()
        decoy = tmp_path / "decoy.mp4"
        decoy.write_bytes(real)
        (folder / "monitor-loops.mp4").unlink()
        (folder / "monitor-loops.mp4").symlink_to(decoy)

        with pytest.raises(ManifestError, match="is a symlink"):
            verify_mod.verify_folder(folder, public_key=public)


class TestValidationRefusals:
    def test_a_non_slug_id_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        _rewrite_catalog(catalog, _entry(id="Monitor_Loops"))
        with pytest.raises(ManifestError, match="not a lowercase hyphenated slug"):
            _publish(release_dir, catalog, key_pair[0])

    def test_a_symlinked_clip_is_refused(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A planted symlink must not be followed and hashed as release media.

        Following it would hash, sign and publish whatever it points at, and the
        signature over those bytes would be perfectly valid — so the refusal has to
        be on the link itself, not on the content it resolves to.
        """
        secret = tmp_path / "not-for-the-cdn.pem"
        secret.write_bytes(b"a local file that must never reach the CDN\n")
        clip = release_dir / "monitor-loops.mp4"
        clip.unlink()
        clip.symlink_to(secret)

        with pytest.raises(ManifestError, match="is a symlink"):
            _publish(release_dir, catalog, key_pair[0])
        assert not (release_dir / "manifest.json").exists()

    def test_a_symlinked_poster_is_refused(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """Both media slots go through the same gate, not just the clip."""
        target = tmp_path / "elsewhere.jpg"
        target.write_bytes(b"\xff\xd8\xff not a release asset")
        poster = release_dir / "monitor-loops.jpg"
        poster.unlink()
        poster.symlink_to(target)

        with pytest.raises(ManifestError, match="is a symlink"):
            _publish(release_dir, catalog, key_pair[0])

    def test_a_fifo_asset_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """Only a regular file can be hashed and signed; a pipe cannot."""
        clip = release_dir / "monitor-loops.mp4"
        clip.unlink()
        try:
            os.mkfifo(clip)
        except (AttributeError, NotImplementedError, OSError):  # pragma: no cover
            pytest.skip("this platform has no mkfifo")
        with pytest.raises(ManifestError, match="not a regular file"):
            _publish(release_dir, catalog, key_pair[0])

    def test_a_missing_poster_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        (release_dir / "monitor-loops.jpg").unlink()
        with pytest.raises(ManifestError, match="is missing"):
            _publish(release_dir, catalog, key_pair[0])

    def test_a_doc_outside_the_tips_allowlist_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        _rewrite_catalog(catalog, _entry(doc="internal-design-note.md"))
        with pytest.raises(ManifestError, match="not in the tips doc allowlist"):
            _publish(release_dir, catalog, key_pair[0])

    def test_an_oversize_clip_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        cap = PLACEHOLDER_CLIP.stat().st_size - 1
        with pytest.raises(ManifestError, match="monitor-loops.mp4 is .* over the .* byte cap"):
            _publish(release_dir, catalog, key_pair[0], "--max-clip-bytes", str(cap))

    def test_an_oversize_poster_is_refused_by_its_own_cap(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A poster has its own ceiling: the runtime bounds it lower than a clip."""
        cap = PLACEHOLDER_POSTER.stat().st_size - 1
        with pytest.raises(ManifestError, match="monitor-loops.jpg is .* over the .* byte cap"):
            _publish(release_dir, catalog, key_pair[0], "--max-poster-bytes", str(cap))
        assert not (release_dir / "manifest.json").exists()

    def test_a_payload_over_the_signed_cap_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """An over-cap release must fail while publishing, not on a client."""
        with pytest.raises(ManifestError, match="over this tool's 200 byte publishing cap"):
            _publish(release_dir, catalog, key_pair[0], "--max-payload-bytes", "200")
        assert not (release_dir / "manifest.json").exists()

    def test_an_oversize_manifest_document_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """The published file has its own cap: it is indented, the signed bytes are not."""
        with pytest.raises(ManifestError, match="over this tool's 200 byte publishing cap"):
            _publish(release_dir, catalog, key_pair[0], "--max-document-bytes", "200")
        assert not (release_dir / "manifest.json").exists()

    def test_too_many_entries_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """The runtime refuses an over-long list whole, so it must not be published."""
        shutil.copyfile(PLACEHOLDER_CLIP, release_dir / "feature-tips.mp4")
        shutil.copyfile(PLACEHOLDER_POSTER, release_dir / "feature-tips.jpg")
        catalog.write_text(
            json.dumps(
                {
                    "entries": [
                        _entry(),
                        _entry(id="feature-tips", feature="feature-tips", doc="feature-tips.md"),
                    ]
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ManifestError, match="over this tool's 1 entry publishing cap"):
            _publish(release_dir, catalog, key_pair[0], "--max-entries", "1")
        assert not (release_dir / "manifest.json").exists()

        # The same two-entry catalog signs fine under the real default.
        folder = _publish(release_dir, catalog, key_pair[0])
        assert len(_read(folder)["entries"]) == 2


class TestReleaseFolder:
    """What the folder itself must look like before anything is signed."""

    def test_a_stray_file_in_the_release_folder_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """`aws s3 sync` uploads the whole folder, so an unnamed file reaches the CDN.

        It would be served from the release prefix under a signature that never
        covered it, and the manifest is the only thing a client checks against.
        """
        (release_dir / "notes.txt").write_text("scratch notes\n", encoding="utf-8")
        with pytest.raises(ManifestError, match="does not name"):
            _publish(release_dir, catalog, key_pair[0])
        assert not (release_dir / "manifest.json").exists()

    def test_a_catalog_entry_without_a_duration_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """Nothing here inspects the media, so the catalog is the only source.

        The runtime shows a clip's length from this field, and an entry that omits
        it would publish a clip the dashboard describes as zero seconds long.
        """
        entry = _entry()
        del entry["duration_s"]
        _rewrite_catalog(catalog, entry)
        with pytest.raises(ManifestError, match="duration_s is required"):
            _publish(release_dir, catalog, key_pair[0])

    def test_an_empty_clip_is_refused(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A zero-byte file hashes and signs cleanly, and plays nowhere."""
        (release_dir / "monitor-loops.mp4").write_bytes(b"")
        with pytest.raises(ManifestError, match="is empty"):
            _publish(release_dir, catalog, key_pair[0])
        assert not (release_dir / "manifest.json").exists()

    def test_a_failed_second_write_removes_the_first(
        self,
        release_dir: Path,
        catalog: Path,
        key_pair: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both generated files or neither.

        A disk that fills between manifest.json and SHA256SUMS would otherwise
        leave a folder the next run refuses as an existing release. The media the
        operator assembled is untouched either way.
        """
        real = publish_mod._write_new_file

        def fail_on_sums(path: Path, data: bytes) -> None:
            if path.name == "SHA256SUMS":
                raise OSError(errno.ENOSPC, "No space left on device")
            real(path, data)

        monkeypatch.setattr(publish_mod, "_write_new_file", fail_on_sums)
        with pytest.raises(OSError, match="No space left"):
            _publish(release_dir, catalog, key_pair[0])
        assert sorted(p.name for p in release_dir.iterdir()) == [
            "monitor-loops.jpg",
            "monitor-loops.mp4",
        ]

    def test_a_write_that_fails_after_creating_the_file_removes_it(self, tmp_path: Path) -> None:
        """A partial generated file is worse than none.

        The exclusive open succeeds and the write then fails (here with a payload
        the handle cannot take, standing in for a full disk); the name must not be
        left behind holding part of the data.
        """
        target = tmp_path / "manifest.json"
        with pytest.raises(TypeError):
            publish_mod._write_new_file(target, None)  # type: ignore[arg-type]
        assert not target.exists()

    def test_the_refusal_of_a_half_release_names_what_to_delete(
        self, release_dir: Path, catalog: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """An interrupted run's leftovers are two known files, and the message says so."""
        (release_dir / "manifest.json").write_bytes(b"{")
        with pytest.raises(ManifestError, match="delete manifest.json and SHA256SUMS"):
            _publish(release_dir, catalog, key_pair[0])

    def test_a_symlinked_release_folder_is_refused(
        self, release_dir: Path, catalog: Path, tmp_path: Path, key_pair: tuple[Path, Path]
    ) -> None:
        """A link at the folder path must be refused, not followed and written into.

        The tool writes two files into whatever it is handed, so a link here would
        put a signed manifest somewhere the operator never named.
        """
        link = tmp_path / "release-link"
        link.symlink_to(release_dir, target_is_directory=True)
        with pytest.raises(ManifestError, match="not a directory"):
            _publish(link, catalog, key_pair[0])
        assert not (release_dir / "manifest.json").exists()
