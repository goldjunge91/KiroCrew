"""What an INLINE interpreter program names -- the credential-mint gate for ``-c`` and stdin.

``python -c "..."`` and the stdin forms (``python - <<'PY'``, ``echo ... | python -``,
``python < file``) run arbitrary Python with the interpreter's authority, so the
``token`` argv word cannot be the mint gate there: the payload can build the verb. What
a payload cannot avoid is NAMING the surface it reaches. This module is that question,
asked of the payload text alone: the mint surface (``_MINT_SURFACE_RE``), a product import
next to a credential word (``_PRODUCT_IMPORT_RE`` + ``_MINT_VERB_RE``), the two
static resolutions that read a hidden name back out (``_fold_inline_literals`` for
``'a' + 'b'`` pieces, ``_decoded_b64_literals`` for base64 literals), and the one
predicate the argv floor calls (``_inline_payload_reaches_cli``).

It sits below ``argv_floor`` in the dependency order and imports only the vocabulary:
the floor decides WHICH tokens are an inline program's payload; this module decides
what that payload names.
"""

from __future__ import annotations

import base64
import binascii
import re

from .vocabulary import _SELF_NAME_RE

#: Dynamic-execution primitives an inline payload uses to hide WHAT it imports: string-
#: concatenated imports (``__import__('kiro_crew.c'+'li')``), name-computed imports
#: (``importlib.import_module(...)``), module runners (``runpy.run_module(...)``) and
#: second-stage decode/eval
#: (``exec(base64.b64decode(...))``). Their presence is NOT a verdict on its own -- read that
#: way, every inline ``getattr``/``eval``/``importlib`` one-liner is a credential mint, while a
#: payload that wants to hide can always do so anyway (``chr()`` arithmetic, a written-then-run
#: file -- see the residual-gap test). It is the trigger for looking HARDER:
#: ``_fold_inline_literals`` joins ``'a'+'b'`` pieces and decodes base64 literals, so the two
#: obfuscations these primitives are actually used for resolve back to the name they hide, and
#: the resolved text is what the mint-surface match reads.
_INLINE_DYNAMIC_EXEC_RE = re.compile(
    r"\b__import__\s*\(|\bimportlib\b|\bimport_module\b|\brunpy\b|\brun_module\b|"
    r"\brun_path\b|\bexec\s*\(|\beval\s*\(|"
    r"\bcompile\s*\(|\bb64decode\b|\bmarshal\b|\bgetattr\s*\("
)

#: The MINT SURFACE an inline program has to name to mint a dashboard token:
#:
#: * the CLI dispatch (``kiro_crew.cli`` / ``kiro_crew.__main__`` / the console-script entry
#:   ``kiro_crew._bootstrap``) or the module that implements the ``token`` subcommand
#:   (``kiro_crew.cli_server``) -- as an import path or as a file path fed to the interpreter
#:   (``python < src/kiro_crew/cli.py``);
#: * ``from kiro_crew import cli`` and its siblings;
#: * a product module whose path names a ``token`` producer (``kiro_crew.dashboard.token_auth``,
#:   ``kiro_crew.dashboard.token_secret``, ``kiro_crew.instances.token_mint``), as an import or a
#:   file path -- ``secret`` is deliberately NOT a path word: ``kiro_crew/dashboard/handlers/
#:   secrets.py`` and ``kiro_crew/secrets/`` are ordinary modules a patch script names, and the
#:   secret READERS are covered by the import-plus-word form below;
#: * a product IMPORT STATEMENT together with a CREDENTIAL WORD -- ``token`` or ``secret`` --
#:   anywhere in the payload (``_PRODUCT_IMPORT_RE`` and ``_MINT_VERB_RE``): ``from
#:   kiro_crew.dashboard.token_auth import generate_token``, ``from kiro_crew.cli_server import
#:   _token``, the attribute route ``import kiro_crew.slack.gateway as g; g.generate_token(...)``
#:   -- a module that merely re-exports the producer under a path with no ``token`` in it --
#:   and the SECOND credential, the internal secret ``/api/token/local`` accepts: ``from
#:   kiro_crew.config.loader import read_local_secret`` reads ``.local_secret`` through product
#:   code, past the sensitive-path floor that fences the file itself. Every function that can
#:   produce a token carries ``token`` (``generate_token``, ``_token``, ``mint_remote_token``)
#:   and every reader of the internal secret carries ``secret`` (``read_local_secret``,
#:   ``read_secret``, ``_internal_secret``); ``test_the_mint_surface_covers_every_token_producer_
#:   in_the_tree`` and ``test_the_credential_word_covers_every_local_secret_reader_in_the_tree``
#:   derive both sets from the tree rather than from a hand-written list. Without a product
#:   import the word is a mention: ``print("kirocrew docs mention token")`` and
#:   ``re.search(r"kirocrew.*token", s)`` stay allowed, and a subprocess sink spelling the shell mint
#:   (``subprocess.run(["kirocrew", "token"])``) belongs to the interpreter-argv companion
#:   rule, which owns sink detection.
#:
#: A payload that imports some OTHER product module (``from kiro_crew.acp import x``) runs
#: product code, but it cannot reach the mint without ALSO spelling one of these -- or
#: handing the bare package name to a DYNAMIC module runner (``_DYNAMIC_IMPORT_RE``):
#: ``runpy.run_module('kiro_crew', run_name='__main__')`` is ``python -m kiro_crew`` spelled
#: without the dotted surface, and ``__import__('kiro_crew')`` reaches the same dispatch by
#: attribute -- or running the INSTALLED CONSOLE SCRIPT's text through a code loader
#: (``_INLINE_CODE_LOADER_RE`` with ``_code_loader_arguments``):
#: ``exec(open(shutil.which('kirocrew')).read())`` is the entry point's ``_bootstrap.main``
#: run in-process, against a ``sys.argv`` the payload chose.  A reach that hides the name
#: from all of these (built at run time from pieces
#: none of ``_fold_inline_literals`` / ``_decoded_b64_literals`` resolve) falls to the
#: residual the sensitive-path floor over the signing key covers.
_MINT_SURFACE_RE = re.compile(
    # The CLI dispatch or the token subcommand's module, as an import or a file path.
    r"kiro_crew[./](?:cli|cli_server|__main__|_bootstrap)(?![a-z0-9_])"
    # A product module or file whose path names a token producer
    # (``kiro_crew.dashboard.token_auth``, ``kiro_crew/instances/token_mint.py``,
    # ``kiro_crew.dashboard.refresh_tokens``); ``llama_tokenizer`` is not one.
    r"|kiro_crew[\w./]*token(?!iz)"
    # ``from kiro_crew import cli`` and siblings.
    # (``[^;]{0,120}`` rather than ``[^\n;]*``: a heredoc payload reaches here one word per
    # line, so the imported names may sit on the lines after ``import``.)
    r"|from\s+kiro_crew\s+import\b[^;]{0,120}?(?<![a-z0-9_.-])(?:cli|cli_server|__main__|_bootstrap)(?![a-z0-9_])"
)
#: A Python import STATEMENT of the product package -- the payload runs product code.  At a
#: statement start (line start, ``;``, or the payload start), so a path in a string
#: (``Path("src/kiro_crew/x.py")``) is not one.  The package may sit ANYWHERE in an
#: ``import`` list (``import os, kiro_crew.instances.run_marker as r``): every name in the
#: list is imported, so the first name is not the statement.
_PRODUCT_IMPORT_RE = re.compile(
    r"(?:^|[;\n])\s*(?:from\s+kiro_crew(?:\.[\w.]*)?\s+import\b"
    r"|import\s+(?:[\w.]+(?:\s+as\s+\w+)?\s*,\s*)*kiro_crew\b)"
)
#: A dynamic module runner or importer HANDED THE PACKAGE NAME: ``runpy.run_module`` /
#: ``run_path`` (the ``-m`` form spelled as a call), ``importlib.import_module`` and
#: ``__import__`` with a ``'kiro_crew'`` literal as the argument.  The runner alone is generic
#: code (``__import__("os").environ`` is a common idiom next to a product import), and the
#: bare name alone is a mention; the two joined in one call are ``python -m kiro_crew`` with
#: the module path hidden in a string.  Matched over the folded view, so the argument may
#: have been spelled in pieces; the quote is optional because a heredoc payload arrives
#: with the shell's quote removal already applied.  ``importlib.util.spec_from_file_location``
#: is deliberately
#: absent: it loads a FILE, and a file that is the mint is caught by its path
#: (``_MINT_SURFACE_RE``).  (``_SELF_NAME_RE`` is the program name ``kirocrew`` / ``kiro-crew``
#: and does not match the underscore -- which is exactly what let the ``runpy`` spelling
#: through, so the package spelling is written out here.)
#: The name may be the first positional argument, a KEYWORD argument
#: (``run_module(mod_name='kiro_crew', run_name='__main__')``, ``import_module(name=...)``),
#: or follow other arguments (``run_module(run_name='__main__', mod_name='kiro_crew')``);
#: the scan stays inside the call's own parentheses.
_DYNAMIC_IMPORT_RE = re.compile(
    r"(?<![a-z0-9_])(?:run_module|run_path|import_module|__import__)\s*\(\s*"
    r"(?:[^()]*,\s*)?(?:\w+\s*=\s*)?"
    r"""["']?kiro_crew(?![a-z0-9])"""
)
#: A code LOADER: a primitive that runs TEXT as Python -- ``exec`` / ``eval`` / ``compile``
#: of something read, ``runpy.run_path`` of a file.  Paired with the console script named as a
#: program (below) it is ``python -m kiro_crew`` spelled through the installed entry point:
#: ``exec(open(shutil.which('kirocrew')).read())`` runs ``kiro_crew._bootstrap.main`` -- the
#: dispatch -- against whatever ``sys.argv`` the payload set.  The loader alone is generic
#: code (``exec(open('patch.py').read())`` is a common patch-script idiom), and the program
#: name alone is a mention (``print('kirocrew docs')``) -- so the name has to sit inside the
#: loader's OWN ARGUMENT (``_code_loader_arguments``).  A loader on one file and the name as
#: prose in the next statement
#: (``runpy.run_path('scripts/collect_env.py'); print('kirocrew env snapshot done')``) is
#: two allowed idioms, not the mint.
#:
#: The subject is the ARGUMENT and not the STATEMENT.  Bounding the gap between the loader
#: and the name by ``[^;\n]`` -- up to the next statement separator -- answers the two rows
#: the scope review confirmed, but Python allows a newline INSIDE parentheses, so the very
#: attack this branch exists to keep denied survives being spelled across two lines:
#: ``exec(\n    open(shutil.which('kirocrew')).read()\n)`` is one statement, and a
#: newline-bounded gap ends at the first line break.  A ``;`` inside a comment does the
#: same.  A balanced walk crosses both, because the call does.
_INLINE_CODE_LOADER_RE = re.compile(r"(?<![a-z0-9_])(?:exec|eval|compile|run_path|execfile)\s*\(")
#: The product's CONSOLE SCRIPT named as a PROGRAM: the bare name a ``which`` resolves
#: (``'kirocrew'``, ``'kiro-crew'``), or its path (``'/venv/bin/kirocrew'``,
#: ``C:\\...\\kirocrew.exe``).  A whole word AND the last path component: ``kirocrew-scratch``
#: and ``kirocrew_ws`` are directories, not the program, and so is a directory NAMED for
#: the product with a file under it (``runpy.run_path('/home/dev/kirocrew/scripts/
#: gen_docs.py')`` loads ``gen_docs.py``; confirmed newly-refused by the scope review).
#: Matched on the folded view, so ``'kiro' + 'crew'`` reads whole; a name built from
#: pieces the fold does not resolve (``''.join(...)``, ``chr()`` arithmetic) is the
#: documented residual.
_CONSOLE_SCRIPT_LITERAL_RE = re.compile(r"(?<![a-z0-9_-])kiro[-.]?crew(?:\.exe)?(?![a-z0-9_/\\-])")
#: How far a loader-call scan walks, and how deep it nests.  Bounds, not judgements: the
#: payload is attacker-shaped text on the synchronous gate path, so the scan answers in
#: linear time whatever it is handed.  They bound the WALK, not the slice: past either bound
#: the call has not been seen to close, so it reads as UNTERMINATED and everything left is
#: taken as the argument -- the fail-closed direction, since a truncated slice would hide a
#: name past the bound and report "the loader was handed something else".
_LOADER_ARG_SCAN_CAP = 4096
_LOADER_NESTING_CAP = 32
#: The string delimiters around a loader's argument -- opening whitespace, an optional
#: ``r``/``b``/``f``/``u`` prefix, and a quote of either kind (single or triple); the same
#: closing.  Peeled so the program the string holds starts at position 0 for the
#: statement-start anchor.  An argument that is not a literal (``open(...).read()``) has
#: nothing to peel and is asked as it is.
_LOADER_ARG_DELIMITERS_RE = re.compile(
    r"""^\s*(?:[rbfu]{1,2})?(?:\"\"\"|'''|[\"'])|(?:\"\"\"|'''|[\"'])\s*$"""
)
#: The CREDENTIAL WORD: the ``token`` verb or a ``token_*`` / ``*_token`` symbol
#: (``generate_token``, ``_token``, ``token_auth``), or the ``secret`` fragment every reader of
#: the internal secret carries (``read_local_secret``, ``read_secret``, ``_internal_secret``,
#: the ``X-Internal-Secret`` header that presents it).  The two are the two credentials that
#: mint a dashboard token: the signing key, and the internal secret ``/api/token/local``
#: accepts.  ``tokens``, ``tokenize`` and ``secrets`` (the stdlib module) are not it.  Read
#: together with a product import statement, or inside a DECODED literal
#: (``base64.b64decode("a2lyb2NyZXcgdG9rZW4=")`` is ``kirocrew token``) next to the product
#: name.  On its own it is a mention.  The price: a dev one-liner that imports a product
#: module AND binds a variable named ``secret`` is denied -- one command in the eight-day
#: corpus, and a rename away.
_MINT_VERB_RE = re.compile(r"(?<![a-z0-9])(?:token|secret)(?![a-z0-9])")

#: Two adjacent string literals, joined by ``+`` (``'kiro_crew.c' + "li"``) or by nothing
#: but whitespace (``'kiro_crew.c' "li"``, Python's implicit concatenation); folded to one.
#: The quote kinds need not match -- Python concatenates them all the same.  A comma
#: between them (``'a', 'b'``) is two arguments, not one string, and is not folded.
_INLINE_STRING_CONCAT_RE = re.compile(r"""(["'])([^"'\n]*)\1\s*\+?\s*(["'])([^"'\n]*)\3""")
#: A quoted literal that COULD be base64: the alphabet, padding, at least 8 characters.
#: Matched CASE-SENSITIVELY on the raw command text -- base64 is case-sensitive, and the
#: floor otherwise reads a lower-cased view in which every encoded literal is already
#: destroyed (``a2lyb2NyZXcgdG9rZW4=`` lower-cased decodes to nothing).
_INLINE_B64_LITERAL_RE = re.compile(r"""["']([A-Za-z0-9+/=_-]{8,})["']""")
_INLINE_LITERAL_FOLD_CAP = 64
#: A Python line continuation -- backslash, newline, the next line's indentation.  Python
#: joins the two lines before it reads the statement, so the view does too: ``import os, \\``
#: + newline + ``kiro_crew.config.loader as l`` is ONE import statement, and the ``\s*,\s*``
#: between its names has to be able to cross it.
_PY_LINE_CONTINUATION_RE = re.compile(r"\\\r?\n[ \t]*")


def _fold_inline_literals(payload: str) -> str:
    """*payload* with ``'a' + 'b'`` and ``'a' 'b'`` runs joined into ``'ab'`` (repeatedly, up to the cap),
    and backslash-newline continuations joined into one line.

    A name split across concatenated pieces (``'kiro_crew.c' + 'li'``) reads whole. The
    view is for MATCHING only -- it is never executed and never returned to a caller as
    the payload.
    """
    view = _PY_LINE_CONTINUATION_RE.sub(" ", payload)
    for _ in range(_INLINE_LITERAL_FOLD_CAP):
        folded = _INLINE_STRING_CONCAT_RE.sub(
            lambda m: m.group(1) + m.group(2) + m.group(4) + m.group(1), view, count=1
        )
        if folded == view:
            break
        view = folded
    return view


def _decoded_b64_literals(raw_text: str) -> "tuple[str, ...]":
    """The lower-cased decoding of every quoted literal in *raw_text* that is valid base64.

    Bounded by ``_INLINE_LITERAL_FOLD_CAP`` literals.  A literal that is not base64 (or
    decodes to nothing printable) contributes nothing; the result is for MATCHING only and
    is never executed.
    """
    decoded: list[str] = []
    # Fold first, so a literal split into pieces (``'a2ly' + 'b2Ny…'``) decodes as one.
    for n, m in enumerate(_INLINE_B64_LITERAL_RE.finditer(_fold_inline_literals(raw_text))):
        if n >= _INLINE_LITERAL_FOLD_CAP:
            break
        literal = m.group(1)
        for decoder in (base64.b64decode, base64.urlsafe_b64decode):
            try:
                text = decoder(literal + "=" * (-len(literal) % 4)).decode("utf-8", "ignore")
            except (ValueError, binascii.Error):
                continue
            if text:
                decoded.append(text.lower())
            break
    return tuple(decoded)


def _code_loader_arguments(view: str) -> "tuple[str, ...]":
    """The text inside each code loader's OWN parentheses, in *view*.

    ``_INLINE_CODE_LOADER_RE`` finds the loader; this says what it was HANDED, which is the
    rule.  ``exec(open(shutil.which('kirocrew')).read())`` loads the console script -- the
    entry point's dispatch, against a ``sys.argv`` the payload chose.
    ``run_path('scripts/collect_env.py'); print('kirocrew env snapshot done')`` loads another
    file and then mentions the product in a status line, which this module's contract calls a
    mention (``_INLINE_CODE_LOADER_RE``: "the program name alone is a mention").

    A balanced walk rather than a bounded character gap, for two reasons.  The name sits two
    levels down, inside ``which(...)``, so ``[^()]*`` stops short of it.  And a gap bounded by
    ``[^;\\n]`` ends at a newline Python allows inside parentheses, so the attack spelled
    across two lines reads as a mention -- the direction a tightening must not fail in.

    Quotes are not tracked: a parenthesis inside a string literal skews the depth and the skew
    ends the scan early, which yields a LONGER argument slice, so the reading denies rather
    than allows.
    """
    args: list[str] = []
    for m in _INLINE_CODE_LOADER_RE.finditer(view):
        start = m.end()  # just past the loader's own ``(``
        depth = 1
        i = start
        closed_at = -1
        limit = min(len(view), start + _LOADER_ARG_SCAN_CAP)
        while i < limit:
            ch = view[i]
            if ch == "(":
                depth += 1
                if depth > _LOADER_NESTING_CAP:
                    break
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    closed_at = i
                    break
            i += 1
        # The bounds bound the WALK, not the slice.  Either one hit means the call was not
        # seen to close, and an unclosed call is judged on everything left rather than on
        # the prefix the walk reached -- shortening the slice would hide a name sitting past
        # the bound and read as "the loader was handed something else", which is the open
        # direction.  The match over the slice is a single linear regex scan.
        args.append(view[start:closed_at] if closed_at >= 0 else view[start:])
    return tuple(args)


def _inline_payload_reaches_cli(payload: str, decoded_literals: "tuple[str, ...]" = ()) -> bool:
    """True if an inline-program payload names the credential MINT, literally or folded.

    The inline forms (``-c``, stdin) run arbitrary Python with the interpreter's authority,
    so the ``token`` argv word cannot be the gate there -- the payload can build the verb.
    What it cannot avoid is NAMING the surface it reaches: the CLI dispatch, the ``token``
    subcommand's module, a token-producing module (``_MINT_SURFACE_RE``), a
    product import statement next to a credential word -- ``token`` or ``secret``
    (``_PRODUCT_IMPORT_RE`` + ``_MINT_VERB_RE``),
    the bare package name handed to a dynamic module runner (``_DYNAMIC_IMPORT_RE``:
    ``runpy.run_module('kiro_crew', run_name='__main__')`` is the ``-m`` form as a call),
    or the console script named as a program in a code loader's OWN ARGUMENT
    (``_INLINE_CODE_LOADER_RE`` + ``_code_loader_arguments`` + ``_CONSOLE_SCRIPT_LITERAL_RE``:
    ``exec(open(shutil.which('kirocrew')).read())`` runs the entry point's dispatch, while
    ``run_path('scripts/x.py'); print('kirocrew done')`` loads another file and mentions us).
    The match runs over ``_fold_inline_literals``, so a module
    name split across ``'a' + 'b'`` pieces reads whole, and over *decoded_literals*
    (``_decoded_b64_literals`` of the raw command), which are asked the same questions
    -- an ``exec(b64decode(...))`` body is a payload one decode away -- and where the
    shell mint itself may be hiding, so there the product name with the credential word
    is the reach too (``_SELF_NAME_RE`` + ``_MINT_VERB_RE``).

    A code loader's string argument (``exec('from kiro_crew.config.loader import
    read_local_secret; ...')``) is a payload one level in and is asked the same questions
    (``_names_the_mint``): a legible import does not stop being one because it sits inside
    ``exec``, and the unwrapped statement is already denied.

    What this deliberately does NOT deny: a payload that merely MENTIONS the package -- a
    ``Path("src/kiro_crew/x.py")`` a patch script edits, a test path handed to
    ``importlib.util.spec_from_file_location`` -- or imports a product module with no mint
    surface in reach (``from kiro_crew.acp import x``). Eight days of this gate's denials were
    143 such commands and zero mints; the un-disableable guarantee for the credential is the
    sensitive-path floor over the signing key, not this heuristic.
    """
    view = _fold_inline_literals(payload)
    if _names_the_mint(view):
        return True
    for decoded in decoded_literals:
        # A decoded literal is read with the same questions as the payload -- an
        # ``exec(b64decode(...))`` body is a payload one decode away -- plus one more: the
        # shell mint itself may be hiding in it (``a2lyb2NyZXcgdG9rZW4=`` is ``kirocrew token``).
        if _SELF_NAME_RE.search(decoded) and _MINT_VERB_RE.search(decoded):
            return True
        if _names_the_mint(decoded):
            return True
    return False


def _names_the_mint(view: str) -> bool:
    """The four questions, over one already-folded view, then again over what each code
    loader in it was handed.

    A loader's ARGUMENT is a payload one level in.  ``exec('from kiro_crew.config.loader
    import read_local_secret; ...')`` puts a legible product import inside a string, where
    ``_PRODUCT_IMPORT_RE``'s statement-start anchor never sees it -- the unwrapped statement
    is denied and the same statement one ``exec`` deeper was not.  So the loader's argument
    text is asked the same questions the payload was, and the walk recurses through nested
    loaders (``exec(compile('...', 'x', 'exec'))``) until the argument carries no loader,
    bounded by the same caps the walk itself has.  This is the legible-import case, not the
    runtime-constructed residual the design accepts: nothing here is built from pieces the
    fold cannot join.

    The console-script question is asked of the loader's argument ONLY -- the name alone,
    anywhere else in the view, is a mention (``_INLINE_CODE_LOADER_RE``).
    """
    if _MINT_SURFACE_RE.search(view):
        return True
    if _PRODUCT_IMPORT_RE.search(view) and _MINT_VERB_RE.search(view):
        return True
    if _DYNAMIC_IMPORT_RE.search(view):
        return True
    for raw_arg in _code_loader_arguments(view):
        # The argument is (usually) a string literal, and its program starts after the
        # opening quote -- ``_PRODUCT_IMPORT_RE`` anchors at a statement start, so the
        # delimiters and any ``r``/``b`` prefix come off before the questions are asked.
        arg = _LOADER_ARG_DELIMITERS_RE.sub("", raw_arg)
        if _CONSOLE_SCRIPT_LITERAL_RE.search(arg):
            return True
        # The argument's own loaders are found by the same finditer, one level in; the
        # depth this can reach is bounded by ``_LOADER_NESTING_CAP`` because each level
        # sits inside the previous level's parentheses.
        if _INLINE_CODE_LOADER_RE.search(arg) and _names_the_mint(arg):
            return True
        if _MINT_SURFACE_RE.search(arg):
            return True
        if _PRODUCT_IMPORT_RE.search(arg) and _MINT_VERB_RE.search(arg):
            return True
        if _DYNAMIC_IMPORT_RE.search(arg):
            return True
    return False
