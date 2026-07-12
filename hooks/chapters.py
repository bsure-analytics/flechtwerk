"""MkDocs build hook: give every page its own interlace ornament.

Each page's source path becomes a slug (``concepts/architecture.md`` ->
``concepts-architecture``, ``index.md`` -> ``index``) written to
``page.meta["ornament"]``. The theme override (``overrides/main.html``) turns
that slug into the page's ``--ms-rail`` / ``--ms-band`` frame ornament, so every
page is framed by a distinct Celtic interlace. Ornament tiles are generated per
slug by ``tools/ornaments/generate.py``.

``page.meta["chapter"]`` (the top-level section) is also exposed for any
section-level styling. Chapter-opener pages opt into the illuminated masthead
with ``opener: true`` in their front matter; this hook leaves that untouched.
"""

from __future__ import annotations


def on_page_markdown(markdown: str, page, config, files) -> str:  # noqa: ANN001 (mkdocs hook signature)
    src = page.file.src_uri
    slug = src[:-3] if src.endswith(".md") else src
    page.meta["ornament"] = slug.replace("/", "-")
    parts = src.split("/")
    page.meta["chapter"] = parts[0] if len(parts) > 1 else "home"
    return markdown
