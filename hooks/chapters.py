"""MkDocs build hook: stamp each page with its chapter.

Every page's top-level path segment (``concepts/architecture.md`` -> ``concepts``,
``index.md`` -> ``home``) is written to ``page.meta["chapter"]`` so the theme
override (``overrides/main.html``) and the stylesheet can select a different
interlace ornament per chapter. Chapter-opener pages opt in with ``opener: true``
in their YAML front matter; this hook leaves that flag untouched.
"""

from __future__ import annotations


def on_page_markdown(markdown: str, page, config, files) -> str:  # noqa: ANN001 (mkdocs hook signature)
    parts = page.file.src_uri.split("/")
    page.meta["chapter"] = parts[0] if len(parts) > 1 else "home"
    return markdown
