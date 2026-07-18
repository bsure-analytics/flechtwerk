#!/usr/bin/env python3
"""Render one distinct Celtic interlace ornament per docs page.

Each docs page has a slug (``concepts/architecture.md`` -> ``concepts-architecture``,
see ``hooks/chapters.py``). This driver maps every slug to a distinct interlace
*style* (from ``style_<name>.py``) plus an accent colour, and renders the
seamless band + rail tiles the theme frames the page with.

Run:  uv run --with pillow --no-project python tools/ornaments/generate.py

To add a page: give it an entry in PAGES with a style + accent that isn't already
taken by a neighbour, then re-run. New styles go in their own ``style_<name>.py``.
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent.parent / "docs" / "assets" / "ornaments"

# Manuscript accent colours (gold primary + sepia outline are the style defaults).
LAPIS = "39,74,122"
GARNET = "124,45,58"
VERDIGRIS = "47,109,94"

# One DISTINCT (style, accent) per page slug — no two pages share a style.
PAGES = {
    "index": ("plait", LAPIS),
    "concepts-index": ("spiral", VERDIGRIS),
    "concepts-typed-attributes": ("threestrand", LAPIS),
    "concepts-architecture": ("gridknot", GARNET),
    "concepts-config-topics": ("triquetra", GARNET),
    "concepts-exactly-once": ("chain", LAPIS),
    "guides-index": ("twistknot", GARNET),
    "guides-getting-started": ("stepfret", VERDIGRIS),
    "guides-extractor": ("braid4", GARNET),
    "guides-mqtt": ("weave", LAPIS),
    "guides-transformer": ("chevron", VERDIGRIS),
    "guides-best-practices": ("twill", GARNET),
    "guides-observability": ("chevron5", LAPIS),
    "api-index": ("braid6", VERDIGRIS),
    "concepts-secrets": ("linkchain", VERDIGRIS),
}


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    failures = []
    for slug, (style, accent) in PAGES.items():
        script = HERE / f"style_{style}.py"
        if not script.exists():
            print(f"!! missing {script.name} for slug {slug}", file=sys.stderr)
            failures.append(slug)
            continue
        cmd = [
            sys.executable, str(script),
            "--accent", accent,
            "--band", str(OUT / f"band-{slug}.png"),
            "--rail", str(OUT / f"rail-{slug}.png"),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"!! {slug} ({style}) failed:\n{result.stderr[-500:]}", file=sys.stderr)
            failures.append(slug)
        else:
            print(f"ok {slug:24} <- style_{style}.py (accent {accent})")
    if failures:
        print(f"\nFAILED: {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"\nrendered {len(PAGES)} page ornaments into {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
