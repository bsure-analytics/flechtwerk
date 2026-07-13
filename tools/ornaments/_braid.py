#!/usr/bin/env python3
"""Shared renderer for strand-woven interlace ornaments.

`strands` cords are modelled as equal-amplitude periodic curves, evenly
offset in phase by ``2*pi/strands``:

    y_i(x) = cy + A * wave(2*pi*x/P + i*2*pi/strands)

and woven with a **height-field** over/under rule: at any pixel covered by
several cords, the one whose sinusoidal height ``z_i(x) = cos(theta_i)`` is
greatest is drawn on top. Because every ``z_i`` is a phase-shifted cosine,
the over/under relationship varies continuously and flips every half period
along each strand — a genuine woven look for any strand count, with no
special-casing of triple crossings.

``wave`` is ``sin`` for round braids or a triangle wave for sharp (chevron)
ones. Rendering matches the other styles: per-pixel analytic coverage at
``SUPERSAMPLE`` x, then a wrap-aware LANCZOS downscale (tile x3, resample,
crop the middle period) so the band is perfectly seamless horizontally; the
rail is the band rotated 90 degrees. Flat matte colours (gold core + thin
sepia outline) on a transparent ground.
"""

from __future__ import annotations

import argparse
import math

from PIL import Image

GOLD = (176, 138, 62)
SEPIA = (42, 33, 24)
SUPERSAMPLE = 4


def parse_rgb(text: str) -> tuple[int, int, int]:
    parts = [int(v) for v in text.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B but got {text!r}")
    return parts[0], parts[1], parts[2]


def _triangle(t: float) -> float:
    """Triangle wave in [-1, 1], period 2*pi, in phase with ``sin``."""
    p = (t / (2.0 * math.pi)) % 1.0
    if p < 0.25:
        return 4.0 * p
    if p < 0.75:
        return 2.0 - 4.0 * p
    return 4.0 * p - 4.0


def render_band(
    primary: tuple[int, int, int],
    accent: tuple[int, int, int],
    outline: tuple[int, int, int],
    height: int,
    strands: int,
    wave: str = "sine",
    amplitude_frac: float = 0.33,
    period_factor: float = 1.45,
    core_frac: float = 0.102,
    outline_frac: float = 0.030,
) -> Image.Image:
    s = SUPERSAMPLE
    period = round(height * period_factor)     # one seamless horizontal period (final px)
    w = period * s                             # supersampled width
    hs = height * s                            # supersampled height
    amplitude = amplitude_frac * height * s
    center_y = (height / 2.0) * s
    r_core = core_frac * height * s
    r_out = r_core + outline_frac * height * s
    wf = math.sin if wave == "sine" else _triangle

    two_pi = 2.0 * math.pi
    n = strands
    yc = [[0.0] * w for _ in range(n)]
    cosang = [[0.0] * w for _ in range(n)]
    z = [[0.0] * w for _ in range(n)]
    for x in range(w):
        base = two_pi * x / w                  # one full period across the width
        for i in range(n):
            th = base + i * (two_pi / n)
            yc[i][x] = center_y + amplitude * wf(th)
            slope = amplitude * (two_pi / w) * math.cos(th)
            cosang[i][x] = 1.0 / math.sqrt(1.0 + slope * slope)
            z[i][x] = math.cos(th)             # height field -> over/under

    # Alternate gold / accent cords so the accent threads visibly through.
    colors = [accent if i % 2 else primary for i in range(n)]
    buf = bytearray(w * hs * 4)

    for py in range(hs):
        row = py * w
        for px in range(w):
            top_i = -1
            top_z = -2.0
            top_core = False
            for i in range(n):
                d = abs(py - yc[i][px]) * cosang[i][px]
                if d <= r_out and z[i][px] > top_z:
                    top_i, top_z, top_core = i, z[i][px], d <= r_core
            if top_i < 0:
                continue
            color = colors[top_i] if top_core else outline
            o = (row + px) * 4
            buf[o] = color[0]
            buf[o + 1] = color[1]
            buf[o + 2] = color[2]
            buf[o + 3] = 255

    supersampled = Image.frombytes("RGBA", (w, hs), bytes(buf))
    wide = Image.new("RGBA", (w * 3, hs))
    for k in range(3):
        wide.paste(supersampled, (k * w, 0))
    wide = wide.resize((period * 3, height), Image.LANCZOS)
    return wide.crop((period, 0, period * 2, height))


def run(strands: int, wave: str = "sine", description: str = "strand-woven braid", **kw) -> None:
    parser = argparse.ArgumentParser(description=f"Celtic interlace ornament: {description}")
    parser.add_argument("--primary", type=parse_rgb, default=GOLD)
    parser.add_argument("--accent", type=parse_rgb, required=True)
    parser.add_argument("--outline", type=parse_rgb, default=SEPIA)
    parser.add_argument("--height", type=int, default=88)
    parser.add_argument("--band", required=True)
    parser.add_argument("--rail", required=True)
    args = parser.parse_args()

    band = render_band(args.primary, args.accent, args.outline, args.height, strands, wave, **kw)
    band.save(args.band)
    band.transpose(Image.ROTATE_90).save(args.rail)
