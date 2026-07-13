#!/usr/bin/env python3
"""Shared renderer for woven-lattice interlace ornaments.

Straight gold weft cords (horizontal) and accent warp cords (vertical) cross
in an over/under pattern. Two patterns are supported:

- ``plain`` — checkerboard over/under (a plain basket weave);
- ``twill`` — 2/2 floats stepping diagonally (a twill, with diagonal ribs).

Cord centres are evenly spaced so the image is periodic on both axes; the
band is rendered at ``SUPERSAMPLE`` x with the same wrap-aware LANCZOS
downscale the other styles use, and the rail is the band rotated 90 degrees.
The over/under period divides both the warp and weft counts, so the pattern
stays seamless in both axes.
"""

from __future__ import annotations

import argparse

from PIL import Image

from _braid import GOLD, SEPIA, SUPERSAMPLE, parse_rgb


def _weft_on_top(j: int, k: int, pattern: str) -> bool:
    if pattern == "twill":
        return ((j + k) % 4) < 2
    return (j + k) % 2 == 0


def render_band(
    primary: tuple[int, int, int],
    accent: tuple[int, int, int],
    outline: tuple[int, int, int],
    height: int,
    pattern: str,
    wefts: int,
    warps: int,
) -> Image.Image:
    s = SUPERSAMPLE
    hs = height * s
    cell = hs // wefts                 # supersampled cell size (square cells)
    w = cell * warps                   # supersampled width (period * s)
    period = w // s
    r_core = 0.34 * cell
    r_out = r_core + 0.05 * cell

    weft_y = [cell * (k + 0.5) for k in range(wefts)]
    warp_x = [cell * (j + 0.5) for j in range(warps)]
    buf = bytearray(w * hs * 4)

    for py in range(hs):
        row = py * w
        k = min(range(wefts), key=lambda i: abs(py - weft_y[i]))
        dh = abs(py - weft_y[k])
        for px in range(w):
            j = min(range(warps), key=lambda i: abs(px - warp_x[i]))
            dv = abs(px - warp_x[j])
            in_h = dh <= r_out
            in_v = dv <= r_out
            if not in_h and not in_v:
                continue
            if in_h and (_weft_on_top(j, k, pattern) or not in_v):
                color = primary if dh <= r_core else outline
            else:
                color = accent if dv <= r_core else outline
            o = (row + px) * 4
            buf[o] = color[0]
            buf[o + 1] = color[1]
            buf[o + 2] = color[2]
            buf[o + 3] = 255

    supersampled = Image.frombytes("RGBA", (w, hs), bytes(buf))
    wide = Image.new("RGBA", (w * 3, hs))
    for kk in range(3):
        wide.paste(supersampled, (kk * w, 0))
    wide = wide.resize((period * 3, height), Image.LANCZOS)
    return wide.crop((period, 0, period * 2, height))


def run(pattern: str, wefts: int, warps: int, description: str) -> None:
    parser = argparse.ArgumentParser(description=f"Celtic interlace ornament: {description}")
    parser.add_argument("--primary", type=parse_rgb, default=GOLD)
    parser.add_argument("--accent", type=parse_rgb, required=True)
    parser.add_argument("--outline", type=parse_rgb, default=SEPIA)
    parser.add_argument("--height", type=int, default=88)
    parser.add_argument("--band", required=True)
    parser.add_argument("--rail", required=True)
    args = parser.parse_args()

    band = render_band(args.primary, args.accent, args.outline, args.height, pattern, wefts, warps)
    band.save(args.band)
    band.transpose(Image.ROTATE_90).save(args.rail)
