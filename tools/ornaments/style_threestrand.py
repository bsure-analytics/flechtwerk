#!/usr/bin/env python3
"""Celtic interlace ornament generator - style "threestrand".

A genuine three-strand flat plait/braid rendered as a seamless, tileable
band (and a vertical rail produced by rotating the band 90 degrees).

Geometry
--------
Three cords are modelled as sine waves of equal amplitude, evenly offset in
phase by 120 degrees:

    y_i(x) = cy + A * sin(2*pi*x/P + i*2*pi/3),   i in {0, 1, 2}

Over one horizontal period P the three cords cross pairwise six times, at
evenly spaced positions. The over/under relationship is a fixed CYCLIC
dominance rule - strand 0 passes over 1, 1 over 2, 2 over 0. That single
rule provably makes every individual strand alternate over, under, over,
under along its length, which is exactly what distinguishes a genuine
three-strand braid from a twisted rope. No triple crossings occur, so the
cyclic (non-transitive) rule never has to resolve three cords at one point.

Rendering
---------
Per-pixel analytic coverage at >=4x supersampling, then a wrap-aware LANCZOS
downscale (the supersampled band is tiled x3 before scaling and the middle
period is cropped out, so the resampling filter never sees a false edge and
the result is perfectly seamless horizontally). Background is transparent;
only the cords (core colour + a thin sepia outline) are opaque. Matte flat
colours - no gradients - so it reads as flat Celtic interlace.
"""

from __future__ import annotations

import argparse
import math

from PIL import Image

SUPERSAMPLE = 4


def parse_rgb(text: str) -> tuple[int, int, int]:
    parts = [int(v) for v in text.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B but got {text!r}")
    return parts[0], parts[1], parts[2]


def render_band(
    primary: tuple[int, int, int],
    accent: tuple[int, int, int],
    outline: tuple[int, int, int],
    height: int,
) -> Image.Image:
    s = SUPERSAMPLE
    period = round(height * 1.45)          # one seamless horizontal period (final px)
    w = period * s                         # supersampled width
    hs = height * s                        # supersampled height
    amplitude = 0.33 * height * s          # vertical sweep of each cord
    center_y = (height / 2.0) * s
    r_core = 0.102 * height * s            # cord half-thickness (core)
    w_out = 0.030 * height * s             # sepia outline width
    r_out = r_core + w_out

    # strand 0 and 2 are gold (dominant), strand 1 is the accent cord.
    strand_colors = (primary, accent, primary)

    # Precompute, per supersampled column, each strand's centre-line y and the
    # cosine of its local slope angle (to convert vertical offset -> roughly
    # perpendicular distance, giving cords of near-constant thickness).
    two_pi = 2.0 * math.pi
    yc = [[0.0] * w for _ in range(3)]
    cosang = [[0.0] * w for _ in range(3)]
    for x in range(w):
        base = two_pi * x / w              # one full period across the width
        for i in range(3):
            th = base + i * (two_pi / 3.0)
            yc[i][x] = center_y + amplitude * math.sin(th)
            slope = amplitude * (two_pi / w) * math.cos(th)
            cosang[i][x] = 1.0 / math.sqrt(1.0 + slope * slope)

    buf = bytearray(w * hs * 4)            # transparent by default

    for py in range(hs):
        row = py * w
        for px in range(w):
            # Gather strands whose cord (core or outline) covers this pixel.
            covering = []
            for i in range(3):
                d = abs(py - yc[i][px]) * cosang[i][px]
                if d <= r_out:
                    covering.append((i, d <= r_core))
            if not covering:
                continue

            if len(covering) == 1:
                idx, is_core = covering[0]
            else:
                # Pick the cord that is "over" every other covering cord.
                idx, is_core = covering[0]
                for cand_i, cand_core in covering:
                    if all(
                        cand_i == oth_i or (oth_i - cand_i) % 3 == 1
                        for oth_i, _ in covering
                    ):
                        idx, is_core = cand_i, cand_core
                        break

            color = strand_colors[idx] if is_core else outline
            o = (row + px) * 4
            buf[o] = color[0]
            buf[o + 1] = color[1]
            buf[o + 2] = color[2]
            buf[o + 3] = 255

    supersampled = Image.frombytes("RGBA", (w, hs), bytes(buf))

    # Wrap-aware downscale: tile x3 horizontally, resample, crop the middle
    # period. This keeps the LANCZOS filter from inventing an edge seam.
    wide = Image.new("RGBA", (w * 3, hs))
    for k in range(3):
        wide.paste(supersampled, (k * w, 0))
    wide = wide.resize((period * 3, height), Image.LANCZOS)
    return wide.crop((period, 0, period * 2, height))


def main() -> None:
    parser = argparse.ArgumentParser(description="Celtic three-strand braid ornament")
    parser.add_argument("--primary", type=parse_rgb, default=parse_rgb("176,138,62"))
    parser.add_argument("--accent", type=parse_rgb, required=True)
    parser.add_argument("--outline", type=parse_rgb, default=parse_rgb("42,33,24"))
    parser.add_argument("--height", type=int, default=88)
    parser.add_argument("--band", required=True)
    parser.add_argument("--rail", required=True)
    args = parser.parse_args()

    band = render_band(args.primary, args.accent, args.outline, args.height)
    band.save(args.band)

    # A horizontally-seamless band becomes a vertically-seamless rail of
    # width = height when rotated 90 degrees.
    rail = band.transpose(Image.ROTATE_90)
    rail.save(args.rail)


if __name__ == "__main__":
    main()
