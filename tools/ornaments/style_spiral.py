"""Celtic interlace style "spiral": a running wave / scroll interlace.

Two bold gold wave-cords run the full length of the band as mirrored sine
scrolls (one rising where the other falls). They therefore meet and cross four
times per period; at every crossing one cord passes cleanly OVER and the other
is broken by the over-cord's sepia outline, and the over/under choice ALTERNATES
along the band -- the hallmark of true two-cord interlace (a clean Celtic
guilloche wave, not a zig-zag). Between each pair of crossings the two cords bow
apart into a lens-shaped "eye"; a small accent leaf sits in every eye, the only
place the accent colour appears.

Design goals (the previous version was muddy -- this one is deliberately spare):

- FEWER, BOLDER cords. Exactly two, both dominant gold, each with a thin crisp
  sepia OUTLINE on every edge so crossings read unambiguously.
- CLEAN over-under: the over-cord is re-asserted within a small disk at each
  crossing, cutting the under-cord with the outline colour -- no merging/mush.
- Matte flat fills; supersampled then LANCZOS-downscaled.

Seamlessness: the cords are exact sines with an integer display period W. Three
whole periods are drawn side by side (so every crossing that straddles a tile
edge is real, neighbouring content), the trio is LANCZOS-downscaled together,
and the MIDDLE period is cropped out -- the band tiles left-to-right with no
seam. The over/under parity is a global crossing index, so it stays consistent
across the crop boundary. The RAIL is the band rotated 90 degrees, so a
horizontally seamless band becomes a vertically seamless rail of width
= --height.

Standard library + Pillow only (no numpy). Uniform CLI; run with:
  uv run --with pillow --no-project python style_spiral.py \
      --accent 47,109,94 --band band.png --rail rail.png
"""

import argparse
import math

from PIL import Image, ImageChops, ImageDraw

SS = 4          # supersample factor (render big, LANCZOS down)
CYCLES = 2      # full sine cycles per period -> 2*CYCLES crossings per period


def parse_rgb(text):
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B, got {text!r}")
    try:
        rgb = tuple(int(p) for p in parts)
    except ValueError:
        raise argparse.ArgumentTypeError(f"R,G,B must be integers: {text!r}")
    if not all(0 <= c <= 255 for c in rgb):
        raise argparse.ArgumentTypeError(f"R,G,B must be 0..255: {text!r}")
    return rgb


class Geometry:
    """All drawing dimensions in supersample space, derived from --height."""

    def __init__(self, height):
        self.disp_h = height
        self.disp_w = max(4, round(2.0 * height))   # integer period -> seamless
        self.h = height * SS
        self.w = self.disp_w * SS                    # one period, supersampled
        self.cy = self.h / 2

        # Mirrored sine cords: y = cy +/- amp * sin(2*pi*CYCLES*x / w).
        self.amp = 0.255 * self.h

        # Two bold gold cords, each with a thin crisp sepia edge.
        self.r_core = 0.088 * self.h
        self.r_out = self.r_core + 0.028 * self.h

        # Accent leaf sitting in each lens-shaped eye between crossings.
        self.leaf_w = 0.070 * self.h
        self.leaf_h = 0.150 * self.h
        self.leaf_out = max(2, round(0.014 * self.h))

        # Curve sampling step (display px) and over-cord re-assert disk.
        self.step = SS
        self.r_disk = 1.75 * self.r_out


def cord_points(g, sign):
    """One mirrored sine cord sampled across three whole periods [0, 3W]."""
    pts = []
    x = 0.0
    xmax = 3 * g.w
    while x <= xmax:
        phase = 2 * math.pi * CYCLES * x / g.w
        pts.append((x, g.cy + sign * g.amp * math.sin(phase)))
        x += g.step
    pts.append((xmax, g.cy))     # sin(3*2pi*CYCLES) == 0 -> clean endpoint
    return pts


def stroke_layer(g, pts, core_col, outline_col):
    """A cord on its own transparent RGBA layer: sepia outline underneath, the
    coloured core on top (thin visible edge on both sides)."""
    layer = Image.new("RGBA", (3 * g.w, g.h), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    line = [(x, y) for (x, y) in pts]
    d.line(line, fill=outline_col + (255,),
           width=int(round(2 * g.r_out)), joint="curve")
    d.line(line, fill=core_col + (255,),
           width=int(round(2 * g.r_core)), joint="curve")
    return layer


def stamp_over(g, canvas, layer, px, py):
    """Re-assert one cord layer on top of the canvas within a disk around a
    crossing, cutting the under strand there (clean over-under, no seam)."""
    disk = Image.new("L", (3 * g.w, g.h), 0)
    ImageDraw.Draw(disk).ellipse(
        [px - g.r_disk, py - g.r_disk, px + g.r_disk, py + g.r_disk], fill=255)
    tmp = layer.copy()
    tmp.putalpha(ImageChops.multiply(disk, layer.split()[3]))
    canvas.alpha_composite(tmp)


def build_band(g, primary, accent, outline):
    """Render three seamless periods, downscale, crop the middle period."""
    pts_a = cord_points(g, +1)    # rises first
    pts_b = cord_points(g, -1)    # falls first (mirror)
    layer_a = stroke_layer(g, pts_a, primary, outline)
    layer_b = stroke_layer(g, pts_b, primary, outline)

    canvas = Image.new("RGBA", (3 * g.w, g.h), (0, 0, 0, 0))
    canvas.alpha_composite(layer_a)
    canvas.alpha_composite(layer_b)

    # Crossings sit where the mirrored sines meet: sin(phase) == 0, i.e.
    # x = n * (w / (2*CYCLES)). Over/under alternates by the global index n, so
    # the weave stays consistent across the crop boundary.
    spacing = g.w / (2 * CYCLES)
    n_cross = 3 * 2 * CYCLES
    for n in range(n_cross + 1):
        x = n * spacing
        if n % 2 == 0:
            stamp_over(g, canvas, layer_a, x, g.cy)
        else:
            stamp_over(g, canvas, layer_b, x, g.cy)

    # An accent leaf in every lens-shaped eye (halfway between crossings, on the
    # centre line where the two cords bow apart) -- the sole accent detail.
    d = ImageDraw.Draw(canvas)
    lw, lh, lo = g.leaf_w, g.leaf_h, g.leaf_out
    for n in range(n_cross):
        ex = (n + 0.5) * spacing
        d.ellipse([ex - lw - lo, g.cy - lh - lo, ex + lw + lo, g.cy + lh + lo],
                  fill=outline + (255,))
        d.ellipse([ex - lw, g.cy - lh, ex + lw, g.cy + lh],
                  fill=accent + (255,))

    down = canvas.resize((3 * g.disp_w, g.disp_h), Image.LANCZOS)
    return down.crop((g.disp_w, 0, 2 * g.disp_w, g.disp_h))


def build_rail(band):
    """Rotate the horizontally-seamless band 90 degrees -> a vertically
    seamless rail of width = --height."""
    return band.transpose(Image.ROTATE_90)


# ------------------------------------------------------------ preview (test)
PARCHMENT = (244, 236, 219)
VELLUM = (32, 28, 22)


def _on_bg(strip, bg):
    plate = Image.new("RGBA", strip.size, bg + (255,))
    plate.alpha_composite(strip)
    return plate


def build_preview(band, rail):
    bw, bh = band.size
    rw, rh = rail.size

    band_reps = 6
    strip = Image.new("RGBA", (bw * band_reps, bh), (0, 0, 0, 0))
    for k in range(band_reps):
        strip.alpha_composite(band, (k * bw, 0))
    band_par = _on_bg(strip, PARCHMENT)
    band_vel = _on_bg(strip, VELLUM)

    rail_reps = 5
    col = Image.new("RGBA", (rw, rh * rail_reps), (0, 0, 0, 0))
    for k in range(rail_reps):
        col.alpha_composite(rail, (0, k * rh))
    rail_par = _on_bg(col, PARCHMENT)
    rail_vel = _on_bg(col, VELLUM)

    pad = 16
    band_block_h = bh * 2 + pad
    rail_block_h = rh * rail_reps
    total_w = max(bw * band_reps, rw * 2 + pad)
    total_h = band_block_h + pad + rail_block_h

    out = Image.new("RGBA", (total_w, total_h), (90, 90, 90, 255))
    out.paste(band_par, (0, 0))
    out.paste(band_vel, (0, bh + pad))
    ry = band_block_h + pad
    out.paste(rail_par, (0, ry))
    out.paste(rail_vel, (rw + pad, ry))
    return out


def main():
    ap = argparse.ArgumentParser(description="Celtic interlace style: spiral.")
    ap.add_argument("--primary", type=parse_rgb, default="176,138,62",
                    help="dominant cord colour R,G,B (gold)")
    ap.add_argument("--accent", type=parse_rgb, required=True,
                    help="accent detail colour R,G,B")
    ap.add_argument("--outline", type=parse_rgb, default="42,33,24",
                    help="thin cord outline colour R,G,B (dark sepia)")
    ap.add_argument("--height", type=int, default=88,
                    help="band height in px (also the rail thickness)")
    ap.add_argument("--band", required=True, help="output horizontal band PNG")
    ap.add_argument("--rail", required=True, help="output vertical rail PNG")
    ap.add_argument("--preview", default=None,
                    help="optional: also write a tiled self-test preview PNG")
    args = ap.parse_args()

    g = Geometry(args.height)
    band = build_band(g, args.primary, args.accent, args.outline)
    rail = build_rail(band)

    band.save(args.band)
    rail.save(args.rail)

    if args.preview:
        build_preview(band, rail).save(args.preview)

    print(f"period_px {g.disp_w} band_h {g.disp_h}")
    print(f"wrote {args.band}")
    print(f"wrote {args.rail}")
    if args.preview:
        print(f"wrote {args.preview}")


if __name__ == "__main__":
    main()
