"""Celtic interlace style "chain": a linked, interwoven chain of rings.

A horizontal chain of overlapping oval rings woven with genuine Celtic
over-under crossings: consecutive rings pass alternately over and under their
neighbours (each ring is *over* at its right-top and left-bottom crossings and
*under* at the other two), so the chain reads as a woven interlace, never a
flat rope or barber-pole.

- The dominant cord colour is the PRIMARY (gold leaf): every chain ring is
  primary, drawn with a thin crisp dark-sepia OUTLINE on both edges.
- The ACCENT cord colour appears as a small ring set inside every loop of the
  chain, framing a tiny primary-gold boss (the jewelled link).

One tile is exactly one integer period, so the band repeats horizontally with
no seam: all drawing is periodic in x with period W and the canvas is exactly
one period (ghost rings on each side wrap the loops that straddle the tile
edge; the accent rings are small enough to never cross the tile boundary).
The RAIL is simply the band rotated 90 degrees, so a horizontally-seamless
band becomes a vertically-seamless rail of width = --height.

Standard library + Pillow only (no numpy). Uniform CLI; run with:
  uv run --with pillow --no-project python style_chain.py \
      --accent 39,74,122 --band band.png --rail rail.png
"""

import argparse

from PIL import Image, ImageChops, ImageDraw

# ---------------------------------------------------------------- structure
SS = 4                       # supersample factor (render big, LANCZOS down)
N = 4                        # rings per horizontal period
BASE_H = 88                  # reference height the geometry was tuned at


def parse_rgb(text: str) -> tuple[int, int, int]:
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B, got {text!r}")
    try:
        r, g, b = (int(p) for p in parts)
    except ValueError:
        raise argparse.ArgumentTypeError(f"R,G,B must be integers: {text!r}")
    if not all(0 <= c <= 255 for c in (r, g, b)):
        raise argparse.ArgumentTypeError(f"R,G,B must be 0..255: {text!r}")
    return (r, g, b)


class Geometry:
    """All drawing dimensions in supersample space, derived from --height."""

    def __init__(self, height: int):
        self.disp_h = height
        # Period width tuned to match the reference proportions (~0.636 * H
        # per ring). Kept a multiple of N so S = W / N is exact -> the ghost
        # ring wrap lands perfectly and the tile stays seamless.
        s_disp = max(2, round(0.636 * height))
        self.disp_w = N * s_disp

        self.h = height * SS
        self.w = self.disp_w * SS
        self.s = s_disp * SS                 # ring centre spacing
        self.y_c = self.h // 2

        k = self.h / (BASE_H * SS)           # uniform scale from the reference
        self.rx = 150 * k                    # ring centreline semi-axis x
        self.ry = 132 * k                    # ring centreline semi-axis y
        self.band = 40 * k                   # cord total thickness
        self.outline = max(2, round(7 * k))  # sepia outline per edge
        self.r_disk = 58 * k                 # over-strand stamp radius

        # jewelled accent ring set inside each loop (fits inside the hole:
        # radius < S/2 so it never crosses the tile boundary -> seamless).
        self.acc_r = 52 * k
        self.acc_band = 20 * k
        self.acc_out = max(2, round(6 * k))
        self.boss_r = 22 * k
        self.boss_out = max(1, round(5 * k))


def annulus_mask(g, cx, cy, a_out, b_out, a_in, b_in):
    """L mask: filled band between an outer and an inner ellipse."""
    m = Image.new("L", (g.w, g.h), 0)
    d = ImageDraw.Draw(m)
    d.ellipse([cx - a_out, cy - b_out, cx + a_out, cy + b_out], fill=255)
    d.ellipse([cx - a_in, cy - b_in, cx + a_in, cy + b_in], fill=0)
    return m


def ring_layer(g, cx, color, outline):
    """A single ring cord on its own transparent RGBA layer: sepia outline on
    both edges, primary core, transparent hole."""
    layer = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    ink_img = Image.new("RGBA", (g.w, g.h), outline + (255,))
    color_img = Image.new("RGBA", (g.w, g.h), color + (255,))

    hw = g.band / 2
    o = g.outline
    band = annulus_mask(g, cx, g.y_c, g.rx + hw, g.ry + hw, g.rx - hw, g.ry - hw)
    core = annulus_mask(g, cx, g.y_c,
                        g.rx + hw - o, g.ry + hw - o,
                        g.rx - hw + o, g.ry - hw + o)

    layer = Image.composite(ink_img, layer, band)
    layer = Image.composite(color_img, layer, core)
    return layer


def stamp_over(g, canvas, over_layer, px, py):
    """Paste the over-strand's cord (with outline) atop the canvas within a
    disk around a crossing, hiding the under strand there."""
    disk = Image.new("L", (g.w, g.h), 0)
    ImageDraw.Draw(disk).ellipse(
        [px - g.r_disk, py - g.r_disk, px + g.r_disk, py + g.r_disk], fill=255)
    alpha = over_layer.split()[3]
    combined = ImageChops.multiply(disk, alpha)
    tmp = over_layer.copy()
    tmp.putalpha(combined)
    canvas.alpha_composite(tmp)


def draw_jewel(g, canvas, cx, cy, primary, accent, outline):
    """A small accent ring (sepia outline) framing a tiny primary boss — the
    jewelled link set inside a loop of the chain."""
    ring = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    ink_img = Image.new("RGBA", (g.w, g.h), outline + (255,))
    acc_img = Image.new("RGBA", (g.w, g.h), accent + (255,))
    r, b, o = g.acc_r, g.acc_band, g.acc_out
    band = annulus_mask(g, cx, cy, r, r, r - b, r - b)
    core = annulus_mask(g, cx, cy, r - o, r - o, r - b + o, r - b + o)
    ring = Image.composite(ink_img, ring, band)
    ring = Image.composite(acc_img, ring, core)
    canvas.alpha_composite(ring)

    d = ImageDraw.Draw(canvas)
    d.ellipse([cx - g.boss_r, cy - g.boss_r, cx + g.boss_r, cy + g.boss_r],
              fill=outline + (255,))
    r2 = g.boss_r - g.boss_out
    d.ellipse([cx - r2, cy - r2, cx + r2, cy + r2], fill=primary + (255,))


def build_band(g, primary, accent, outline):
    """Render one seamless horizontal period, downscaled with LANCZOS."""
    # Ghost rings on each side so loops straddling the tile edge wrap
    # correctly (everything is periodic in x with period W -> seamless).
    layers = {i: ring_layer(g, g.s // 2 + i * g.s, primary, outline)
              for i in range(-1, N + 1)}

    canvas = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    for i in range(-1, N + 1):
        canvas.alpha_composite(layers[i])

    # Over/under rule (global, hence periodic and consistent): at the TOP
    # crossing the LEFT ring is over; at the BOTTOM crossing the RIGHT ring is
    # over. Each ring is therefore over at its right-top and left-bottom
    # crossings and under at the other two -> genuine woven alternation.
    dy = g.ry * (1 - (g.s / (2 * g.rx)) ** 2) ** 0.5
    y_top = int(round(g.y_c - dy))
    y_bot = int(round(g.y_c + dy))
    for i in range(-1, N):
        px = (i + 1) * g.s        # crossing x between ring i and ring i+1
        stamp_over(g, canvas, layers[i], px, y_top)      # left ring over (top)
        stamp_over(g, canvas, layers[i + 1], px, y_bot)  # right ring over (bot)

    # A jewelled accent link inside every loop (periodic, small enough to stay
    # clear of the tile edges -> seamless).
    for i in range(N):
        draw_jewel(g, canvas, g.s // 2 + i * g.s, g.y_c, primary, accent, outline)

    return canvas.resize((g.disp_w, g.disp_h), Image.LANCZOS)


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
    ap = argparse.ArgumentParser(description="Celtic interlace style: chain.")
    ap.add_argument("--primary", type=parse_rgb, default="176,138,62",
                    help="dominant cord colour R,G,B (gold)")
    ap.add_argument("--accent", type=parse_rgb, required=True,
                    help="accent cord colour R,G,B")
    ap.add_argument("--outline", type=parse_rgb, default="42,33,24",
                    help="thin cord outline colour R,G,B (dark sepia)")
    ap.add_argument("--height", type=int, default=88,
                    help="band height in px (also the rail thickness)")
    ap.add_argument("--band", required=True, help="output horizontal band PNG")
    ap.add_argument("--rail", required=True, help="output vertical rail PNG")
    ap.add_argument("--preview", default=None,
                    help="optional: also write a tiled self-test preview PNG")
    args = ap.parse_args()

    primary = args.primary if isinstance(args.primary, tuple) else parse_rgb(args.primary)
    outline = args.outline if isinstance(args.outline, tuple) else parse_rgb(args.outline)

    g = Geometry(args.height)
    band = build_band(g, primary, args.accent, outline)
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
