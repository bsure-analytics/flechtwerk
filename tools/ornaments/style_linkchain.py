#!/usr/bin/env python3
"""linkchain -- an interlocked rectangular chain-link band.

Style: a chain of alternating rectangular links, the angular cousin of the
round-ring ``chain`` style. Tall square links alternate with short wide links
threaded through them, every joint a strictly orthogonal crossing (a vertical
bar of one link passing a horizontal bar of the other). Over/under alternates
diagonally around each joint -- the short link rides OVER at the top of one
side and the bottom of the other -- so every link reads as physically threaded
through its neighbours. Thematically at home on the encryption page: links in
a chain, none removable without breaking one.

Colours: PRIMARY (gold leaf) is the link colour; every bar carries a thin
crisp dark-sepia OUTLINE on both edges; the ACCENT appears as a small
sepia-framed diamond stud set in the open centre of every tall link -- a jewel
in the lock. Matte, supersampled >=4x then LANCZOS-downscaled.

Seamless by construction: the centreline layout is exactly periodic in x
(period P), so we render 3 periods and crop the middle one -- every join is
seamless. The BAND is one period (width P). The RAIL is the band rotated 90
degrees, so a horizontally-seamless band becomes a vertically-seamless rail
of width == --height.

Uniform CLI (every docsite style is invoked identically):

    uv run --with pillow --no-project python style_linkchain.py \
        --primary R,G,B --accent R,G,B --outline R,G,B \
        --height INT --band PATH --rail PATH
"""

import argparse

from PIL import Image, ImageChops, ImageDraw

SS = 4  # supersample factor (>= 4), LANCZOS-downscaled for crisp matte edges

# ---------------------------------------------------------------------------
# Fixed unit-grid topology (independent of pixel height).
#
# One period spans 10 grid units in x and the centrelines span y in [-3, +3].
# A TALL link (half-size TALL_W x TALL_H) sits at x == k*P; a SHORT link
# (half-size SHORT_W x SHORT_H) sits at x == k*P + P/2 and overlaps the tall
# links on both sides, threading through their openings.
PERIOD_U = 10.0   # grid units per period in x
TALL_W = 3.0      # tall-link half-width  (bars at x = kP -/+ 3)
TALL_H = 3.0      # tall-link half-height (bars at y = -/+ 3)
SHORT_W = 2.6     # short-link half-width (bars at x = kP+5 -/+ 2.6)
SHORT_H = 1.4     # short-link half-height (bars at y = -/+ 1.4)
SPAN_U = 6.0      # centreline vertical span (y in [-3, 3])
MARGIN_U = 0.95   # top/bottom breathing room (units) so links clear the edge

# Crossings per period, in grid units. The short link's horizontal bars cross
# the tall links' vertical bars at four points per joint pair; the SHORT link
# rides OVER at the top-left and bottom-right of its span, the TALL links at
# the other two -- the diagonal alternation that reads as threading.
CROSS_L_X = TALL_W            # tall link k's right bar
CROSS_R_X = PERIOD_U - TALL_W # tall link k+1's left bar
CROSS_Y = SHORT_H             # short link's bars at y = -/+ SHORT_H

N_TILES = 3       # render 3 periods, crop the middle one


def parse_rgb(text: str) -> tuple[int, int, int]:
    parts = text.split(",")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B but got {text!r}")
    r, g, b = (int(p) for p in parts)
    for v in (r, g, b):
        if not 0 <= v <= 255:
            raise argparse.ArgumentTypeError(f"channel out of range 0..255 in {text!r}")
    return (r, g, b)


class Geometry:
    """All drawing dimensions in supersampled pixels, derived from --height."""

    def __init__(self, height: int):
        self.disp_h = height
        self.disp_w = max(4, round(1.30 * height))  # one period, display px

        self.s = SS
        self.h = height * SS
        # supersampled px per grid unit, split for x and y so the tile is exact
        self.ux = self.disp_w * SS / PERIOD_U
        self.uy = self.h / (SPAN_U + 2 * MARGIN_U)
        self.y0 = self.h / 2.0                      # centre line (y_unit == 0)

        uu = min(self.ux, self.uy)
        self.hw = 0.26 * uu                         # bar half-width (fill)
        self.o = max(1.0, 0.085 * uu)               # outline per edge
        self.stamp = self.hw + 2.4 * self.o         # over-stamp half-side

    def px(self, x_unit: float) -> float:
        # x measured from the left of the FULL (3-period) render; the middle
        # period lands at canvas x in [disp_w, 2*disp_w]*SS.
        return (x_unit + PERIOD_U) * self.ux

    def py(self, y_unit: float) -> float:
        return self.y0 + y_unit * self.uy


def link_vertices(cx: float, half_w: float, half_h: float):
    """Closed rectangular centreline for one link, chained corner to corner."""
    return [
        (cx - half_w, -half_h),
        (cx + half_w, -half_h),
        (cx + half_w, half_h),
        (cx - half_w, half_h),
        (cx - half_w, -half_h),
    ]


def add_segment(draw, g, p, q, half):
    """Stamp one axis-aligned bar segment as a rectangle, extended by `half`
    at both ends so right-angle corners fill with crisp square miters."""
    x0, y0 = g.px(p[0]), g.py(p[1])
    x1, y1 = g.px(q[0]), g.py(q[1])
    xa, xb = min(x0, x1) - half, max(x0, x1) + half
    ya, yb = min(y0, y1) - half, max(y0, y1) + half
    draw.rectangle([xa, ya, xb, yb], fill=255)


def link_layer(g, links, primary, outline):
    """All links of one kind on one transparent RGBA layer: a continuous sepia
    outline with a gold core, built from axis-aligned bar rectangles."""
    fill_m = Image.new("L", (g.w, g.h), 0)
    out_m = Image.new("L", (g.w, g.h), 0)
    fd, od = ImageDraw.Draw(fill_m), ImageDraw.Draw(out_m)
    for verts in links:
        for p, q in zip(verts, verts[1:]):
            add_segment(od, g, p, q, g.hw + g.o)
            add_segment(fd, g, p, q, g.hw)

    layer = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    layer = Image.composite(Image.new("RGBA", (g.w, g.h), outline + (255,)), layer, out_m)
    layer = Image.composite(Image.new("RGBA", (g.w, g.h), primary + (255,)), layer, fill_m)
    return layer


def stamp_over(g, canvas, over_layer, x_unit, y_unit):
    """Re-assert the OVER link within a small square around a crossing, hiding
    the under link and showing the over link's outline breaking it."""
    cx, cy = g.px(x_unit), g.py(y_unit)
    mask = Image.new("L", (g.w, g.h), 0)
    ImageDraw.Draw(mask).rectangle(
        [cx - g.stamp, cy - g.stamp, cx + g.stamp, cy + g.stamp], fill=255)
    combined = ImageChops.multiply(mask, over_layer.split()[3])
    tmp = over_layer.copy()
    tmp.putalpha(combined)
    canvas.alpha_composite(tmp)


def draw_stud(g, canvas, x_unit, y_unit, accent, outline):
    """A small sepia-framed accent diamond set in a tall link's open centre."""
    cx, cy = g.px(x_unit), g.py(y_unit)
    rx, ry = 1.05 * g.ux, 1.05 * g.uy
    ix, iy = rx - 2.6 * g.o, ry - 2.6 * g.o
    d = ImageDraw.Draw(canvas)
    d.polygon([(cx, cy - ry), (cx + rx, cy), (cx, cy + ry), (cx - rx, cy)],
              fill=outline + (255,))
    d.polygon([(cx, cy - iy), (cx + ix, cy), (cx, cy + iy), (cx - ix, cy)],
              fill=accent + (255,))


def render_band(g, primary, accent, outline):
    g.w = g.disp_w * SS * N_TILES
    ks = range(-2, N_TILES + 2)
    tall = [link_vertices(PERIOD_U * k, TALL_W, TALL_H) for k in ks]
    short = [link_vertices(PERIOD_U * k + PERIOD_U / 2, SHORT_W, SHORT_H) for k in ks]

    layer_tall = link_layer(g, tall, primary, outline)
    layer_short = link_layer(g, short, primary, outline)

    canvas = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    canvas.alpha_composite(layer_tall)
    canvas.alpha_composite(layer_short)  # base order: short on top everywhere

    # Fix over/under per crossing across every rendered period: the TALL links
    # ride over at the bottom-left and top-right of each short link's span.
    for k in ks:
        stamp_over(g, canvas, layer_tall, CROSS_L_X + PERIOD_U * k, CROSS_Y)
        stamp_over(g, canvas, layer_tall, CROSS_R_X + PERIOD_U * k, -CROSS_Y)

    for k in ks:
        draw_stud(g, canvas, PERIOD_U * k, 0.0, accent, outline)

    # Crop the middle period -> exactly one seamless period, then LANCZOS down.
    x0 = g.disp_w * SS
    crop = canvas.crop((x0, 0, x0 + g.disp_w * SS, g.h))
    return crop.resize((g.disp_w, g.disp_h), Image.LANCZOS)


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
    band_reps, rail_reps, pad = 6, 5, 16

    strip = Image.new("RGBA", (bw * band_reps, bh), (0, 0, 0, 0))
    for k in range(band_reps):
        strip.alpha_composite(band, (k * bw, 0))
    band_par, band_vel = _on_bg(strip, PARCHMENT), _on_bg(strip, VELLUM)

    col = Image.new("RGBA", (rw, rh * rail_reps), (0, 0, 0, 0))
    for k in range(rail_reps):
        col.alpha_composite(rail, (0, k * rh))
    rail_par, rail_vel = _on_bg(col, PARCHMENT), _on_bg(col, VELLUM)

    band_block_h = bh * 2 + pad
    total_w = max(bw * band_reps, rw * 2 + pad)
    total_h = band_block_h + pad + rh * rail_reps
    out = Image.new("RGBA", (total_w, total_h), (90, 90, 90, 255))
    out.paste(band_par, (0, 0))
    out.paste(band_vel, (0, bh + pad))
    ry = band_block_h + pad
    out.paste(rail_par, (0, ry))
    out.paste(rail_vel, (rw + pad, ry))
    return out


def main():
    ap = argparse.ArgumentParser(description="linkchain rectangular chain-link generator")
    ap.add_argument("--primary", type=parse_rgb, default="176,138,62", help="dominant gold link colour")
    ap.add_argument("--accent", type=parse_rgb, required=True, help="accent stud colour")
    ap.add_argument("--outline", type=parse_rgb, default="42,33,24", help="thin sepia bar outline")
    ap.add_argument("--height", type=int, default=88, help="band height / rail thickness in px")
    ap.add_argument("--band", required=True, help="output path: horizontal band PNG")
    ap.add_argument("--rail", required=True, help="output path: vertical rail PNG")
    ap.add_argument("--preview", default=None, help="optional: also write a tiled self-test preview PNG")
    args = ap.parse_args()

    primary = args.primary if isinstance(args.primary, tuple) else parse_rgb(args.primary)
    outline = args.outline if isinstance(args.outline, tuple) else parse_rgb(args.outline)

    g = Geometry(args.height)
    band = render_band(g, primary, args.accent, outline)
    rail = band.transpose(Image.ROTATE_90)

    band.save(args.band)
    rail.save(args.rail)

    if args.preview:
        build_preview(band, rail).save(args.preview)

    print("band:", args.band, band.size)
    print("rail:", args.rail, rail.size)
    if args.preview:
        print("preview:", args.preview)


if __name__ == "__main__":
    main()
