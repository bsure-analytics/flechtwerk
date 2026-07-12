#!/usr/bin/env python3
"""stepfret -- an angular Celtic STEP / KEY (meander / fret) interlace band.

Style: a crisp right-angled key pattern. Two identical gold meander cords run
left->right, one a half-period phase shift of the other, weaving through each
other with strictly ORTHOGONAL crossings (a vertical arm of one cord passing a
horizontal arm of the other -- a clean "+" crossing, the sharpest possible
over-under). Over/under alternates: cord A rides OVER at the first crossing of
every period, cord B rides OVER at the second. The stepped path -- long arm,
notch, centre run -- gives the angular staircase silhouette of a Celtic key,
welcome variety against the curvier interlace styles.

Colours: PRIMARY (gold leaf) is the dominant cord colour; every cord carries a
thin crisp dark-sepia OUTLINE on both edges; the ACCENT appears as a small
square stud (sepia-framed) set in the open cell of every fret step. Matte,
supersampled >=4x then LANCZOS-downscaled.

Seamless by construction: the centreline is exactly periodic in x (period P),
so we render 3 periods and crop the middle one -- every join is seamless. The
BAND is one period (width P). The RAIL is the band rotated 90 degrees, so a
horizontally-seamless band becomes a vertically-seamless rail of width
== --height.

Uniform CLI (every docsite style is invoked identically):

    uv run --with pillow --no-project python style_stepfret.py \
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
# Cord A's centreline over one period (a stepped key: long left arm down, a
# notch up to the centre, a centre run, then up to the top):
A_BASE = [
    (2.5, -3.0),  # top of the full-height left arm
    (2.5, 3.0),   # DOWN  -- full vertical arm  (crosses B's centre run: X1)
    (5.0, 3.0),   # right along the bottom
    (5.0, 0.0),   # UP to the centre (the notch)
    (8.5, 0.0),   # right along the centre run  (crossed by B's arm: X2)
    (8.5, -3.0),  # UP to the top
    (12.5, -3.0), # top run -> joins the next period's first vertex
]
PERIOD_U = 10.0   # grid units per period in x
PHASE_U = 5.0     # cord B is cord A shifted half a period in x
SPAN_U = 6.0      # centreline vertical span (y in [-3, 3])
MARGIN_U = 0.95   # top/bottom breathing room (units) so cords clear the edge

# Crossings per period, in grid units, with the cord that rides OVER.
# X1 = (2.5, 0): A's full arm over B's centre run.
# X2 = (7.5, 0): B's full arm over A's centre run.
CROSS_A_X = 2.5   # A-over crossing x within a period
CROSS_B_X = 7.5   # B-over crossing x within a period
CROSS_Y = 0.0

# Accent studs: centre (in grid units) of the open cell of each fret step.
STUDS_U = [(6.75, -1.5), (1.75, 1.5)]

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
        self.u = self.disp_w / PERIOD_U             # display px per grid unit

        self.s = SS
        self.h = height * SS
        # supersampled px per grid unit, split for x and y so the tile is exact
        self.ux = self.disp_w * SS / PERIOD_U
        self.uy = self.h / (SPAN_U + 2 * MARGIN_U)
        self.y0 = self.h / 2.0                      # centre line (y_unit == 0)

        uu = min(self.ux, self.uy)
        self.hw = 0.28 * uu                         # cord half-width (fill)
        self.o = max(1.0, 0.085 * uu)               # outline per edge
        self.stamp = self.hw + 2.4 * self.o         # over-stamp half-side

    def px(self, x_unit: float) -> float:
        # x measured from the left of the FULL (3-period) render; period 0
        # starts at x_unit == 2.5 - PERIOD_U in A_BASE terms, so we offset so
        # that the middle period lands at canvas x in [disp_w, 2*disp_w]*SS.
        return (x_unit + PERIOD_U) * self.ux

    def py(self, y_unit: float) -> float:
        return self.y0 + y_unit * self.uy


def cord_vertices(base):
    """Chain the per-period base polyline across the full 3-period render
    (plus a guard period each side so wrapping arms are present)."""
    verts = []
    for k in range(-2, N_TILES + 2):
        seg = [(x + PERIOD_U * k, y) for (x, y) in base]
        if verts and verts[-1] == seg[0]:
            seg = seg[1:]
        verts.extend(seg)
    return verts


def add_segment(draw, g, p, q, half):
    """Stamp one axis-aligned cord segment as a rectangle, extended by `half`
    at both ends so right-angle corners fill with crisp square miters."""
    x0, y0 = g.px(p[0]), g.py(p[1])
    x1, y1 = g.px(q[0]), g.py(q[1])
    xa, xb = min(x0, x1) - half, max(x0, x1) + half
    ya, yb = min(y0, y1) - half, max(y0, y1) + half
    draw.rectangle([xa, ya, xb, yb], fill=255)


def cord_layer(g, verts, primary, outline):
    """One cord on its own transparent RGBA layer: a continuous sepia outline
    with a gold core, built from axis-aligned segment rectangles."""
    fill_m = Image.new("L", (g.w, g.h), 0)
    out_m = Image.new("L", (g.w, g.h), 0)
    fd, od = ImageDraw.Draw(fill_m), ImageDraw.Draw(out_m)
    for p, q in zip(verts, verts[1:]):
        add_segment(od, g, p, q, g.hw + g.o)
        add_segment(fd, g, p, q, g.hw)

    layer = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    layer = Image.composite(Image.new("RGBA", (g.w, g.h), outline + (255,)), layer, out_m)
    layer = Image.composite(Image.new("RGBA", (g.w, g.h), primary + (255,)), layer, fill_m)
    return layer


def stamp_over(g, canvas, over_layer, x_unit, y_unit):
    """Re-assert the OVER cord within a small square around a crossing, hiding
    the under cord and showing the over cord's outline breaking it."""
    cx, cy = g.px(x_unit), g.py(y_unit)
    mask = Image.new("L", (g.w, g.h), 0)
    ImageDraw.Draw(mask).rectangle(
        [cx - g.stamp, cy - g.stamp, cx + g.stamp, cy + g.stamp], fill=255)
    combined = ImageChops.multiply(mask, over_layer.split()[3])
    tmp = over_layer.copy()
    tmp.putalpha(combined)
    canvas.alpha_composite(tmp)


def draw_stud(g, canvas, x_unit, y_unit, accent, outline):
    """A small sepia-framed accent square set in an open fret cell."""
    cx, cy = g.px(x_unit), g.py(y_unit)
    r = 0.9 * g.hw + g.o
    ri = r - g.o - 0.35 * g.o
    d = ImageDraw.Draw(canvas)
    d.rectangle([cx - r, cy - r, cx + r, cy + r], fill=outline + (255,))
    d.rectangle([cx - ri, cy - ri, cx + ri, cy + ri], fill=accent + (255,))


def render_band(g, primary, accent, outline):
    g.w = g.disp_w * SS * N_TILES
    verts_a = cord_vertices(A_BASE)
    verts_b = cord_vertices([(x + PHASE_U, y) for (x, y) in A_BASE])

    layer_a = cord_layer(g, verts_a, primary, outline)
    layer_b = cord_layer(g, verts_b, primary, outline)

    canvas = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    canvas.alpha_composite(layer_a)
    canvas.alpha_composite(layer_b)   # base order: B on top everywhere

    # Fix over/under per crossing across every rendered period.
    for k in range(-2, N_TILES + 2):
        stamp_over(g, canvas, layer_a, CROSS_A_X + PERIOD_U * k, CROSS_Y)  # A over
        stamp_over(g, canvas, layer_b, CROSS_B_X + PERIOD_U * k, CROSS_Y)  # B over

    for k in range(-2, N_TILES + 2):
        for sx, sy in STUDS_U:
            draw_stud(g, canvas, sx + PERIOD_U * k, sy, accent, outline)

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
    ap = argparse.ArgumentParser(description="stepfret Celtic key/fret interlace generator")
    ap.add_argument("--primary", type=parse_rgb, default="176,138,62", help="dominant gold cord colour")
    ap.add_argument("--accent", type=parse_rgb, required=True, help="accent stud colour")
    ap.add_argument("--outline", type=parse_rgb, default="42,33,24", help="thin sepia cord outline")
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
