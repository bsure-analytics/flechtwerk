#!/usr/bin/env python3
"""Celtic interlace ornament generator - style "triquetra".

A running border built from a chain of triquetra (trinity) knots. Each knot is
a genuine trefoil ribbon: a single closed cord with three lobes and three
self-crossings woven strictly over-under-over (the trefoil knot *is* the
triquetra, topologically). The knots are laid point-up in a row and interlink
at their lower lobes so the border reads as one continuous chain, and a colour
rhythm runs GOLD, GOLD, ACCENT along it -- gold stays dominant while the accent
knot gives the row its beat.

Weaving is decided by a per-cord height field z: every sampled point of every
cord carries a z, and wherever two cords cross the point with the greater z runs
*over*. Inside one knot z = sin(3t) makes the three self-crossings alternate
automatically; between knots a small per-knot phase makes neighbouring links
lace over/under each other like true chain links. The "under" strand is
physically cut with a gap under the "over" strand's footprint, so every outline
strokes in one pass and every fill in a second -- crisp matte cords, no seams
between passes.

Uniform CLI (every style script is invoked identically by the docsite driver):

    uv run --with pillow --no-project python style_triquetra.py \
        --accent R,G,B [--primary R,G,B] [--outline R,G,B] [--height N] \
        --band band.png --rail rail.png [--preview preview.png]

The BAND is exactly one seamless horizontal period (three knots wide, tiles
left-to-right with no seam). The RAIL is that band rotated 90 degrees (tiles
top-to-bottom). Only cords are opaque; everything else is transparent RGBA.
Rendered at >=4x supersampling and LANCZOS-downscaled for crisp matte edges.
Standard library + Pillow only (no numpy).
"""

from __future__ import annotations

import argparse
import math

from PIL import Image, ImageDraw

# --- supersampling ----------------------------------------------------------

SS = 4  # >= 3 required; render this many times larger, then LANCZOS down.

# One period is a colour rhythm of this many interlinked trinity knots.
KNOTS_PER_PERIOD = 3

# Outline half-thickness per side, in supersampled px. Set in render_band().
OUTLINE_PX = 0.0


# --- colour helpers ---------------------------------------------------------


def parse_color(text: str) -> tuple[int, int, int]:
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected 'R,G,B', got {text!r}")
    try:
        r, g, b = (int(p) for p in parts)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"non-integer channel in {text!r}") from exc
    for c in (r, g, b):
        if not 0 <= c <= 255:
            raise argparse.ArgumentTypeError(f"channel out of range in {text!r}")
    return r, g, b


# --- geometry ---------------------------------------------------------------


class Cord:
    """A single ribbon: a poly-line of (x, y, z) samples plus widths/colour.

    z is the weave height (bigger = runs over). fill_w / full_w are the coloured
    core width and the outer (outline-included) width, in supersampled pixels.
    """

    def __init__(self, pts, color, fill_w, full_w, closed):
        self.pts = pts  # list[(x, y, z)]
        self.color = color
        self.fill_w = fill_w
        self.full_w = full_w
        self.closed = closed
        self.keep: list[bool] = []


def _trefoil_raw(t: float) -> tuple[float, float, float]:
    """Classic 3-fold trefoil projection with a weave-height z.

    x = sin t + 2 sin 2t ; y = cos t - 2 cos 2t gives a three-lobed curve with
    three self-crossings. z = sin 3t alternates the over/under around the loop
    so the three crossings weave over-under-over on their own.
    """
    x = math.sin(t) + 2.0 * math.sin(2.0 * t)
    y = math.cos(t) - 2.0 * math.cos(2.0 * t)
    z = math.sin(3.0 * t)
    return x, y, z


def _orientation_rotation(samples: int = 1440) -> float:
    """Rotation that lifts one lobe to straight up (90 deg).

    Lobes sit at the maxima of the radius; rotate the whole curve so one of them
    points up, which puts the other two down-left and down-right -- the iconic
    point-up triquetra, and exactly the pair of lower lobes that interlink with
    the neighbouring knots to form the chain.
    """
    best_t, best_r = 0.0, -1.0
    for i in range(samples):
        t = 2.0 * math.pi * i / samples
        x, y, _ = _trefoil_raw(t)
        r = x * x + y * y
        if r > best_r:
            best_r, best_t = r, t
    x, y, _ = _trefoil_raw(best_t)
    return math.pi / 2.0 - math.atan2(y, x)


def _unit_trefoil(rot: float, n: int) -> list[tuple[float, float, float]]:
    """One oriented, unit-normalised trefoil loop (centred, vertical extent 1)."""
    cos_r, sin_r = math.cos(rot), math.sin(rot)
    raw = [
        (
            (math.sin(t) + 2.0 * math.sin(2.0 * t)) * cos_r
            - (math.cos(t) - 2.0 * math.cos(2.0 * t)) * sin_r,
            (math.sin(t) + 2.0 * math.sin(2.0 * t)) * sin_r
            + (math.cos(t) - 2.0 * math.cos(2.0 * t)) * cos_r,
            math.sin(3.0 * t),
        )
        for t in (2.0 * math.pi * i / n for i in range(n))
    ]
    xs = [p[0] for p in raw]
    ys = [p[1] for p in raw]
    cx = (min(xs) + max(xs)) / 2.0
    cy = (min(ys) + max(ys)) / 2.0
    scale = 1.0 / (max(ys) - min(ys))
    return [((x - cx) * scale, (y - cy) * scale, z) for x, y, z in raw]


def build_cords(height_px: float, primary, accent) -> tuple[list[Cord], int]:
    """Build every cord of the band and return (cords, period_in_ss_px).

    The period is snapped to a multiple of SS so the downscaled tile has an
    integer width and its two vertical edges sit exactly one geometric period
    apart -- a perfect horizontal seam.
    """
    rot = _orientation_rotation()
    unit = _unit_trefoil(rot, n=600)  # normalised loop, vertical extent == 1
    unit_xs = [p[0] for p in unit]
    unit_w = max(unit_xs) - min(unit_xs)  # knot width when its height is 1

    # Fit a knot into the band, leaving room for the ribbon's own thickness so
    # the top/bottom lobes do not clip the band edges.
    gold_fill = 0.10 * height_px
    gold_full = gold_fill + 2.0 * OUTLINE_PX
    knot_v = height_px - gold_full - 0.09 * height_px
    knot_w = knot_v * unit_w

    # Knot spacing: lace only the outer tips of the lower lobes with the
    # neighbours so each triquetra keeps its identity while the row still reads
    # as one continuous chain. One period == KNOTS_PER_PERIOD knots.
    step = knot_w * 0.92
    period = int(round(KNOTS_PER_PERIOD * step / SS)) * SS
    step = period / KNOTS_PER_PERIOD  # exact, so the colour rhythm stays periodic
    yc = height_px * 0.5

    scaled = [(x * knot_v, y * knot_v, z) for x, y, z in unit]

    # A small per-knot phase (period 3) tips the height field of neighbouring
    # knots the opposite way at their shared crossings, so consecutive links
    # lace over/under each other -- real chain links, not stacked blobs.
    cords: list[Cord] = []
    for k in range(-4, 3 * KNOTS_PER_PERIOD + 4):
        cx = step * k
        bias = 0.9 * math.sin(2.0 * math.pi * k / 3.0)
        color = accent if (k % KNOTS_PER_PERIOD) == (KNOTS_PER_PERIOD - 1) else primary
        loop = [(x + cx, yc - y, z + bias) for x, y, z in scaled]  # y down on screen
        cords.append(Cord(loop, color, gold_fill, gold_full, closed=True))

    return cords, period


# --- crossing detection & weave-driven gap cutting --------------------------


def _seg_intersect(p1, p2, p3, p4):
    """Return (ix, iy, t, u) if segments p1-p2 and p3-p4 properly cross."""
    x1, y1 = p1[0], p1[1]
    x2, y2 = p2[0], p2[1]
    x3, y3 = p3[0], p3[1]
    x4, y4 = p4[0], p4[1]
    rx, ry = x2 - x1, y2 - y1
    sx, sy = x4 - x3, y4 - y3
    denom = rx * sy - ry * sx
    if abs(denom) < 1e-9:
        return None
    qpx, qpy = x3 - x1, y3 - y1
    t = (qpx * sy - qpy * sx) / denom
    u = (qpx * ry - qpy * rx) / denom
    if 0.0 <= t <= 1.0 and 0.0 <= u <= 1.0:
        return x1 + t * rx, y1 + t * ry, t, u
    return None


def _cut_under_cords(cords: list[Cord]) -> None:
    """Find every crossing; cut a gap in the lower-z (under) cord at each.

    x-bucketing keeps this to near-neighbour segment tests. Each cord gets a
    keep-mask the draw step splits into runs.
    """
    for c in cords:
        c.keep = [True] * len(c.pts)

    segs = []
    for ci, c in enumerate(cords):
        n = len(c.pts)
        last = n if c.closed else n - 1
        for i in range(last):
            segs.append((ci, i, (i + 1) % n))

    all_x = [p[0] for c in cords for p in c.pts]
    minx, maxx = min(all_x), max(all_x)
    nb = max(1, int((maxx - minx) / 40) + 1)
    bw = (maxx - minx) / nb + 1e-6
    buckets: dict[int, list[int]] = {}
    for si, (ci, i, j) in enumerate(segs):
        a = cords[ci].pts[i]
        b = cords[ci].pts[j]
        lo = int((min(a[0], b[0]) - minx) / bw)
        hi = int((max(a[0], b[0]) - minx) / bw)
        for bkt in range(lo, hi + 1):
            buckets.setdefault(bkt, []).append(si)

    seen: set[tuple[int, int]] = set()
    for sis in buckets.values():
        m = len(sis)
        for a_ in range(m):
            sa = sis[a_]
            ca, ia, ja = segs[sa]
            for b_ in range(a_ + 1, m):
                sb = sis[b_]
                key = (sa, sb) if sa < sb else (sb, sa)
                if key in seen:
                    continue
                seen.add(key)
                cb, ib, jb = segs[sb]
                if ca == cb:
                    n = len(cords[ca].pts)
                    d = abs(ia - ib)
                    d = min(d, n - d)
                    if d <= 2:
                        continue  # neighbouring segments of one cord
                pa1 = cords[ca].pts[ia]
                pa2 = cords[ca].pts[ja]
                pb1 = cords[cb].pts[ib]
                pb2 = cords[cb].pts[jb]
                hit = _seg_intersect(pa1, pa2, pb1, pb2)
                if hit is None:
                    continue
                ix, iy, t, u = hit
                za = pa1[2] + t * (pa2[2] - pa1[2])
                zb = pb1[2] + u * (pb2[2] - pb1[2])
                dax, day = pa2[0] - pa1[0], pa2[1] - pa1[1]
                dbx, dby = pb2[0] - pb1[0], pb2[1] - pb1[1]
                la = math.hypot(dax, day) or 1e-9
                lb = math.hypot(dbx, dby) or 1e-9
                sin_th = abs((dax * dby - day * dbx) / (la * lb))
                sin_th = max(sin_th, 0.34)  # clamp for shallow crossings
                if za >= zb:
                    over, under = cords[ca], cords[cb]
                else:
                    over, under = cords[cb], cords[ca]
                gap = (over.full_w / 2.0) / sin_th + OUTLINE_PX
                gap2 = gap * gap
                for pi in range(len(under.pts)):
                    px, py, _ = under.pts[pi]
                    if (px - ix) ** 2 + (py - iy) ** 2 <= gap2:
                        under.keep[pi] = False


def _runs(cord: Cord) -> list[list[tuple[float, float, float]]]:
    """Split a cord into drawable poly-lines at its cut gaps."""
    n = len(cord.pts)
    keep = cord.keep
    if cord.closed:
        if all(keep):
            return [cord.pts + [cord.pts[0]]]
        start = next((i for i in range(n) if not keep[i]), 0)
        order = [(start + k) % n for k in range(n)]
    else:
        order = list(range(n))
    out: list[list] = []
    cur: list = []
    for idx in order:
        if keep[idx]:
            cur.append(cord.pts[idx])
        elif cur:
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return [r for r in out if len(r) >= 2]


# --- rasterisation ----------------------------------------------------------


def _stroke(draw, pts, color, width):
    """Thick round-jointed poly-line: a disc at every vertex plus a thick
    segment between consecutive vertices. Avoids the spike artefacts of
    Pillow's joint="curve" on dense, gently-curving paths.
    """
    r = width / 2.0
    w = max(1, int(round(width)))
    for p in pts:
        draw.ellipse((p[0] - r, p[1] - r, p[0] + r, p[1] + r), fill=color)
    for a, b in zip(pts, pts[1:]):
        draw.line([(a[0], a[1]), (b[0], b[1])], fill=color, width=w)


def render_band(height_final: int, primary, accent, outline):
    """Render one seamless period at supersample, then LANCZOS-downscale."""
    global OUTLINE_PX
    OUTLINE_PX = 2.2 * SS

    height_ss = height_final * SS
    cords, period = build_cords(height_ss, primary, accent)

    # Crop exactly one period. Knot centres sit at multiples of `step`; put the
    # left crop edge half a period left of the knot at k = KNOTS_PER_PERIOD so
    # the window is fully surrounded by neighbours on both sides. `period` is a
    # multiple of SS -> crop and downscale land on integer boundaries and the
    # two edges are exactly one geometric period apart -> perfect seam.
    x_left = period + period // 2
    crop_w = period
    canvas_w = x_left + crop_w + period

    img = Image.new("RGBA", (canvas_w, height_ss), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    _cut_under_cords(cords)

    for c in cords:  # pass 1: every outline (dark, wide)
        for run in _runs(c):
            _stroke(draw, run, outline, c.full_w)
    for c in cords:  # pass 2: every fill (colour, narrow)
        for run in _runs(c):
            _stroke(draw, run, c.color, c.fill_w)

    band_ss = img.crop((x_left, 0, x_left + crop_w, height_ss))
    return band_ss.resize((crop_w // SS, height_final), Image.LANCZOS)


# --- self-test preview ------------------------------------------------------

PARCHMENT = (244, 236, 219)
VELLUM = (32, 28, 22)


def _on_bg(strip, bg):
    plate = Image.new("RGBA", strip.size, bg + (255,))
    plate.alpha_composite(strip)
    return plate


def build_preview(band, rail):
    """Band tiled 6x and rail tiled 5x, each on parchment and dark vellum."""
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
    total_w = max(bw * band_reps, rw * 2 + pad)
    total_h = band_block_h + pad + rh * rail_reps

    out = Image.new("RGBA", (total_w, total_h), (90, 90, 90, 255))
    out.paste(band_par, (0, 0))
    out.paste(band_vel, (0, bh + pad))
    ry = band_block_h + pad
    out.paste(rail_par, (0, ry))
    out.paste(rail_vel, (rw + pad, ry))
    return out


# --- CLI --------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Celtic triquetra-chain ornament.")
    ap.add_argument("--primary", type=parse_color, default="176,138,62")
    ap.add_argument("--accent", type=parse_color, required=True)
    ap.add_argument("--outline", type=parse_color, default="42,33,24")
    ap.add_argument("--height", type=int, default=88)
    ap.add_argument("--band", required=True)
    ap.add_argument("--rail", required=True)
    ap.add_argument("--preview", default=None,
                    help="optional: also write a tiled self-test preview PNG")
    args = ap.parse_args()

    primary = (*args.primary, 255)
    accent = (*args.accent, 255)
    outline = (*args.outline, 255)

    band = render_band(args.height, primary, accent, outline)
    band.save(args.band)

    # Rail: rotate the horizontally-seamless band 90 deg -> vertically seamless.
    rail = band.rotate(90, expand=True)
    rail.save(args.rail)

    if args.preview:
        build_preview(band, rail).save(args.preview)

    print(f"period_px {band.size[0]} band_h {band.size[1]}")
    print(f"wrote {args.band}")
    print(f"wrote {args.rail}")
    if args.preview:
        print(f"wrote {args.preview}")


if __name__ == "__main__":
    main()
