"""Celtic interlace style "spiral": a running spiral / wave-scroll border.

A dominant gold cord flows the length of the band, rolling into a prominent
self-crossing curl (a spiral eye) once per period and swooping back out. A
thinner accent cord runs mirrored and offset by half a period, so gold up-curls
and accent down-curls alternate — a continuous flowing scroll of linked
S-spirals. A small gold-ringed accent boss marks every curl's eye.

Genuine Celtic weaving, not a flat rope or barber-pole:

- Every loop is a real self-crossing curl; the flowing connector cord passes
  *over* the curl it meets, so each spiral reads as a rolled-under scroll.
- Where the gold and accent cords cross between the curls they weave over/under
  and *alternate* along the band (over, under, over, ...), the hallmark of true
  interlace.
- The gold PRIMARY cord is thick and dominant; the ACCENT cord is a slimmer
  companion. Both carry a thin crisp dark-sepia OUTLINE on every edge.

Seamlessness: all geometry is a prolate cycloid, exactly periodic with an
integer period W. Three periods are rendered and LANCZOS-downscaled together,
then the middle period is cropped out, so every crossing that straddles a tile
edge is drawn from real neighbouring content — the band tiles left-to-right
with no seam. The RAIL is the band rotated 90 degrees, so a horizontally
seamless band becomes a vertically seamless rail of width = --height.

Standard library + Pillow only (no numpy). Uniform CLI; run with:
  uv run --with pillow --no-project python style_spiral.py \
      --accent 47,109,94 --band band.png --rail rail.png
"""

import argparse
import math

from PIL import Image, ImageChops, ImageDraw

SS = 4          # supersample factor (render big, LANCZOS down)


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
        self.h = height * SS
        self.cy = self.h / 2

        # Advancing epitrochoid (a prolate cycloid rounded by a 2nd harmonic):
        #   x = a*t - b*sin(t) - c*sin(2t),  y = cy -/+ (b*cos(t) + c*cos(2t)).
        # b makes a loop (a curl) once per period; the c term rounds that
        # teardrop into a near-circular coil (a spiral eye) whose neighbours
        # overlap, so the curls link into a continuous running scroll.
        self.b = 0.206 * self.h
        self.c = 0.120 * self.h
        a0 = self.b / 2.32
        # Integer display period so S = W/period is exact -> seamless crop.
        self.disp_w = max(4, round(2 * math.pi * a0 / SS))
        self.w = self.disp_w * SS               # one period, supersampled
        self.a = self.w / (2 * math.pi)         # exact period == self.w

        # Two identical gold cords weave into a symmetric double scroll, so the
        # band reads as all-gold interlace. Each carries a crisp sepia outline
        # (per edge).
        self.r_core = 0.083 * self.h
        self.r_out = self.r_core + 0.030 * self.h

        # Sampling step in t (fine enough that stamped disks overlap smoothly).
        self.dt = 0.015

        # Gold-ringed accent boss at each curl's eye (the spiral centre) —
        # the only place the accent colour appears.
        self.boss_r = 0.060 * self.h
        self.boss_ring = max(2, round(0.020 * self.h))


# ------------------------------------------------------------------ geometry
def cord_points(g, y_sign, x_shift):
    """Prolate-cycloid polyline over enough t to cover x in [-W, 2W]."""
    pts = []
    t = -9.0
    while t <= 15.0:
        x = (g.a * t - g.b * math.sin(t) - g.c * math.sin(2 * t)
             + g.w + x_shift)                              # +g.w -> draw origin
        y = g.cy + y_sign * (-(g.b * math.cos(t) + g.c * math.cos(2 * t)))
        pts.append((x, y))
        t += g.dt
    return pts


def seg_int(p1, p2, p3, p4):
    """Intersection point of segments p1p2 and p3p4, or None."""
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3
    x4, y4 = p4
    d = (x2 - x1) * (y4 - y3) - (y2 - y1) * (x4 - x3)
    if d == 0:
        return None
    ua = ((x3 - x1) * (y4 - y3) - (y3 - y1) * (x4 - x3)) / d
    ub = ((x3 - x1) * (y2 - y1) - (y3 - y1) * (x2 - x1)) / d
    if 0.0 <= ua <= 1.0 and 0.0 <= ub <= 1.0:
        return (x1 + ua * (x2 - x1), y1 + ua * (y2 - y1))
    return None


def find_self_loops(pts, gap):
    """Self-crossing loops as (i1, i2, point) index intervals (i1 < i2)."""
    loops = []
    n = len(pts)
    used = [False] * n
    for i in range(n - 1):
        if used[i]:
            continue
        xa0, ya0 = pts[i]
        xa1, ya1 = pts[i + 1]
        lo_x, hi_x = (xa0, xa1) if xa0 < xa1 else (xa1, xa0)
        lo_y, hi_y = (ya0, ya1) if ya0 < ya1 else (ya1, ya0)
        for j in range(i + gap, n - 1):
            xb0, yb0 = pts[j]
            xb1, yb1 = pts[j + 1]
            if max(xb0, xb1) < lo_x or min(xb0, xb1) > hi_x:
                continue
            if max(yb0, yb1) < lo_y or min(yb0, yb1) > hi_y:
                continue
            hit = seg_int(pts[i], pts[i + 1], pts[j], pts[j + 1])
            if hit is not None:
                loops.append((i, j, hit))
                for k in range(i, j + 1):
                    if 0 <= k < n:
                        used[k] = True
                break
    return loops


def find_inter(ptsA, ptsB, min_gap):
    """Crossings between cord A and cord B as (iA, iB, point)."""
    hits = []
    nA, nB = len(ptsA), len(ptsB)
    for i in range(nA - 1):
        xa0, ya0 = ptsA[i]
        xa1, ya1 = ptsA[i + 1]
        lo_x, hi_x = (xa0, xa1) if xa0 < xa1 else (xa1, xa0)
        lo_y, hi_y = (ya0, ya1) if ya0 < ya1 else (ya1, ya0)
        for j in range(nB - 1):
            xb0, yb0 = ptsB[j]
            xb1, yb1 = ptsB[j + 1]
            if max(xb0, xb1) < lo_x or min(xb0, xb1) > hi_x:
                continue
            if max(yb0, yb1) < lo_y or min(yb0, yb1) > hi_y:
                continue
            hit = seg_int(ptsA[i], ptsA[i + 1], ptsB[j], ptsB[j + 1])
            if hit is not None:
                hits.append((i, j, hit))
    # De-duplicate crossings found on several adjacent segment pairs.
    dedup = []
    for iA, iB, hit in hits:
        if all((hit[0] - h[0]) ** 2 + (hit[1] - h[1]) ** 2 > min_gap ** 2
               for _, _, h in dedup):
            dedup.append((iA, iB, hit))
    return dedup


# -------------------------------------------------------------------- render
def in_loop_flags(n, loops):
    flag = [False] * n
    for i1, i2, _ in loops:
        for k in range(i1, i2 + 1):
            flag[k] = True
    return flag


def runs_from_flag(pts, flag, want):
    """Contiguous point runs whose in-loop flag == want."""
    runs = []
    cur = []
    for k, p in enumerate(pts):
        if flag[k] == want:
            cur.append(p)
        elif cur:
            runs.append(cur)
            cur = []
    if cur:
        runs.append(cur)
    return runs


def stroke_layer(g, runs, core_col, outline_col, r_core, r_out):
    """A cord (or set of cord runs) on its own transparent RGBA layer:
    sepia outline stamped first, coloured core on top."""
    layer = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for run in runs:
        for (x, y) in run:
            d.ellipse([x - r_out, y - r_out, x + r_out, y + r_out],
                      fill=outline_col + (255,))
    for run in runs:
        for (x, y) in run:
            d.ellipse([x - r_core, y - r_core, x + r_core, y + r_core],
                      fill=core_col + (255,))
    return layer


def stamp_over(g, canvas, layer, px, py, r_disk):
    """Re-assert one cord layer on top of the canvas within a disk around a
    crossing, hiding the under strand there (clean over-under, no seam)."""
    disk = Image.new("L", (g.w, g.h), 0)
    ImageDraw.Draw(disk).ellipse(
        [px - r_disk, py - r_disk, px + r_disk, py + r_disk], fill=255)
    tmp = layer.copy()
    tmp.putalpha(ImageChops.multiply(disk, layer.split()[3]))
    canvas.alpha_composite(tmp)


def build_band(g, primary, accent, outline):
    """Render one seamless horizontal period, downscaled with LANCZOS."""
    # Two identical gold cords, mirrored about the centre line and offset by
    # half a period: the up-curls and down-curls interlock into a symmetric
    # running double-scroll that reads as all-gold interlace.
    pts_up = cord_points(g, y_sign=+1, x_shift=0.0)
    pts_dn = cord_points(g, y_sign=-1, x_shift=g.w / 2)
    cords = [pts_up, pts_dn]
    rc, ro = g.r_core, g.r_out

    # Self-crossing curls per cord; connectors are everything else.
    loops = [find_self_loops(p, gap=8) for p in cords]
    flags = [in_loop_flags(len(cords[c]), loops[c]) for c in range(2)]
    conn = [runs_from_flag(cords[c], flags[c], False) for c in range(2)]
    curl = [runs_from_flag(cords[c], flags[c], True) for c in range(2)]

    layer_conn = [stroke_layer(g, conn[c], primary, outline, rc, ro)
                  for c in range(2)]
    layer_curl = [stroke_layer(g, curl[c], primary, outline, rc, ro)
                  for c in range(2)]

    def cord_layer(c, idx):
        return layer_curl[c] if flags[c][idx] else layer_conn[c]

    # Disk radius used to re-assert an over-strand at a crossing.
    r_disk = 1.9 * ro

    canvas = Image.new("RGBA", (g.w, g.h), (0, 0, 0, 0))
    for c in range(2):
        canvas.alpha_composite(layer_conn[c])
        canvas.alpha_composite(layer_curl[c])

    # Self-crossing curls: the flowing connector passes OVER the rolled curl,
    # so the spiral reads as tucked under itself.
    for c in range(2):
        for i1, i2, hit in loops[c]:
            stamp_over(g, canvas, layer_conn[c], hit[0], hit[1], r_disk)

    # Inter-cord crossings: alternate over/under along the band (true weave).
    inter = find_inter(pts_up, pts_dn, min_gap=0.9 * g.r_out)
    inter.sort(key=lambda h: h[2][0])
    for k, (i0, i1, hit) in enumerate(inter):
        if k % 2 == 0:
            stamp_over(g, canvas, cord_layer(0, i0), hit[0], hit[1], r_disk)
        else:
            stamp_over(g, canvas, cord_layer(1, i1), hit[0], hit[1], r_disk)

    # A gold-ringed accent boss at every curl's eye (the spiral centre).
    d = ImageDraw.Draw(canvas)
    br, bring = g.boss_r, g.boss_ring
    for c in range(2):
        for i1, i2, hit in loops[c]:
            run = cords[c][i1:i2 + 1]
            ex = sum(p[0] for p in run) / len(run)
            ey = sum(p[1] for p in run) / len(run)
            d.ellipse([ex - br, ey - br, ex + br, ey + br],
                      fill=outline + (255,))
            r1 = br - max(1, round(0.010 * g.h))
            d.ellipse([ex - r1, ey - r1, ex + r1, ey + r1],
                      fill=primary + (255,))
            r2 = r1 - bring
            d.ellipse([ex - r2, ey - r2, ex + r2, ey + r2],
                      fill=accent + (255,))

    # Render three periods together, downscale, crop the middle period so the
    # tile edges are sampled from real neighbouring content (seamless).
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
