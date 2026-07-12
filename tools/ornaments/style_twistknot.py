"""twistknot ornament -- a two-strand eyelet-chain Celtic interlace.

Style: twistknot (adapted from tools/ornaments/gen-guides.py).

Two gold cords weave as phase-opposed sine waves, bowing apart between
crossings to form a chain of pointed-oval EYELETS, then crossing over/under
each other with strict alternation. Gold is the dominant cord colour; a
pointed-oval (vesica) accent "eye" is set in every eyelet -- echoing the
eyelet shape -- as the sole accent. Each cord carries a crisp dark-sepia
outline. Matte, rendered supersampled and downscaled with LANCZOS.

Uniform CLI (every docsite style is invoked identically):

    uv run --with pillow --no-project python style_twistknot.py \
        --primary R,G,B --accent R,G,B --outline R,G,B \
        --height INT --band PATH --rail PATH

Horizontal seamlessness by construction: the pattern is exactly periodic in
x with period P, so we render 3 periods and crop the middle one -- tile joins
are seamless. The BAND is one period (width P). The RAIL is the band rotated
90 degrees, so a horizontally-seamless band becomes a vertically-seamless
rail of width == --height.
"""

import argparse
import math

from PIL import Image, ImageDraw

SS = 4  # supersample factor (>= 3), LANCZOS-downscaled for crisp matte edges


def parse_rgb(text: str) -> tuple[int, int, int]:
    parts = text.split(",")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B but got {text!r}")
    r, g, b = (int(p) for p in parts)
    for v in (r, g, b):
        if not 0 <= v <= 255:
            raise argparse.ArgumentTypeError(f"channel out of range 0..255 in {text!r}")
    return (r, g, b)


def vesica_points(cx, cy, rx, ry, n=48):
    """Polygon vertices of a pointed-oval (vesica) with vertical long axis.

    rx is the half-width, ry the half-height (ry > rx gives the pointed
    almond). Built as the intersection of two circles centred left/right on
    the horizontal axis, so the top and bottom meet in true points.
    """
    if ry <= rx:
        ry = rx * 1.0001
    off = (ry * ry - rx * rx) / (2.0 * rx)  # circle-centre offset from axis
    radius = off + rx
    right = [
        (cx - off + math.sqrt(max(0.0, radius * radius - y * y)), cy + y)
        for y in (ry * (2.0 * i / n - 1.0) for i in range(n + 1))
    ]
    left = [
        (cx + off - math.sqrt(max(0.0, radius * radius - y * y)), cy + y)
        for y in (ry * (1.0 - 2.0 * i / n) for i in range(n + 1))
    ]
    return right + left


def render_band(primary, accent, outline, height):
    h = height
    p = 2 * round(0.9 * h)            # one horizontal period == band width
    amp = round(0.30 * h)             # sine amplitude
    cy = h / 2.0                      # vertical centre
    w_band = max(4, round(0.14 * h))  # cord fill width
    ol = max(1, round(0.025 * h))     # outline thickness (each side)

    gap_x = w_band * 1.05             # under-strand stops short of a crossing
    ext_x = w_band * 1.05             # over-strand laps past a crossing
    half = p / 2.0                    # crossing spacing

    def strand_y(strand, x):
        sign = 1.0 if strand == 1 else -1.0
        return cy + sign * amp * math.sin(2.0 * math.pi * x / p)

    def is_over(strand, k):
        # alternating weave: even crossings -> strand 1 over, odd -> strand 2
        return strand == (1 if k % 2 == 0 else 2)

    def in_under_gap(strand, x):
        k = round(x / half)
        xk = k * half
        return abs(x - xk) < gap_x and not is_over(strand, k)

    def outline_subpaths(strand, xlo, xhi, step):
        subpaths, cur, x = [], [], xlo
        while x <= xhi:
            if in_under_gap(strand, x):
                if cur:
                    subpaths.append(cur)
                    cur = []
            else:
                cur.append((x, strand_y(strand, x)))
            x += step
        if cur:
            subpaths.append(cur)
        return subpaths

    def lobe_points(strand, k, step):
        x0, x1 = k * half, (k + 1) * half
        xa = x0 - ext_x if is_over(strand, k) else x0 + gap_x
        xb = x1 + ext_x if is_over(strand, k + 1) else x1 - gap_x
        pts, x = [], xa
        while x <= xb:
            pts.append((x, strand_y(strand, x)))
            x += step
        pts.append((xb, strand_y(strand, xb)))
        return pts

    s = SS
    big_w, big_h = 3 * p * s, h * s
    img = Image.new("RGBA", (big_w, big_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    w_fill = w_band * s
    w_out = (w_band + 2 * ol) * s
    shift = p                 # pattern x in [-p, 2p] -> canvas x = (x + p) * s
    step = 0.5
    xlo, xhi = -p, 2 * p

    def plot(pts):
        return [((x + shift) * s, y * s) for (x, y) in pts]

    def stamp(pts, r, color):
        # stroke as a dense run of overlapping disks: no vertex notches, a
        # perfectly smooth thick cord after the LANCZOS downscale.
        for (x, y) in pts:
            draw.ellipse([x - r, y - r, x + r, y + r], fill=color)

    # pass 1: continuous sepia cord outlines (broken only at under-gaps)
    for strand in (1, 2):
        for sub in outline_subpaths(strand, xlo, xhi, step):
            stamp(plot(sub), w_out / 2.0, outline)

    # pass 2: per-lobe gold fills (over ends lap past, under ends stop short)
    for k in range(-2, 4):
        for strand in (1, 2):
            stamp(plot(lobe_points(strand, k, step)), w_fill / 2.0, primary)

    # pass 3: a pointed-oval accent "eye" set in every eyelet
    ry, rx = 0.52 * amp, 0.24 * amp
    for k in range(-2, 5):
        xc = (k + 0.5) * half
        cxp, cyp = (xc + shift) * s, cy * s
        draw.polygon(vesica_points(cxp, cyp, (rx + ol) * s, (ry + ol) * s), fill=outline)
        draw.polygon(vesica_points(cxp, cyp, rx * s, ry * s), fill=accent)

    # crop the middle period [p*s, 2p*s] -> exactly one period -> seamless
    crop = img.crop((p * s, 0, 2 * p * s, big_h))
    return crop.resize((p, h), Image.LANCZOS)


def main():
    ap = argparse.ArgumentParser(description="twistknot Celtic interlace band/rail generator")
    ap.add_argument("--primary", type=parse_rgb, default="176,138,62", help="dominant gold cord colour")
    ap.add_argument("--accent", type=parse_rgb, required=True, help="accent (eyelet eye) colour")
    ap.add_argument("--outline", type=parse_rgb, default="42,33,24", help="thin sepia cord outline")
    ap.add_argument("--height", type=int, default=88, help="band height / rail thickness in px")
    ap.add_argument("--band", required=True, help="output path: horizontal band PNG")
    ap.add_argument("--rail", required=True, help="output path: vertical rail PNG")
    args = ap.parse_args()

    primary = args.primary if isinstance(args.primary, tuple) else parse_rgb(args.primary)
    outline = args.outline if isinstance(args.outline, tuple) else parse_rgb(args.outline)

    band = render_band(primary, args.accent, outline, args.height)
    band.save(args.band)

    # rail: rotate 90 degrees -> horizontal seamlessness becomes vertical.
    rail = band.transpose(Image.ROTATE_90)
    rail.save(args.rail)

    print("band:", args.band, band.size)
    print("rail:", args.rail, rail.size)


if __name__ == "__main__":
    main()
