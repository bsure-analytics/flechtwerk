"""guides ornament -- a manuscript eyelet-chain (twistknot) interlace band.

Chapter: guides.  Base style: twistknot (adapted from scratchpad/knots/gen_twistknot.py).

Two gold strands weave as phase-opposed sine waves, bowing apart between
crossings to form a chain of pointed-oval eyelets, then crossing over/under
each other with strict alternation. A monk's illuminated hand: gold leaf cords
with a crisp dark-sepia outline, and a garnet "eye" set in every eyelet as the
sole accent. Rendered supersampled and downscaled with LANCZOS.

Horizontally seamless by construction: the pattern is exactly periodic in x, so
we render 3 periods and crop the middle one -- the tile joins are seamless. The
band IS one period (width P). The rail is the band rotated 90 degrees, so a
horizontally-seamless band becomes a vertically-seamless rail.

Run:  uv run --with pillow --no-project python gen-guides.py
"""

import math
from PIL import Image, ImageDraw

CHAPTER = "guides"
OUT_DIR = "/Users/christian/projects/bsure-analytics/flechtwerk/docs/assets/ornaments"

# --- final (display) geometry, in px ---
H = 88                  # band height (2x of a 44px display band)
P = 160                 # one horizontal period == band width
A = 26                  # sine amplitude
CY = H / 2              # vertical centre
W_BAND = 12            # cord fill width
OUTLINE = 2            # dark outline thickness (each side)

SS = 4                 # supersample factor

# --- manuscript palette (RGB) ---
INK      = (42, 33, 24)     # dark sepia -- crisp thin cord outline
GOLD     = (176, 138, 62)   # dominant cord colour (gold leaf)
GARNET   = (124, 45, 58)    # deep red accent (this chapter's accent)


def strand_y(strand, x):
    """y of a strand centreline at x. strand 1 bulges +, strand 2 bulges -."""
    sign = 1.0 if strand == 1 else -1.0
    return CY + sign * A * math.sin(2 * math.pi * x / P)


def is_over(strand, k):
    """Is `strand` the OVER strand at crossing index k (x = k*P/2)?

    Alternates: at even crossings strand 1 is over, at odd crossings strand 2.
    """
    if k % 2 == 0:
        return strand == 1
    return strand == 2


GAP_X = W_BAND * 1.05   # under strand stops this far (in x) short of a crossing
EXT_X = W_BAND * 1.05   # over-strand lobe fills lap this far past a crossing


def in_under_gap(strand, x):
    """True if `strand` is diving UNDER near the nearest crossing at x."""
    k = round(x / (P / 2.0))
    xk = k * P / 2.0
    if abs(x - xk) >= GAP_X:
        return False
    return not is_over(strand, k)


def outline_subpaths(strand, xlo, xhi, step):
    """Continuous centreline subpaths of a strand, broken only at under-gaps."""
    subpaths = []
    cur = []
    x = xlo
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


def lobe_fill_points(strand, k, step):
    """Fill points for one lobe (crossing k..k+1).

    Over ends lap past the crossing; under ends stop short. No caps -- the
    continuous outline already borders the cord, and lobes abut at crossings.
    """
    x0 = k * P / 2.0
    x1 = (k + 1) * P / 2.0
    xa = x0 - EXT_X if is_over(strand, k) else x0 + GAP_X
    xb = x1 + EXT_X if is_over(strand, k + 1) else x1 - GAP_X
    pts = []
    x = xa
    while x <= xb:
        pts.append((x, strand_y(strand, x)))
        x += step
    pts.append((xb, strand_y(strand, xb)))
    return pts


def render_band():
    s = SS
    # render 3 periods wide, crop the middle -> seamless by construction
    big_w = 3 * P * s
    big_h = H * s
    img = Image.new("RGBA", (big_w, big_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    w_fill = W_BAND * s
    w_out = (W_BAND + 2 * OUTLINE) * s

    shift = P            # pattern x in [-P, 2P] -> canvas x = (x + P) * s
    step = 0.5           # sampling step in pattern-x
    xlo, xhi = -P, 2 * P

    def plot(pts):
        return [((x + shift) * s, y * s) for (x, y) in pts]

    def stamp(pts, r, color):
        """Stroke a path as a dense run of overlapping disks -- no vertex
        notches, perfectly smooth thick cord after LANCZOS downscale."""
        for (x, y) in pts:
            draw.ellipse([x - r, y - r, x + r, y + r], fill=color)

    # --- pass 1: continuous sepia outlines (broken only at under-gaps) ---
    for strand in (1, 2):
        for sub in outline_subpaths(strand, xlo, xhi, step):
            stamp(plot(sub), w_out / 2, INK)

    # --- pass 2: per-lobe gold fills (no caps) ---
    for k in range(-2, 4):
        for strand in (1, 2):
            stamp(plot(lobe_fill_points(strand, k, step)), w_fill / 2, GOLD)

    # --- pass 3: a garnet "eye" set in every eyelet (the accent) ---
    # eyelet centres sit at x = (k + 0.5) * P/2, always vertically centred.
    rx, ry = 4.2 * s, 6.4 * s          # a vertical lens, echoing the eyelet
    ox, oy = rx + OUTLINE * s, ry + OUTLINE * s
    for k in range(-2, 5):
        xc = (k + 0.5) * P / 2.0
        X = (xc + shift) * s
        Y = CY * s
        draw.ellipse([X - ox, Y - oy, X + ox, Y + oy], fill=INK)
        draw.ellipse([X - rx, Y - ry, X + rx, Y + ry], fill=GARNET)

    # crop the middle period -> [P*s, 2P*s]
    crop = img.crop((P * s, 0, 2 * P * s, big_h))
    band = crop.resize((P, H), Image.LANCZOS)
    return band


def main():
    band = render_band()
    band_path = f"{OUT_DIR}/band-{CHAPTER}.png"
    band.save(band_path)

    # rail: rotate 90 degrees -> a horizontally-seamless band becomes a
    # vertically-seamless rail of width H (88 px).
    rail = band.transpose(Image.ROTATE_90)
    rail_path = f"{OUT_DIR}/rail-{CHAPTER}.png"
    rail.save(rail_path)

    print("band:", band_path, band.size)
    print("rail:", rail_path, rail.size)


if __name__ == "__main__":
    main()
