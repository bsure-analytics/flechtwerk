#!/usr/bin/env python3
"""gridknot -- a seamless Celtic interlace band/rail generator (Pillow only).

Style: interlocking DIAMOND knot nodes on a diagonal lattice with a small
central "eye".  Cords run at 45 degrees between edge midpoints of grid cells,
crossings alternate strictly OVER / UNDER, and interior breaklines fold the
plait into interlocking diamond knot nodes closed around one gold "eye" per
period.

Colours: PRIMARY (gold) is the dominant cord colour, ACCENT is the single
accent cord, OUTLINE (dark sepia) is the crisp thin cord border.  The knot
"eye" loop is drawn in the primary colour so gold stays dominant.  Matte,
supersampled then LANCZOS-downscaled, horizontally seamless / tileable.

Uniform CLI (every docsite style is invoked identically):

    uv run --with pillow --no-project python style_gridknot.py \
        --accent 39,74,122 \
        --band band-gridknot.png --rail rail-gridknot.png
"""

import argparse
import math

from PIL import Image, ImageDraw

# --------------------------------------------------------------------------
# fixed lattice topology (independent of the requested pixel height)
# --------------------------------------------------------------------------
SS = 4                       # supersample factor (>= 3; downscaled with LANCZOS)
NR = 3                       # vertical spans -> node rows r = 0..NR
NC = 6                       # columns per horizontal period
N_TILES = 3                  # render 3 periods, crop the middle one -> seamless

C_MIN = 0
C_MAX = N_TILES * NC         # physical column range covered by the full render

# runtime geometry (pixels in supersampled space); assigned in main()
S = 0                        # lattice spacing
MARGIN_V = 0                 # top/bottom room for the round turn caps
SUPER_H = 0                  # supersampled band height (= height * SS)
FILL_W = 0                   # coloured cord width
OUTLINE_W = 0                # cord width including the dark border

# direction vectors in (c, r) space
DIRS = [(1, 1), (1, -1), (-1, 1), (-1, -1)]

# breaklines: interior nodes that TURN instead of CROSS, keyed by (c % NC, r)
# so the pattern repeats across tiles.  'H' reflects vertical motion (a
# horizontal wall), 'V' reflects horizontal motion (a vertical wall).  This
# particular set folds the diagonal plait into interlocking diamond knots
# with a closed diamond "eye" at the centre of each period.
BREAK_PATTERN = {
    (2, 2): "V",
    (3, 1): "H",
    (4, 2): "V",
    (5, 1): "V",
}


# --------------------------------------------------------------------------
# lattice model
# --------------------------------------------------------------------------
def node_exists(c, r):
    return 0 <= r <= NR and (c + r) % 2 == 0 and C_MIN <= c <= C_MAX


def node_type(c, r):
    """Return 'CROSS' or 'TURN'."""
    if r == 0 or r == NR:
        return "TURN"
    if (c % NC, r) in BREAK_PATTERN:
        return "TURN"
    return "CROSS"


def break_kind(c, r):
    if r == 0 or r == NR:
        return "H"      # boundary caps are horizontal reflections
    return BREAK_PATTERN[(c % NC, r)]


def px(c, r):
    return (c * S, MARGIN_V + r * S)


def qnode_exists(cq, r):
    return 0 <= r <= NR and (cq + r) % 2 == 0


def qexit_dir(cq, r, d):
    if node_type(cq, r) == "CROSS":
        return d
    if break_kind(cq, r) == "H":
        return (d[0], -d[1])
    return (-d[0], d[1])


def ukey(cq, r, dx, dy):
    """Canonical undirected key for a quotient edge."""
    a = (cq, r)
    b = ((cq + dx) % NC, r + dy)
    return frozenset((a, b))


def trace_loops():
    """Trace closed cords on the wrapped (period-quotient) graph.

    Returns (edge_role, loops) where edge_role maps an undirected quotient
    edge (ukey) -> "primary" | "accent", and loops is the list of cords
    (each a list of directed edges) sorted longest-first.  The accent role is
    assigned to exactly one interior loop (never the longest, never the
    "eye") so the primary colour stays the most frequent cord.
    """
    visited = set()                     # directed edges
    loops = []
    for cq0 in range(NC):
        for r0 in range(NR + 1):
            if not qnode_exists(cq0, r0):
                continue
            for d0 in DIRS:
                if not (0 <= r0 + d0[1] <= NR):
                    continue
                if (cq0, r0, d0[0], d0[1]) in visited:
                    continue
                edges = []
                cq, r, d = cq0, r0, d0
                guard = 0
                while True:
                    guard += 1
                    if guard > 100000:
                        break
                    e = (cq, r, d[0], d[1])
                    if e in visited:
                        break
                    visited.add(e)
                    ncq = (cq + d[0]) % NC
                    nr = r + d[1]
                    visited.add((ncq, nr, -d[0], -d[1]))
                    edges.append(e)
                    if not (0 <= nr <= NR):
                        break
                    d = qexit_dir(ncq, nr, d)
                    cq, r = ncq, nr
                if edges:
                    loops.append(edges)

    loops.sort(key=len, reverse=True)
    n = len(loops)
    accent_idx = None
    if n >= 3:
        # shortest NON-eye loop (eye is the very shortest, index n - 1)
        for i in range(n - 2, 0, -1):
            accent_idx = i
            break
    edge_role = {}
    for i, edges in enumerate(loops):
        role = "accent" if i == accent_idx else "primary"
        for (cq, r, dx, dy) in edges:
            edge_role[ukey(cq, r, dx, dy)] = role
    return edge_role, loops


def role_of(c, r, dx, dy, edge_role):
    return edge_role.get(ukey(c % NC, r, dx, dy), "primary")


def physical_segments(edge_role):
    """Yield (p0, p1, role) for every physical lattice segment, once."""
    seen = set()
    segs = []
    for c in range(C_MIN, C_MAX + 1):
        for r in range(NR + 1):
            if not node_exists(c, r):
                continue
            for (dx, dy) in DIRS:
                to = (c + dx, r + dy)
                if not node_exists(*to):
                    continue
                key = frozenset({(c, r), to})
                if key in seen:
                    continue
                seen.add(key)
                role = role_of(c, r, dx, dy, edge_role)
                segs.append((px(c, r), px(*to), role))
    return segs


def crossings_list(edge_role):
    crossings = []
    for c in range(C_MIN, C_MAX + 1):
        for r in range(1, NR):
            if not node_exists(c, r) or node_type(c, r) != "CROSS":
                continue
            over_diag_a = (c % 2 == 0)
            over_vec = (1, 1) if over_diag_a else (1, -1)
            over_role = role_of(c, r, over_vec[0], over_vec[1], edge_role)
            crossings.append(
                {"pos": px(c, r), "over_vec": over_vec, "over_role": over_role}
            )
    return crossings


# --------------------------------------------------------------------------
# drawing helpers
# --------------------------------------------------------------------------
def draw_seg(draw, p0, p1, width, color):
    draw.line([p0, p1], fill=color, width=width, joint="curve")
    r = width / 2.0
    for (x, y) in (p0, p1):
        draw.ellipse([x - r, y - r, x + r, y + r], fill=color)


def render_full(primary, accent, outline):
    role_color = {"accent": accent, "primary": primary}

    width = C_MAX * S
    img = Image.new("RGBA", (width + 1, SUPER_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    edge_role, loops = trace_loops()
    segs = physical_segments(edge_role)
    crossings = crossings_list(edge_role)

    # pass A: all dark outlines
    for (p0, p1, _role) in segs:
        draw_seg(draw, p0, p1, OUTLINE_W, outline)
    # pass B: all coloured fills
    for (p0, p1, role) in segs:
        draw_seg(draw, p0, p1, FILL_W, role_color[role])

    # pass C: restamp the OVER cord at every crossing so over/under is correct
    length = 0.62 * S
    for x in crossings:
        (cx, cy) = x["pos"]
        vx, vy = x["over_vec"]
        norm = math.hypot(vx, vy)
        ux, uy = vx / norm, vy / norm
        q0 = (cx - length * ux, cy - length * uy)
        q1 = (cx + length * ux, cy + length * uy)
        draw_seg(draw, q0, q1, OUTLINE_W, outline)
        draw_seg(draw, q0, q1, FILL_W, role_color[x["over_role"]])

    return img, len(loops)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def parse_rgb(text):
    parts = [int(v) for v in text.split(",")]
    if len(parts) != 3 or any(not 0 <= v <= 255 for v in parts):
        raise argparse.ArgumentTypeError(f"expected R,G,B in 0..255, got {text!r}")
    return (parts[0], parts[1], parts[2], 255)


def main():
    ap = argparse.ArgumentParser(description="gridknot Celtic interlace generator")
    ap.add_argument("--primary", type=parse_rgb, default=parse_rgb("176,138,62"),
                    help="dominant cord colour R,G,B (gold)")
    ap.add_argument("--accent", type=parse_rgb, required=True,
                    help="accent cord colour R,G,B")
    ap.add_argument("--outline", type=parse_rgb, default=parse_rgb("42,33,24"),
                    help="thin cord outline colour R,G,B (dark sepia)")
    ap.add_argument("--height", type=int, default=88,
                    help="band height in px (also the rail thickness)")
    ap.add_argument("--band", required=True, help="output horizontal band PNG")
    ap.add_argument("--rail", required=True, help="output vertical rail PNG")
    args = ap.parse_args()

    height = args.height
    if height < 8:
        ap.error("--height must be at least 8")

    # derive supersampled geometry so the downscaled band is exactly `height`
    global S, MARGIN_V, SUPER_H, FILL_W, OUTLINE_W
    SUPER_H = height * SS
    S = SUPER_H // (NR + 1)
    MARGIN_V = (SUPER_H - NR * S) // 2
    FILL_W = max(6, round(S * 0.515))     # ~34 when S == 66
    OUTLINE_W = FILL_W + max(4, round(S * 0.18))

    full, nstrands = render_full(args.primary, args.accent, args.outline)

    # Downsample the WHOLE periodic render first, then crop the middle period.
    # Resizing before cropping lets the LANCZOS kernel at the tile boundary
    # sample real neighbouring-period pixels (they exist in the full render)
    # instead of clamping to a crop edge -> the tile boundary stays seamless.
    width = C_MAX * S                       # drop the +1 guard column
    full = full.crop((0, 0, width, SUPER_H))

    period_final = round(NC * S / SS)
    full_final_w = N_TILES * period_final
    small = full.resize((full_final_w, height), Image.LANCZOS)

    band = small.crop((period_final, 0, 2 * period_final, height))
    band.save(args.band)

    # rail: rotate the horizontally-seamless band 90 degrees (lossless) so it
    # becomes seamless top-to-bottom -> a vertical rail of width == height.
    rail = band.transpose(Image.ROTATE_90)
    rail.save(args.rail)

    print("strands:", nstrands)
    print("band:", band.size, args.band)
    print("rail:", rail.size, args.rail)


if __name__ == "__main__":
    main()
