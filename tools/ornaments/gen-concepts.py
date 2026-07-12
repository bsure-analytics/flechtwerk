#!/usr/bin/env python3
"""concepts ornament -- a seamless manuscript interlace band for the "concepts"
chapter, adapted from the gridknot generator.

Base style: gridknot -- cords run at 45 degrees between edge midpoints of grid
cells, crossings alternate strictly over/under, and interior breaklines fold
the plait into interlocking diamond knot nodes with a small closed diamond
"eye" per period.

Manuscript palette: GOLD is the dominant cord colour, VERDIGRIS the single
accent, dark sepia INK the crisp thin cord outline.  The small knot "eye" is
gold.  Horizontally seamless / tileable.  Pillow only.

Run:  uv run --with pillow --no-project python gen-concepts.py
"""

import math
import os

from PIL import Image

# --------------------------------------------------------------------------
# geometry / sizing
# --------------------------------------------------------------------------
SS = 3                       # supersample factor
S = 66                       # lattice spacing (supersampled px)
NR = 3                       # vertical spans -> node rows r = 0..NR
NC = 6                       # columns per horizontal period
MARGIN_V = 33                # top/bottom room for the round turn caps
N_TILES = 3                  # render 3 periods, crop the middle one -> seamless

BAND_H = NR * S + 2 * MARGIN_V          # 264 supersampled -> 88 final
PERIOD_W = NC * S                       # 396 supersampled -> 132 final

# cord widths (supersampled px)
FILL_W = 34
OUTLINE_W = 46               # dark border = (OUTLINE_W - FILL_W)/2 each side

# --------------------------------------------------------------------------
# manuscript palette (RGB + alpha)
# --------------------------------------------------------------------------
INK = (42, 33, 24, 255)          # dark sepia -- crisp thin cord outline
GOLD = (176, 138, 62, 255)       # gold leaf -- dominant cord colour
VERDIGRIS = (47, 109, 94, 255)   # teal-green accent

# --------------------------------------------------------------------------
# lattice model
# --------------------------------------------------------------------------
# node (c, r) exists when (c + r) even and 0 <= r <= NR.
C_MIN = 0
C_MAX = N_TILES * NC        # 18


def node_exists(c, r):
    return 0 <= r <= NR and (c + r) % 2 == 0 and C_MIN <= c <= C_MAX


# breaklines: interior nodes that TURN instead of CROSS.
# 'H' = horizontal wall (reflects vertical motion), 'V' = vertical wall.
# Keyed by (c % NC, r) so the pattern is periodic across tiles.
BREAK_PATTERN = {
    (2, 2): "V",
    (3, 1): "H",
    (4, 2): "V",
    (5, 1): "V",
}


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


# direction vectors in (c, r) space
DIRS = [(1, 1), (1, -1), (-1, 1), (-1, -1)]


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
    """Trace closed cords on the wrapped graph.

    Returns (edge_color, loops) where edge_color maps an undirected quotient
    edge (ukey) -> rgba, and loops is the list of cords (each a list of
    directed edges) sorted longest-first.
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
    # Colour policy for the "concepts" chapter: GOLD dominates, VERDIGRIS is
    # the single accent, the small "eye" loop is gold.  The verdigris accent
    # is placed on exactly one interior cord loop (never the longest, never
    # the eye) so gold stays the most-frequent cord.
    n = len(loops)
    eye_idx = n - 1 if n >= 3 else None
    accent_idx = None
    if n >= 3:
        # pick the shortest NON-eye loop as the verdigris accent
        for i in range(n - 2, 0, -1):
            accent_idx = i
            break
    edge_color = {}
    for i, edges in enumerate(loops):
        color = VERDIGRIS if i == accent_idx else GOLD
        for (cq, r, dx, dy) in edges:
            edge_color[ukey(cq, r, dx, dy)] = color
    return edge_color, loops


def color_of(c, r, dx, dy, edge_color):
    return edge_color.get(ukey(c % NC, r, dx, dy), GOLD)


def physical_segments(edge_color):
    """Yield (p0, p1, color) for every physical lattice segment, once."""
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
                col = color_of(c, r, dx, dy, edge_color)
                segs.append((px(c, r), px(*to), col))
    return segs


def crossings_list(edge_color):
    crossings = []
    for c in range(C_MIN, C_MAX + 1):
        for r in range(1, NR):
            if not node_exists(c, r) or node_type(c, r) != "CROSS":
                continue
            over_diag_A = (c % 2 == 0)
            over_vec = (1, 1) if over_diag_A else (1, -1)
            over_color = color_of(c, r, over_vec[0], over_vec[1], edge_color)
            crossings.append(
                {"pos": px(c, r), "over_vec": over_vec, "over_color": over_color}
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


def render_full():
    from PIL import ImageDraw

    W = C_MAX * S
    img = Image.new("RGBA", (W + 1, BAND_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    edge_color, loops = trace_loops()
    segs = physical_segments(edge_color)
    crossings = crossings_list(edge_color)

    # pass A: all dark outlines
    for (p0, p1, _col) in segs:
        draw_seg(draw, p0, p1, OUTLINE_W, INK)
    # pass B: all coloured fills
    for (p0, p1, col) in segs:
        draw_seg(draw, p0, p1, FILL_W, col)

    # pass C: restamp the OVER cord at every crossing so over/under is correct
    L = 0.62 * S
    for x in crossings:
        (cx, cy) = x["pos"]
        vx, vy = x["over_vec"]
        norm = math.hypot(vx, vy)
        ux, uy = vx / norm, vy / norm
        p0 = (cx - L * ux, cy - L * uy)
        p1 = (cx + L * ux, cy + L * uy)
        draw_seg(draw, p0, p1, OUTLINE_W, INK)
        draw_seg(draw, p0, p1, FILL_W, x["over_color"])

    return img, len(loops)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    outdir = os.path.dirname(os.path.abspath(__file__))
    full, nstrands = render_full()

    # Downsample the WHOLE periodic render first, then crop the middle period.
    # Resizing before cropping means the LANCZOS kernel at the tile boundary
    # samples real neighbouring-period pixels (they exist in the full render)
    # instead of clamping to a crop edge -> the tile boundary stays seamless.
    W = C_MAX * S                    # 1188 supersampled (drop the +1 guard col)
    full = full.crop((0, 0, W, BAND_H))
    small = full.resize((W // SS, BAND_H // SS), Image.LANCZOS)

    x0 = (NC * S) // SS              # start of middle tile in final px
    band = small.crop((x0, 0, x0 + PERIOD_W // SS, BAND_H // SS))
    band_path = os.path.join(outdir, "band-concepts.png")
    band.save(band_path)

    # rail: rotate the horizontally-seamless band 90 degrees (lossless) so it
    # becomes seamless top-to-bottom -> a vertical rail of width 88.
    rail = band.transpose(Image.ROTATE_90)
    rail_path = os.path.join(outdir, "rail-concepts.png")
    rail.save(rail_path)

    print("strands:", nstrands)
    print("band:", band.size, band_path)
    print("rail:", rail.size, rail_path)


if __name__ == "__main__":
    main()
