"""Celtic interlace style: PLAIT -- the classic 2-strand diagonal basket-weave.

Two families of straight 45deg diagonal cords (slope +1 and slope -1) cross in a
lattice, over/under alternating in a checkerboard at every crossing. Strands make
rounded u-turns at the top and bottom edges, so there are no loose ends. One
horizontal period contains four full-width zig-zag cords -- three primary (gold)
plus one accent -- so the primary clearly dominates while the accent weaves through.

Uniform ornament CLI (the docsite driver calls every style identically):

    uv run --with pillow --no-project python style_plait.py \
        --accent 39,74,122 --band band.png --rail rail.png

Writes a horizontally-seamless BAND (height = --height, width = one period) and a
vertically-seamless RAIL (the band rotated 90 degrees). Both are transparent RGBA
with only the cords opaque; rendered supersampled then LANCZOS-downscaled.
"""

import argparse

from PIL import Image, ImageDraw

# ---------------------------------------------------------------------------
# Fixed geometry knobs (fractions / supersample -- independent of height)
# ---------------------------------------------------------------------------
S = 4                 # supersample factor (>= 3 required)
EXT = 0.55            # crossing segment half-length as a fraction of P
TURN = 0.62           # turn polyline reach toward neighbour (fraction of edge)
MLO, MHI = -4, 12     # lattice column range (covers [0, W] plus wide margins)


def parse_rgb(text):
    parts = text.split(",")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(f"expected R,G,B, got {text!r}")
    try:
        rgb = tuple(int(p) for p in parts)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected integers in R,G,B, got {text!r}")
    if not all(0 <= c <= 255 for c in rgb):
        raise argparse.ArgumentTypeError(f"channels must be 0..255, got {text!r}")
    return rgb


# ---------------------------------------------------------------------------
# Lattice helpers
# ---------------------------------------------------------------------------
def is_point(r, m):
    return (m - r) % 2 == 0


def edge_key(a, b):
    return tuple(sorted([a, b]))


def down_neighbours(r, m):
    return [(r + 1, m - 1), (r + 1, m + 1)]


def up_neighbours(r, m):
    return [(r - 1, m - 1), (r - 1, m + 1)]


def build_band(primary, accent, outline, height):
    """Render one seamless horizontal period of the plait as an RGBA image."""
    # --- height-derived geometry (isometric: row spacing == P for 45deg) ---
    P = max(8, round(height / 5.5))        # lattice spacing (display px)
    W = 8 * P                              # tile width = one over/under period
    H = height
    intervals = max(2, round(0.72 * H / P))  # number of row gaps
    rows = intervals + 1
    y0 = (H - intervals * P) / 2.0         # top turn row (bottom mirrors at H-y0)

    k = P / 16.0                           # scale cord widths with the lattice
    wf = 9.0 * k                           # cord fill width
    wout = wf + 2 * (1.4 * k)              # cord outline width (thin sepia edge)

    palette = [primary, primary, accent, primary]

    def yof(r):
        return y0 + (H - 2 * y0) * r / (rows - 1)

    def pt(r, m):
        return (m * P, yof(r))

    def valid(r, m):
        return 0 <= r < rows and MLO <= m <= MHI and is_point(r, m)

    # --- union-find over lattice edges -> strands ---
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for r in range(rows):
        for m in range(MLO, MHI + 1):
            if not is_point(r, m):
                continue
            node = (r, m)
            for nb in down_neighbours(r, m):
                if valid(*nb):
                    find(edge_key(node, nb))
            if 0 < r < rows - 1:
                # crossing: +1 strand = UL + LR edge ; -1 strand = UR + LL edge
                ul, lr = (r - 1, m - 1), (r + 1, m + 1)
                ur, ll = (r - 1, m + 1), (r + 1, m - 1)
                if valid(*ul) and valid(*lr):
                    union(edge_key(node, ul), edge_key(node, lr))
                if valid(*ur) and valid(*ll):
                    union(edge_key(node, ur), edge_key(node, ll))
            else:
                # turn point: the two edges form one hairpin strand
                nbs = down_neighbours(r, m) if r == 0 else up_neighbours(r, m)
                nbs = [n for n in nbs if valid(*n)]
                if len(nbs) == 2:
                    union(edge_key(node, nbs[0]), edge_key(node, nbs[1]))

    # colour each strand by its top-turn column (periodic mod 8 -> 4 strands)
    comp_colour = {}
    for m in range(MLO, MHI + 1):
        if not is_point(0, m):
            continue
        nbs = [n for n in down_neighbours(0, m) if valid(*n)]
        if not nbs:
            continue
        rep = find(edge_key((0, m), nbs[0]))
        comp_colour[rep] = palette[(m // 2) % 4]

    def edge_colour(a, b):
        return comp_colour.get(find(edge_key(a, b)), primary)

    # --- rendering ---
    img = Image.new("RGBA", (W * S, H * S), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    def sc(p):
        return (p[0] * S, p[1] * S)

    def stroke(pts, width, colour):
        draw.line([sc(p) for p in pts], fill=colour + (255,),
                  width=max(1, int(round(width * S))), joint="curve")

    def dot(center, width, colour):
        r = width * S / 2.0
        x, y = sc(center)
        draw.ellipse([x - r, y - r, x + r, y + r], fill=colour + (255,))

    # turns first (no over/under at the edges)
    for r in (0, rows - 1):
        for m in range(MLO, MHI + 1):
            if not is_point(r, m):
                continue
            nb = down_neighbours(r, m) if r == 0 else up_neighbours(r, m)
            nb = [n for n in nb if valid(*n)]
            if len(nb) != 2:
                continue
            apex = pt(r, m)
            col = edge_colour((r, m), nb[0])
            ends = [(apex[0] + TURN * (pt(*n)[0] - apex[0]),
                     apex[1] + TURN * (pt(*n)[1] - apex[1])) for n in nb]
            poly = [ends[0], apex, ends[1]]
            stroke(poly, wout, outline)
            stroke(poly, wf, col)
            dot(apex, wf, col)   # smooth the rounded hairpin

    # crossings: draw UNDER strand then OVER strand
    def seg_pts(m, r, kind):
        x, y = pt(r, m)
        d = EXT * P
        if kind == "+":      # UL <-> LR (down-right axis)
            return [(x - d, y - d), (x + d, y + d)]
        return [(x - d, y + d), (x + d, y - d)]   # UR <-> LL (up-right axis)

    for r in range(1, rows - 1):
        for m in range(MLO, MHI + 1):
            if not is_point(r, m):
                continue
            col_plus = (edge_colour((r, m), (r + 1, m + 1)) if valid(r + 1, m + 1)
                        else edge_colour((r, m), (r - 1, m - 1)))
            col_minus = (edge_colour((r, m), (r + 1, m - 1)) if valid(r + 1, m - 1)
                         else edge_colour((r, m), (r - 1, m + 1)))

            plus_over = ((m + r) // 2) % 2 == 0
            under_kind, under_col, over_kind, over_col = (
                ("-", col_minus, "+", col_plus) if plus_over
                else ("+", col_plus, "-", col_minus))

            stroke(seg_pts(m, r, under_kind), wout, outline)
            stroke(seg_pts(m, r, under_kind), wf, under_col)
            stroke(seg_pts(m, r, over_kind), wout, outline)
            stroke(seg_pts(m, r, over_kind), wf, over_col)

    band = img.resize((W, H), Image.LANCZOS)
    return band


def main():
    ap = argparse.ArgumentParser(description="Render the PLAIT Celtic interlace band + rail.")
    ap.add_argument("--primary", type=parse_rgb, default=parse_rgb("176,138,62"),
                    help="dominant cord colour R,G,B (default gold 176,138,62)")
    ap.add_argument("--accent", type=parse_rgb, required=True,
                    help="accent cord colour R,G,B (required)")
    ap.add_argument("--outline", type=parse_rgb, default=parse_rgb("42,33,24"),
                    help="cord outline colour R,G,B (default sepia 42,33,24)")
    ap.add_argument("--height", type=int, default=88,
                    help="band height in px / rail thickness (default 88)")
    ap.add_argument("--band", required=True, help="output path for the horizontal band PNG")
    ap.add_argument("--rail", required=True, help="output path for the vertical rail PNG")
    args = ap.parse_args()

    band = build_band(args.primary, args.accent, args.outline, args.height)
    band.save(args.band)

    # Rail = band rotated 90deg: a horizontally-seamless band becomes a
    # vertically-seamless vertical rail whose width equals the band height.
    rail = band.rotate(90, expand=True)
    rail.save(args.rail)

    print(f"band {band.size}  rail {rail.size}  period={band.width}px")


if __name__ == "__main__":
    main()
