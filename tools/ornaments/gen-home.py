"""Render the 'home' chapter manuscript interlace ornament (base style: PLAIT).

Adapted from scratchpad/knots/gen_plait.py. Changes:
  1. Manuscript palette -- gold-dominant cords, sepia outline, lapis accent only.
  2. Thinner band -- final height 88 px (a 44 px display band at 2x).
  3. Genuine over-under interlace and true horizontal seamlessness preserved.

The plait is the classic Celtic basket-weave: two families of straight 45deg
diagonal bands (slope +1 and slope -1) crossing in a lattice, over/under
alternating in a checkerboard at every crossing. Strands u-turn (rounded) at
the top and bottom edges so there are no loose ends. Each strand is a full-width
zig-zag cord; one period contains four such cords -- three gold + one lapis,
so gold clearly dominates and the lapis weaves through as the accent.

Run with:
    uv run --with pillow --no-project python gen-home.py
"""

from PIL import Image

# ---------------------------------------------------------------------------
# Manuscript palette (RGB) -- a monk's illuminated hand
# ---------------------------------------------------------------------------
INK = (42, 33, 24)        # dark sepia -- the crisp thin cord outline
GOLD = (176, 138, 62)     # gold leaf -- the DOMINANT cord colour
LAPIS = (39, 74, 122)     # deep blue accent (this chapter's accent)

# Four strands per period; gold dominant, one lapis accent cord.
PALETTE = [GOLD, GOLD, LAPIS, GOLD]

# ---------------------------------------------------------------------------
# Geometry (final display px, then supersampled)
# ---------------------------------------------------------------------------
S = 4                     # supersample factor
P = 16                    # lattice spacing (display px)
W = 8 * P                 # tile width = one over/under period = 128
H = 88                    # band height (final)
ROWS = 5                  # r = 0..4 : rows 0 & 4 are turns, 1..3 crossings
Y0 = 12                   # y of top turn row (bottom turn mirrors at H-Y0)

WF = 9.0                  # cord fill width (display)
WOUT = WF + 2 * 1.4       # cord outline width (display) -> crisp thin sepia edge
EXT = 0.55                # crossing segment half-length as fraction of P
TURN = 0.62               # turn polyline reach toward neighbour (fraction of edge)

MLO, MHI = -4, 12         # lattice column range (covers [0,W] plus margins)


def yof(r):
    # Even vertical spacing with turns at Y0 (top) and H-Y0 (bottom).
    return Y0 + (H - 2 * Y0) * r / (ROWS - 1)


def pt(r, m):
    return (m * P, yof(r))


def is_point(r, m):
    return (m - r) % 2 == 0


def edge_key(a, b):
    return tuple(sorted([a, b]))


# ---------------------------------------------------------------------------
# Union-find over lattice edges -> strands
# ---------------------------------------------------------------------------
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


def down_neighbours(r, m):
    return [(r + 1, m - 1), (r + 1, m + 1)]


def up_neighbours(r, m):
    return [(r - 1, m - 1), (r - 1, m + 1)]


def valid(r, m):
    return 0 <= r < ROWS and MLO <= m <= MHI and is_point(r, m)


# Register all edges into union-find and connect same-strand edges.
for r in range(ROWS):
    for m in range(MLO, MHI + 1):
        if not is_point(r, m):
            continue
        node = (r, m)
        for nb in down_neighbours(r, m):
            if valid(*nb):
                find(edge_key(node, nb))

        if r in (1, 2, 3):
            # crossing: +1 strand = UL edge + LR edge ; -1 strand = UR + LL
            ul = (r - 1, m - 1)
            lr = (r + 1, m + 1)
            ur = (r - 1, m + 1)
            ll = (r + 1, m - 1)
            if valid(*ul) and valid(*lr):
                union(edge_key(node, ul), edge_key(node, lr))
            if valid(*ur) and valid(*ll):
                union(edge_key(node, ur), edge_key(node, ll))
        elif r == 0 or r == ROWS - 1:
            # turn point: the two edges are one strand hairpin
            nbs = down_neighbours(r, m) if r == 0 else up_neighbours(r, m)
            nbs = [n for n in nbs if valid(*n)]
            if len(nbs) == 2:
                union(edge_key(node, nbs[0]), edge_key(node, nbs[1]))

# Colour each strand by its top-turn column (periodic mod 8 -> 4 strands).
comp_colour = {}
for m in range(MLO, MHI + 1):
    if not is_point(0, m):
        continue
    nbs = [n for n in down_neighbours(0, m) if valid(*n)]
    if not nbs:
        continue
    rep = find(edge_key((0, m), nbs[0]))
    comp_colour[rep] = PALETTE[(m // 2) % 4]


def edge_colour(a, b):
    return comp_colour.get(find(edge_key(a, b)), GOLD)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
img = Image.new("RGBA", (W * S, H * S), (0, 0, 0, 0))
from PIL import ImageDraw  # noqa: E402
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


# --- turns first (no over/under) ---
for r in (0, ROWS - 1):
    for m in range(MLO, MHI + 1):
        if not is_point(r, m):
            continue
        nb = down_neighbours(r, m) if r == 0 else up_neighbours(r, m)
        nb = [n for n in nb if valid(*n)]
        if len(nb) != 2:
            continue
        apex = pt(r, m)
        col = edge_colour((r, m), nb[0])
        ends = []
        for n in nb:
            a = pt(*n)
            ends.append((apex[0] + TURN * (a[0] - apex[0]),
                         apex[1] + TURN * (a[1] - apex[1])))
        poly = [ends[0], apex, ends[1]]
        stroke(poly, WOUT, INK)
        stroke(poly, WF, col)
        dot(apex, WF, col)   # smooth the rounded hairpin


# --- crossings: draw UNDER strand then OVER strand ---
def seg_pts(m, r, kind):
    x, y = pt(r, m)
    d = EXT * P
    if kind == "+":          # UL <-> LR  (down-right axis)
        return [(x - d, y - d), (x + d, y + d)]
    return [(x - d, y + d), (x + d, y - d)]   # UR <-> LL (up-right axis)


for r in (1, 2, 3):
    for m in range(MLO, MHI + 1):
        if not is_point(r, m):
            continue
        col_plus = edge_colour((r, m), (r + 1, m + 1)) if valid(r + 1, m + 1) \
            else edge_colour((r, m), (r - 1, m - 1))
        col_minus = edge_colour((r, m), (r + 1, m - 1)) if valid(r + 1, m - 1) \
            else edge_colour((r, m), (r - 1, m + 1))

        plus_over = ((m + r) // 2) % 2 == 0
        under_kind, under_col, over_kind, over_col = (
            ("-", col_minus, "+", col_plus) if plus_over
            else ("+", col_plus, "-", col_minus))

        # under
        stroke(seg_pts(m, r, under_kind), WOUT, INK)
        stroke(seg_pts(m, r, under_kind), WF, under_col)
        # over
        stroke(seg_pts(m, r, over_kind), WOUT, INK)
        stroke(seg_pts(m, r, over_kind), WF, over_col)


# ---------------------------------------------------------------------------
# Downscale + save deliverables
# ---------------------------------------------------------------------------
OUT = "/Users/christian/projects/bsure-analytics/flechtwerk/docs/assets/ornaments"

band = img.resize((W, H), Image.LANCZOS)
band.save(f"{OUT}/band-home.png")

# Rail = band rotated 90deg: a horizontally-seamless band becomes a
# vertically-seamless vertical rail of width 88.
rail = band.rotate(90, expand=True)
rail.save(f"{OUT}/rail-home.png")

print(f"band {band.size}  rail {rail.size}  period={W}px")


# ---------------------------------------------------------------------------
# Self-check preview: band tiled 6x horizontally + rail tiled 5x vertically,
# each on parchment and on dark vellum, stacked into one image.
# ---------------------------------------------------------------------------
PARCH = (244, 236, 219, 255)
VELLUM = (32, 28, 22, 255)


def on_bg(fg, cols, rows, bg):
    tw, th = fg.size
    base = Image.new("RGBA", (tw * cols, th * rows), bg)
    for j in range(rows):
        for i in range(cols):
            base.alpha_composite(fg, (i * tw, j * th))
    return base


band_parch = on_bg(band, 6, 1, PARCH)
band_vellum = on_bg(band, 6, 1, VELLUM)
rail_parch = on_bg(rail, 1, 5, PARCH)
rail_vellum = on_bg(rail, 1, 5, VELLUM)

pad = 16
top_w = band_parch.width
bands_h = band_parch.height * 2 + pad
rails_h = rail_parch.height
rails_w = rail_parch.width * 2 + pad
canvas_w = max(top_w, rails_w)
canvas_h = bands_h + pad + rails_h

preview = Image.new("RGBA", (canvas_w, canvas_h), (90, 90, 90, 255))
preview.alpha_composite(band_parch, (0, 0))
preview.alpha_composite(band_vellum, (0, band_parch.height + pad))
ry = bands_h + pad
preview.alpha_composite(rail_parch, (0, ry))
preview.alpha_composite(rail_vellum, (rail_parch.width + pad, ry))

PREV = ("/private/tmp/claude-502/"
        "-Users-christian-projects-bsure-analytics-flechtwerk/"
        "62cea7c3-c888-4d03-995a-d027288cd3f8/scratchpad/orn")
preview.save(f"{PREV}/preview-home.png")
print(f"preview {preview.size} -> {PREV}/preview-home.png")
