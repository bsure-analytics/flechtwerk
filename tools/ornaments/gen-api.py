"""Manuscript interlaced-chain border ornament (chapter: api, style: chain).

A horizontal chain of overlapping oval rings, adapted from the seamless
Celtic chain generator to an illuminated-manuscript hand:

- Every main cord is GOLD (gold leaf), the dominant colour, drawn with a
  crisp thin dark-sepia outline.
- Consecutive rings pass alternately OVER and UNDER their neighbours, giving
  a genuine woven interlace (not a rope).
- Each loop frames a small jewel accent ring whose cord alternates LAPIS and
  GARNET around the period, set around a tiny gold boss.

One tile = one integer period, so the band repeats horizontally with no seam
(ghost rings on each side wrap the loops that straddle the tile edge, and the
colour/accent patterns are periodic with the ring index). The rail is simply
the band rotated 90 degrees: a horizontally-seamless band becomes a
vertically-seamless rail.

Pure standard library + Pillow. Run with:
  uv run --with pillow --no-project python gen-api.py
"""

import os

from PIL import Image, ImageChops, ImageDraw

# ---------------------------------------------------------------- palette
INK = (42, 33, 24)        # dark sepia — the crisp thin cord outline
GOLD = (176, 138, 62)     # gold leaf — the dominant cord colour
LAPIS = (39, 74, 122)     # deep blue accent
GARNET = (124, 45, 58)    # deep red accent

# Main chain: every ring is gold (gold-dominant, cohesive family look).
# Accent jewel rings alternate lapis / garnet around the period.
ACCENTS = [LAPIS, GARNET]

# ---------------------------------------------------------------- geometry
SS = 4                       # supersample factor
DISP_W, DISP_H = 224, 88     # display tile size (period x height)
W, H = DISP_W * SS, DISP_H * SS

N = 4                        # rings per period (even -> accents alternate seamlessly)
S = W // N                   # centre spacing
Y_C = H // 2                 # vertical centre of the chain
RX = 150                     # ring semi-axis x (supersample space)
RY = 132                     # ring semi-axis y
BAND = 40                    # cord total thickness
OUTLINE = 7                  # dark outline thickness on each edge
R_DISK = 58                  # over-strand stamp radius at a crossing

# jewel accent ring inside each loop
ACC_R = 52                   # accent ring outer semi-axis
ACC_BAND = 20                # accent cord thickness
ACC_OUT = 6                  # accent outline thickness
BOSS_R = 22                  # gold boss radius inside the accent ring
BOSS_OUT = 5                 # gold boss outline thickness

HERE = os.path.dirname(os.path.abspath(__file__))
PREVIEW_PATH = (
    "/private/tmp/claude-502/-Users-christian-projects-bsure-analytics-"
    "flechtwerk/62cea7c3-c888-4d03-995a-d027288cd3f8/scratchpad/orn/"
    "preview-api.png"
)


def annulus_mask(cx, cy, a_out, b_out, a_in, b_in):
    """L mask: filled band between an outer and an inner ellipse."""
    m = Image.new("L", (W, H), 0)
    d = ImageDraw.Draw(m)
    d.ellipse([cx - a_out, cy - b_out, cx + a_out, cy + b_out], fill=255)
    d.ellipse([cx - a_in, cy - b_in, cx + a_in, cy + b_in], fill=0)
    return m


def ring_layer(cx, color):
    """A single ring cord on its own transparent RGBA layer: sepia outline
    on both edges, gold-leaf core, transparent hole."""
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ink_img = Image.new("RGBA", (W, H), INK + (255,))
    color_img = Image.new("RGBA", (W, H), color + (255,))

    hw = BAND / 2
    band = annulus_mask(cx, Y_C, RX + hw, RY + hw, RX - hw, RY - hw)
    core = annulus_mask(cx, Y_C,
                        RX + hw - OUTLINE, RY + hw - OUTLINE,
                        RX - hw + OUTLINE, RY - hw + OUTLINE)

    layer = Image.composite(ink_img, layer, band)
    layer = Image.composite(color_img, layer, core)
    return layer


def stamp_over(canvas, over_layer, px, py):
    """Paste the over-strand's cord (with outline) atop the canvas within
    a disk around a crossing, hiding the under strand there."""
    disk = Image.new("L", (W, H), 0)
    ImageDraw.Draw(disk).ellipse(
        [px - R_DISK, py - R_DISK, px + R_DISK, py + R_DISK], fill=255)
    alpha = over_layer.split()[3]
    combined = ImageChops.multiply(disk, alpha)
    tmp = over_layer.copy()
    tmp.putalpha(combined)
    canvas.alpha_composite(tmp)


def draw_jewel(canvas, cx, cy, accent):
    """A small accent ring (lapis or garnet, sepia outline) framing a tiny
    gold boss — the jewel set inside a loop of the chain."""
    # accent ring
    ring = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ink_img = Image.new("RGBA", (W, H), INK + (255,))
    acc_img = Image.new("RGBA", (W, H), accent + (255,))
    band = annulus_mask(cx, cy, ACC_R, ACC_R, ACC_R - ACC_BAND, ACC_R - ACC_BAND)
    core = annulus_mask(cx, cy,
                        ACC_R - ACC_OUT, ACC_R - ACC_OUT,
                        ACC_R - ACC_BAND + ACC_OUT, ACC_R - ACC_BAND + ACC_OUT)
    ring = Image.composite(ink_img, ring, band)
    ring = Image.composite(acc_img, ring, core)
    canvas.alpha_composite(ring)

    # gold boss
    d = ImageDraw.Draw(canvas)
    d.ellipse([cx - BOSS_R, cy - BOSS_R, cx + BOSS_R, cy + BOSS_R],
              fill=INK + (255,))
    r2 = BOSS_R - BOSS_OUT
    d.ellipse([cx - r2, cy - r2, cx + r2, cy + r2], fill=GOLD + (255,))


def build_tile():
    # Ghost rings on each side so loops straddling the tile edge wrap
    # correctly (everything is periodic in x with period W -> seamless).
    layers = {i: ring_layer(S // 2 + i * S, GOLD) for i in range(-1, N + 1)}

    canvas = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    for i in range(-1, N + 1):
        canvas.alpha_composite(layers[i])

    # Over/under rule (global, hence consistent -> proper alternation):
    # at the TOP crossing the LEFT ring is over; at the BOTTOM crossing the
    # RIGHT ring is over. Each ring then alternates over/under around itself.
    dy = RY * (1 - (S / (2 * RX)) ** 2) ** 0.5
    y_top = int(round(Y_C - dy))
    y_bot = int(round(Y_C + dy))
    for i in range(-1, N):
        px = (i + 1) * S          # crossing x between ring i and ring i+1
        stamp_over(canvas, layers[i], px, y_top)      # left ring over (top)
        stamp_over(canvas, layers[i + 1], px, y_bot)  # right ring over (bottom)

    # A jewel in each loop; the accent colour alternates around the period
    # (index-parity keyed, so the wrap stays seamless).
    for i in range(N):
        draw_jewel(canvas, S // 2 + i * S, Y_C, ACCENTS[i % len(ACCENTS)])

    return canvas.resize((DISP_W, DISP_H), Image.LANCZOS)


def build_rail(band):
    """Rotate the horizontally-seamless band 90 degrees -> a vertically
    seamless rail of width DISP_H."""
    return band.transpose(Image.ROTATE_90)


# ------------------------------------------------------------- self-check
PARCHMENT = (244, 236, 219)
VELLUM = (32, 28, 22)


def _on_bg(strip, bg):
    plate = Image.new("RGBA", strip.size, bg + (255,))
    plate.alpha_composite(strip)
    return plate


def build_preview(band, rail):
    bw, bh = band.size          # 224 x 88
    rw, rh = rail.size          # 88 x 224

    # bands tiled 6x horizontally, on both backgrounds, stacked
    band_reps = 6
    strip = Image.new("RGBA", (bw * band_reps, bh), (0, 0, 0, 0))
    for k in range(band_reps):
        strip.alpha_composite(band, (k * bw, 0))
    band_par = _on_bg(strip, PARCHMENT)
    band_vel = _on_bg(strip, VELLUM)

    # rails tiled 5x vertically, on both backgrounds, side by side
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
    band = build_tile()
    rail = build_rail(band)

    band_path = os.path.join(HERE, "band-api.png")
    rail_path = os.path.join(HERE, "rail-api.png")
    band.save(band_path)
    rail.save(rail_path)

    os.makedirs(os.path.dirname(PREVIEW_PATH), exist_ok=True)
    build_preview(band, rail).save(PREVIEW_PATH)

    print("period_px", DISP_W, "band_h", DISP_H)
    print("wrote", band_path)
    print("wrote", rail_path)
    print("wrote", PREVIEW_PATH)


if __name__ == "__main__":
    main()
