"""
Generate share card PNG for /incident/rseth-2026-04-18.

Output: public/share/incident/rseth-2026-04-18.png (1200x630)
Fonts:  IBM Plex Sans + IBM Plex Mono, committed to fonts/ at the repo root
        to match the rest of the Basis design system. Liberation is the
        fallback if the TTF files ever go missing at build time.

Design:
  Paper background, black typography.
  Eyebrow: INCIDENT / RSETH-2026-04-18
  Title:   rsETH Pre-Exploit Scoring
  Body:    4-row bar chart — Exploit History component
           rsETH 10 vs stETH 100, rETH 100, eETH 100.
  Footer:  basisprotocol.xyz/incident/rseth-2026-04-18
"""

from __future__ import annotations

import os
from PIL import Image, ImageDraw, ImageFont

W, H = 1200, 630

PAPER = (245, 242, 236)
PAPER_WARM = (240, 236, 227)
INK = (10, 10, 10)
INK_MID = (58, 58, 58)
INK_LIGHT = (106, 106, 106)
INK_FAINT = (154, 154, 154)
RULE_MID = (200, 196, 188)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FONTS_DIR = os.path.join(ROOT, "fonts")

# Primary: IBM Plex committed at repo root. Fallback: Liberation (metric-
# compatible with Helvetica/Courier) from the base image, used only if the
# Plex TTFs are missing from the build context for any reason.
FONT_SANS = os.path.join(FONTS_DIR, "IBMPlexSans-Regular.ttf")
FONT_SANS_BOLD = os.path.join(FONTS_DIR, "IBMPlexSans-Bold.ttf")
FONT_MONO = os.path.join(FONTS_DIR, "IBMPlexMono-Regular.ttf")
# IBM Plex Mono Bold is not currently bundled; use regular Mono for both
# places the card previously used bold-mono (value label on highlighted row).
FONT_MONO_BOLD = FONT_MONO

_FALLBACK_SANS = "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
_FALLBACK_SANS_BOLD = "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
_FALLBACK_MONO = "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf"


def _font(path: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        # Try fallback path, then PIL default as last resort.
        fallback_map = {
            FONT_SANS: _FALLBACK_SANS,
            FONT_SANS_BOLD: _FALLBACK_SANS_BOLD,
            FONT_MONO: _FALLBACK_MONO,
        }
        fb = fallback_map.get(path)
        if fb:
            try:
                return ImageFont.truetype(fb, size)
            except Exception:
                pass
        return ImageFont.load_default()


def _draw_bar(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int,
              value: int, max_value: int, label: str, val_label: str,
              highlight: bool) -> None:
    # Row label (left)
    font_label = _font(FONT_MONO_BOLD if highlight else FONT_MONO, 18)
    draw.text((x, y + (h // 2) - 11), label, font=font_label, fill=INK if highlight else INK_MID)

    # Bar track
    bar_x0 = x + 200
    bar_x1 = x + w - 90
    bar_w = bar_x1 - bar_x0
    fill_w = int(bar_w * (value / max_value)) if max_value else 0
    bar_y0 = y + (h // 2) - 10
    bar_y1 = y + (h // 2) + 10

    # Track outline
    draw.rectangle([bar_x0, bar_y0, bar_x1, bar_y1], outline=RULE_MID, width=1)
    # Filled portion
    if fill_w > 0:
        draw.rectangle([bar_x0, bar_y0, bar_x0 + fill_w, bar_y1],
                       fill=INK if highlight else INK_LIGHT)

    # Value label (right)
    font_val = _font(FONT_MONO_BOLD if highlight else FONT_MONO, 20)
    draw.text((bar_x1 + 14, y + (h // 2) - 13), val_label, font=font_val,
              fill=INK if highlight else INK_MID)


def generate(output_path: str) -> None:
    img = Image.new("RGB", (W, H), PAPER)
    d = ImageDraw.Draw(img)

    # Outer border
    d.rectangle([30, 30, W - 30, H - 30], outline=INK, width=3)

    # Top-left brand block
    f_brand = _font(FONT_SANS_BOLD, 22)
    d.text((70, 70), "BASIS PROTOCOL", font=f_brand, fill=INK)
    f_brand_sub = _font(FONT_MONO, 13)
    d.text((70, 100), "Risk surfaces for on-chain finance", font=f_brand_sub, fill=INK_LIGHT)

    # Eyebrow
    f_eyebrow = _font(FONT_MONO_BOLD, 13)
    d.text((70, 170), "INCIDENT  /  RSETH-2026-04-18", font=f_eyebrow, fill=INK_LIGHT)

    # Title
    f_title = _font(FONT_SANS_BOLD, 46)
    d.text((70, 200), "rsETH Pre-Exploit Scoring", font=f_title, fill=INK)

    # Subtitle
    f_sub = _font(FONT_SANS, 18)
    d.text((70, 262), "Exploit History component · pinned 2026-04-20", font=f_sub, fill=INK_MID)

    # Separator
    d.line([(70, 305), (W - 70, 305)], fill=RULE_MID, width=1)

    # Bars
    rows = [
        ("rsETH",  10,  "10",   True),
        ("stETH",  100, "100",  False),
        ("rETH",   100, "100",  False),
        ("eETH",   100, "100",  False),
    ]
    row_h = 52
    start_y = 330
    for i, (label, val, val_label, hl) in enumerate(rows):
        _draw_bar(d, 70, start_y + i * row_h, W - 140, row_h, val, 100,
                  label, val_label, hl)

    # Footer separator
    d.line([(70, H - 90), (W - 70, H - 90)], fill=RULE_MID, width=1)

    # Footer text
    f_foot_l = _font(FONT_MONO_BOLD, 16)
    d.text((70, H - 75), "basisprotocol.xyz/incident/rseth-2026-04-18",
           font=f_foot_l, fill=INK)
    f_foot_r = _font(FONT_MONO, 12)
    caption = "Audit: /audits/lsti_rseth_audit_2026-04-20 · Higher = fewer/no known exploits"
    # Right-align caption
    bbox = d.textbbox((0, 0), caption, font=f_foot_r)
    cap_w = bbox[2] - bbox[0]
    d.text((W - 70 - cap_w, H - 71), caption, font=f_foot_r, fill=INK_LIGHT)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    img.save(output_path, format="PNG", optimize=True)
    print(f"Wrote {output_path} ({os.path.getsize(output_path)} bytes)")


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    out = os.path.join(root, "public", "share", "incident", "rseth-2026-04-18.png")
    generate(out)
