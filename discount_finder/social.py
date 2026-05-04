"""Per-run artifacts for sharing freshly-found discount codes.

Two outputs, both restricted to ``is_fresh=True`` codes from the current run
and sorted alphabetically by company:

* ``new_codes.txt`` — numbered "N. Company - CODE" list.
* ``carousel_NN.png`` — Instagram-friendly 1080x1350 image(s); paginated
  into a carousel when one page can't hold all the codes.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

# Canvas: 4:5 portrait — Instagram's recommended feed aspect.
CANVAS_W, CANVAS_H = 1080, 1350

# Slate-900 background with amber pills for the codes.
BG_COLOR = (15, 23, 42)
DIVIDER_COLOR = (30, 41, 59)
TEXT_COLOR = (248, 250, 252)
MUTED_COLOR = (148, 163, 184)
PILL_BG = (251, 191, 36)
PILL_TEXT = (15, 23, 42)
BADGE_BG = (30, 41, 59)
BADGE_TEXT = (251, 191, 36)

HEADER_HEIGHT = 240
FOOTER_HEIGHT = 130
ROW_HEIGHT = 80
ROWS_PER_PAGE = 12
SIDE_PAD = 70

TITLE_SIZE = 60
SUBTITLE_SIZE = 30
ROW_SIZE = 36
CODE_SIZE = 34
BADGE_SIZE = 24
FOOTER_SIZE = 26

# Probed in order; first hit wins. Falls back to PIL's bitmap default.
_FONT_PATHS_REGULAR = [
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/Library/Fonts/Arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "C:/Windows/Fonts/arial.ttf",
]
_FONT_PATHS_BOLD = [
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
    "/Library/Fonts/Arial Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
]

# Headline per market. Belgium → Dutch (diski.nl's audience is Dutch-speaking).
# Falls back to the English title for any market not listed here.
_TITLES = {
    "germany": "NEUE RABATTCODES",
    "belgium": "NIEUWE KORTINGSCODES",
    "uk": "NEW DISCOUNT CODES",
}
_DEFAULT_TITLE = "NEW DISCOUNT CODES"

# Human-friendly subtitle label per market. Falls back to ``market.title()``
# which works for single-word names like "germany" → "Germany" but produces
# ugly output for short codes like "uk" → "Uk".
_MARKET_LABELS = {
    "germany": "Germany",
    "belgium": "Belgium",
    "uk": "United Kingdom",
}


def _sort_fresh(fresh: list[dict]) -> list[dict]:
    return sorted(fresh, key=lambda e: (e["company"].lower(), e["code"].upper()))


def write_text_list(fresh: list[dict], out_dir: Path) -> Path | None:
    """Numbered alphabetised "N. Company - CODE" list. Returns None if no codes."""
    if not fresh:
        return None
    sorted_fresh = _sort_fresh(fresh)
    lines = [
        f"{i}. {e['company']} - {e['code']}"
        for i, e in enumerate(sorted_fresh, start=1)
    ]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "new_codes.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_carousel_images(
    fresh: list[dict],
    out_dir: Path,
    market: str,
    run_date: date,
) -> list[Path]:
    """One image per page (max ROWS_PER_PAGE codes). Returns saved paths."""
    if not fresh:
        return []

    from PIL import Image, ImageDraw, ImageFont

    def load_font(paths: list[str], size: int) -> "ImageFont.ImageFont":
        for p in paths:
            try:
                return ImageFont.truetype(p, size)
            except OSError:
                continue
        return ImageFont.load_default()

    title_font = load_font(_FONT_PATHS_BOLD, TITLE_SIZE)
    subtitle_font = load_font(_FONT_PATHS_REGULAR, SUBTITLE_SIZE)
    row_font = load_font(_FONT_PATHS_REGULAR, ROW_SIZE)
    code_font = load_font(_FONT_PATHS_BOLD, CODE_SIZE)
    badge_font = load_font(_FONT_PATHS_BOLD, BADGE_SIZE)
    footer_font = load_font(_FONT_PATHS_REGULAR, FOOTER_SIZE)

    sorted_fresh = _sort_fresh(fresh)
    pages: list[list[tuple[int, dict]]] = []
    for i, entry in enumerate(sorted_fresh, start=1):
        if not pages or len(pages[-1]) >= ROWS_PER_PAGE:
            pages.append([])
        pages[-1].append((i, entry))

    out_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    total_pages = len(pages)

    for page_idx, page in enumerate(pages, start=1):
        img = Image.new("RGB", (CANVAS_W, CANVAS_H), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # Header
        title = _TITLES.get(market, _DEFAULT_TITLE)
        _text_centered(draw, title, CANVAS_W // 2, 105, title_font, TEXT_COLOR)
        market_label = _MARKET_LABELS.get(market, market.title())
        subtitle = f"{market_label}  ·  {run_date.strftime('%b %d, %Y')}"
        _text_centered(draw, subtitle, CANVAS_W // 2, 180, subtitle_font, MUTED_COLOR)

        # Body
        body_top = HEADER_HEIGHT
        for row_idx, (n, entry) in enumerate(page):
            row_top = body_top + row_idx * ROW_HEIGHT
            row_cy = row_top + ROW_HEIGHT // 2

            # Number badge (left)
            badge_radius = 26
            badge_cx = SIDE_PAD + badge_radius
            draw.ellipse(
                (badge_cx - badge_radius, row_cy - badge_radius,
                 badge_cx + badge_radius, row_cy + badge_radius),
                fill=BADGE_BG,
            )
            _text_centered(draw, f"{n:02d}", badge_cx, row_cy, badge_font, BADGE_TEXT)

            # Code pill (right) — measure first so we can clamp the company name.
            code = entry["code"].upper()
            pill_left = _draw_code_pill(
                draw, code, CANVAS_W - SIDE_PAD, row_cy, code_font,
                text_fill=PILL_TEXT, bg_fill=PILL_BG,
            )

            # Company (between badge and pill, ellipsised if it would overlap).
            company_x = badge_cx + badge_radius + 24
            max_w = pill_left - company_x - 24
            company = _truncate_to_width(draw, entry["company"], row_font, max_w)
            _text_left_centered(draw, company, company_x, row_cy, row_font, TEXT_COLOR)

            # Divider under every row except the last on the page
            if row_idx < len(page) - 1:
                divider_y = row_top + ROW_HEIGHT - 1
                draw.line(
                    [(SIDE_PAD, divider_y), (CANVAS_W - SIDE_PAD, divider_y)],
                    fill=DIVIDER_COLOR, width=1,
                )

        # Footer
        footer_cy = CANVAS_H - FOOTER_HEIGHT // 2
        if total_pages > 1:
            _text_centered(
                draw, f"{page_idx} / {total_pages}",
                CANVAS_W // 2, footer_cy - 20, footer_font, MUTED_COLOR,
            )
        _text_centered(
            draw, "@diski.nl",
            CANVAS_W // 2, footer_cy + 22, footer_font, MUTED_COLOR,
        )

        path = out_dir / f"carousel_{page_idx:02d}.png"
        img.save(path, "PNG", optimize=True)
        paths.append(path)

    return paths


def _draw_code_pill(draw, text, x_right, cy, font, text_fill, bg_fill):
    pad_x, pad_y = 26, 12
    bbox = draw.textbbox((0, 0), text, font=font)
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    pill_w = w + 2 * pad_x
    pill_h = h + 2 * pad_y
    pill_left = x_right - pill_w
    pill_top = cy - pill_h // 2
    draw.rounded_rectangle(
        (pill_left, pill_top, pill_left + pill_w, pill_top + pill_h),
        radius=pill_h // 2, fill=bg_fill,
    )
    draw.text(
        (pill_left + pad_x - bbox[0], pill_top + pad_y - bbox[1]),
        text, font=font, fill=text_fill,
    )
    return pill_left


def _text_centered(draw, text, cx, cy, font, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    draw.text((cx - w // 2 - bbox[0], cy - h // 2 - bbox[1]), text, font=font, fill=fill)


def _text_left_centered(draw, text, x, cy, font, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    h = bbox[3] - bbox[1]
    draw.text((x, cy - h // 2 - bbox[1]), text, font=font, fill=fill)


def _truncate_to_width(draw, text: str, font, max_width: int) -> str:
    bbox = draw.textbbox((0, 0), text, font=font)
    if bbox[2] - bbox[0] <= max_width:
        return text
    ellipsis = "…"
    s = text
    while s:
        s = s[:-1]
        candidate = s.rstrip() + ellipsis
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if bbox[2] - bbox[0] <= max_width:
            return candidate
    return ellipsis
