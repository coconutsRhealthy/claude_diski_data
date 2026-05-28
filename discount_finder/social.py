"""Per-run artifacts for sharing freshly-found discount codes.

Two outputs, both restricted to ``is_fresh=True`` codes from the current run
and sorted alphabetically by company:

* ``new_codes.txt`` — numbered "N. Company - CODE" list.
* ``carousel_NN.png`` — Instagram-friendly 1080x1920 (9:16) image(s);
  paginated into a carousel when one page can't hold all the codes.

The carousel visual style mirrors the Angular ``insta-carousel`` reference
(soft pastel-purple gradient, deep-purple title, hot-pink codes with a
dotted leader). Pagination is driven by the same formula the Angular
component uses: rows-per-page is computed from available body height
divided by a fixed row footprint, so per-line spacing stays identical
across slides and the bottom row is never clipped.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

# ---- Canvas (9:16 — Instagram reels / stories) ----------------------------
CANVAS_W, CANVAS_H = 1080, 1920

# ---- Palette (lifted from insta-carousel.component.css) ------------------
GRADIENT_STOPS = [
    (0.00, (250, 245, 251)),  # #faf5fb
    (0.55, (241, 233, 245)),  # #f1e9f5
    (1.00, (233, 223, 240)),  # #e9dff0
]
TITLE_COLOR    = (75, 0, 130)     # #4B0082 — deep purple
DATE_COLOR     = (255, 79, 163)   # #ff4fa3 — hot pink
SUBTITLE_COLOR = (154, 138, 166)  # #9a8aa6 — muted purple
BADGE_BG       = (232, 226, 239)  # ≈ rgba(75,0,130,0.08) flattened over the bg
BADGE_TEXT     = (75, 0, 130)
SHOP_COLOR     = (58, 58, 58)     # #3a3a3a
LEADER_COLOR   = (212, 200, 223)  # #d4c8df
CODE_COLOR     = (255, 79, 163)   # #ff4fa3 — hot pink
PCT_COLOR      = (138, 123, 150)  # #8a7b96
FOOTER_COLOR   = (154, 138, 166)  # #9a8aa6

# ---- Layout (design pixels) ----------------------------------------------
TOP_PAD     = 64
BOTTOM_PAD  = 56
SIDE_PAD    = 70
HEADER_MB   = 36   # space below the header block
FOOTER_MT   = 32   # space above the footer line

# Reserved vertical room for header + footer, used by pagination math.
# Matches the Angular component's ~215 / 80 estimates: title 58 + date 34 +
# subtitle 30 with line-height + 6px gaps + margin-bottom, and a single
# footer line with its margin-top.
HEADER_BLOCK = 215
FOOTER_BLOCK = 80
PAGINATION_SAFETY = 16   # small buffer so the last row never gets clipped

# ---- Font sizing ----------------------------------------------------------
TITLE_SIZE    = 58
DATE_SIZE     = 34
SUBTITLE_SIZE = 30
BADGE_SIZE    = 30
BODY_SIZE     = 36          # company + code use the same base size
PCT_SIZE      = round(BODY_SIZE * 0.78)
FOOTER_SIZE   = 28

# Per-line footprint — locks vertical rhythm regardless of how full the slide is.
ROW_HEIGHT = BODY_SIZE * 1.7   # ≈ 61.2 px

# ---- Fonts ----------------------------------------------------------------
# Probed in order; first hit wins. macOS + slim debian (the droplet's base
# image) are both covered. No bundled font.
_FONT_PATHS_REGULAR = [
    "/System/Library/Fonts/Helvetica.ttc",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
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

# ---- Localization ---------------------------------------------------------
# Slide headline per market. Mixed-case to match the Angular reference's
# "Kortingscodes" vibe. New markets fall back to the English title.
_TITLES = {
    "germany": "Rabattcodes",
    "belgium": "Kortingscodes",
    "uk":      "Discount codes",
    "france":  "Codes promo",
}
_DEFAULT_TITLE = "Discount codes"

# Footer line per market — invitation to click the bio link. Keep it short:
# this whole line lives in the footer band.
_FOOTERS = {
    "germany": "Mehr Codes? Link in Bio",
    "belgium": "Meer codes? Link in bio",
    "uk":      "More codes? Link in bio",
    "france":  "Plus de codes ? Lien en bio",
}
_DEFAULT_FOOTER = "More codes? Link in bio"

# Per-market month names and date formats. Year omitted: the carousel is
# always "today's drop" and the cleaner header fits the design.
_MONTH_NAMES = {
    "germany": ["Januar", "Februar", "März", "April", "Mai", "Juni",
                "Juli", "August", "September", "Oktober", "November", "Dezember"],
    "belgium": ["januari", "februari", "maart", "april", "mei", "juni",
                "juli", "augustus", "september", "oktober", "november", "december"],
    "uk":      ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"],
    "france":  ["janvier", "février", "mars", "avril", "mai", "juin",
                "juillet", "août", "septembre", "octobre", "novembre", "décembre"],
}
_DATE_FORMATS = {
    "germany": "{d}. {m}",   # "6. Mai"
    "belgium": "{d} {m}",     # "6 mei"
    "uk":      "{d} {m}",     # "6 May"
    "france":  "{d} {m}",     # "6 mai"
}


def _format_date(market: str, run_date: date) -> str:
    months = _MONTH_NAMES.get(market)
    fmt = _DATE_FORMATS.get(market)
    if not months or not fmt:
        return run_date.strftime("%b %d")
    return fmt.format(d=run_date.day, m=months[run_date.month - 1])


def _sort_fresh(fresh: list[dict]) -> list[dict]:
    return sorted(fresh, key=lambda e: (e["company"].lower(), e["code"].upper()))


def write_text_list(fresh: list[dict], out_dir: Path) -> Path | None:
    """Numbered alphabetised "N. Company - CODE - post_url" list.

    The post URL is omitted on lines where it's missing rather than
    rendering a dangling " - ". Returns None if there are no codes.
    """
    if not fresh:
        return None
    sorted_fresh = _sort_fresh(fresh)
    lines = []
    for i, e in enumerate(sorted_fresh, start=1):
        line = f"{i}. {e['company']} - {e['code']}"
        post_url = (e.get("post_url") or "").strip()
        if post_url:
            line += f" - {post_url}"
        lines.append(line)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "new_codes.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _rows_per_page() -> int:
    """How many discount rows fit on one slide.

    Mirrors ``computeMaxLinesPerSlide`` in the Angular component: take the
    body's vertical budget, divide by the fixed row footprint, floor.
    """
    available = (
        CANVAS_H - TOP_PAD - BOTTOM_PAD - HEADER_BLOCK - FOOTER_BLOCK - PAGINATION_SAFETY
    )
    return max(1, int(available // ROW_HEIGHT))


def _paginate(entries: list[dict]) -> list[list[dict]]:
    per = _rows_per_page()
    return [entries[i : i + per] for i in range(0, len(entries), per)]


def write_carousel_images(
    fresh: list[dict],
    out_dir: Path,
    market: str,
    run_date: date,
) -> list[Path]:
    """One slide per page (max ``_rows_per_page()`` codes). Returns saved paths."""
    if not fresh:
        return []

    from PIL import Image, ImageDraw, ImageFont

    def load_font(paths: list[str], size: int):
        for p in paths:
            try:
                return ImageFont.truetype(p, size)
            except OSError:
                continue
        return ImageFont.load_default()

    title_font    = load_font(_FONT_PATHS_BOLD, TITLE_SIZE)
    date_font     = load_font(_FONT_PATHS_BOLD, DATE_SIZE)
    subtitle_font = load_font(_FONT_PATHS_BOLD, SUBTITLE_SIZE)
    badge_font    = load_font(_FONT_PATHS_BOLD, BADGE_SIZE)
    shop_font     = load_font(_FONT_PATHS_BOLD, BODY_SIZE)
    code_font     = load_font(_FONT_PATHS_BOLD, BODY_SIZE)
    pct_font      = load_font(_FONT_PATHS_BOLD, PCT_SIZE)
    footer_font   = load_font(_FONT_PATHS_BOLD, FOOTER_SIZE)

    sorted_fresh = _sort_fresh(fresh)
    pages = _paginate(sorted_fresh)
    total_pages = len(pages)

    out_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []

    title_text = _TITLES.get(market, _DEFAULT_TITLE)
    date_text = _format_date(market, run_date)
    subtitle_text = "diski.nl"
    footer_text = _FOOTERS.get(market, _DEFAULT_FOOTER)

    for page_idx, page in enumerate(pages, start=1):
        img = _make_gradient_canvas(CANVAS_W, CANVAS_H, GRADIENT_STOPS)
        draw = ImageDraw.Draw(img)

        # -------- Header (left brand stack + right page badge) ------------
        x = SIDE_PAD
        y = TOP_PAD
        title_h = _text_height(draw, title_text, title_font)
        date_h = _text_height(draw, date_text, date_font)
        sub_h = _text_height(draw, subtitle_text, subtitle_font)
        gap = 6
        draw.text((x, y), title_text, font=title_font, fill=TITLE_COLOR)
        y += title_h + gap
        draw.text((x, y), date_text, font=date_font, fill=DATE_COLOR)
        y += date_h + gap
        draw.text((x, y), subtitle_text, font=subtitle_font, fill=SUBTITLE_COLOR)

        # Page badge pill (top-right), vertically aligned with the title.
        badge_text = f"{page_idx} / {total_pages}"
        _draw_badge_pill(
            draw,
            badge_text,
            x_right=CANVAS_W - SIDE_PAD,
            cy=TOP_PAD + title_h // 2,
            font=badge_font,
            text_fill=BADGE_TEXT,
            bg_fill=BADGE_BG,
        )

        # -------- Body rows ------------------------------------------------
        body_top = TOP_PAD + HEADER_BLOCK
        for row_idx, entry in enumerate(page):
            row_top = body_top + int(row_idx * ROW_HEIGHT)
            row_cy = row_top + int(ROW_HEIGHT // 2)

            company = entry["company"]
            code = (entry["code"] or "").strip()
            percentage = (entry.get("percentage") or "").strip() if entry.get("percentage") else ""

            # Right-side: percentage hugs the right edge if present, then code.
            cursor_right = CANVAS_W - SIDE_PAD
            if percentage:
                pct_w = _text_width(draw, percentage, pct_font)
                _text_left_centered(draw, percentage, cursor_right - pct_w, row_cy, pct_font, PCT_COLOR)
                cursor_right -= pct_w + 14

            code_w = _text_width(draw, code, code_font)
            code_left = cursor_right - code_w
            _text_left_centered(draw, code, code_left, row_cy, code_font, CODE_COLOR)

            # Left-side: company, clamped so it never overruns the code.
            shop_x = SIDE_PAD
            shop_max = code_left - shop_x - 28   # leave room for leader gap
            company_clamped = _truncate_to_width(draw, company, shop_font, shop_max)
            shop_w = _text_width(draw, company_clamped, shop_font)
            _text_left_centered(draw, company_clamped, shop_x, row_cy, shop_font, SHOP_COLOR)

            # Dotted leader between company end and code start. Sit it on the
            # text baseline (slightly above the row vertical center).
            leader_y = row_cy + int(BODY_SIZE * 0.18)
            _draw_dotted_leader(
                draw,
                x1=shop_x + shop_w + 14,
                x2=code_left - 14,
                y=leader_y,
                color=LEADER_COLOR,
            )

        # -------- Footer ---------------------------------------------------
        footer_y = CANVAS_H - BOTTOM_PAD - _text_height(draw, footer_text, footer_font)
        _text_centered_x(draw, footer_text, CANVAS_W // 2, footer_y, footer_font, FOOTER_COLOR)

        path = out_dir / f"carousel_{page_idx:02d}.png"
        img.save(path, "PNG", optimize=True)
        paths.append(path)

    return paths


# --------------------------------------------------------------------------
# Drawing primitives
# --------------------------------------------------------------------------

def _make_gradient_canvas(w: int, h: int, stops: list[tuple[float, tuple[int, int, int]]]):
    """Vertical (top→bottom) interpolation through the given color stops.

    The Angular CSS gradient is 150° (mostly down, slight right tilt); the
    color stops are so close in luminance that pure vertical reads as the
    same gradient at glance. Pure-Python row fill keeps the dependency
    surface to PIL only.
    """
    from PIL import Image
    img = Image.new("RGB", (w, h))
    px = img.load()
    sorted_stops = sorted(stops, key=lambda s: s[0])
    for y in range(h):
        t = y / max(1, h - 1)
        color = _interp_stops(sorted_stops, t)
        for x in range(w):
            px[x, y] = color
    return img


def _interp_stops(stops, t):
    for i in range(len(stops) - 1):
        t0, c0 = stops[i]
        t1, c1 = stops[i + 1]
        if t <= t1:
            span = max(1e-9, t1 - t0)
            f = (t - t0) / span
            return (
                int(c0[0] + (c1[0] - c0[0]) * f),
                int(c0[1] + (c1[1] - c0[1]) * f),
                int(c0[2] + (c1[2] - c0[2]) * f),
            )
    return stops[-1][1]


def _draw_dotted_leader(draw, x1: int, x2: int, y: int, color, radius: int = 2, gap: int = 8) -> None:
    """Series of small filled circles — close-enough analogue of CSS
    ``border-bottom: 2px dotted``."""
    if x2 <= x1:
        return
    step = radius * 2 + gap
    x = x1
    while x + radius <= x2:
        draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=color)
        x += step


def _draw_badge_pill(draw, text, x_right, cy, font, text_fill, bg_fill):
    pad_x, pad_y = 22, 10
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


def _text_centered_x(draw, text, cx, y_top, font, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    w = bbox[2] - bbox[0]
    draw.text((cx - w // 2 - bbox[0], y_top - bbox[1]), text, font=font, fill=fill)


def _text_left_centered(draw, text, x, cy, font, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    h = bbox[3] - bbox[1]
    draw.text((x, cy - h // 2 - bbox[1]), text, font=font, fill=fill)


def _text_width(draw, text, font) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _text_height(draw, text, font) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[3] - bbox[1]


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
